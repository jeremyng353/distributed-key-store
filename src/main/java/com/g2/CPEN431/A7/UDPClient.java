package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g2.CPEN431.A7.util.ByteOrder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;

public class UDPClient {
    private static final int MAX_PACKET_SIZE = 16384;
    private static final int MAX_RETRIES = 3;
    private static final int DEFAULT_TIMEOUT = 100;

    private DatagramSocket socket = null;

    public UDPClient() {
        try {
            this.socket = new DatagramSocket();
            socket.setSoTimeout(DEFAULT_TIMEOUT);
        } catch (IOException e) {
            System.err.println("Couldn't open socket!");
            e.printStackTrace();
        }
    }

    public Message.Msg request(InetAddress address, int port, byte[] buf) {
        return request(address, port, buf, DEFAULT_TIMEOUT);
    }

    /**
     * The main interface of this client. Sends a request message to the specified
     * address and port with a request payload, and receives the response.
     *
     * @param address the address of the server
     * @param port    the server port
     * @param payload a buffer containing the request payload.
     * @param timeout the retry timeout
     * @return UDPResponse containing the response packet data and packet size
     */
    public Message.Msg request(InetAddress address, int port, byte[] payload, int timeout) {
        byte[] uuid = null;

        int current_timeout = timeout;

        for (int i = 0; i <= MAX_RETRIES; i++) {
            try {
                // Build request payload
                if (uuid == null) {
                    uuid = generateUUID(address, port);
                }

                try {
                    socket.setSoTimeout(current_timeout);
                } catch (SocketException e) {
                    e.printStackTrace();
                }

                byte[] checksumByteArray = concatenateByteArrays(uuid, payload);
                long checksum = computeChecksum(checksumByteArray);

                byte[] messageBuffer = Message.Msg.newBuilder().setMessageID(ByteString.copyFrom(uuid))
                        .setPayload(ByteString.copyFrom(payload))
                        .setCheckSum(checksum)
                        .build()
                        .toByteArray();

                // If the buffer is larger than 16 KB, then truncate
                int messageBufferSize = Math.min(MAX_PACKET_SIZE, messageBuffer.length);
                DatagramPacket packet = new DatagramPacket(messageBuffer, messageBufferSize, address, port);
                socket.send(packet);

                byte[] rcvBuf = new byte[MAX_PACKET_SIZE];
                packet = new DatagramPacket(rcvBuf, rcvBuf.length);
                socket.receive(packet);

                // Trim packet and parse into Message object
                byte[] trimmedResponse = new byte[packet.getLength()];
                System.arraycopy(rcvBuf, 0, trimmedResponse, 0, packet.getLength());
                Message.Msg rcvMessage = Message.Msg.parseFrom(trimmedResponse);
                byte[] receivedUUID = rcvMessage.getMessageID().toByteArray();

                if (!validateChecksum(rcvMessage)) {
//                    System.out.println("Received packet with corrupted data. Retrying " + (i + 1) + " of " + MAX_RETRIES);
                } else if (!Arrays.equals(receivedUUID, uuid)) {
//                    System.out.println("Received corrupted packet. Retrying " + (i + 1) + " of " + MAX_RETRIES);
                } else {
                    return rcvMessage;
                }
            } catch (SocketTimeoutException e) {
                if (i < MAX_RETRIES) {
//                    System.out.println("Socket timed out. Retrying " + (i + 1) + " of " + MAX_RETRIES);
                    current_timeout *= 2;
                }
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

//        System.err.println("Could not receive a response after " + MAX_RETRIES + " retries");
        return null;
    }

    private byte[] generateUUID(InetAddress address, int port) throws IOException {
        byte[] addressBytes = address.getAddress();
        byte[] portBytes = new byte[4];
        ByteOrder.int2leb(port, portBytes, 0);

        Random random = new Random();
        byte[] randomBytes = new byte[2];
        random.nextBytes(randomBytes);

        byte[] systemTimeBytes = ByteBuffer.wrap(new byte[8]).putLong(System.nanoTime()).array();

        return concatenateByteArrays(addressBytes, portBytes, randomBytes, systemTimeBytes);
    }

    private long computeChecksum(byte[] buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        return crc32.getValue();
    }

    private boolean validateChecksum(Message.Msg message) throws IOException {
        byte[] receivedUUID = message.getMessageID().toByteArray();
        byte[] payloadByteArray = message.getPayload().toByteArray();
        byte[] expectedChecksumByteArray = concatenateByteArrays(receivedUUID, payloadByteArray);
        long expectedChecksum = computeChecksum(expectedChecksumByteArray);
        long receivedChecksum = message.getCheckSum();

        return expectedChecksum == receivedChecksum;
    }

    private byte[] concatenateByteArrays(byte[]... byteArrays) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] byteArray: byteArrays) {
            outputStream.write(byteArray);
        }

        return outputStream.toByteArray();
    }

    public void replicaRequest(InetAddress replicaAddress, int replicaPort, byte[] payload, String clientIp, int clientPort, ByteString messageID) {
        // TODO: no timeout for this function since we don't expect a response from replicas, but
        // maybe we should have a response...

        try {
            

            byte[] checksumByteArray = concatenateByteArrays(messageID.toByteArray(), payload);
            long checksum = computeChecksum(checksumByteArray);

            byte[] messageBuffer = Message.Msg.newBuilder()
                    .setPayload(ByteString.copyFrom(payload))
                    .setCheckSum(checksum)
                    .setClientIp(clientIp)
                    .setClientPort(clientPort)
                    .setMessageID(messageID)
                    .build()
                    .toByteArray();

            // If the buffer is larger than 16 KB, then truncate
            int messageBufferSize = Math.min(MAX_PACKET_SIZE, messageBuffer.length);
            DatagramPacket replicaPacket = new DatagramPacket(
                    messageBuffer,
                    messageBufferSize,
                    InetAddress.getByName("localhost"),
                    replicaPort
            );

            socket.send(replicaPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendClientResponse(byte[] payload, String clientIp, int clientPort) {
        byte[] uuid = new byte[0];
        byte[] checksumByteArray = new byte[0];
        try {
            uuid = generateUUID(InetAddress.getByName(clientIp), clientPort);
            checksumByteArray = concatenateByteArrays(uuid, payload);
            long checksum = computeChecksum(checksumByteArray);

            byte[] messageBuffer = Message.Msg.newBuilder().setMessageID(ByteString.copyFrom(uuid))
                    .setPayload(ByteString.copyFrom(payload))
                    .setCheckSum(checksum)
                    .build()
                    .toByteArray();

            int messageBufferSize = Math.min(MAX_PACKET_SIZE, messageBuffer.length);
            DatagramPacket clientPacket = new DatagramPacket(
                    messageBuffer,
                    messageBufferSize,
                    InetAddress.getByName(clientIp),
                    clientPort
            );
            System.out.println("sending back to client from UDPClient at port:");
            System.out.println(clientPort);
            socket.send(clientPacket);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}
