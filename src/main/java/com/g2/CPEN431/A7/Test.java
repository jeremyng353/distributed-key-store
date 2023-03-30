package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g2.CPEN431.A7.util.ByteOrder;
import com.google.protobuf.ByteString;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.zip.CRC32;

public class Test {
    private static final int MAX_PACKET_SIZE = 16384;
    public static void main( String[] args ) throws SocketException {
        DatagramSocket socket = new DatagramSocket();
        ArrayList<KeyValueRequest.KVRequest> reqList = new ArrayList<>();

        reqList.add(buildPut(
                ByteString.copyFrom(new byte[]{1}),
                ByteString.copyFrom(new byte[]{0}),
                1
        ));

        reqList.add(buildGet(
                ByteString.copyFrom(new byte[]{1})
        ));

        reqList.add(buildRemove(
                ByteString.copyFrom(new byte[]{1})
        ));

        reqList.add(buildGet(
                ByteString.copyFrom(new byte[]{1})
        ));

        for (KeyValueRequest.KVRequest kvRequest : reqList) {
            byte[] messageBuffer = buildMessage(kvRequest.toByteArray()).toByteArray();

            // If the buffer is larger than 16 KB, then truncate
            int messageBufferSize = Math.min(MAX_PACKET_SIZE, messageBuffer.length);
            DatagramPacket packet = null;
            try {
                packet = new DatagramPacket(messageBuffer, messageBufferSize, InetAddress.getLocalHost(), 4445);
                socket.send(packet);

                byte[] rcvBuf = new byte[MAX_PACKET_SIZE];
                packet = new DatagramPacket(rcvBuf, rcvBuf.length);
                socket.receive(packet);

                Message.Msg message = Server.readRequest(packet);
                KeyValueResponse.KVResponse kvResponse = KeyValueResponse.KVResponse.parseFrom(message.getPayload());

                // TODO: print everything out
                System.out.println("----- RECEIVE PACKET -----");
                System.out.println("Node: " + packet.getPort());
                System.out.println("Status: " + kvResponse.getErrCode());

                if (kvRequest.getCommand() == Server.GET) {
                    System.out.println("Get value: " + kvResponse.getValue());
                }

            } catch (IOException | PacketCorruptionException e) {
                throw new RuntimeException(e);
            }
        }



    }

    private static byte[] generateUUID(InetAddress address, int port) throws IOException {
        byte[] addressBytes = address.getAddress();
        byte[] portBytes = new byte[4];
        ByteOrder.int2leb(port, portBytes, 0);

        Random random = new Random();
        byte[] randomBytes = new byte[2];
        random.nextBytes(randomBytes);

        byte[] systemTimeBytes = ByteBuffer.wrap(new byte[8]).putLong(System.nanoTime()).array();

        return concatenateByteArrays(addressBytes, portBytes, randomBytes, systemTimeBytes);
    }

    private static boolean validateChecksum(Message.Msg message) throws IOException {
        byte[] receivedUUID = message.getMessageID().toByteArray();
        byte[] payloadByteArray = message.getPayload().toByteArray();
        byte[] expectedChecksumByteArray = concatenateByteArrays(receivedUUID, payloadByteArray);
        long expectedChecksum = computeChecksum(expectedChecksumByteArray);
        long receivedChecksum = message.getCheckSum();

        return expectedChecksum == receivedChecksum;
    }

    private static byte[] concatenateByteArrays(byte[]... byteArrays) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (byte[] byteArray: byteArrays) {
            outputStream.write(byteArray);
        }

        return outputStream.toByteArray();
    }

    private static long computeChecksum(byte[] buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        return crc32.getValue();
    }

    public static KeyValueRequest.KVRequest buildPut(ByteString key, ByteString value, int version) {
        KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Server.PUT)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();

        return kvRequest;
    }

    public static KeyValueRequest.KVRequest buildGet(ByteString key) {
        KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Server.GET)
                .setKey(key)
                .build();

        return kvRequest;
    }

    public static KeyValueRequest.KVRequest buildRemove(ByteString key) {
        KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Server.REMOVE)
                .setKey(key)
                .build();

        return kvRequest;
    }

    public static Message.Msg buildMessage(byte[] payload) {
        try {
            byte[] uuid = generateUUID(InetAddress.getLocalHost(), 1234);

            byte[] checksumByteArray = concatenateByteArrays(uuid, payload);
            long checksum = computeChecksum(checksumByteArray);

            Message.Msg message = Message.Msg.newBuilder().setMessageID(ByteString.copyFrom(uuid))
                    .setPayload(ByteString.copyFrom(payload))
                    .setCheckSum(checksum)
                    .build();

            return message;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}


