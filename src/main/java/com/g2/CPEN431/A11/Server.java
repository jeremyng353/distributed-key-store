package com.g2.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.CRC32;

public class Server {

    // response code constant values
    private static final int SUCCESS = 0x00;
    private static final int NO_KEY_ERR = 0x01;
    private static final int NO_MEM_ERR = 0x02;
    private static final int UKN_CMD = 0x05;

    // command codes constant values
    public static final int PUT = 0x01;
    public static final int GET = 0x02;
    public static final int REMOVE = 0x03;
    public static final int SHUTDOWN = 0x04;
    public static final int WIPEOUT = 0x05;
    public static final int IS_ALIVE = 0x06;
    public static final int GET_PID = 0x07;
    public static final int GET_MS_ID = 0x08;
    public static final int GET_MS_LIST = 0x22;
    public static final int REPLICA_PUT = 0x23;
    public static final int REPLICA_REMOVE = 0x24;
    public static final int REPLICA_GET = 0x25;

    private final String ip;
    private final int port;
    ConsistentHash consistentHash;
    private final MemberMonitor memberMonitor;
    private final UDPClient udpClient = new UDPClient();

    private final long pid;

    public Server(int port, ConsistentHash consistentHash, MemberMonitor memberMonitor) {
        // Adapted from https://www.baeldung.com/java-get-ip-address
        String urlString = "http://checkip.amazonaws.com/";
        URL url = null;
        try {
            url = new URL(urlString);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
                ip = br.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        this.port = port;
        this.consistentHash = consistentHash;
        this.memberMonitor = memberMonitor;
        this.pid = ProcessHandle.current().pid();
    }

    /**
     * This function builds the message to be sent back to the client.
     * @param messageID: The response messageID associated with the incoming request
     * @param payload: The return code and any other data associated with the response
     * @param checksum: Checksum built to verify the packet
     * @return The byte array to be sent in the Datagram packet
     */
    public static byte[] buildMessage(ByteString messageID, ByteString payload, long checksum) {
        Message.Msg message = Message.Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(payload)
                .setCheckSum(checksum)
                .build();
        return message.toByteArray();
    }

    /**
     * This function builds the checksum using the parameters
     * @param messageID: ByteString containing the uniqueID
     * @param payload: ByteString containing the studentID
     * @return A long containing the checksum for verification with server
     */
    public static long buildChecksum(ByteString messageID, ByteString payload) {
        CRC32 checksum = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(messageID.size() + payload.size());

        buf.put(messageID.toByteArray());
        buf.put(payload.toByteArray());
        checksum.update(buf.array());

        return checksum.getValue();
    }

    /**
     * This function builds the response payload to add to the message sent
     * @param errCode: Integer response code to add into the payload
     * @return A ByteString containing the response code
     */
    public static ByteString buildResPayload(int errCode) {
        KeyValueResponse.KVResponse resPayload = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errCode)
                .build();
        return resPayload.toByteString();
    }

    /**
     * This function builds the response payload following a get operation
     * @param errCode: Integer response code to add into the payload
     * @param value: The value associated with the provided key in ByteString
     * @param version: The version Integer associated with the key value pair
     * @return A ByteString containing the response code, value and version
     */
    public static ByteString buildResPayload(int errCode, ByteString value, int version) {
        KeyValueResponse.KVResponse resPayload = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errCode)
                .setValue(value)
                .setVersion(version)
                .build();
        return resPayload.toByteString();
    }

    /**
     * This function builds the response payload following a getPID operation
     * @param errCode: Integer response code to add into the payload
     * @param pid: Long representing the Java processID
     * @return A ByteString containing the response code and processID
     */
    public static ByteString buildResPayload(int errCode, long pid) {
        KeyValueResponse.KVResponse resPayload = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errCode)
                .setPid((int)pid)
                .build();
        return resPayload.toByteString();
    }

    /**
     * This function builds the response payload following a getMembershipCount operation
     * @param errCode: Integer response code to add into the payload
     * @param membershipCount: The count of current active members
     * @return A ByteString containing the response code and membershipCount
     */
    public static ByteString buildResPayload(int errCode, int membershipCount) {
        KeyValueResponse.KVResponse resPayload = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errCode)
                .setMembershipCount(membershipCount)
                .build();
        return resPayload.toByteString();
    }

    /**
     * This function builds the response payload following a getMembershipInfo operation
     * @param errCode: Integer response code to add into the payload
     * @param membershipInfo: The count of current active members
     * @return A ByteString containing the response code and membershipCount
     */
    public static ByteString buildResPayload(int errCode, KeyValueResponse.KVResponse.MembershipInfo[] membershipInfo) {
        KeyValueResponse.KVResponse.Builder resPayloadBuilder = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(errCode);

        for (int i = 0; i < membershipInfo.length; i++) {
            resPayloadBuilder.addMembershipInfo(i, membershipInfo[i]);
        }

        return resPayloadBuilder.build().toByteString();
    }

    /**
     * This function builds and verifies the incoming packet into a message
     * @param packet: The incoming Datagram packet to be read
     * @return The message translation of the incoming Datagram packet
     * @throws InvalidProtocolBufferException: This exception is thrown when an operation error occurs with parseFrom() function
     * @throws PacketCorruptionException: This exception is thrown when either the messageID or checksum is incorrect and corrupt
     */
    public static Message.Msg readRequest(DatagramPacket packet) throws InvalidProtocolBufferException, PacketCorruptionException {
        // Truncate data to match correct data length
        byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
        Message.Msg message = Message.Msg.parseFrom(data);

        // Verify the messageID and the checksum is correct
        if (message.getCheckSum() != buildChecksum(message.getMessageID(), message.getPayload())) {
            throw new PacketCorruptionException();
        }
        return message;
    }

    /**
     * This function reads the incoming message and executes the contained command
     * @param message: The incoming message containing the operation details
     * @return A ByteString containing the operation response payload to be sent back
     * @throws InvalidProtocolBufferException: This exception is thrown when an operation error occurs with parseFrom() function
     */
    public ByteString exeCommand(Message.Msg message, DatagramPacket packet) throws InvalidProtocolBufferException, UnknownHostException {
        // get kvrequest from message
        KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.parseFrom(message.getPayload());
        int status;
        ByteString response;
        // parse which command to execute
        // for each command, run corresponding memory command, store response in cache and return response
        if (kvRequest.getCommand() != GET_MS_LIST) {
            System.out.println(port + ": " + String.format("0x%08X", kvRequest.getCommand()));
        }
        switch (kvRequest.getCommand()) {
            case PUT -> {
                // determine which node should handle request
                ByteString key = kvRequest.getKey();

                AddressPair nodeAddress = consistentHash.getNode(key);
                // System.out.println("Sending request from node at ip: " + ip + ", port: " + port);

                if (nodeAddress.getIp().equals(ip) && nodeAddress.getPort() == port) {
                    // if this node should handle the request, put and forward to next replica
                    ByteString value = kvRequest.getValue();
                    int version = kvRequest.getVersion();
                    status = Memory.put(key, value, version);
                    if (status != NO_MEM_ERR) {
                        response = buildResPayload(status);
                        RequestCache.put(message.getMessageID(), response);
                        writeReplicas(
                                key,
                                value,
                                version,
                                message.getClientIp(),
                                message.getClientPort(),
                                message.getMessageID()
                        );
                        System.out.println("received ACK for PUT (0x1)");
                        return response;
                    }
                    System.out.println("[" + port + "]: Out of memory!");
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(packet, nodeAddress);
                }

                return null;
            }
            case GET -> {
                // determine which node should handle request
                ByteString key = kvRequest.getKey();
                AddressPair nodeAddress = consistentHash.getNode(key);
                if (nodeAddress.getIp().equals(ip) && nodeAddress.getPort() == port) {
                    // if this node should handle the request, forward request to tail of replica chain
                    status = Memory.isStored(key);
                    if (status == SUCCESS) {
                        Pair<ByteString, Integer> keyValue = Memory.get(key);
                        return buildResPayload(status, keyValue.getFirst(), keyValue.getSecond());
                    } else {
                        return buildResPayload(status);
                    }
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(packet, nodeAddress);
                }

                return null;
            }
            case REMOVE -> {
                // determine which node should handle request
                ByteString key = kvRequest.getKey();
                AddressPair nodeAddress = consistentHash.getNode(key);
                if (nodeAddress.getIp().equals(ip) && nodeAddress.getPort() == port) {
                    // if this node should handle the request, remove and forward request to next replica
                    status = Memory.remove(key);
                    response = buildResPayload(status);
                    RequestCache.put(message.getMessageID(), response);
                    writeReplicas(
                            key,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID()
                    );
                    System.out.println("received ACK for REM (0x3)");
                    return response;
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(packet, nodeAddress);
                }

                return null;
            }
            case SHUTDOWN -> {
                status = Memory.shutdown();
                response = buildResPayload(status);
                return response;
            }
            case WIPEOUT -> {
                status = Memory.erase();
                RequestCache.erase();
                response = buildResPayload(status);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case IS_ALIVE -> {
                status = SUCCESS;
                response = buildResPayload(status);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case GET_PID -> {
                status = SUCCESS;
                response = buildResPayload(status, pid);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case GET_MS_ID -> {
                status = SUCCESS;
                int membershipCount = consistentHash.membershipCount();
                response = buildResPayload(status, membershipCount);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case GET_MS_LIST -> {
                status = SUCCESS;
                KeyValueResponse.KVResponse.MembershipInfo[] membershipInfos = memberMonitor
                        .getMembershipInfo()
                        .entrySet()
                        .stream()
                        .map((entry) -> KeyValueResponse.KVResponse.MembershipInfo.newBuilder()
                                .setAddressPair(entry.getKey().toString())
                                // TODO: Update this so that if the node refers to the current node, use the current time
                                .setTime(entry.getKey().getPort() == port ? System.currentTimeMillis() : entry.getValue())
                                .build())
                        .toArray(KeyValueResponse.KVResponse.MembershipInfo[]::new);
                response = buildResPayload(status, membershipInfos);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case REPLICA_PUT -> {
                Memory.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                System.out.println(port + ": PUT reply with ACK");
                udpClient.reply(packet);
                return null;
            }
            case REPLICA_REMOVE -> {
                ByteString key = kvRequest.getKey();
                Memory.remove(key);
                System.out.println("REMOVE reply with ACK");
                udpClient.reply(packet);
                return null;
            }
            default -> {
                status = UKN_CMD;
                response = buildResPayload(status);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
        }
    }

    // for PUT
    public void writeReplicas(ByteString key, ByteString value, int version, String clientIp, int clientPort, ByteString messageID) {
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(REPLICA_PUT)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();

        ArrayList<AddressPair> replicaList = new ArrayList<>(memberMonitor.getReplicas());
        for (AddressPair node : replicaList) {
            try {
                udpClient.waitReplicaRequest(
                        InetAddress.getByName("localhost"),
                        node.getPort(),
                        request.toByteArray(),
                        clientIp,
                        clientPort,
                        messageID
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // for REMOVEs
    public void writeReplicas(ByteString key, String clientIp, int clientPort, ByteString messageID) {
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(REPLICA_REMOVE)
                .setKey(key)
                .build();

        ArrayList<AddressPair> replicaList = memberMonitor.getReplicas();
        for (AddressPair node : replicaList) {
            try {
                udpClient.waitReplicaRequest(
                        InetAddress.getByName("localhost"),
                        node.getPort(),
                        request.toByteArray(),
                        clientIp,
                        clientPort,
                        messageID
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
