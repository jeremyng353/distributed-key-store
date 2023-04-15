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
import java.util.HashMap;
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
    public static final int TAIL_GET = 0x25;
    public static final int ACK_PUT = 0x27;
    public static final int ACK_REMOVE = 0x28;
    public static final int ACK_GET = 0x29;
    public static final int REPLICA_ACK_PUT = 0x30;
    public static final int REPLICA_ACK_REMOVE = 0x31;
    public static final int TAIL_ACK_PUT = 0x33;
    public static final int TAIL_ACK_REMOVE = 0x34;

    private final String ip;
    private final int port;
    ConsistentHash consistentHash;
    private MemberMonitor memberMonitor;
    private final UDPClient udpClient = new UDPClient();

    private final long pid;
    private Memory memory;
    private HashMap<ByteString, Integer> keyAck = new HashMap<>();

    public Server(int port) {
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
        this.pid = ProcessHandle.current().pid();
    }

    public void setMemory(Memory memory) {
        this.memory = memory;
    }

    public void setConsistentHash(ConsistentHash consistentHash) {
        this.consistentHash = consistentHash;
    }

    public void setMemoryMonitor(MemberMonitor memberMonitor) {
        this.memberMonitor = memberMonitor;
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
    public ByteString exeCommand(Message.Msg message) throws InvalidProtocolBufferException, UnknownHostException {
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
                    if (memory.hasSpace() == NO_MEM_ERR) {
                        return buildResPayload(NO_MEM_ERR);
                    } else {
                        ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                        if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                            memory.lockKey(key);

                            ByteString value = kvRequest.getValue();
                            int version = kvRequest.getVersion();
                            requestReplicas(
                                    key,
                                    value,
                                    version,
                                    REPLICA_PUT,
                                    message.getClientIp(),
                                    message.getClientPort(),
                                    message.getMessageID()
                            );
                        } else {
                            memory.storeMessage(key, message);
                        }
                    }
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(message, nodeAddress);
                }

                return null;
            }
            case GET -> {
                // determine which node should handle request
                ByteString key = kvRequest.getKey();
                AddressPair nodeAddress = consistentHash.getNode(key);
                if (nodeAddress.getIp().equals(ip) && nodeAddress.getPort() == port) {
                    ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                    if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                        memory.lockKey(key);
                        requestTailRead(key, message.getClientIp(), message.getClientPort(), message.getMessageID());
                    } else {
                        memory.storeMessage(key, message);
                    }
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(message, nodeAddress);
                }

                return null;
            }
            case REMOVE -> {
                // determine which node should handle request
                ByteString key = kvRequest.getKey();
                AddressPair nodeAddress = consistentHash.getNode(key);
                if (nodeAddress.getIp().equals(ip) && nodeAddress.getPort() == port) {
                    ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                    if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                        memory.lockKey(key);

                        requestReplicas(
                                key,
                                REPLICA_REMOVE,
                                message.getClientIp(),
                                message.getClientPort(),
                                message.getMessageID()
                        );
                    } else {
                        memory.storeMessage(key, message);
                    }
                } else {
                    // call another node to handle the request
                    consistentHash.callNode(message, nodeAddress);
                }

                return null;
            }
            case SHUTDOWN -> {
                status = memory.shutdown();
                response = buildResPayload(status);
                return response;
            }
            case WIPEOUT -> {
                status = memory.erase();
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
                ByteString key = kvRequest.getKey();
                // TODO: if NO_MEM_ERR, don't send ACK
                ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                    memory.lockKey(key);
                    ByteString value = kvRequest.getValue();
                    int version = kvRequest.getVersion();
                    ackHead(
                            key,
                            value,
                            version,
                            ACK_PUT,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID(),
                            message.getHeadPort()
                    );
                } else {
                    memory.storeMessage(key, message);
                }

                return null;
            }
            case REPLICA_REMOVE -> {
                ByteString key = kvRequest.getKey();
                ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                    memory.lockKey(key);
                    ackHead(
                            key,
                            ACK_REMOVE,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID(),
                            message.getHeadPort()
                    );
                } else {
                    memory.storeMessage(key, message);
                }

                return null;
            }
            case TAIL_GET -> {
                ByteString key = kvRequest.getKey();

                ArrayList<Message.Msg> storedMessages = memory.getStoredMessages(key);
                if (memory.getStoredMessages(key) == null || storedMessages.isEmpty()) {
                    status = memory.isStored(key);
                    if (status == SUCCESS) {
                        Pair<ByteString, Integer> keyValue = memory.get(kvRequest.getKey());
                        response = buildResPayload(status, keyValue.getFirst(), keyValue.getSecond());
                    } else {
                        response = buildResPayload(status);
                    }
                    ackHead(
                            key,
                            ACK_GET,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID(),
                            message.getHeadPort()
                    );

                    RequestCache.put(message.getMessageID(), response);
                    return response;
                } else {
                    memory.storeMessage(key, message);
                }
                return null;
            }
            case ACK_PUT -> {
                ByteString key = kvRequest.getKey();

                int counter = 1;
                if (keyAck.containsKey(key)) {
                    counter = keyAck.get(key) + 1;
                }

                // TODO: what happens if doesn't hit 3 ACKs? timeout?
                if (counter == memberMonitor.getReplicas().size()) {
                    ByteString value = kvRequest.getValue();
                    int version = kvRequest.getVersion();
                    sendFinalAck(
                            key,
                            value,
                            version,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID()
                    );

                    memory.put(key, value, version);
                    keyAck.remove(key);
                } else {
                    keyAck.put(key, counter);
                }

                return null;
            }
            case ACK_REMOVE -> {
                ByteString key = kvRequest.getKey();

                int counter = 1;
                if (keyAck.containsKey(key)) {
                    counter = keyAck.get(key) + 1;
                }

                // TODO: what happens if doesn't hit 3 ACKs? timeout?
                if (counter == memberMonitor.getReplicas().size()) {
                    sendFinalAck(
                            key,
                            message.getClientIp(),
                            message.getClientPort(),
                            message.getMessageID()
                    );

                    memory.remove(key);
                    keyAck.remove(key);
                } else {
                    keyAck.put(key, counter);
                }

                return null;
            }
            case ACK_GET -> {
                ByteString key = kvRequest.getKey();
                memory.removeLock(key);
                return null;
            }
            case REPLICA_ACK_PUT -> {
                ByteString key = kvRequest.getKey();
                memory.put(key, kvRequest.getValue(), kvRequest.getVersion());
                memory.removeLock(key);
                return null;
            }
            case REPLICA_ACK_REMOVE -> {
                ByteString key = kvRequest.getKey();
                memory.remove(key);
                memory.removeLock(key);
                return null;
            }
            case TAIL_ACK_PUT -> {
                ByteString key = kvRequest.getKey();
                status = memory.put(key, kvRequest.getValue(), kvRequest.getVersion());
                response = buildResPayload(status);
                memory.removeLock(key);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            case TAIL_ACK_REMOVE -> {
                ByteString key = kvRequest.getKey();
                status = memory.remove(key);
                response = buildResPayload(status);
                memory.removeLock(key);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
            default -> {
                status = UKN_CMD;
                response = buildResPayload(status);
                RequestCache.put(message.getMessageID(), response);
                return response;
            }
        }
    }

    // requestReplica for GET/REMOVEs
    public void requestReplicas(ByteString key, int command, String clientIp, int clientPort, ByteString messageID, String headIp, int headPort) {
        KeyValueRequest.KVRequest replicaRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command)
                .setKey(key)
                .build();

        sendRequest(clientIp, clientPort, messageID, replicaRequest, headIp, headPort);
    }

    // requestReplica for head GET/REMOVEs from the head node
    public void requestReplicas(ByteString key, int command, String clientIp, int clientPort, ByteString messageID) {
        requestReplicas(key, command, clientIp, clientPort, messageID, this.ip, this.port);
    }

    // requestReplica for PUTs
    public void requestReplicas(ByteString key, ByteString value, int version, int command, String clientIp, int clientPort, ByteString messageID, String headIp, int headPort) {
        KeyValueRequest.KVRequest replicaRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();

        sendRequest(clientIp, clientPort, messageID, replicaRequest, headIp, headPort);
    }

    // requestReplica for PUTs from the head node
    public void requestReplicas(ByteString key, ByteString value, int version, int command, String clientIp, int clientPort, ByteString messageID){
        requestReplicas(key, value, version, command, clientIp, clientPort, messageID, this.ip, this.port);
    }

    private void sendRequest(String clientIp, int clientPort, ByteString messageID, KeyValueRequest.KVRequest replicaRequest, String headIp, int headPort) {
        try {
            for (AddressPair node : memberMonitor.getReplicas()) {
                udpClient.replicaRequest(
                        InetAddress.getByName("localhost"),/*node.getIp()),*/
                        node.getPort(),
                        replicaRequest.toByteArray(),
                        clientIp,
                        clientPort,
                        messageID,
                        headIp,
                        headPort
                );
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void requestTailRead(ByteString key, String clientIp, int clientPort, ByteString messageID) {
        KeyValueRequest.KVRequest replicaRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(TAIL_GET)
                .setKey(key)
                .build();
        AddressPair tailNode = memberMonitor.getReplicas().get(memberMonitor.getReplicas().size()-1);

        try {
            udpClient.replicaRequest(
                    InetAddress.getByName("localhost"),
                    tailNode.getPort(),
                    replicaRequest.toByteArray(),
                    clientIp,
                    clientPort,
                    messageID,
                    this.ip,
                    this.port
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void ackHead(ByteString key, ByteString value, int version, int command, String clientIp, int clientPort, ByteString messageID, int headPort) {
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();

        try {
            udpClient.replicaRequest(
                    InetAddress.getByName("localhost"),
                    headPort,
                    request.toByteArray(),
                    clientIp,
                    clientPort,
                    messageID,
                    this.ip,
                    this.port
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void ackHead(ByteString key, int command, String clientIp, int clientPort, ByteString messageID, int headPort) {
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(command)
                .setKey(key)
                .build();

        try {
            udpClient.replicaRequest(
                    InetAddress.getByName("localhost"),
                    headPort,
                    request.toByteArray(),
                    clientIp,
                    clientPort,
                    messageID,
                    this.ip,
                    this.port
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    // for PUT
    public void sendFinalAck(ByteString key, ByteString value, int version, String clientIp, int clientPort, ByteString messageID) {
        // send REPLICA_ACK_PUT to replicas (not tail)
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(REPLICA_ACK_PUT)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();

        ArrayList<AddressPair> replicaList = memberMonitor.getReplicas();
        for (int i = 0; i < replicaList.size()-1; i++) {
            try {
                udpClient.replicaRequest(
                        InetAddress.getByName("localhost"),
                        replicaList.get(i).getPort(),
                        request.toByteArray(),
                        clientIp,
                        clientPort,
                        messageID,
                        ip,
                        port
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        // send TAIL_ACK_PUT to tail
        KeyValueRequest.KVRequest tailRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(TAIL_ACK_PUT)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .build();


        try {
            udpClient.replicaRequest(
                    InetAddress.getByName("localhost"),
                    replicaList.get(replicaList.size()-1).getPort(),
                    tailRequest.toByteArray(),
                    clientIp,
                    clientPort,
                    messageID,
                    this.ip,
                    this.port
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    // for REMOVE
    public void sendFinalAck(ByteString key, String clientIp, int clientPort, ByteString messageID) {
        // send REPLICA_ACK_PUT to replicas (not tail)
        KeyValueRequest.KVRequest request = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(REPLICA_ACK_REMOVE)
                .setKey(key)
                .build();

        ArrayList<AddressPair> replicaList = memberMonitor.getReplicas();
        for (int i = 0; i < replicaList.size()-1; i++) {
            try {
                udpClient.replicaRequest(
                        InetAddress.getByName("localhost"),
                        replicaList.get(i).getPort(),
                        request.toByteArray(),
                        clientIp,
                        clientPort,
                        messageID,
                        ip,
                        port
                );
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        // send TAIL_ACK_PUT to tail
        KeyValueRequest.KVRequest tailRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(TAIL_ACK_REMOVE)
                .setKey(key)
                .build();

        try {
            udpClient.replicaRequest(
                    InetAddress.getByName("localhost"),
                    replicaList.get(replicaList.size()-1).getPort(),
                    tailRequest.toByteArray(),
                    clientIp,
                    clientPort,
                    messageID,
                    this.ip,
                    this.port
            );
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
