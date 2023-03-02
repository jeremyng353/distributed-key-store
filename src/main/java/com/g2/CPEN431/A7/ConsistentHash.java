package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

public class ConsistentHash {
    public int port;
    private final DatagramSocket socket;

    // Ring where key is hash cutoff for the node and value is the ip and port of the node
    private TreeMap<Integer, AddressPair> nodeRing = new TreeMap<>();

    public ConsistentHash(int port) {
        this.port = port;
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function adds a node to the node ring
     * @param addressPair: The ip and the port of the node
     */
    public void addNode(AddressPair addressPair) {
        System.out.println("Hashing addresspair: " + addressPair.toString() + " " + addressPair.hashCode());
        nodeRing.put(Objects.hashCode(addressPair), addressPair);
    }

    /**
     * This function determines which node should handle the operation
     * @param key: The key of the request that will be hashed
     * @return An AddressPair containing the IP and port of the node
     */
    public AddressPair getNode(ByteString key) {
        if (nodeRing.size() == 0) {
            return null;
        }

        // match hash of key to the least entry that has a strictly greater key
        Map.Entry<Integer, AddressPair> nextEntry = nodeRing.higherEntry(Math.abs(key.hashCode()) % 256);
        return nextEntry != null ? nextEntry.getValue() : nodeRing.firstEntry().getValue();
    }

    public AddressPair removeNode(AddressPair addressPair) {
        return nodeRing.remove(Objects.hashCode(addressPair));
    }

    /**
     * This function forwards a request from this node to another node
     * @param packet: The packet to be forwarded to another node
     * @param nodeAddress: The address of the node to forward the packet to
     */
    public void callNode(DatagramPacket packet, AddressPair nodeAddress) {
        try {
            Message.Msg message = Server.readRequest(packet);
            Message.Msg forwardMessage = Message.Msg.newBuilder(message)
                    .setClientIp(packet.getAddress().getHostAddress())
                    .setClientPort(packet.getPort())
                    .build();
            DatagramPacket forwardPacket = new DatagramPacket(
                    forwardMessage.toByteArray(),
                    forwardMessage.toByteArray().length,
                    InetAddress.getByName("localhost"),
                    nodeAddress.getPort());
            socket.send(forwardPacket);
        } catch (IOException | PacketCorruptionException e) {
            e.printStackTrace();
        }
    }

    private static long buildChecksum(ByteString messageID, ByteString payload) {
        CRC32 checksum = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(messageID.size() + payload.size());

        buf.put(messageID.toByteArray());
        buf.put(payload.toByteArray());
        checksum.update(buf.array());

        return checksum.getValue();
    }
    private static void readRequest(DatagramPacket packet) throws InvalidProtocolBufferException, PacketCorruptionException {
        // Truncate data to match correct data length
        byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
        Message.Msg message = Message.Msg.parseFrom(data);

        // Verify the messageID and the checksum is correct
        if (message.getCheckSum() != buildChecksum(message.getMessageID(), message.getPayload())) {
            throw new PacketCorruptionException();
        }
    }
}
