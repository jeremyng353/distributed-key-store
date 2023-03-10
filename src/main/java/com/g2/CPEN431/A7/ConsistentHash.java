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
            socket.setSoTimeout(2000);
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
        nodeRing.put(addressPair.hashCode(), addressPair);
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
        Map.Entry<Integer, AddressPair> nextEntry = nodeRing.higherEntry(key.hashCode());
        return nextEntry != null ? nextEntry.getValue() : nodeRing.firstEntry().getValue();
    }

    public AddressPair removeNode(AddressPair addressPair) {
        return nodeRing.remove(addressPair.hashCode());
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

            // socket.receive();
        } catch (IOException | PacketCorruptionException e) {
            e.printStackTrace();
        }
    }

    public boolean containsNode(AddressPair addressPair){
        return nodeRing.containsKey(addressPair.hashCode());
    }
    public int membershipCount(){
        return nodeRing.size();
    }
}