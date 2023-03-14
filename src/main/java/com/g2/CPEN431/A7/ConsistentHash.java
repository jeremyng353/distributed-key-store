package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.*;
import java.util.*;

public class ConsistentHash {
    public int port;
    private final DatagramSocket socket;

    // Ring where key is hash cutoff for the node and value is the ip and port of the node
    private TreeMap<Integer, AddressPair> nodeRing = new TreeMap<>();
    private HashMap<AddressPair, Integer> savedHashes = new HashMap<>();

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
        int addressHash = Objects.hashCode(addressPair);

        while (nodeRing.containsKey(addressHash)) {
            addressHash = (addressHash + 1) % 256;
        }
        System.out.println("[" + port + "]: Hashing addresspair: " + addressPair.toString() + " " + addressHash);

        // Save the address hash in case it rejoins, since the hash used may be different from the internal hashCode()
        savedHashes.put(addressPair, addressHash);
        nodeRing.put(addressHash, addressPair);
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
        return nodeRing.remove(savedHashes.get(addressPair));
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

    public boolean containsNode(AddressPair addressPair){
        return nodeRing.containsValue(addressPair);
    }
    public int membershipCount(){
        return nodeRing.size();
    }
}