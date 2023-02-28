package com.g2.CPEN431.A7;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static com.g2.CPEN431.A7.App.MAX_INCOMING_PACKET_SIZE;

public class ConsistentHash {
    public int port;
    private DatagramSocket socket;

    // Ring where key is hash cutoff for the node and value is the ip and port of the node
    private TreeMap<Integer, AddressPair> nodeRing = new TreeMap<>();

    public ConsistentHash(int port) {
        this.port = port;
        try {
            socket = new DatagramSocket(50000 + port);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function adds a node to the node ring
     * @param addressPair: The ip and the port of the node
     */
    public void addNode(AddressPair addressPair) {
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
        Map.Entry<Integer, AddressPair> nextEntry = nodeRing.higherEntry(key.hashCode());
        return nextEntry != null ? nextEntry.getValue() : nodeRing.firstEntry().getValue();
    }

    /**
     * This function forwards a request from this node to another node
     * @param packet: The packet to be forwarded to another node
     * @param nodeAddress: The address of the node to forward the packet to
     * @return A DatagramPacket containing the response from the other node
     */
    public DatagramPacket callNode(DatagramPacket packet, AddressPair nodeAddress) {
        DatagramPacket forwardedPacket = null;
        System.out.println("calling node at ip: " + nodeAddress.getIp() + ", port: " + nodeAddress.getPort());
        try {
            // nodeAddress.getIp()
            socket.send(new DatagramPacket(packet.getData(), packet.getLength(), InetAddress.getByName("localhost"), nodeAddress.getPort()));
            byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];
            forwardedPacket = new DatagramPacket(buf, buf.length);
            socket.receive(forwardedPacket);
            System.out.println("received packet");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return forwardedPacket;
    }
}
