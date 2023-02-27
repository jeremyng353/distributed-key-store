package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.TreeMap;

import static com.g2.CPEN431.A7.App.MAX_INCOMING_PACKET_SIZE;

public class ConsistentHash {

    public String ip;
    public int port;

    // Ring where key is hash cutoff for the node and value is the ip and port of the node
    private TreeMap<Integer, AddressPair> nodeRing = new TreeMap<>();

    // TODO: write javadocs + implement the key to be spread out? or randomly generated key?
    public void addNode(AddressPair addressPair) {
        nodeRing.put(key, addressPair);
    }

    /**
     * This function determines which node should handle the operation
     * @param kvRequest: The request to be handled
     * @return An AddressPair containing the IP and port of the node
     */
    public AddressPair getNode(KeyValueRequest.KVRequest kvRequest) {
        if (nodeRing.size() == 0) {
            return null;
        }

        // match hash of key to the least entry that has a strictly greater key
        Map.Entry<Integer, AddressPair> nextEntry = nodeRing.higherEntry(kvRequest.getKey().hashCode());
        return nextEntry != null ? nextEntry.getValue() : nodeRing.firstEntry().getValue();
    }

    /**
     * This function forwards a request from this node to another node
     * @param packet: The packet to be forwarded to another node
     * @return A DatagramPacket containing the response from the other node
     */
    public DatagramPacket callNode(DatagramPacket packet) {
        DatagramSocket socket;
        DatagramPacket forwardedPacket = null;
        try {
            socket = new DatagramSocket(50000);
            socket.send(new DatagramPacket(packet.getData(), packet.getLength(), InetAddress.getByName(ip), port));
            byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];
            forwardedPacket = new DatagramPacket(buf, buf.length);
            socket.receive(forwardedPacket);
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return forwardedPacket;
    }
}
