package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.protobuf.ByteString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Map;
import java.util.TreeMap;

public class ConsistentHash {

    public String ip;
    public int port;

    // Ring where key is hash cutoff for the node and value is the ip and port of the node
    private static TreeMap<Integer, AddressPair> nodeRing = new TreeMap<>();

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
}
