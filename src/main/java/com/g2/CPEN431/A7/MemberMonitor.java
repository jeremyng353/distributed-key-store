package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static com.g2.CPEN431.A7.Server.GET_MS_LIST;

public class MemberMonitor implements Runnable {

    // A HashMap to store node information
    private final HashMap<AddressPair, Long> nodeStore;
    private final Random random;
    private final UDPClient udpClient;
    private final AddressPair self;
    private final ConsistentHash consistentHash;

    //dummy time until we set the amount of nodes
    public static final int DEFAULT_INTERVAL = 500;
    final int NUM_NODES = 20;
    final int SAFETY_MARGIN = 3000;

    public MemberMonitor(ArrayList<AddressPair> initialMembership, AddressPair selfAddress, ConsistentHash consistentHash) {
        this.nodeStore = new HashMap<>();
        this.random = new Random();
        this.udpClient = new UDPClient();
        this.self = selfAddress;
        this.consistentHash = consistentHash;

        Long currentTime = System.currentTimeMillis();
        for (AddressPair addressPair : initialMembership) {
            nodeStore.put(addressPair, currentTime);
        }
    }

    @Override
    public void run() {
        // Update itself in the nodestore to be the latest time
        nodeStore.put(self, System.currentTimeMillis());

        Set<AddressPair> nodes = nodeStore.keySet();
        int index = random.nextInt(nodes.size());
        AddressPair node = (AddressPair) nodes.toArray()[index];


        // Make sure it's not trying to contact itself or a dead node
        while (node.equals(self) || isDead(node)) {
            index = random.nextInt(nodes.size());
            node = (AddressPair) nodes.toArray()[index];
        }

        KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(GET_MS_LIST)
                .build();

        try {
            Message.Msg nodeResponse = udpClient.request(
                    // TODO: Replace this with the node IP
                    InetAddress.getByName("localhost"),
                    node.getPort(),
                    kvRequest.toByteArray());
            if (nodeResponse != null) {
                KeyValueResponse.KVResponse.parseFrom(nodeResponse.getPayload())
                        .getMembershipInfoList()
                        .forEach((membershipInfo -> {
                            AddressPair checkAddressPair = new AddressPair(membershipInfo.getAddressPair());
                            long checkLastAlive = Math.max(
                                    membershipInfo.getTime(),
                                    nodeStore.get(checkAddressPair));
                            // Note that we're using system default time zone, which we'll need to keep in mind when we check if a node is alive
                            nodeStore.put(checkAddressPair, checkLastAlive);
                        }));
//                        System.out.println("[" + self.getPort() + "]: " + nodeStore);
                for (Map.Entry<AddressPair, Long> entry : nodeStore.entrySet()) {
                    if (isDead(entry.getKey())) {
//                        System.out.println("[" + self.getPort() + "]: Detected node " + entry.getKey() + " to be dead!");
                        consistentHash.removeNode(entry.getKey());
                    }
                }
            } else {
//                System.out.println("No response from node " + node + ", it may be dead?");
//                        consistentHash.removeNode(node);
            }

        } catch (UnknownHostException e) {
            System.err.println("Error while getting IP for node: " + node.getIp());
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            System.err.println("Couldn't parse protocol buffer");
            e.printStackTrace();
        }
    }

    public Map<AddressPair, Long> getMembershipInfo() {
        return this.nodeStore;
    }

    private boolean isDead(AddressPair addressPair) {
        return System.currentTimeMillis() - nodeStore.get(addressPair) > (DEFAULT_INTERVAL * (Math.log(NUM_NODES) / Math.log(2) + SAFETY_MARGIN));
    }
}
