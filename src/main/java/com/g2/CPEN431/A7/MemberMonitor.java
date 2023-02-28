package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

import static com.g2.CPEN431.A7.Server.GET_MS_LIST;

public class MemberMonitor implements Runnable{

    // A HashMap to store node information
    private final HashMap<AddressPair, LocalDateTime> nodeStore;
    private final Random random;
    private final UDPClient udpClient;

    //dummy time until we set the amount of nodes
    final int FULL_INFECTION_TIME = 10000;

    public MemberMonitor(ArrayList<AddressPair> initialMembership) {
        this.nodeStore = new HashMap<>();
        this.random = new Random();
        this.udpClient = new UDPClient();

        LocalDateTime currentTime = LocalDateTime.now();
        for (AddressPair addressPair : initialMembership) {
            nodeStore.put(addressPair, currentTime);
        }
    }

    @Override
    public run() {
        TimerTask pullEpidemic = new TimerTask() {
            @Override
            public void run() {
                // grab a random node from the nodeStore at random, and pull its status
                // update the nodeStore if the value received is older than what is saved
                // if no value is returned past the timeout, then check if the node is past the FULL_INFECTION_TIME limit
                    // if so, nodeStore.get(randomNode.InetAddress).setBoolean(false)
                Set<AddressPair> nodes = nodeStore.keySet();
                int index = random.nextInt(nodes.size());
                AddressPair node = (AddressPair) nodes.toArray()[index];

                KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                        .setCommand(GET_MS_LIST)
                        .build();

                try {
                    Message.Msg nodeResponse = udpClient.request(
                            InetAddress.getByName(node.getIp()),
                            node.getPort(),
                            kvRequest.toByteArray());
                    KeyValueResponse.KVResponse.parseFrom(nodeResponse.getPayload())
                            .getMembershipInfoList()
                            .forEach((membershipInfo -> {
                                AddressPair checkAddressPair = new AddressPair(membershipInfo.getAddressPair());
                                long checkLastAlive = Math.max(
                                        membershipInfo.getTime(),
                                        nodeStore.get(checkAddressPair).toEpochSecond(ZoneOffset.UTC));
                                // Note that we're using system default time zone, which we'll need to keep in mind when we check if a node is alive
                                nodeStore.put(
                                        checkAddressPair,
                                        LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(checkLastAlive),
                                                ZoneId.systemDefault()));
                    }));

                } catch (UnknownHostException e) {
                    System.err.println("Error while getting IP for node: " + node.getIp());
                    e.printStackTrace();
                } catch (InvalidProtocolBufferException e) {
                    System.err.println("Couldn't parse protocol buffer");
                    e.printStackTrace();
                }
            }
        };

        Timer timer = new Timer("Send Timer");
        timer.schedule(pullEpidemic, 3000);

        while (true) {
            //listen for other nodes
        }

    }

    public Map<AddressPair, LocalDateTime> getMembershipInfo() {
        return this.nodeStore;
    }
}
