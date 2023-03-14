package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class KeyTransferer implements Runnable {

    private final AddressPair destinationAddress;
    private final int sourceNodeHash;
    private final int destinationNodeHash;
    private final UDPClient udpClient;

    public KeyTransferer(AddressPair destinationAddress, int sourceNodeHash, int destinationNodeHash) {
        this.sourceNodeHash =sourceNodeHash;
        this.destinationAddress = destinationAddress;
        this.destinationNodeHash = destinationNodeHash;
        this.udpClient = new UDPClient();
    }

    @Override
    public void run() {
        Memory.getAllEntries()
                .filter(entry -> {
                    int keyHash = Math.abs(entry.getKey().hashCode()) % 256;
                    if (sourceNodeHash < destinationNodeHash) {
                        return (keyHash < sourceNodeHash) || (keyHash >= destinationNodeHash);
                    } else {
                        return keyHash < destinationNodeHash;
                    }
                })
                .forEach(entry -> {
                    KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.newBuilder()
                            .setCommand(Server.PUT)
                            .setKey(entry.getKey())
                            .setValue(entry.getValue().getFirst())
                            .setVersion(entry.getValue().getSecond())
                            .build();

                    try {
                        udpClient.request(InetAddress.getByName(
                                destinationAddress.getIp()),
                                destinationAddress.getPort(),
                                kvRequest.toByteArray());
                    } catch (UnknownHostException e) {
                        System.err.println("Error getting destination InetAddress for ip: " + destinationAddress.getIp());
                        e.printStackTrace();
                    }
                });
    }
}
