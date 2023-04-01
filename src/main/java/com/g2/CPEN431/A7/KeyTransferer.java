package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.protobuf.ByteString;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.stream.Stream;

public class KeyTransferer implements Runnable {

    private final boolean transferAll;
    private final AddressPair destinationAddress;
    private final int sourceNodeHash;
    private final int destinationNodeHash;
    private final UDPClient udpClient;

    public KeyTransferer(AddressPair destinationAddress, int sourceNodeHash, int destinationNodeHash) {
        this(destinationAddress, sourceNodeHash, destinationNodeHash, false);
//        this.sourceNodeHash =sourceNodeHash;
//        this.destinationAddress = destinationAddress;
//        this.destinationNodeHash = destinationNodeHash;
//        this.udpClient = new UDPClient();
    }

    public KeyTransferer(AddressPair destinationAddress, int sourceNodeHash, int destinationNodeHash, boolean transferAll) {
        this.transferAll = transferAll;
        this.sourceNodeHash =sourceNodeHash;
        this.destinationAddress = destinationAddress;
        this.destinationNodeHash = destinationNodeHash;
        this.udpClient = new UDPClient();
    }

    @Override
    public void run() {
        Stream<Map.Entry<ByteString, Pair<ByteString, Integer>>> memory = Memory.getAllEntries();

        if (transferAll) {
            memory = memory.filter(entry -> {
                int keyHash = Math.abs(entry.getKey().hashCode()) % 256;
                if (sourceNodeHash < destinationNodeHash) {
                    return (keyHash < sourceNodeHash) || (keyHash >= destinationNodeHash);
                } else {
                    return keyHash < destinationNodeHash;
                }
            });
        }

        memory.forEach(entry -> {
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
