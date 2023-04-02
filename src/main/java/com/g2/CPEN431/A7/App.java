package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

import static com.g2.CPEN431.A7.MemberMonitor.DEFAULT_INTERVAL;

public class App
{
    public static final int MAX_INCOMING_PACKET_SIZE = 16 * 1024;   // 16 kilobyte buffer to receive packets

    // List of node IP:port
    // public static ArrayList<AddressPair> nodeList = new ArrayList<>();

    public static void main( String[] args ) throws IOException {
        // multiple nodes on one ec2 instance --> create multiple sockets, do in another branch
        String currentIp = args[0];
        int port = Integer.parseInt(args[1]);
        // socket for all incoming packets
        DatagramSocket inputSocket = new DatagramSocket(port);


        // TODO: add nodes to consistentHash, maybe hardcode in a txt file?
        ConsistentHash consistentHash = new ConsistentHash(currentIp, port);

        File nodeList = new File("nodes.txt");
        Scanner myReader = new Scanner(nodeList);
        ArrayList<AddressPair> initialNodes = new ArrayList<>();
        while (myReader.hasNextLine()) {
            String ip = myReader.nextLine();
            String nodePort = myReader.nextLine();
            AddressPair addressPair = new AddressPair(ip, Integer.parseInt(nodePort));
            consistentHash.addNode(addressPair);
            initialNodes.add(addressPair);
        }

        MemberMonitor memberMonitor = new MemberMonitor(initialNodes, new AddressPair(currentIp, port), consistentHash);
        //create a thread to monitor the other servers in the system
        TimerTask pullEpidemic = new TimerTask() {
            @Override
            public void run() {
                memberMonitor.run();
            }
        };
        Timer timer = new Timer("Send Timer");
        timer.scheduleAtFixedRate(pullEpidemic, DEFAULT_INTERVAL, DEFAULT_INTERVAL);

        // print listening port to console
        int localPort = inputSocket.getLocalPort();
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        System.out.println("Server is Listening at " + localAddress + " on port " + localPort + "...");

        Server server = new Server(port, consistentHash, memberMonitor);

        // PriorityBlockingQueue<DatagramPacket> packetQueue = new PriorityBlockingQueue<>(25, new PairComparator());
        PriorityBlockingQueue<Message.Msg> messageQueue = new PriorityBlockingQueue<>(25, new PairComparator());

        // Thread to receive all incoming packets
        new Thread(() -> {
            try {
                while (true) {
                    byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    inputSocket.receive(packet);

                    Message.Msg message = Server.readRequest(packet);
                    KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.parseFrom(message.getPayload());
                    if (!message.hasTimestamp() || !message.hasClientIp() || !message.hasClientPort()) {
                        message = Message.Msg.newBuilder(message)
                                .setClientIp(message.hasClientIp() ? message.getClientIp() : packet.getAddress().getHostAddress())
                                .setClientPort(message.hasClientPort() ? message.getClientPort() : packet.getPort())
                                .setTimestamp(kvRequest.getCommand() == Server.SHUTDOWN ? 0 : System.currentTimeMillis())
                                .build();
                    }
                    messageQueue.put(message);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (PacketCorruptionException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // Thread to process packets and send responses to client
        new Thread(() -> {
            try {
                // socket for sending response to clients
                DatagramSocket clientOutputSocket = new DatagramSocket();

                while (true) {
                    // DatagramPacket parsePacket = packetQueue.take();
                    Message.Msg message = messageQueue.take();

                    // Message.Msg message = Server.readRequest(parsePacket);

                    ByteString kvResponse;
                    // if message cached retrieved cached response otherwise execute command
                    if (RequestCache.isStored(message.getMessageID())) {
                        kvResponse = RequestCache.get(message.getMessageID());
                    } else {
                        kvResponse = server.exeCommand(message);
                    }

                    /*

                    int packetPort = parsePacket.getPort();
                    InetAddress address = parsePacket.getAddress();

                    if (message.hasClientPort() && message.hasClientIp()) {
                        packetPort = message.getClientPort();
                        address = InetAddress.getByName(message.getClientIp());
                    }

                     */

                    if (kvResponse != null) {
                        // build checksum and response message
                        long checksum = Server.buildChecksum(message.getMessageID(), kvResponse);
                        byte[] resMessage = Server.buildMessage(message.getMessageID(), kvResponse, checksum);

                        // load message into packet to send back to client
                        clientOutputSocket.send(new DatagramPacket(
                                resMessage,
                                resMessage.length,
                                InetAddress.getByName(message.getClientIp()),
                                message.getClientPort()
                        ));
                    }
                }

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();
    }

    private static class PairComparator implements Comparator<Message.Msg> {
        public int compare(Message.Msg msg1, Message.Msg msg2) {
            return (int) (msg1.getTimestamp() - msg2.getTimestamp());
        }
    }
}