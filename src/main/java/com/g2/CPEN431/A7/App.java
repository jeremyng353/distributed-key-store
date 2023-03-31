package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

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

        // ConsistentHash setup
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

        Comparator<Message.Msg> comparator = Comparator.comparing(Message.Msg::getTimestamp);

        // MemberMonitor setup
        MemberMonitor memberMonitor = new MemberMonitor(initialNodes, new AddressPair(currentIp, port), consistentHash);
        // create a thread to monitor the other servers in the system
        TimerTask pullEpidemic = new TimerTask() {
            @Override
            public void run() {
                memberMonitor.run();
            }
        };
        Timer timer = new Timer("Send Timer");
        timer.scheduleAtFixedRate(pullEpidemic, DEFAULT_INTERVAL, DEFAULT_INTERVAL);

        Server server = new Server(port, consistentHash, memberMonitor);
        PriorityBlockingQueue<Message.Msg> messageQueue = new PriorityBlockingQueue<>(25, comparator);

        // Thread to receive all incoming packets
        new Thread(() -> {
            try {
                DatagramSocket inputSocket = new DatagramSocket(port);
                while (true) {
                    byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    inputSocket.receive(packet);
                    Message.Msg message = Server.readRequest(packet);

                    // ensure that all messages have the optional fields
                    if (!message.hasClientIp() || ! message.hasClientPort() || !message.hasTimestamp()) {
                        message = Message.Msg.newBuilder(message)
                                .setClientIp(message.hasClientIp() ? message.getClientIp() : packet.getAddress().getHostAddress())
                                .setClientPort(message.hasClientPort() ? message.getClientPort() : packet.getPort())
                                .setTimestamp(message.hasTimestamp() ? message.getTimestamp() : System.currentTimeMillis())
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
                    Message.Msg message = messageQueue.take();

                    ByteString kvResponse;
                    // if message cached retrieved cached response otherwise execute command
                    if (RequestCache.isStored(message.getMessageID())) {
                        kvResponse = RequestCache.get(message.getMessageID());
                    } else {
                        kvResponse = server.exeCommand(message);
                    }

                    if (kvResponse != null) {
                        // build checksum and response message
                        long checksum = Server.buildChecksum(message.getMessageID(), kvResponse);
                        byte[] resMessage = Server.buildMessage(message.getMessageID(), kvResponse, checksum);

                        // load message into packet to send back to client
                        DatagramPacket packet = new DatagramPacket(
                                resMessage,
                                resMessage.length,
                                InetAddress.getByName(message.getClientIp()),
                                message.getClientPort()
                        );
                        clientOutputSocket.send(packet);
                    }
                }

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();

        System.out.println("Server is Listening at " + InetAddress.getLocalHost().getHostAddress() + " on port " + port + "...");
    }
}