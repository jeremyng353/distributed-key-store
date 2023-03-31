package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

        LinkedBlockingQueue<DatagramPacket> packetQueue = new LinkedBlockingQueue<>();


        // Thread to receive all incoming packets
        new Thread(() -> {
            try {
                while (true) {
                    byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    inputSocket.receive(packet);
                    packetQueue.put(packet);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();

        // Thread to process packets and send responses to client
        new Thread(() -> {
            try {
                // socket for sending response to clients
                DatagramSocket clientOutputSocket = new DatagramSocket();

                while (true) {
                    DatagramPacket parsePacket = packetQueue.take();

                    Message.Msg message = Server.readRequest(parsePacket);

                    ByteString kvResponse;
                    // if message cached retrieved cached response otherwise execute command
                    if (RequestCache.isStored(message.getMessageID())) {
                        kvResponse = RequestCache.get(message.getMessageID());
                    } else {
                        kvResponse = server.exeCommand(message, parsePacket);
                    }

                    int packetPort = parsePacket.getPort();
                    InetAddress address = parsePacket.getAddress();

                    if (message.hasClientPort() && message.hasClientIp()) {
                        packetPort = message.getClientPort();
                        address = InetAddress.getByName(message.getClientIp());
                    }

                    if (kvResponse != null) {
                        // build checksum and response message
                        long checksum = Server.buildChecksum(message.getMessageID(), kvResponse);
                        byte[] resMessage = Server.buildMessage(message.getMessageID(), kvResponse, checksum);

                        // load message into packet to send back to client
                        parsePacket = new DatagramPacket(resMessage, resMessage.length, address, packetPort);
                        clientOutputSocket.send(parsePacket);
                    }
                }

            } catch (InterruptedException | PacketCorruptionException | IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }).start();



        /*

        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // listen for next packet
                inputSocket.receive(packet);
                Message.Msg message = Server.readRequest(packet);

                ByteString kvResponse;
                // if message cached retrieved cached response otherwise execute command
                if (RequestCache.isStored(message.getMessageID())) {
                    kvResponse = RequestCache.get(message.getMessageID());
                } else {
                    kvResponse = server.exeCommand(message, packet);
                }

                int packetPort = packet.getPort();
                InetAddress address = packet.getAddress();

                if (message.hasClientPort() && message.hasClientIp()) {

                    packetPort = message.getClientPort();
                    address = InetAddress.getByName(message.getClientIp());
                }

                if (kvResponse != null) {
                    // build checksum and response message
                    long checksum = Server.buildChecksum(message.getMessageID(), kvResponse);
                    byte[] resMessage = Server.buildMessage(message.getMessageID(), kvResponse, checksum);

                    // load message into packet to send back to client
                    packet = new DatagramPacket(resMessage, resMessage.length, address, packetPort);
                    inputSocket.send(packet);
                }

            } catch (PacketCorruptionException e) {
                System.out.println("the packet is corrupt");
            }
        }

         */
    }
}
