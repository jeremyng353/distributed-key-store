package com.g2.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import static com.g2.CPEN431.A11.MemberMonitor.DEFAULT_INTERVAL;

public class App
{
    public static final int MAX_INCOMING_PACKET_SIZE = 16 * 1024;   // 16 kilobyte buffer to receive packets

    // List of node IP:port
    // public static ArrayList<AddressPair> nodeList = new ArrayList<>();

    public static void main( String[] args ) throws IOException {
        String currentIp = args[0];
        int port = Integer.parseInt(args[1]);
        DatagramSocket socket = new DatagramSocket(port);
        byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];

        ConsistentHash consistentHash = new ConsistentHash(currentIp, port);

        // read list of nodes from nodes.txt
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

        // create a thread to monitor the other servers in the system
        MemberMonitor memberMonitor = new MemberMonitor(initialNodes, new AddressPair(currentIp, port), consistentHash);
        TimerTask pullEpidemic = new TimerTask() {
            @Override
            public void run() {
                memberMonitor.run();
            }
        };
        Timer timer = new Timer("Send Timer");
        timer.scheduleAtFixedRate(pullEpidemic, DEFAULT_INTERVAL, DEFAULT_INTERVAL);

        // print listening port to console
        int localPort = socket.getLocalPort();
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        System.out.println("Server is Listening at " + localAddress + " on port " + localPort + "...");

        Server server = new Server(port, consistentHash, memberMonitor);

        while (true) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // listen for next packet
                socket.receive(packet);
                Message.Msg message = Server.readRequest(packet);

                // set clientIp, clientPort so every message has the values set
                if (!message.hasClientIp() || !message.hasClientPort()) {
                    message = Message.Msg.newBuilder(message)
                            .setClientIp(message.hasClientIp() ? message.getClientIp() : packet.getAddress().getHostAddress())
                            .setClientPort(message.hasClientPort() ? message.getClientPort() : packet.getPort())
                            .build();
                }

                ByteString kvResponse;
                // if message cached retrieved cached response otherwise execute command
                if (RequestCache.isStored(message.getMessageID())) {
                    kvResponse = RequestCache.get(message.getMessageID());
                } else {
                    kvResponse = server.exeCommand(message);
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
                    socket.send(packet);
                }

            } catch (PacketCorruptionException e) {
                System.out.println("the packet is corrupt");
            }
        }
    }
}
