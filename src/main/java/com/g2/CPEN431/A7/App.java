package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;

public class App
{
    public static final int MAX_INCOMING_PACKET_SIZE = 16 * 1024;   // 16 kilobyte buffer to receive packets

    // List of node IP:port
    // public static ArrayList<AddressPair> nodeList = new ArrayList<>();

    public static void main( String[] args ) throws IOException {
        // multiple nodes on one ec2 instance --> create multiple sockets, do in another branch
        int port = Integer.parseInt(args[0]);
        DatagramSocket socket = new DatagramSocket(port);
        byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];

        // TODO: add nodes to consistentHash, maybe hardcode in a txt file?
        ConsistentHash consistentHash = new ConsistentHash(port);

        File nodeList = new File("nodes.txt");
        Scanner myReader = new Scanner(nodeList);
        while (myReader.hasNextLine()) {
            String ip = myReader.nextLine();
            String nodePort = myReader.nextLine();
            consistentHash.addNode(new AddressPair(ip, Integer.parseInt(nodePort)));
        }

        //create a thread to monitor the other servers in the system
        MemberMonitor memberMonitor = new MemberMonitor(new ArrayList<>());
        Thread monitorThread = new Thread(memberMonitor);
        monitorThread.start();

        // print listening port to console
        int localPort = socket.getLocalPort();
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        System.out.println("Server is Listening at " + localAddress + " on port " + localPort + "...");

        Server server = new Server(port, consistentHash, memberMonitor);

        while(true) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // listen for next packet
                socket.receive(packet);
                // System.out.println("Node " + port + " received a packet from node " + packet.getPort());

                Message.Msg message = Server.readRequest(packet);

                Object kvResponse;
                // if message cached retrieved cached response otherwise execute command
                if (RequestCache.isStored(message.getMessageID())) {
                    kvResponse = RequestCache.get(message.getMessageID());
                } else {
                    kvResponse = server.exeCommand(message, packet);
                }

                int packetPort = packet.getPort();
                InetAddress address = packet.getAddress();

                if (kvResponse instanceof DatagramPacket) {
                    packet = new DatagramPacket(((DatagramPacket) kvResponse).getData(), ((DatagramPacket) kvResponse).getLength(), address, packetPort);
                } else {
                    // build checksum and response message
                    long checksum = Server.buildChecksum(message.getMessageID(), (ByteString) kvResponse);
                    byte[] resMessage = Server.buildMessage(message.getMessageID(), (ByteString) kvResponse, checksum);

                    // load message into packet to send back to client
                    packet = new DatagramPacket(resMessage, resMessage.length, address, packetPort);
                    // System.out.println("not instance of datagrampacket");
                }

                // System.out.println("Node " + port + " is sending the packet now");
                socket.send(packet);
            } catch (PacketCorruptionException e) {
                System.out.println("the packet is corrupt");
            }
        }
    }
}
