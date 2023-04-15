package com.g2.CPEN431.A12;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class ServerWorker implements Runnable {

    private final Server server;
    private final DatagramSocket socket;
    private final DatagramPacket packet;

    public ServerWorker(Server server, DatagramSocket socket, DatagramPacket packet) {
        this.server = server;
        this.socket = socket;
        this.packet = packet;
    }

    @Override
    public void run() {
        try {
            Message.Msg message = Server.readRequest(packet);
            Object kvResponse;
            DatagramPacket responsePacket;

            // if message cached retrieved cached response otherwise execute command
            if (RequestCache.isStored(message.getMessageID())) {
                kvResponse = RequestCache.get(message.getMessageID());
            } else {
                kvResponse = server.exeCommand(message, packet);
            }

            int packetPort = packet.getPort();
            InetAddress address = packet.getAddress();

            if (kvResponse instanceof DatagramPacket) {
                responsePacket = new DatagramPacket(((DatagramPacket) kvResponse).getData(), ((DatagramPacket) kvResponse).getLength(), address, packetPort);
            } else {
                // build checksum and response message
                long checksum = Server.buildChecksum(message.getMessageID(), (ByteString) kvResponse);
                byte[] resMessage = Server.buildMessage(message.getMessageID(), (ByteString) kvResponse, checksum);

                // load message into packet to send back to client
                responsePacket = new DatagramPacket(resMessage, resMessage.length, address, packetPort);
                // System.out.println("not instance of datagrampacket");
            }

            // System.out.println("Node " + port + " is sending the packet now");
            socket.send(responsePacket);
        } catch (IOException | PacketCorruptionException e) {
            e.printStackTrace();
        }
    }
}
