package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.*;

public class App
{
    public static final int MAX_INCOMING_PACKET_SIZE = 16 * 1024;   // 16 kilobyte buffer to receive packets

    public static void main( String[] args ) throws IOException {

        DatagramSocket socket = new DatagramSocket(4445);
        byte[] buf = new byte[MAX_INCOMING_PACKET_SIZE];

        // print listening port to console
        int localPort = socket.getLocalPort();
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        System.out.println("Server is Listening at " + localAddress + " on port " + localPort + "...");

        while(true) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // listen for next packet
                socket.receive(packet);

                Message.Msg message = Server.readRequest(packet);

                ByteString kvResponse;
                // if message cached retrieved cached response otherwise execute command
                if (RequestCache.isStored(message.getMessageID())) {
                    kvResponse = RequestCache.get(message.getMessageID());
                } else {
                    kvResponse = Server.exeCommand(message);
                }

                // build checksum and response message
                long checksum = Server.buildChecksum(message.getMessageID(), kvResponse);
                byte[] resMessage = Server.buildMessage(message.getMessageID(), kvResponse, checksum);

                // load message into packet to send back to client
                int port = packet.getPort();
                InetAddress address = packet.getAddress();
                packet = new DatagramPacket(resMessage, resMessage.length, address, port);

                socket.send(packet);
            } catch (PacketCorruptionException e) {
                System.out.println("the packet is corrupt");
            }
        }
    }
}
