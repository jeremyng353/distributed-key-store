package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g2.CPEN431.A7.application.ServerApplication;
import com.g2.CPEN431.A7.cache.Cache;
import com.g2.CPEN431.A7.util.MessageUtil;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class Server {
    public static final int MAX_PACKET_LENGTH = 16384; // 16 kB

    private final ServerApplication application;
    private final Cache<ByteString, Message.Msg> cache;
    private final DatagramSocket socket;

    public Server(ServerApplication application, Cache<ByteString, Message.Msg> cache, int port) throws SocketException {
        this.application = application;
        this.cache = cache;
        this.socket = new DatagramSocket(port);
    }

    public void serve() {
        System.out.println("Server is starting on port " + socket.getPort());
        System.out.println("Server is starting with max heap size: " + Runtime.getRuntime().maxMemory() + " bytes");
        while (!socket.isClosed()) {
            try {
                byte[] requestBuffer = new byte[MAX_PACKET_LENGTH];
                DatagramPacket request = new DatagramPacket(requestBuffer, MAX_PACKET_LENGTH);
                socket.receive(request);

                byte[] trimmedRequest = new byte[request.getLength()];
                System.arraycopy(requestBuffer, 0, trimmedRequest, 0, request.getLength());
                Message.Msg message = Message.Msg.parseFrom(trimmedRequest);

                if (MessageUtil.validateChecksum(message)) {
                    Message.Msg response = cache.get(message.getMessageID());
                    if (response == null) {
                        response = this.application.handleRequest(message);
                        cache.put(message.getMessageID(), response);
                    }

                    InetAddress clientAddress = request.getAddress();
                    int clientPort = request.getPort();
                    DatagramPacket responsePacket = new DatagramPacket(
                            response.toByteArray(),
                            response.getSerializedSize(),
                            clientAddress,
                            clientPort);
                    socket.send(responsePacket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
