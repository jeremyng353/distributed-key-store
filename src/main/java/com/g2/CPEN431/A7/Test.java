package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.CRC32;

class Test {
    // put get rem rem get
    public void test() {
        Random random = new Random();
        DatagramSocket socket = null;

        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 5; i++) {
            int port = random.nextInt(20) + 4445;
            int command;
            if (i == 0) {
                command = 1;
            } else if (i == 1 || i == 4) {
                command = 2;
            } else {
                command = 3;
            }

            byte[] key = {0};
            byte[] value = {1};
            ByteString bsKey = ByteString.copyFrom(key);
            ByteString bsValue = ByteString.copyFrom(value);

            KeyValueRequest.KVRequest req = KeyValueRequest.KVRequest.newBuilder()
                    .setCommand(command)
                    .setKey(bsKey)
                    .setValue(bsValue)
                    .setVersion(0)
                    .build();

            byte[] id = {(byte) i};

            ByteString bsId = ByteString.copyFrom(id);

            ByteString payload = req.toByteString();
            Message.Msg msg = Message.Msg.newBuilder()
                    .setPayload(payload)
                    .setMessageID(bsId)
                    .setCheckSum(buildChecksum(bsId, payload))
                    .build();

            byte[] arrMsg = msg.toByteArray();

            try {
                DatagramPacket packet = new DatagramPacket(arrMsg, arrMsg.length, InetAddress.getByName("localhost"), port);
                socket.send(packet);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public long buildChecksum(ByteString messageID, ByteString payload) {
        CRC32 checksum = new CRC32();
        ByteBuffer buf = ByteBuffer.allocate(messageID.size() + payload.size());

        buf.put(messageID.toByteArray());
        buf.put(payload.toByteArray());
        checksum.update(buf.array());

        return checksum.getValue();
    }
}
