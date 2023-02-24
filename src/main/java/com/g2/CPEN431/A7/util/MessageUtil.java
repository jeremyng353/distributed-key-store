package com.g2.CPEN431.A7.util;

import ca.NetSysLab.ProtocolBuffers.Message;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

public class MessageUtil {
    public static long computeChecksum(byte[] buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        return crc32.getValue();
    }

    public static byte[] concatenateByteArrays(byte[]... byteArrays) {
        int byteArrayLength = Arrays.stream(byteArrays)
                .map(byteArray -> byteArray.length)
                .reduce(0, Integer::sum);
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteArrayLength);
        Arrays.stream(byteArrays)
                .forEach(byteBuffer::put);
        return byteBuffer.array();
    }

    public static boolean validateChecksum(Message.Msg message) {
        byte[] receivedUUID = message.getMessageID().toByteArray();
        byte[] payloadByteArray = message.getPayload().toByteArray();
        byte[] expectedChecksumByteArray = concatenateByteArrays(receivedUUID, payloadByteArray);
        long expectedChecksum = computeChecksum(expectedChecksumByteArray);
        long receivedChecksum = message.getCheckSum();

        return expectedChecksum == receivedChecksum;
    }
}
