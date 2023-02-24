package com.g2.CPEN431.A7.application;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g2.CPEN431.A7.cache.Cache;
import com.g2.CPEN431.A7.store.Store;
import com.g2.CPEN431.A7.util.MessageUtil;

public class KeyValueStoreApplication implements ServerApplication {

    private static final int MAX_KEY_LENGTH = 32;
    private static final int MAX_VALUE_LENGTH = 10000;
    private static final int DEFAULT_OVERLOAD_WAIT_TIME = 100;

    private final Store store;
    private final Cache<ByteString, Message.Msg> cache;

    public KeyValueStoreApplication(Store store, Cache<ByteString, Message.Msg> cache) {
        this.store = store;
        this.cache = cache;
    }

    @Override
    public Message.Msg handleRequest(Message.Msg request) {
        KeyValueResponse.KVResponse kvResponse = null;

        try {
            KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.parseFrom(request.getPayload());
            switch (kvRequest.getCommand()) {
                case CommandConstants.PUT -> kvResponse = handlePut(
                        kvRequest.getKey(),
                        kvRequest.getVersion(),
                        kvRequest.getValue());
                case CommandConstants.GET -> kvResponse = handleGet(kvRequest.getKey(), kvRequest.getVersion());
                case CommandConstants.REMOVE -> kvResponse = handleRemove(kvRequest.getKey(), kvRequest.getVersion());
                case CommandConstants.SHUTDOWN -> System.exit(0);
                case CommandConstants.WIPEOUT -> kvResponse = handleWipeout();
                case CommandConstants.IS_ALIVE -> kvResponse = handleIsAlive();
                default -> kvResponse = handleDefault();
            }
        } catch (Exception e) {
            // General catch all for exceptions, report a KVStore failure
            e.printStackTrace();
            kvResponse = KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.KVSTORE_FAILURE)
                    .build();
        }

        ByteString serializedKvResponse = kvResponse.toByteString();

        // Compute checksum
        byte[] checksumByteArray = MessageUtil.concatenateByteArrays(
                request.getMessageID().toByteArray(),
                serializedKvResponse.toByteArray());
        long checksum = MessageUtil.computeChecksum(checksumByteArray);

        return Message.Msg.newBuilder()
                .setMessageID(request.getMessageID())
                .setCheckSum(checksum)
                .setPayload(serializedKvResponse)
                .build();
    }

    @Override
    public Message.Msg generateAtCapacityError(Message.Msg request) {
        ByteString serializedKvResponse = KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(ErrorCodes.TEMP_SYSTEM_OVERLOAD)
                .setOverloadWaitTime(DEFAULT_OVERLOAD_WAIT_TIME)
                .build()
                .toByteString();

        // Compute checksum
        byte[] checksumByteArray = MessageUtil.concatenateByteArrays(
                request.getMessageID().toByteArray(),
                serializedKvResponse.toByteArray());
        long checksum = MessageUtil.computeChecksum(checksumByteArray);
        return Message.Msg.newBuilder()
                .setMessageID(request.getMessageID())
                .setCheckSum(checksum)
                .setPayload(serializedKvResponse)
                .build();
    }

    private KeyValueResponse.KVResponse handlePut(ByteString key, int version, ByteString value) {
        try {
            if (key.size() > MAX_KEY_LENGTH || key.size() == 0) {
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(ErrorCodes.INVALID_KEY)
                        .build();
            }
            if (value.size() > MAX_VALUE_LENGTH) {
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(ErrorCodes.INVALID_VALUE)
                        .build();
            }
            if ((Runtime.getRuntime().freeMemory()) < (2097152)) {
                return KeyValueResponse.KVResponse.newBuilder()
                        .setErrCode(ErrorCodes.OUT_OF_SPACE)
                        .build();
            }
            store.put(key, version, value);
            return KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.SUCCESS)
                    .setValue(value)
                    .setVersion(version)
                    .build();
        } catch (OutOfMemoryError e) {
            // If we check to make sure that there's enough memory to create a packet, then this should never be a case.
            // But this might change if we create more objects from the server code.
            System.err.println("Ran out of memory while storing! Check implementation.");
            e.printStackTrace();
            return KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.OUT_OF_SPACE)
                    .build();
        }
    }

    private KeyValueResponse.KVResponse handleGet(ByteString key, int version) {
        Integer latestVersion = store.getLatestVersion(key);
        if (latestVersion == null) {
            return KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.NON_EXISTENT_KEY)
                    .build();
        }

        ByteString value = store.get(key, latestVersion);

        KeyValueResponse.KVResponse.Builder kvResponseBuilder = KeyValueResponse.KVResponse.newBuilder();
        if (value != null) {
            kvResponseBuilder.setValue(value);
        }
        return kvResponseBuilder
                .setErrCode(ErrorCodes.SUCCESS)
                .setVersion(latestVersion)
                .build();
    }

    private KeyValueResponse.KVResponse handleRemove(ByteString key, int version) {
        Integer latestVersion = store.getLatestVersion(key);
        if (latestVersion == null) {
            return KeyValueResponse.KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.NON_EXISTENT_KEY)
                    .build();
        }

        ByteString value = store.remove(key, latestVersion);
        KeyValueResponse.KVResponse.Builder kvResponseBuilder = KeyValueResponse.KVResponse.newBuilder();
        if (value != null) {
            kvResponseBuilder.setValue(value);
        }
        return kvResponseBuilder
                .setErrCode(ErrorCodes.SUCCESS)
                .setVersion(latestVersion)
                .build();
    }

    private KeyValueResponse.KVResponse handleWipeout() {
        store.wipe();
        cache.invalidateAll();
        Runtime.getRuntime().gc();
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(ErrorCodes.SUCCESS)
                .build();
    }

    private KeyValueResponse.KVResponse handleIsAlive() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(ErrorCodes.SUCCESS)
                .build();
    }

    private KeyValueResponse.KVResponse handleDefault() {
        return KeyValueResponse.KVResponse.newBuilder()
                .setErrCode(ErrorCodes.UNRECOGNIZED_COMMAND)
                .build();
    }

    private static final class CommandConstants {
        private static final int PUT = 0x01;
        private static final int GET = 0x02;
        private static final int REMOVE = 0x03;
        private static final int SHUTDOWN = 0x04;
        private static final int WIPEOUT = 0x05;
        private static final int IS_ALIVE = 0x06;
    }

    private static final class ErrorCodes {
        private static final int SUCCESS = 0x00;
        private static final int NON_EXISTENT_KEY = 0x01;
        private static final int OUT_OF_SPACE = 0x02;
        private static final int TEMP_SYSTEM_OVERLOAD = 0x03;
        private static final int KVSTORE_FAILURE = 0x04;
        private static final int UNRECOGNIZED_COMMAND = 0x05;
        private static final int INVALID_KEY = 0x06;
        private static final int INVALID_VALUE = 0x07;
    }
}
