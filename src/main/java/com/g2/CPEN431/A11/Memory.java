package com.g2.CPEN431.A11;

import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import static com.g2.CPEN431.A11.MemberMonitor.DEFAULT_INTERVAL;

public class Memory {

    // Memory class constant values
    // Response codes
    private static final int SUCCESS = 0x00;
    private static final int NO_KEY_ERR = 0x01;
    private static final int NO_MEM_ERR = 0x02;
    private static final int BAD_KEY_ERR = 0x06;
    private static final int BAD_VALUE_ERR = 0x07;

    // Memory buffer to look for out of memory error
    private static final int MIN_MEMORY_BUFFER = 2250000;   // 2.25 mb buffer

    // Max key and value sizes
    private static final int MAX_KEY_SIZE = 32;
    private static final int MAX_VALUE_SIZE = 10000;

    // Memory store
    private static final ConcurrentHashMap<ByteString, Pair<ByteString, Integer>> store = new ConcurrentHashMap<>();

    private Cache<ByteString, ArrayList<Message.Msg>> keyLocks;

    public Memory(Server server, UDPClient udpClient) {
        keyLocks = CacheBuilder.newBuilder()
                .expireAfterWrite(5, TimeUnit.MINUTES) // set expiration time to 5 minutes
                .removalListener(new RemovalListener<ByteString, ArrayList<Message.Msg>>() {
                    public void onRemoval(RemovalNotification<ByteString, ArrayList<Message.Msg>> notification) {
                        ArrayList<Message.Msg> storedMessages = notification.getValue();

                        for (Message.Msg msg : storedMessages) {
                            try {
                                ByteString response = server.exeCommand(msg);
                                if (response != null) {
                                    udpClient.sendClientResponse(response.toByteArray(), msg.getClientIp(), msg.getClientPort());
                                }
                            } catch (InvalidProtocolBufferException | UnknownHostException e) {
                                throw new RuntimeException(e);
                            }

                        }

                    }
                })
                .build();
    }

    /**
     * This function puts a key value pair into the memory store
     * @param key: ByteString key associated with the key value pair
     * @param value: ByteString value associated with the key value pair
     * @param version: Integer version value associated with the key value pair
     * @return An Integer response code depending on the operations outcome
     */
    public int put(ByteString key, ByteString value, int version) {
        // check memory is sufficient for a put operation
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long totalFree = Runtime.getRuntime().maxMemory() - used;
        if (totalFree < MIN_MEMORY_BUFFER) return NO_MEM_ERR;

        // check key and value sizes
        if (key.size() > MAX_KEY_SIZE) return BAD_KEY_ERR;
        if (value.size() > MAX_VALUE_SIZE) return BAD_VALUE_ERR;

        Pair<ByteString, Integer> values = new Pair<>(value, version);
        store.put(key, values);
        return SUCCESS;
    }

    /**
     * This function checks the memory if a key is stored in it
     * @param key: ByteString key to check for
     * @return An Integer response code depending on the operations outcome
     */
    public int isStored(ByteString key) {
        // check key size
        if (key.size() > MAX_KEY_SIZE || key.size() == 0) return BAD_KEY_ERR;
        if (store.containsKey(key)) return SUCCESS;
        return NO_KEY_ERR;
    }

    /**
     * This function gets the value-version pair associated with the key
     * @param key: ByteString key to get value-version pair for
     * @return Pair containing the value and version associated with the key
     * null is not a possibility of being returned since isStored(key) is called prior to this function
     */
    public Pair<ByteString, Integer> get(ByteString key) {
        if (store.containsKey(key)) {
            return store.get(key);
        }
        return null;
    }

    /**
     * This function gets all entries in memory. This returns a stream in order to optimize
     * memory overhead.
     * @return Stream representing all the entries in memory.
     */
    public Stream<Map.Entry<ByteString, Pair<ByteString, Integer>>> getAllEntries() {
        return store.entrySet().parallelStream();
    }

    /**
     * This function removes the key value pair associated with given key
     * @param key: ByteString key to remove the key value pair for
     * @return An Integer response code depending on the operations outcome
     */
    public int remove(ByteString key) {
        // check key size
        if (key.size() > MAX_KEY_SIZE) return BAD_KEY_ERR;
        if (store.containsKey(key)) {
            store.remove(key);
            return SUCCESS;
        }
        return NO_KEY_ERR;
    }

    /**
     * This function clears the memory store and the cache
     * @return An Integer response code depending on the operations outcome
     */
    public int erase() {
        store.clear();
        RequestCache.erase();
        return SUCCESS;
    }

    /**
     * This function closes the server
     * @return An Integer response code depending on the operations outcome
     */
    public int shutdown() {
        System.exit(SUCCESS);
        return SUCCESS;
    }

    public void lockKey(ByteString key, Server server, UDPClient udpClient) {
        keyLocks.put(key, new ArrayList<>());
    }

    public void removeLock(ByteString key) {
        keyLocks.invalidate(key);
    }

    public void storeMessage(ByteString key, Message.Msg msg) {
        keyLocks.getIfPresent(key).add(msg);
    }

    public ArrayList<Message.Msg> getStoredMessages(ByteString key) {
        return keyLocks.getIfPresent(key);
    }
}
