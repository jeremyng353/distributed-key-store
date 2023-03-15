package com.g2.CPEN431.A9;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;

import java.util.concurrent.TimeUnit;

public class RequestCache {
    private final long ENTRY_TIMEOUT = 1000; // 1 seconds
    private final Cache<ByteString, ByteString> cache;

    public RequestCache() {
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess(ENTRY_TIMEOUT, TimeUnit.MILLISECONDS)
                .build();
    }

    /**
     * This function puts a messageID response pair into the cache for retries
     * @param key: This key represents the messageID of an incoming packet
     * @param response: The response to be 'cached' for possible retries
     */
    public void put(ByteString key, ByteString response) {
        cache.put(key, response);
    }

    /**
     * This function checks the cache to see if the provided messageID is stored
     * @param key: This key represents the messageID of an incoming packet
     * @return A boolean for whether the key is stored or not
     */
    public boolean isStored(ByteString key) {
        return cache.asMap().containsKey(key);
    }

    /**
     * This function gets the response store for a messageID
     * @param key: This key represents the messageID of an incoming packet
     * @return A ByteString representing the response previously sent
     * null cannot be returned since isStored() is run prior to this function
     */
    public ByteString get(ByteString key) {
        return cache.getIfPresent(key);
    }

    /**
     * This function removes all the mappings in the cache and garbage collects
     */
    public void erase() {
        cache.invalidateAll();
        cache.cleanUp();
        Runtime.getRuntime().gc();
    }
}
