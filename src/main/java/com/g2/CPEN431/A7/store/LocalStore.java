package com.g2.CPEN431.A7.store;

import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public class LocalStore implements Store {

    private final Map<StoreKey, ByteString> store;
    private final Map<ByteString, Integer> versionStore;

    public LocalStore() {
        this.store = new HashMap<>();
        this.versionStore = new HashMap<>();
    }

    @Override
    public void put(ByteString key, int version, ByteString value) throws OutOfMemoryError {
        StoreKey storeKey = new StoreKey(key, version);
        store.put(storeKey, value);
        versionStore.put(key, version);
    }

    /**
     *
     * @param key
     * @param version THIS IS IGNORED
     * @return
     */
    @Override
    public ByteString get(ByteString key, int version) {
        // Right now, we just always return the latest version regardless of what version is actually specified
        int realVersion = getLatestVersion(key);
        StoreKey storeKey = new StoreKey(key, realVersion);
        return store.get(storeKey);
    }

    /**
     *
     * @param key
     * @param version THIS IS IGNORED
     * @return
     */
    @Override
    public ByteString remove(ByteString key, int version) {
        Integer realVersion = getLatestVersion(key);
        if (realVersion == null) {
            return null;
        }

        StoreKey storeKey = new StoreKey(key, realVersion);
        versionStore.remove(key);
        return store.remove(storeKey);
    }

    @Override
    public void wipe() {
        store.clear();
        versionStore.clear();
    }

    /**
     *
     * @param key
     * @return the latest version of the object, null if the key doesn't exist
     */
    @Override
    public Integer getLatestVersion(ByteString key) {
        return versionStore.get(key);
    }
}
