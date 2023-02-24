package com.g2.CPEN431.A7.store;

import com.google.protobuf.ByteString;

import java.util.Objects;

public class StoreKey {
    private final int version;
    private final ByteString key;

    public StoreKey(ByteString key, int version) {
        this.key = key;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public ByteString getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreKey storeKey = (StoreKey) o;
        return version == storeKey.version && key.equals(storeKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, key);
    }
}
