package com.g2.CPEN431.A7.store;

import com.google.protobuf.ByteString;

public interface Store {
    void put(ByteString key, int version, ByteString value) throws OutOfMemoryError;
    ByteString get(ByteString key, int version);
    ByteString remove(ByteString key, int version);
    void wipe();
    Integer getLatestVersion(ByteString key);
}
