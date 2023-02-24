package com.g2.CPEN431.A7;

import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.protobuf.ByteString;
import com.g2.CPEN431.A7.application.KeyValueStoreApplication;
import com.g2.CPEN431.A7.cache.Cache;
import com.g2.CPEN431.A7.cache.GuavaCache;
import com.g2.CPEN431.A7.store.LocalStore;
import com.g2.CPEN431.A7.store.Store;

import java.net.SocketException;

public class A7 {
    public static void main(String[] args) throws SocketException {
        Store store = new LocalStore();
        Cache<ByteString, Message.Msg> cache = new GuavaCache<>(5000, 5000);
        KeyValueStoreApplication application = new KeyValueStoreApplication(store, cache);
        Server server = new Server(application, cache,8001);
        server.serve();
    }
}
