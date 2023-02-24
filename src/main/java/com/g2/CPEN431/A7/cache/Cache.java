package com.g2.CPEN431.A7.cache;

public interface Cache<K, V> {
    void put(K key, V value);
    V get(K key);
    void invalidateAll();
    boolean isFull();
}
