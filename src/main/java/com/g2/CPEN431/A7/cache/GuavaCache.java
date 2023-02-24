package com.g2.CPEN431.A7.cache;

import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class GuavaCache<K, V> implements Cache<K, V> {

    private final int maxSize;
    private final com.google.common.cache.Cache<K, V> cache;

    public GuavaCache(int maxSize, long timeout) {
        this.maxSize = maxSize;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(timeout, TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public void put(K key, V value) {
        this.cache.put(key, value);
    }

    @Override
    public V get(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        Runtime.getRuntime().gc();
    }

    @Override
    public boolean isFull() {
        return cache.size() >= maxSize;
    }
}
