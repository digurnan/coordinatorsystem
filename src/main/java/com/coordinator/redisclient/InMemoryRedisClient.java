package com.coordinator.redisclient;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process stand-in for the coordination service, explicitly permitted
 * by the assignment ("You may use libraries for the coordination
 * primitive itself (a Redis client, an etcd client, an in-memory
 * stand-in)"). Used so the core algorithm (RedisLock + ProtectedResource
 * + fencing) can be verified end-to-end with zero external dependencies
 * and no running Redis server -- see README for why that mattered in the
 * environment this project was built in.
 *
 * Semantics are written to match real Redis for the operations this
 * project actually uses: NX+PX set, compare-and-delete, compare-and-
 * expire, and INCR. Expiry is lazy (checked on access), which matches
 * Redis's *observable* behaviour closely enough for this purpose, even
 * though real Redis also does active background expiry.
 */
public class InMemoryRedisClient implements RedisCoordinationClient {

    private static final class Entry {
        final String value;
        final long expiresAtNanos; // absolute deadline, System.nanoTime() clock

        Entry(String value, long expiresAtNanos) {
            this.value = value;
            this.expiresAtNanos = expiresAtNanos;
        }

        boolean isExpired(long nowNanos) {
            return nowNanos >= expiresAtNanos;
        }
    }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    // A single global mutex is deliberately simple: this class exists to
    // verify correctness of the algorithm above it, not to be a
    // performant Redis replacement. Real Redis gets its atomicity from
    // being single-threaded per command, not from fine-grained locking
    // either.
    private final Object mutex = new Object();

    @Override
    public boolean setIfAbsent(String key, String value, long ttlMillis) {
        long now = System.nanoTime();
        synchronized (mutex) {
            Entry existing = store.get(key);
            if (existing != null && !existing.isExpired(now)) {
                return false;
            }
            store.put(key, new Entry(value, now + ttlMillis * 1_000_000L));
            return true;
        }
    }

    @Override
    public boolean compareAndDelete(String key, String expectedValue) {
        long now = System.nanoTime();
        synchronized (mutex) {
            Entry existing = store.get(key);
            if (existing == null || existing.isExpired(now) || !existing.value.equals(expectedValue)) {
                return false;
            }
            store.remove(key);
            return true;
        }
    }

    @Override
    public boolean compareAndExpire(String key, String expectedValue, long ttlMillis) {
        long now = System.nanoTime();
        synchronized (mutex) {
            Entry existing = store.get(key);
            if (existing == null || existing.isExpired(now) || !existing.value.equals(expectedValue)) {
                return false;
            }
            store.put(key, new Entry(existing.value, now + ttlMillis * 1_000_000L));
            return true;
        }
    }

    @Override
    public long incrementAndGet(String key) {
        return counters.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    @Override
    public String get(String key) {
        long now = System.nanoTime();
        Entry existing = store.get(key);
        if (existing == null || existing.isExpired(now)) {
            return null;
        }
        return existing.value;
    }
}
