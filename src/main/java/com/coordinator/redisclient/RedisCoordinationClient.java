package com.coordinator.redisclient;

/**
 * The exact, minimal set of atomic primitives RedisLock needs from
 * "a coordination service" (the assignment: "you can assume something
 * Redis-like, or etcd/ZooKeeper-like; your choice, state your
 * assumption").
 *
 * Keeping this as a narrow interface, rather than coding RedisLock
 * directly against a Jedis client, buys two things:
 *
 *   1. RedisLock's logic is testable without a running Redis server, via
 *      {@link InMemoryRedisClient} -- the assignment explicitly allows an
 *      "in-memory stand-in" for the coordination primitive itself.
 *   2. It makes explicit exactly which primitives the safety argument in
 *      DESIGN.md depends on: atomic conditional-set-with-expiry, atomic
 *      conditional-delete, atomic conditional-extend, and a monotonic
 *      counter. Nothing else. If you swapped this for etcd or ZooKeeper,
 *      these are the four operations you'd need to reproduce.
 */
public interface RedisCoordinationClient {

    /**
     * Atomically: if {@code key} does not currently exist (or has
     * expired), set it to {@code value} with the given TTL and return
     * true. Otherwise do nothing and return false.
     * Equivalent to Redis: SET key value NX PX ttlMillis
     */
    boolean setIfAbsent(String key, String value, long ttlMillis);

    /**
     * Atomically: if the current value stored at {@code key} equals
     * {@code expectedValue}, delete it and return true. Otherwise do
     * nothing and return false.
     * Equivalent to a Redis EVAL of a GET-then-DEL Lua script -- this
     * must be atomic so a worker can never delete a lock it no longer
     * owns (the classic bug in a naive "just DEL on release" design).
     */
    boolean compareAndDelete(String key, String expectedValue);

    /**
     * Atomically: if the current value stored at {@code key} equals
     * {@code expectedValue}, reset its TTL to {@code ttlMillis} and
     * return true. Otherwise do nothing and return false. Used for lock
     * renewal / heartbeating.
     */
    boolean compareAndExpire(String key, String expectedValue, long ttlMillis);

    /**
     * Atomically increment and return the counter stored at {@code key},
     * implicitly starting at 0 if absent. This is the fencing-token
     * source: for a given key it must never go backwards or repeat, for
     * as long as that entity exists, regardless of how many times the
     * lock on that entity has been acquired, expired, or stolen.
     */
    long incrementAndGet(String key);

    /**
     * Current raw value at {@code key}, or null. Debug/inspection only --
     * never used to make a correctness decision.
     */
    String get(String key);
}
