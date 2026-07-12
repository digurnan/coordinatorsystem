package com.coordinator.redisclient;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.Collections;

/**
 * Real coordination service: a single Redis instance, reached via Jedis.
 *
 * Stated assumption (see DESIGN.md): the coordination service is one
 * non-clustered Redis instance, not a Redis Cluster and not a multi-node
 * Redlock deployment. That is the simplest thing that matches the
 * inherited design note's "we've been running this for months and it's
 * basically fine" -- and the interesting judgment calls in this exercise
 * are about what a TTL lock does and doesn't guarantee, not about
 * building multi-node consensus (we're explicitly told not to
 * reimplement Raft). A single instance is also a single point of
 * failure; that tradeoff is named, not hidden -- see "production" in
 * DESIGN.md.
 *
 * JedisPooled (not a bare Jedis) is used because the simulation harness
 * drives this from many concurrent worker threads, and a single Jedis
 * connection is not thread-safe.
 */
public class JedisRedisClient implements RedisCoordinationClient, AutoCloseable {

    // GET-then-DEL, atomically: a worker may only delete the lock key if
    // it still holds it. Prevents releasing a lock that already expired
    // and was reacquired by someone else.
    private static final String COMPARE_AND_DELETE_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  return redis.call('del', KEYS[1]) "
                    + "else "
                    + "  return 0 "
                    + "end";

    // GET-then-PEXPIRE, atomically: only the current owner may renew.
    private static final String COMPARE_AND_EXPIRE_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  return redis.call('pexpire', KEYS[1], ARGV[2]) "
                    + "else "
                    + "  return 0 "
                    + "end";

    private final JedisPooled jedis;

    public JedisRedisClient(String host, int port) {
        this.jedis = new JedisPooled(host, port);
    }

    public JedisRedisClient(String connectionUri) {
        this.jedis = new JedisPooled(connectionUri);
    }

    @Override
    public boolean setIfAbsent(String key, String value, long ttlMillis) {
        SetParams params = SetParams.setParams().nx().px(ttlMillis);
        String result = jedis.set(key, value, params);
        return "OK".equals(result);
    }

    @Override
    public boolean compareAndDelete(String key, String expectedValue) {
        Object result = jedis.eval(
                COMPARE_AND_DELETE_SCRIPT,
                Collections.singletonList(key),
                Collections.singletonList(expectedValue));
        return asBoolean(result);
    }

    @Override
    public boolean compareAndExpire(String key, String expectedValue, long ttlMillis) {
        Object result = jedis.eval(
                COMPARE_AND_EXPIRE_SCRIPT,
                Collections.singletonList(key),
                Arrays.asList(expectedValue, Long.toString(ttlMillis)));
        return asBoolean(result);
    }

    @Override
    public long incrementAndGet(String key) {
        return jedis.incr(key);
    }

    @Override
    public String get(String key) {
        return jedis.get(key);
    }

    private static boolean asBoolean(Object luaResult) {
        return (luaResult instanceof Long) && (Long) luaResult != 0L;
    }

    @Override
    public void close() {
        jedis.close();
    }
}
