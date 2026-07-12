package com.coordinator.redisclient;

import com.coordinator.lock.AcquiredLock;
import com.coordinator.lock.RedisLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the exact same RedisLock logic as {@code RedisLockTest}, but
 * against a real Redis instance via Jedis, to confirm the in-memory
 * stand-in's semantics actually match Redis for the operations this
 * project depends on.
 *
 * <p>Skipped unless REDIS_HOST is set. Run:
 * <pre>
 *   docker-compose up -d
 *   REDIS_HOST=localhost mvn test
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "REDIS_HOST", matches = ".+")
class JedisRedisClientIntegrationTest {

    private static JedisRedisClient client;

    @BeforeAll
    static void connect() {
        String host = System.getenv("REDIS_HOST");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        client = new JedisRedisClient(host, port);
    }

    @AfterAll
    static void disconnect() {
        if (client != null) client.close();
    }

    @Test
    void mutualExclusionAgainstRealRedis() {
        RedisLock lockManager = new RedisLock(client);
        String key = "it-acct-" + System.nanoTime();
        AcquiredLock l1 = lockManager.tryAcquire(key, 2000);
        AcquiredLock l2 = lockManager.tryAcquire(key, 2000);
        assertNotNull(l1);
        assertNull(l2);
        assertTrue(l1.release());
    }

    @Test
    void fencingTokenIncreasesAgainstRealRedis() {
        RedisLock lockManager = new RedisLock(client);
        String key = "it-acct-fence-" + System.nanoTime();
        AcquiredLock l1 = lockManager.tryAcquire(key, 2000);
        long t1 = l1.fencingToken();
        l1.release();
        AcquiredLock l2 = lockManager.tryAcquire(key, 2000);
        assertTrue(l2.fencingToken() > t1);
        l2.release();
    }
}
