package com.coordinator.lock;

import com.coordinator.redisclient.RedisCoordinationClient;

import java.util.UUID;

/**
 * Per-entity distributed lock manager, built on whatever
 * {@link RedisCoordinationClient} implementation is wired in (real Redis
 * via Jedis, or the in-memory stand-in). See DESIGN.md for the full
 * argument; the short version:
 *
 * <p>This class gives ADVISORY mutual exclusion only: {@code setIfAbsent}
 * to acquire, {@code compareAndDelete} to release safely (a worker can
 * never release a lock it no longer owns), {@code compareAndExpire} to
 * renew.
 *
 * <p>It also hands out a fencing token from a monotonic counter that is
 * entirely independent of the lock's own identity and TTL. That token is
 * the actual safety mechanism: it lets the protected resource (see
 * {@code ProtectedResource}) reject a write from a worker whose lock
 * expired and was reacquired by someone else, even though that worker
 * has no way of knowing this happened to it.
 *
 * <p>Treat "I hold the lock" as "I have permission to attempt a write,
 * tagged with proof of how fresh that permission is" -- never as a
 * correctness guarantee by itself.
 */
public class RedisLock {

    private static final String LOCK_KEY_PREFIX = "lock:";
    private static final String FENCE_KEY_PREFIX = "fence:";

    private final RedisCoordinationClient client;

    public RedisLock(RedisCoordinationClient client) {
        this.client = client;
    }

    private static String lockKey(String entityKey) {
        return LOCK_KEY_PREFIX + entityKey;
    }

    private static String fenceKey(String entityKey) {
        return FENCE_KEY_PREFIX + entityKey;
    }

    /**
     * Single, non-blocking acquisition attempt. Returns null if someone
     * else currently holds the lock on this entity.
     */
    public AcquiredLock tryAcquire(String entityKey, long ttlMillis) {
        String owner = UUID.randomUUID().toString();
        boolean acquired = client.setIfAbsent(lockKey(entityKey), owner, ttlMillis);
        if (!acquired) {
            return null;
        }
        // Independent of the lock key/TTL above -- never reset, never
        // reused, for the life of this entity, no matter how many times
        // the lock itself is acquired, expires, or gets stolen.
        long fencingToken = client.incrementAndGet(fenceKey(entityKey));
        return new AcquiredLock(this, entityKey, owner, fencingToken, ttlMillis);
    }

    /**
     * Blocking acquire: polls until the lock is obtained or
     * {@code timeoutMillis} elapses, then throws.
     *
     * <p>Polling is a stated simplification -- see DESIGN.md
     * "production" for what would replace it at real scale (keyspace
     * notifications / a wait list) so contention doesn't spin CPU and
     * network.
     */
    public AcquiredLock acquire(String entityKey, long ttlMillis, long retryIntervalMillis, long timeoutMillis) {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (true) {
            AcquiredLock lock = tryAcquire(entityKey, ttlMillis);
            if (lock != null) {
                return lock;
            }
            if (System.nanoTime() >= deadline) {
                throw new LockNotAcquiredException(
                        "could not acquire lock for '" + entityKey + "' within " + timeoutMillis + "ms");
            }
            sleepQuietly(retryIntervalMillis);
        }
    }

    boolean release(AcquiredLock lock) {
        return client.compareAndDelete(lockKey(lock.entityKey()), lock.ownerToken());
    }

    boolean extend(AcquiredLock lock, long newTtlMillis) {
        return client.compareAndExpire(lockKey(lock.entityKey()), lock.ownerToken(), newTtlMillis);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
