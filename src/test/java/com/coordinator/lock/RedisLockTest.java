package com.coordinator.lock;

import com.coordinator.redisclient.InMemoryRedisClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisLockTest {

    @Test
    void secondAcquireFailsWhileFirstHoldsLock() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-1", 2000);
        AcquiredLock l2 = lockManager.tryAcquire("acct-1", 2000);
        assertNotNull(l1);
        assertNull(l2);
    }

    @Test
    void releaseAllowsReacquire() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-2", 2000);
        assertTrue(l1.release());
        assertNotNull(lockManager.tryAcquire("acct-2", 2000));
    }

    @Test
    void ttlExpiryAllowsReacquire() throws InterruptedException {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        lockManager.tryAcquire("acct-3", 100);
        Thread.sleep(200);
        assertNotNull(lockManager.tryAcquire("acct-3", 2000));
    }

    @Test
    void cannotReleaseSomeoneElsesLock() throws InterruptedException {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-4", 100);
        Thread.sleep(200); // l1's TTL expires
        AcquiredLock l2 = lockManager.tryAcquire("acct-4", 2000);
        assertNotNull(l2);

        assertFalse(l1.release(), "a lock that already expired must not release a newer owner's lock");
        assertNull(lockManager.tryAcquire("acct-4", 2000), "l2 must still hold the lock");
    }

    @Test
    void fencingTokenStrictlyIncreasesAcrossReacquires() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-5", 2000);
        long t1 = l1.fencingToken();
        l1.release();
        AcquiredLock l2 = lockManager.tryAcquire("acct-5", 2000);
        assertTrue(l2.fencingToken() > t1);
    }

    @Test
    void blockingAcquireTimesOut() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        lockManager.tryAcquire("acct-6", 5000);
        assertThrows(LockNotAcquiredException.class,
                () -> lockManager.acquire("acct-6", 5000, 20, 100));
    }
}
