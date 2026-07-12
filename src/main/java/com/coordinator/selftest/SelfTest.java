package com.coordinator.selftest;

import com.coordinator.lock.AcquiredLock;
import com.coordinator.lock.LockNotAcquiredException;
import com.coordinator.lock.RedisLock;
import com.coordinator.redisclient.InMemoryRedisClient;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.StaleFencingTokenException;
import com.coordinator.sim.Simulate;

/**
 * Zero-dependency test runner: no JUnit, no Maven, no network required --
 * just the JDK's own javac/java. This exists because the environment
 * this project was originally assembled in had no outbound network
 * access at all (no Maven Central, no way to install or run a real Redis
 * server), so this is what was actually compiled and run to verify the
 * algorithm before it shipped. See README for the full explanation.
 *
 * <p>It exercises the exact same production classes (RedisLock,
 * ProtectedResource, JobRunner, and the Simulate scenarios) as the JUnit
 * suite under {@code src/test} -- only the {@code RedisCoordinationClient}
 * differs ({@link InMemoryRedisClient} here vs. Jedis-backed real Redis
 * via {@code Simulate --redis-host}).
 *
 * <p>Run:
 * <pre>
 *   mkdir -p out
 *   javac -d out $(find src/main/java -name '*.java')
 *   java -cp out com.coordinator.selftest.SelfTest
 * </pre>
 */
public final class SelfTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        check("mutual exclusion: second acquire fails while first holds lock", SelfTest::testMutualExclusion);
        check("release allows re-acquire", SelfTest::testReleaseAllowsReacquire);
        check("TTL expiry allows re-acquire", SelfTest::testTtlExpiryAllowsReacquire);
        check("cannot release someone else's lock (stale owner token)", SelfTest::testCannotReleaseOthersLock);
        check("fencing token strictly increases across re-acquires", SelfTest::testFencingTokenIncreases);
        check("blocking acquire times out with LockNotAcquiredException", SelfTest::testAcquireTimesOut);
        check("resource accepts strictly increasing tokens", SelfTest::testResourceAcceptsIncreasing);
        check("resource rejects stale token", SelfTest::testResourceRejectsStale);
        check("resource rejects exact-token replay", SelfTest::testResourceRejectsReplay);

        check("simulation: baseline (low contention)", () -> Simulate.scenarioBaseline(new InMemoryRedisClient()));
        check("simulation: high contention", () -> Simulate.scenarioHighContention(new InMemoryRedisClient()));
        check("simulation: stalled worker caught by fencing token",
                () -> Simulate.scenarioStalledWorkerCaughtByFencing(new InMemoryRedisClient()));

        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private interface ThrowingBooleanSupplier {
        boolean get() throws Exception;
    }

    private static void check(String name, ThrowingBooleanSupplier test) {
        try {
            boolean ok = test.get();
            if (ok) {
                passed++;
                System.out.println("PASS  " + name);
            } else {
                failed++;
                System.out.println("FAIL  " + name);
            }
        } catch (Exception e) {
            failed++;
            System.out.println("FAIL  " + name + "  (threw " + e + ")");
        }
    }

    static boolean testMutualExclusion() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-1", 2000);
        AcquiredLock l2 = lockManager.tryAcquire("acct-1", 2000);
        return l1 != null && l2 == null;
    }

    static boolean testReleaseAllowsReacquire() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-2", 2000);
        boolean released = l1.release();
        AcquiredLock l2 = lockManager.tryAcquire("acct-2", 2000);
        return released && l2 != null;
    }

    static boolean testTtlExpiryAllowsReacquire() throws InterruptedException {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        lockManager.tryAcquire("acct-3", 100);
        Thread.sleep(200);
        AcquiredLock l2 = lockManager.tryAcquire("acct-3", 2000);
        return l2 != null;
    }

    static boolean testCannotReleaseOthersLock() throws InterruptedException {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-4", 100);
        Thread.sleep(200); // l1 expires
        AcquiredLock l2 = lockManager.tryAcquire("acct-4", 2000);
        boolean l2Acquired = l2 != null;
        boolean l1ReleaseSucceeded = l1.release(); // must be false: l1 no longer owns the key
        AcquiredLock l3 = lockManager.tryAcquire("acct-4", 2000); // must still fail: l2 holds it
        return l2Acquired && !l1ReleaseSucceeded && l3 == null;
    }

    static boolean testFencingTokenIncreases() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        AcquiredLock l1 = lockManager.tryAcquire("acct-5", 2000);
        long t1 = l1.fencingToken();
        l1.release();
        AcquiredLock l2 = lockManager.tryAcquire("acct-5", 2000);
        return l2.fencingToken() > t1;
    }

    static boolean testAcquireTimesOut() {
        RedisLock lockManager = new RedisLock(new InMemoryRedisClient());
        lockManager.tryAcquire("acct-6", 5000);
        try {
            lockManager.acquire("acct-6", 5000, 20, 100);
            return false; // should have thrown
        } catch (LockNotAcquiredException e) {
            return true;
        }
    }

    static boolean testResourceAcceptsIncreasing() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 1, "w1", v -> v + 1);
        res.write("e1", 2, "w2", v -> v + 1);
        return res.value("e1") == 2;
    }

    static boolean testResourceRejectsStale() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 5, "w1", v -> v + 100);
        try {
            res.write("e1", 3, "w2", v -> v + 1);
            return false;
        } catch (StaleFencingTokenException e) {
            return res.value("e1") == 100;
        }
    }

    static boolean testResourceRejectsReplay() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 7, "w1", v -> v + 1);
        try {
            res.write("e1", 7, "w1-retry", v -> v + 1);
            return false;
        } catch (StaleFencingTokenException e) {
            return true;
        }
    }
}
