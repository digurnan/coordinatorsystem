package com.coordinator.sim;

import com.coordinator.lock.RedisLock;
import com.coordinator.redisclient.InMemoryRedisClient;
import com.coordinator.redisclient.JedisRedisClient;
import com.coordinator.redisclient.RedisCoordinationClient;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.WriteRecord;
import com.coordinator.worker.JobRunner;
import com.coordinator.worker.WorkerResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * CLI harness exercising the coordination component with multiple
 * concurrent workers contending for the same entity key, per the
 * assignment's requirement #2 ("A small simulation or test harness...").
 *
 * <p>Three scenarios:
 * <ul>
 *   <li>{@link #scenarioBaseline} - light contention, sanity check.</li>
 *   <li>{@link #scenarioHighContention} - many workers, short TTL, heavy
 *       contention -- checks the lock alone serializes correctly under
 *       load.</li>
 *   <li>{@link #scenarioStalledWorkerCaughtByFencing} - the operating
 *       condition the inherited design note never accounted for: a
 *       worker stalls past its TTL (GC pause), loses the lock, and tries
 *       to write anyway after another worker has already taken over.
 *       Demonstrates that the *resource*, not the lock, is what actually
 *       prevents corruption here.</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   java -cp target/classes com.coordinator.sim.Simulate
 *   java -cp target/classes com.coordinator.sim.Simulate --redis-host localhost --redis-port 6379
 * </pre>
 */
public final class Simulate {

    public static void main(String[] args) throws Exception {
        String redisHost = argValue(args, "--redis-host", null);
        int redisPort = Integer.parseInt(argValue(args, "--redis-port", "6379"));

        RedisCoordinationClient client;
        if (redisHost != null) {
            System.out.println("Using real Redis at " + redisHost + ":" + redisPort);
            client = new JedisRedisClient(redisHost, redisPort);
        } else {
            System.out.println("Using in-memory Redis stand-in (pass --redis-host to run against real Redis)");
            client = new InMemoryRedisClient();
        }

        boolean p1 = scenarioBaseline(client);
        boolean p2 = scenarioHighContention(client);
        boolean p3 = scenarioStalledWorkerCaughtByFencing(client);

        if (client instanceof AutoCloseable) {
            ((AutoCloseable) client).close();
        }

        System.out.println();
        if (p1 && p2 && p3) {
            System.out.println("ALL SCENARIOS PASSED");
            System.exit(0);
        } else {
            System.out.println("SOME SCENARIOS FAILED");
            System.exit(1);
        }
    }

    public static boolean scenarioBaseline(RedisCoordinationClient client) throws InterruptedException {
        String entity = "acct-baseline-" + System.nanoTime();
        RedisLock lockManager = new RedisLock(client);
        ProtectedResource resource = new ProtectedResource();
        int n = 20;

        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<WorkerResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = "w" + i;
            futures.add(pool.submit(() -> JobRunner.run(
                    id, lockManager, resource, entity, 800, 10 + (long) (Math.random() * 40), 1,
                    0, 0, 5000)));
        }
        int committed = collectCommitted(futures);
        pool.shutdown();

        long finalValue = resource.value(entity);
        List<WriteRecord> log = resource.log(entity);
        boolean tokensOrdered = isStrictlyIncreasing(log);
        boolean ok = finalValue == n && committed == n && tokensOrdered;

        System.out.printf("[baseline] workers=%d committed=%d final_value=%d strictly_increasing_tokens=%b => %s%n",
                n, committed, finalValue, tokensOrdered, ok ? "PASS" : "FAIL");
        return ok;
    }

    public static boolean scenarioHighContention(RedisCoordinationClient client) throws InterruptedException {
        String entity = "acct-contention-" + System.nanoTime();
        RedisLock lockManager = new RedisLock(client);
        ProtectedResource resource = new ProtectedResource();
        int n = 50;

        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<WorkerResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = "w" + i;
            futures.add(pool.submit(() -> JobRunner.run(
                    id, lockManager, resource, entity, 300, 1 + (long) (Math.random() * 10), 1,
                    0, 0, 8000)));
        }
        int committed = collectCommitted(futures);
        pool.shutdown();

        long finalValue = resource.value(entity);
        List<WriteRecord> log = resource.log(entity);
        boolean tokensOrdered = isStrictlyIncreasing(log);
        boolean ok = finalValue == n && committed == n && tokensOrdered;

        System.out.printf("[high-contention] workers=%d committed=%d final_value=%d strictly_increasing_tokens=%b => %s%n",
                n, committed, finalValue, tokensOrdered, ok ? "PASS" : "FAIL");
        return ok;
    }

    /**
     * The scenario the inherited design note never tested. Worker A
     * acquires the lock, then stalls (models a GC pause) for well longer
     * than the TTL. The lock expires; Worker B acquires it, does its
     * work, commits, releases. Worker A then "wakes up" with no idea
     * time passed and tries to commit too.
     *
     * <p>A lock-only design ("if I hold the lock, I can just write")
     * would let both writes through -- that's the double charge /
     * duplicated shipment from the prompt. With fencing enforced at the
     * resource, A's token is stale and gets rejected: exactly one write
     * commits.
     */
    public static boolean scenarioStalledWorkerCaughtByFencing(RedisCoordinationClient client)
            throws InterruptedException, ExecutionException {
        String entity = "acct-stall-" + System.nanoTime();
        RedisLock lockManager = new RedisLock(client);
        ProtectedResource resource = new ProtectedResource();
        long ttl = 300;

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<WorkerResult> futA = pool.submit(() -> JobRunner.run(
                "worker-A(stalled)", lockManager, resource, entity, ttl, 10, 100,
                ttl * 3, 0, 2000));
        Thread.sleep(50); // let A grab the lock before B tries
        Future<WorkerResult> futB = pool.submit(() -> JobRunner.run(
                "worker-B", lockManager, resource, entity, ttl, 10, 1,
                0, 0, ttl * 4));

        WorkerResult resultA = futA.get();
        WorkerResult resultB = futB.get();
        pool.shutdown();

        List<WriteRecord> log = resource.log(entity);
        List<WriteRecord> rejected = resource.rejected(entity);
        long finalValue = resource.value(entity);

        System.out.println("[stalled-worker] " + resultA);
        System.out.println("[stalled-worker] " + resultB);
        System.out.println("  committed: " + log);
        System.out.println("  rejected:  " + rejected);
        System.out.println("  final resource value: " + finalValue);

        boolean exactlyOneEach =
                log.size() == 1 && rejected.size() == 1
                        && ((resultA.outcome == WorkerResult.Outcome.COMMITTED
                                && resultB.outcome == WorkerResult.Outcome.REJECTED_STALE)
                        || (resultB.outcome == WorkerResult.Outcome.COMMITTED
                                && resultA.outcome == WorkerResult.Outcome.REJECTED_STALE));

        System.out.println("  => " + (exactlyOneEach ? "PASS (fencing token caught the stale writer)" : "FAIL"));
        return exactlyOneEach;
    }

    private static int collectCommitted(List<Future<WorkerResult>> futures) throws InterruptedException {
        int committed = 0;
        for (Future<WorkerResult> f : futures) {
            try {
                if (f.get().outcome == WorkerResult.Outcome.COMMITTED) committed++;
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return committed;
    }

    private static boolean isStrictlyIncreasing(List<WriteRecord> log) {
        long prev = 0;
        for (WriteRecord r : log) {
            if (r.fencingToken <= prev) return false;
            prev = r.fencingToken;
        }
        return true;
    }

    private static String argValue(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return def;
    }
}
