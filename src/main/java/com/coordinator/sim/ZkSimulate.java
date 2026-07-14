package com.coordinator.sim;

import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.WriteRecord;
import com.coordinator.worker.JobRunner;
import com.coordinator.worker.WorkerResult;
import com.coordinator.zklock.ZooKeeperLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * CLI harness that runs a handful of concurrency scenarios against a
 * real ZooKeeper ensemble (see docker-compose.yml), using {@link
 * ZooKeeperLock}. Requires a reachable ensemble -- there is no
 * in-memory stand-in here, because the interesting behavior (watches,
 * session-based liveness, native sequence-number fencing) is exactly the
 * part not worth faking with a simplified local substitute. See
 * DESIGN.md, "Testing strategy," for how the algorithm was checked
 * independently of a live ensemble before this was wired up.
 *
 * <p>Two scenarios beyond baseline/high-contention specifically probe
 * the behavior that differs from the Redis version:
 * <ul>
 *   <li>{@link #scenarioSlowButAlive} -- a worker that is merely slow
 *       (not dead) keeps its lock the whole time with no manual
 *       heartbeat call anywhere in {@code JobRunner}, unlike the Redis
 *       harness's stalled-worker scenario, where an equivalent sleep
 *       duration loses the lock because nothing calls {@code
 *       extend()}.</li>
 *   <li>{@link #scenarioSessionLossCaughtByFencing} -- the ZK analogue
 *       of the Redis stalled-worker scenario: worker A's session is
 *       force-closed (modeling the ensemble reaping a session whose
 *       heartbeats stopped arriving during a GC pause) while A is still
 *       "working"; worker B, already queued behind A via a watch,
 *       acquires next and commits; A wakes later and is rejected by the
 *       same fencing check used on the Redis side.</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   docker-compose up -d zookeeper
 *   java -cp target/classes com.coordinator.sim.ZkSimulate --zk-host localhost:2181
 * </pre>
 */
public final class ZkSimulate {

    public static void main(String[] args) throws Exception {
        String zkHost = argValue(args, "--zk-host", "localhost:2181");
        System.out.println("Using ZooKeeper ensemble at " + zkHost);

        boolean p1 = scenarioBaseline(zkHost);
        boolean p2 = scenarioHighContention(zkHost);
        boolean p3 = scenarioSlowButAlive(zkHost);
        boolean p4 = scenarioSessionLossCaughtByFencing(zkHost);

        System.out.println();
        if (p1 && p2 && p3 && p4) {
            System.out.println("ALL SCENARIOS PASSED");
            System.exit(0);
        } else {
            System.out.println("SOME SCENARIOS FAILED");
            System.exit(1);
        }
    }

    public static boolean scenarioBaseline(String zkHost) throws Exception {
        String entity = "acct-baseline-" + System.nanoTime();
        ProtectedResource resource = new ProtectedResource();
        int n = 20;

        try (ZooKeeperLock lockManager = new ZooKeeperLock(zkHost, 6000)) {
            ExecutorService pool = Executors.newFixedThreadPool(n);
            List<Future<WorkerResult>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = "w" + i;
                futures.add(pool.submit(() -> JobRunner.run(
                        id, lockManager, resource, entity, 10 + (long) (Math.random() * 40), 1,
                        0, 0, 15000)));
            }
            int committed = collectCommitted(futures);
            pool.shutdown();

            long finalValue = resource.value(entity);
            List<WriteRecord> log = resource.log(entity);
            boolean tokensOrdered = isStrictlyIncreasing(log);
            boolean ok = finalValue == n && committed == n && tokensOrdered;

            System.out.printf("[zk-baseline] workers=%d committed=%d final_value=%d strictly_increasing_tokens=%b => %s%n",
                    n, committed, finalValue, tokensOrdered, ok ? "PASS" : "FAIL");
            return ok;
        }
    }

    public static boolean scenarioHighContention(String zkHost) throws Exception {
        String entity = "acct-contention-" + System.nanoTime();
        ProtectedResource resource = new ProtectedResource();
        int n = 50;

        try (ZooKeeperLock lockManager = new ZooKeeperLock(zkHost, 6000)) {
            ExecutorService pool = Executors.newFixedThreadPool(n);
            List<Future<WorkerResult>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = "w" + i;
                futures.add(pool.submit(() -> JobRunner.run(
                        id, lockManager, resource, entity, 1 + (long) (Math.random() * 10), 1,
                        0, 0, 20000)));
            }
            int committed = collectCommitted(futures);
            pool.shutdown();

            long finalValue = resource.value(entity);
            List<WriteRecord> log = resource.log(entity);
            boolean tokensOrdered = isStrictlyIncreasing(log);
            boolean ok = finalValue == n && committed == n && tokensOrdered;

            System.out.printf("[zk-high-contention] workers=%d committed=%d final_value=%d strictly_increasing_tokens=%b => %s%n",
                    n, committed, finalValue, tokensOrdered, ok ? "PASS" : "FAIL");
            return ok;
        }
    }

    /**
     * The case a fixed-TTL Redis lock with no wired-up heartbeat cannot
     * survive: a worker that is slow but alive. One ZooKeeper session,
     * one long-ish job, no manual renewal call anywhere in {@code
     * JobRunner} -- and it still commits, because session heartbeating
     * happens on the client library's own background thread, unaffected
     * by this worker sleeping.
     */
    public static boolean scenarioSlowButAlive(String zkHost) throws Exception {
        String entity = "acct-slow-" + System.nanoTime();
        ProtectedResource resource = new ProtectedResource();

        try (ZooKeeperLock lockManager = new ZooKeeperLock(zkHost, 6000)) {
            WorkerResult result = JobRunner.run(
                    "worker-slow", lockManager, resource, entity,
                    4000, 1, 0, 0, 10000);
            boolean ok = result.outcome == WorkerResult.Outcome.COMMITTED;
            System.out.println("[zk-slow-but-alive] " + result + " => " + (ok ? "PASS" : "FAIL"));
            return ok;
        }
    }

    /**
     * The scenario that demonstrates the whole point of this design:
     * worker A's underlying ZooKeeper session is force-closed by a
     * supervisor thread while A is still "working" -- modeling the
     * ensemble reaping a session whose heartbeats stopped arriving during
     * a GC pause. Worker B, queued behind A via a watch on A's znode, is
     * woken, acquires a fresh (higher) fencing token, and commits. Worker
     * A wakes later, still holding its now-stale token, and its write is
     * rejected by {@code ProtectedResource} -- the fencing check, not
     * lock possession, is what actually prevented the double write.
     */
    public static boolean scenarioSessionLossCaughtByFencing(String zkHost) throws Exception {
        String entity = "acct-session-loss-" + System.nanoTime();
        ProtectedResource resource = new ProtectedResource();

        ZooKeeperLock lockA = new ZooKeeperLock(zkHost, 6000);
        ZooKeeperLock lockB = new ZooKeeperLock(zkHost, 6000);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<WorkerResult> futA = pool.submit(() -> JobRunner.run(
                "worker-A(session-lost)", lockA, resource, entity, 3000, 100, 0, 0, 10000));
        Thread.sleep(300); // let A acquire before B queues up behind it

        Future<WorkerResult> futB = pool.submit(() -> JobRunner.run(
                "worker-B", lockB, resource, entity, 10, 1, 0, 0, 15000));
        Thread.sleep(300); // let B create its znode and start watching A's

        // Force A's session closed -- models the ensemble reaping a dead
        // session server-side. A's application thread is still asleep
        // inside JobRunner.run and has no way to know this happened.
        lockA.close();

        WorkerResult resultA = futA.get();
        WorkerResult resultB = futB.get();
        pool.shutdown();
        lockB.close();

        List<WriteRecord> log = resource.log(entity);
        List<WriteRecord> rejected = resource.rejected(entity);

        System.out.println("[zk-session-loss] " + resultA);
        System.out.println("[zk-session-loss] " + resultB);
        System.out.println("  committed: " + log);
        System.out.println("  rejected:  " + rejected);

        boolean exactlyOneEach =
                log.size() == 1 && rejected.size() == 1
                        && ((resultA.outcome == WorkerResult.Outcome.COMMITTED
                                && resultB.outcome == WorkerResult.Outcome.REJECTED_STALE)
                        || (resultB.outcome == WorkerResult.Outcome.COMMITTED
                                && resultA.outcome == WorkerResult.Outcome.REJECTED_STALE));

        System.out.println("  => " + (exactlyOneEach
                ? "PASS (fencing token caught the writer whose session had expired)" : "FAIL"));
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
