package com.coordinator.worker;

import com.coordinator.lock.AcquiredEntityLock;
import com.coordinator.lock.EntityLockManager;
import com.coordinator.lock.LockNotAcquiredException;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.StaleFencingTokenException;

/**
 * Simulates one worker doing one job against a protected entity:
 * acquire lock -&gt; do work (optionally with an injected stall modeling a
 * GC pause / descheduled thread / blocked syscall) -&gt; optionally delay
 * before sending the write (modeling a slow/reordered network) -&gt;
 * attempt the write -&gt; best-effort release.
 *
 * <p>Written against the backend-agnostic {@link EntityLockManager} /
 * {@link AcquiredEntityLock} contract rather than directly against
 * ZooKeeper's client API -- see {@code ZkSimulate}, which runs this
 * exact same method against a real ensemble, and {@link
 * com.coordinator.springapp.LockController}, which drives it over HTTP.
 * Per-lock session/timeout policy lives with whichever {@link
 * EntityLockManager} the caller constructs, not here.
 *
 * <p>Deliberately does NOT re-check lock possession before writing. A
 * real worker cannot reliably know whether it still holds the lock --
 * it can stall (a GC pause, a blocked syscall) and wake up later with no
 * awareness that time passed, let alone that its lock was reclaimed in
 * the meantime. The safety property has to come from the resource
 * checking the fencing token, not from the worker being careful about
 * something it cannot actually observe.
 */
public final class JobRunner {

    private JobRunner() {
    }

    public static WorkerResult run(
            String workerId,
            EntityLockManager lockManager,
            ProtectedResource resource,
            String entityKey,
            long workMillis,
            long amount,
            long stallMillis,
            long preWriteDelayMillis,
            long lockTimeoutMillis
    ) {
        AcquiredEntityLock lock;
        try {
            lock = lockManager.acquire(entityKey, lockTimeoutMillis);
        } catch (LockNotAcquiredException e) {
            return new WorkerResult(workerId, WorkerResult.Outcome.LOCK_UNAVAILABLE, null, e.getMessage());
        }

        // The "job".
        sleepQuietly(workMillis);

        // Models a GC pause / blocked syscall / noisy-neighbour CPU
        // steal: the worker makes no progress here and has no idea how
        // much wall-clock time passes. Depending on the backend, this
        // may or may not cost the worker its lock:
        //   - Redis: only costs the lock if stallMillis exceeds the TTL
        //     and nothing called extend() during the stall.
        //   - ZooKeeper: only costs the lock if the stall is severe
        //     enough to also stop the client's session heartbeat thread
        //     (a real GC pause would; a merely slow Thread.sleep() on
        //     this thread alone would not, since the heartbeat runs
        //     elsewhere). This simulation approximates both cases the
        //     same way at the JobRunner level; see the harnesses for how
        //     each backend's scenario setup reflects the distinction.
        if (stallMillis > 0) {
            sleepQuietly(stallMillis);
        }

        // Models a slow/reordered network: the worker believes it's
        // about to write, but the request doesn't actually land for a
        // while.
        if (preWriteDelayMillis > 0) {
            sleepQuietly(preWriteDelayMillis);
        }

        WorkerResult result;
        try {
            long newValue = resource.write(entityKey, lock.fencingToken(), workerId, old -> old + amount);
            result = new WorkerResult(workerId, WorkerResult.Outcome.COMMITTED, lock.fencingToken(),
                    "new_value=" + newValue);
        } catch (StaleFencingTokenException e) {
            result = new WorkerResult(workerId, WorkerResult.Outcome.REJECTED_STALE, lock.fencingToken(),
                    e.getMessage());
        }

        // Best-effort; may legitimately no-op if the lock was already
        // stolen / the session already expired. That's expected, not a
        // bug -- see AcquiredEntityLock#release.
        lock.release();
        return result;
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
