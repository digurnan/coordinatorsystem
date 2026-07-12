package com.coordinator.worker;

import com.coordinator.lock.AcquiredLock;
import com.coordinator.lock.LockNotAcquiredException;
import com.coordinator.lock.RedisLock;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.StaleFencingTokenException;

/**
 * Simulates one worker doing one job against a protected entity:
 * acquire lock -&gt; do work (optionally with an injected stall modeling a
 * GC pause / descheduled thread / blocked syscall) -&gt; optionally delay
 * before sending the write (modeling a slow/reordered network) -&gt;
 * attempt the write -&gt; best-effort release.
 *
 * <p>Deliberately does NOT re-check lock possession before writing. A
 * real worker cannot reliably know whether it still holds the lock --
 * that is precisely the failure mode the exercise describes ("a worker
 * can stall... then wake up and keep going exactly where it left off,
 * with no awareness that time passed"). The safety property has to come
 * from the resource checking the fencing token, not from the worker
 * being careful about something it cannot actually observe.
 */
public final class JobRunner {

    private JobRunner() {
    }

    public static WorkerResult run(
            String workerId,
            RedisLock lockManager,
            ProtectedResource resource,
            String entityKey,
            long ttlMillis,
            long workMillis,
            long amount,
            long stallMillis,
            long preWriteDelayMillis,
            long lockTimeoutMillis
    ) {
        AcquiredLock lock;
        try {
            lock = lockManager.acquire(entityKey, ttlMillis, 20, lockTimeoutMillis);
        } catch (LockNotAcquiredException e) {
            return new WorkerResult(workerId, WorkerResult.Outcome.LOCK_UNAVAILABLE, null, e.getMessage());
        }

        // The "job".
        sleepQuietly(workMillis);

        // Models a GC pause / blocked syscall / noisy-neighbour CPU
        // steal: the worker makes no progress here and has no idea how
        // much wall-clock time passes. The lock may expire and be
        // reacquired by someone else during this sleep.
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
        // stolen. That's expected, not a bug -- see AcquiredLock#release.
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
