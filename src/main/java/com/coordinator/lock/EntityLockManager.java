package com.coordinator.lock;

/**
 * Backend-agnostic contract for "acquire exclusive-ish access to an
 * entity key, get back proof of how fresh that access is." {@code
 * com.coordinator.zklock.ZooKeeperLock} (session-based, ZooKeeper)
 * implements this, which is what lets {@code JobRunner} and the
 * simulation harness run the identical worker logic against it without
 * the caller needing to know anything about znodes, watches, or
 * sessions.
 *
 * <p>Kept one level above any single backend's own primitives on
 * purpose: a TTL-key store and ZooKeeper's ephemeral-znode-plus-watch
 * model don't shape the same way, and collapsing them into one
 * lowest-common-denominator interface would hide exactly the
 * differences DESIGN.md argues matter. This interface sits at
 * "acquire/release/fencing token," which any backend can express
 * honestly without pretending to be shaped like a different one.
 */
public interface EntityLockManager {

    /**
     * Blocks (via whatever backend-appropriate mechanism -- for
     * ZooKeeper, a watch on the immediate predecessor znode) until the
     * lock on {@code entityKey} is acquired or {@code
     * acquireTimeoutMillis} elapses.
     *
     * @throws LockNotAcquiredException if the timeout elapses first, or
     *         the backend fails in a way that makes acquisition
     *         impossible (e.g. a ZooKeeper session dying mid-attempt).
     */
    AcquiredEntityLock acquire(String entityKey, long acquireTimeoutMillis);
}
