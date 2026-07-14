package com.coordinator.lock;

/**
 * A held (or believed-to-be-held) lock on one entity key, plus the
 * fencing token that came with it. See {@link EntityLockManager} for why
 * this exists as a backend-agnostic type, and DESIGN.md's "the
 * guarantee" section for why the fencing token -- not lock possession --
 * is the thing the rest of the system should trust.
 */
public interface AcquiredEntityLock {

    String entityKey();

    /**
     * The monotonic proof-of-freshness to present to the protected
     * resource. Strictly greater than every fencing token previously
     * issued for this entity: for ZooKeeper, the sequence number the
     * ensemble assigned the ephemeral znode on creation -- native to the
     * primitive, nothing extra to maintain, and safe across leader
     * failover because it's assigned as part of the replicated log.
     */
    long fencingToken();

    /**
     * Best-effort release. Returns false if this lock had already been
     * lost before this call (session expiry, most likely) and possibly
     * reacquired by someone else -- an expected outcome, not an error
     * condition.
     */
    boolean release();

    /**
     * Best-effort renewal / heartbeat. For ZooKeeper, liveness is a
     * session property, heartbeated automatically by the client library
     * in the background -- there is nothing for a caller to actively
     * renew, so this is a documented no-op that returns true while the
     * session is alive.
     *
     * <p>A caller must still treat a {@code false} return (or a
     * subsequent failed write) as "I can no longer assume exclusivity"
     * -- never treat a past successful renewal as an ongoing guarantee.
     */
    boolean extend(long newTtlMillis);
}
