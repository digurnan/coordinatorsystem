package com.coordinator.lock;

/**
 * A held lock on a single entity key, plus the fencing token that came
 * with it. See {@link RedisLock} for why the fencing token -- not lock
 * possession itself -- is the thing the rest of the system should trust.
 */
public final class AcquiredLock {
    private final RedisLock manager;
    private final String entityKey;
    private final String ownerToken;
    private final long fencingToken;
    private volatile long ttlMillis;

    AcquiredLock(RedisLock manager, String entityKey, String ownerToken, long fencingToken, long ttlMillis) {
        this.manager = manager;
        this.entityKey = entityKey;
        this.ownerToken = ownerToken;
        this.fencingToken = fencingToken;
        this.ttlMillis = ttlMillis;
    }

    public String entityKey() {
        return entityKey;
    }

    /** The monotonic proof-of-freshness to present to the protected
     * resource. Strictly greater than every fencing token previously
     * issued for this entity. */
    public long fencingToken() {
        return fencingToken;
    }

    public long ttlMillis() {
        return ttlMillis;
    }

    String ownerToken() {
        return ownerToken;
    }

    /**
     * Best-effort heartbeat/renewal. Returns false (does not throw) if
     * the lock had already been lost -- callers must treat that as "I
     * can no longer assume exclusivity going forward" and lean on the
     * resource's fencing check for correctness, not on this call having
     * succeeded in the past.
     */
    public boolean extend(long newTtlMillis) {
        boolean ok = manager.extend(this, newTtlMillis);
        if (ok) {
            this.ttlMillis = newTtlMillis;
        }
        return ok;
    }

    /**
     * Best-effort release. Returns false if this lock had already
     * expired and possibly been reacquired by someone else -- that is an
     * expected outcome, not an error condition, and callers should not
     * treat it as one.
     */
    public boolean release() {
        return manager.release(this);
    }
}
