package com.coordinator.zklock;

import com.coordinator.lock.AcquiredEntityLock;

/**
 * A held ZooKeeper lock: the ephemeral sequential znode this worker
 * created, plus the sequence number ZooKeeper assigned it, used directly
 * as the fencing token. See {@link ZooKeeperLock} for the full argument.
 */
public final class AcquiredZkLock implements AcquiredEntityLock {

    private final ZooKeeperLock manager;
    private final String entityKey;
    private final String path;
    private final long fencingToken;

    AcquiredZkLock(ZooKeeperLock manager, String entityKey, String path, long fencingToken) {
        this.manager = manager;
        this.entityKey = entityKey;
        this.path = path;
        this.fencingToken = fencingToken;
    }

    @Override
    public String entityKey() {
        return entityKey;
    }

    @Override
    public long fencingToken() {
        return fencingToken;
    }

    @Override
    public boolean release() {
        return manager.release(path);
    }

    /**
     * ZooKeeper has no per-lock TTL to extend -- liveness is the
     * client's session, heartbeated automatically in the background by
     * the ZooKeeper client library for as long as the process is alive
     * and able to run that thread. There is nothing for a caller to do
     * here; this always returns true. The case this can't paper over --
     * the client process itself stalling hard enough to stop the
     * heartbeat thread too -- is exactly the case fencing exists to
     * catch, same as on the Redis side.
     */
    @Override
    public boolean extend(long newTtlMillis) {
        return true;
    }
}
