package com.coordinator.zklock;

import com.coordinator.lock.AcquiredEntityLock;
import com.coordinator.lock.EntityLockManager;
import com.coordinator.lock.LockNotAcquiredException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Implements the standard "sequential ephemeral znode" lock recipe
 * (ZooKeeper Recipes and Solutions guide, "Locks"; this hand-rolls a
 * simplified version of what Curator's {@code InterProcessMutex} does,
 * to keep this project dependency-light and the algorithm visible --
 * see DESIGN.md for why that trade-off is named explicitly rather than
 * just reached for Curator).
 *
 * <p>See DESIGN.md, section 6, for the full argument for why this
 * approach was chosen over the alternatives considered; the short
 * version:
 *
 * <ul>
 *   <li><b>Liveness is a session property, not a per-key TTL.</b> The
 *       ZooKeeper client heartbeats the session on a background I/O
 *       thread; the ephemeral znode is deleted automatically, server
 *       side, when the session that created it expires. There is no
 *       separate {@code extend()} a worker must remember to call for a
 *       merely-slow-but-alive job -- only a stall that stops the client
 *       process itself (a full GC pause, not just this thread blocking
 *       on I/O) risks losing the lock.</li>
 *   <li><b>The fencing token is not a value this class asks for -- it is
 *       the sequence number ZooKeeper itself assigns the znode on
 *       creation</b>, strictly increasing per parent znode, assigned
 *       atomically as part of the replicated state machine (Zab), and
 *       therefore surviving leader failover. That is a strictly stronger
 *       guarantee than Redis's {@code INCR} counter, which can roll back
 *       if a stale replica is promoted after an ungraceful failover (see
 *       DESIGN.md, "the guarantee," assumption 1).</li>
 *   <li><b>Waiting is watch-driven, not polling.</b> A blocked acquirer
 *       watches exactly one sibling znode -- the one immediately ahead
 *       of it in sequence order, not the current holder and not the
 *       whole list -- which avoids both busy-polling and the herd effect
 *       where every waiter wakes on every single release.</li>
 * </ul>
 *
 * <p>One connection = one ZooKeeper session = one lock manager instance.
 * Session timeout is negotiated at connect time between what this class
 * requests and the ensemble's configured {@code minSessionTimeout} /
 * {@code maxSessionTimeout} (server defaults are commonly a handful of
 * seconds, driven by {@code tickTime}) -- unlike a Redis TTL, you cannot
 * generally push this down to the low hundreds of milliseconds. See
 * DESIGN.md for what that means for the TTL-decision tension.
 */
public final class ZooKeeperLock implements EntityLockManager, AutoCloseable {

    private static final String LOCK_ROOT = "/coordinator/locks";
    private static final String NODE_PREFIX = "lock-";

    private final ZooKeeper zk;

    public ZooKeeperLock(String connectString, int sessionTimeoutMillis) throws IOException, InterruptedException {
        CountDownLatch connected = new CountDownLatch(1);
        this.zk = new ZooKeeper(connectString, sessionTimeoutMillis, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        if (!connected.await(sessionTimeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new IOException("timed out connecting to ZooKeeper at " + connectString);
        }
        try {
            ensurePersistentPath(LOCK_ROOT);
        } catch (KeeperException e) {
            throw new IOException("failed to initialize " + LOCK_ROOT, e);
        }
    }

    /** Idempotent mkdir -p for a persistent znode path. */
    private void ensurePersistentPath(String path) throws InterruptedException, KeeperException {
        StringBuilder cur = new StringBuilder();
        for (String part : path.split("/")) {
            if (part.isEmpty()) continue;
            cur.append('/').append(part);
            try {
                if (zk.exists(cur.toString(), false) == null) {
                    zk.create(cur.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException.NodeExistsException raceWithAnotherClient) {
                // benign: another process created the same prefix concurrently
            }
        }
    }

    private static String entityPath(String entityKey) {
        return LOCK_ROOT + "/" + entityKey;
    }

    @Override
    public AcquiredEntityLock acquire(String entityKey, long acquireTimeoutMillis) {
        String parent = entityPath(entityKey);
        try {
            ensurePersistentPath(parent);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockNotAcquiredException("interrupted creating entity path for " + entityKey, e);
        } catch (KeeperException e) {
            throw new LockNotAcquiredException("failed to create entity path for " + entityKey, e);
        }

        String myPath;
        try {
            myPath = zk.create(parent + "/" + NODE_PREFIX, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            throw new LockNotAcquiredException("failed to create lock node for " + entityKey, e);
        }
        String myNodeName = stripParent(myPath);

        // The sequence number ZooKeeper embedded in the node name IS the
        // fencing token -- see the class doc. Captured now, before we've
        // necessarily acquired the lock, because it's assigned atomically
        // at creation time regardless of queue position.
        long fencingToken = parseSequence(myNodeName);

        long deadlineNanos = System.nanoTime() + acquireTimeoutMillis * 1_000_000L;
        try {
            while (true) {
                List<String> children = zk.getChildren(parent, false);
                Collections.sort(children);
                int myIndex = children.indexOf(myNodeName);
                if (myIndex < 0) {
                    // Our own ephemeral node is gone without us releasing
                    // it -- only possible if our session died in the
                    // meantime. We cannot claim a lock we no longer have a
                    // session backing.
                    throw new LockNotAcquiredException(
                            "lock node for " + entityKey + " disappeared before acquisition (session loss)");
                }
                if (myIndex == 0) {
                    return new AcquiredZkLock(this, entityKey, myPath, fencingToken);
                }

                // Watch only the immediate predecessor -- not the holder,
                // not the whole list -- to avoid the herd effect.
                String predecessor = parent + "/" + children.get(myIndex - 1);
                CountDownLatch predecessorGone = new CountDownLatch(1);
                Stat stat = zk.exists(predecessor, event -> {
                    if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                        predecessorGone.countDown();
                    }
                });
                if (stat == null) {
                    continue; // predecessor already gone; re-check the live list
                }

                long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    abandon(myPath);
                    throw new LockNotAcquiredException(
                            "could not acquire lock for " + entityKey + " within " + acquireTimeoutMillis + "ms");
                }
                predecessorGone.await(remainingMillis, TimeUnit.MILLISECONDS);
                if (System.nanoTime() >= deadlineNanos) {
                    abandon(myPath);
                    throw new LockNotAcquiredException(
                            "could not acquire lock for " + entityKey + " within " + acquireTimeoutMillis + "ms");
                }
                // Loop back and re-read the child list regardless of
                // whether the watch fired or we're about to time out --
                // that list is the only source of truth for "do I hold
                // the lock yet," not any local assumption.
            }
        } catch (KeeperException | InterruptedException e) {
            abandon(myPath);
            throw new LockNotAcquiredException("error acquiring lock for " + entityKey, e);
        }
    }

    boolean release(String path) {
        try {
            zk.delete(path, -1); // -1: skip version check; this is our own ephemeral node
            return true;
        } catch (KeeperException.NoNodeException alreadyGoneViaSessionExpiry) {
            // Expected outcome if the session already died and the
            // ensemble reclaimed it first -- same "best-effort release"
            // contract as AcquiredZkLock#release.
            return false;
        } catch (KeeperException | InterruptedException e) {
            return false;
        } catch (IllegalStateException alreadyClosed) {
            // The ZooKeeper client object itself was already closed (our
            // own close(), or -- in the simulation harness -- an external
            // party force-closing the session to model a reaped worker).
            // Either way the ephemeral node is already gone; nothing to do.
            return false;
        }
    }

    /**
     * Best-effort cleanup of our own not-yet-acquired queue position on
     * timeout, so we don't leave a phantom waiter for others to watch
     * forever. Not correctness-critical -- the node is ephemeral and
     * self-cleans on session end regardless -- but keeps the queue tidy
     * for the common case of a bounded-wait caller giving up.
     */
    private void abandon(String path) {
        try {
            zk.delete(path, -1);
        } catch (KeeperException | InterruptedException ignored) {
            // fine either way; the node was ours and will be reclaimed on
            // session end if this delete doesn't land
        }
    }

    private static long parseSequence(String nodeName) {
        return Long.parseLong(nodeName.substring(NODE_PREFIX.length()));
    }

    private static String stripParent(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    @Override
    public void close() throws InterruptedException {
        zk.close();
    }
}
