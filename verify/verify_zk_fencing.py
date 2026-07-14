"""
Independent verification of the ZooKeeper-flavored lock algorithm before
writing the Java. This models the standard "sequential ephemeral znode"
ZK lock recipe at the level of client-observable semantics:

  - create_ephemeral_sequential(entity, session) assigns a strictly
    increasing sequence number per entity, atomically, server-side.
    That sequence number is what the Java ZooKeeperLock uses as the
    fencing token -- no separate counter needed, unlike Redis's INCR.
  - The lowest live sequence number for an entity holds the lock.
  - A blocked acquirer watches only its immediate predecessor (not the
    whole list, not the holder) to avoid the herd effect.
  - Session liveness is independent of the ensemble libraries' internal
    ping thread, which is why a worker that's merely slow (blocked on
    I/O, doing CPU work) does NOT lose its lock the way it would under
    a fixed Redis TTL with no wired-up extend() call. Only a session
    that stops heartbeating entirely (simulated here as an explicit
    server-side expiry, independent of when the worker's own thread
    wakes up) gets its ephemeral znodes reaped.

Two scenarios the Redis version couldn't cleanly demonstrate are added
here on purpose:
  - "slow but alive": a worker sleeps past what would have been a short
    Redis TTL, but keeps its lock the whole time and commits -- ZK's
    session model buys this for free.
  - "session expired" (the ZK analogue of the Redis stalled-worker
    scenario): the ensemble expires the session out from under the
    worker while it's stalled; a waiting worker is woken via watch,
    acquires a new (higher) fencing token, and commits; the original
    worker wakes later, tries to write with its now-stale token, and is
    rejected by the same fencing check used in the Redis version.
"""
import threading
import time


class Ensemble:
    """In-process stand-in for the ZK ensemble's relevant behavior."""

    def __init__(self):
        self._lock = threading.Lock()
        self._seq_counters = {}   # entity -> last assigned seq
        self._children = {}       # entity -> {seq: session_id}
        self._watchers = {}       # (entity, seq) -> [Event, ...]

    def create_ephemeral_sequential(self, entity, session_id):
        with self._lock:
            seq = self._seq_counters.get(entity, 0) + 1
            self._seq_counters[entity] = seq
            self._children.setdefault(entity, {})[seq] = session_id
            return seq

    def get_children(self, entity):
        with self._lock:
            return sorted(self._children.get(entity, {}).keys())

    def watch_deletion(self, entity, seq):
        """Returns an Event that fires when `seq` is deleted. Fires
        immediately if it's already gone (mirrors ZK's exists()-with-
        watch semantics: check and register atomically)."""
        ev = threading.Event()
        with self._lock:
            if seq not in self._children.get(entity, {}):
                ev.set()
            else:
                self._watchers.setdefault((entity, seq), []).append(ev)
        return ev

    def delete(self, entity, seq):
        with self._lock:
            existed = self._children.get(entity, {}).pop(seq, None) is not None
            evs = self._watchers.pop((entity, seq), [])
        for ev in evs:
            ev.set()
        return existed

    def expire_session(self, entity, session_id):
        """Models the ensemble reclaiming every ephemeral znode owned by
        a session whose heartbeats stopped arriving -- server-side,
        independent of whatever the client process is doing."""
        with self._lock:
            dead = [seq for seq, sid in self._children.get(entity, {}).items() if sid == session_id]
        for seq in dead:
            self.delete(entity, seq)


class LockNotAcquired(Exception):
    pass


class AcquiredZkLock:
    def __init__(self, ensemble, entity, seq, session_id):
        self.ensemble = ensemble
        self.entity = entity
        self.seq = seq
        self.session_id = session_id
        self.fencing_token = seq

    def release(self):
        return self.ensemble.delete(self.entity, self.seq)


class ZkLock:
    def __init__(self, ensemble, session_id):
        self.ensemble = ensemble
        self.session_id = session_id

    def acquire(self, entity, timeout_s):
        seq = self.ensemble.create_ephemeral_sequential(entity, self.session_id)
        deadline = time.monotonic() + timeout_s
        while True:
            kids = self.ensemble.get_children(entity)
            if seq not in kids:
                raise LockNotAcquired(f"session lost before acquiring lock on {entity}")
            idx = kids.index(seq)
            if idx == 0:
                return AcquiredZkLock(self.ensemble, entity, seq, self.session_id)
            predecessor = kids[idx - 1]
            ev = self.ensemble.watch_deletion(entity, predecessor)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.ensemble.delete(entity, seq)
                raise LockNotAcquired(f"timed out waiting for lock on {entity}")
            ev.wait(remaining)
            # Re-check the live child list regardless of whether the
            # watch fired or we timed out -- that list is the only
            # source of truth, same principle as the Redis version never
            # trusting "I haven't heard otherwise" as proof of anything.


# ---- Protected resource: identical fencing logic to the Redis version ----

class StaleFencingToken(Exception):
    pass


class ProtectedResource:
    def __init__(self):
        self._mutex = threading.Lock()
        self._last_accepted = {}
        self._state = {}
        self.log = []
        self.rejected_log = []

    def write(self, entity, fencing_token, writer, amount):
        with self._mutex:
            last = self._last_accepted.get(entity, 0)
            if fencing_token <= last:
                self.rejected_log.append((entity, fencing_token, writer))
                raise StaleFencingToken(
                    f"entity={entity} writer={writer} token={fencing_token} <= last_accepted={last}")
            new_value = self._state.get(entity, 0) + amount
            self._state[entity] = new_value
            self._last_accepted[entity] = fencing_token
            self.log.append((entity, fencing_token, writer, new_value))
            return new_value

    def value(self, entity):
        with self._mutex:
            return self._state.get(entity, 0)


def job_runner(worker_id, zk_lock, resource, entity, work_s, amount, timeout_s, results, idx):
    try:
        lock = zk_lock.acquire(entity, timeout_s)
    except LockNotAcquired as e:
        results[idx] = ("LOCK_UNAVAILABLE", None, str(e))
        return
    time.sleep(work_s)
    try:
        new_value = resource.write(entity, lock.fencing_token, worker_id, amount)
        results[idx] = ("COMMITTED", lock.fencing_token, f"new_value={new_value}")
    except StaleFencingToken as e:
        results[idx] = ("REJECTED_STALE", lock.fencing_token, str(e))
    lock.release()


def scenario_baseline():
    entity = f"acct-baseline-{time.time_ns()}"
    ensemble = Ensemble()
    resource = ProtectedResource()
    n = 20
    threads, results = [], [None] * n
    for i in range(n):
        zk_lock = ZkLock(ensemble, session_id=f"session-{i}")
        t = threading.Thread(target=job_runner, args=(
            f"w{i}", zk_lock, resource, entity, (5 + (i % 5)) / 1000.0, 1, 5, results, i))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    committed = sum(1 for r in results if r[0] == "COMMITTED")
    final_value = resource.value(entity)
    tokens = [r[1] for r in resource.log if r[0] == entity]
    ordered = all(tokens[i] < tokens[i + 1] for i in range(len(tokens) - 1))
    ok = final_value == n and committed == n and ordered
    print(f"[zk-baseline] workers={n} committed={committed} final_value={final_value} "
          f"strictly_increasing_tokens={ordered} => {'PASS' if ok else 'FAIL'}")
    return ok


def scenario_high_contention():
    entity = f"acct-contention-{time.time_ns()}"
    ensemble = Ensemble()
    resource = ProtectedResource()
    n = 50
    threads, results = [], [None] * n
    for i in range(n):
        zk_lock = ZkLock(ensemble, session_id=f"session-{i}")
        t = threading.Thread(target=job_runner, args=(
            f"w{i}", zk_lock, resource, entity, (1 + (i % 3)) / 1000.0, 1, 8, results, i))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    committed = sum(1 for r in results if r[0] == "COMMITTED")
    final_value = resource.value(entity)
    tokens = [r[1] for r in resource.log if r[0] == entity]
    ordered = all(tokens[i] < tokens[i + 1] for i in range(len(tokens) - 1))
    ok = final_value == n and committed == n and ordered
    print(f"[zk-high-contention] workers={n} committed={committed} final_value={final_value} "
          f"strictly_increasing_tokens={ordered} => {'PASS' if ok else 'FAIL'}")
    return ok


def scenario_slow_but_alive():
    """The case Redis's fixed-TTL-with-no-heartbeat design cannot survive:
    a worker that is merely slow, not dead. Under ZK's session model this
    is a non-event -- no manual extend() call required."""
    entity = f"acct-slow-{time.time_ns()}"
    ensemble = Ensemble()
    resource = ProtectedResource()
    results = [None]
    zk_lock = ZkLock(ensemble, session_id="session-slow")
    # Sleeps 0.3s -- longer than the 0.3s Redis TTL used in the earlier
    # Redis stalled-worker scenario, which would have lost the lock here.
    # The session is never told to expire, modeling that the ping thread
    # is unaffected by this worker doing legitimately slow work.
    t = threading.Thread(target=job_runner, args=(
        "worker-slow", zk_lock, resource, entity, 0.3, 1, 5, results, 0))
    t.start()
    t.join()
    ok = results[0][0] == "COMMITTED"
    print(f"[zk-slow-but-alive] {results[0]} => {'PASS' if ok else 'FAIL'}")
    return ok


def scenario_session_expired():
    """ZK analogue of the Redis stalled-worker scenario. Worker A's
    session is force-expired by the ensemble (modeling a GC pause that
    stops even the client's ping thread) while A is still 'working'.
    Worker B, already queued behind A, is woken by the watch on A's
    znode being deleted, acquires a new fencing token, and commits.
    Worker A wakes later and tries to write with its now-stale token."""
    entity = f"acct-expire-{time.time_ns()}"
    ensemble = Ensemble()
    resource = ProtectedResource()
    results = [None, None]

    zk_lock_a = ZkLock(ensemble, session_id="session-A")
    zk_lock_b = ZkLock(ensemble, session_id="session-B")

    tA = threading.Thread(target=job_runner, args=(
        "worker-A(session-expires)", zk_lock_a, resource, entity, 0.9, 100, 2, results, 0))
    tA.start()
    time.sleep(0.05)  # let A acquire (seq=1) before B queues up behind it

    tB = threading.Thread(target=job_runner, args=(
        "worker-B", zk_lock_b, resource, entity, 0.05, 1, 2, results, 1))
    tB.start()
    time.sleep(0.05)  # let B create its znode and start watching A's

    # Force A's session to expire server-side after a short timeout --
    # independent of A's own 0.9s sleep, exactly like a real ZK ensemble
    # reaping a session whose pings stopped arriving mid-GC-pause.
    threading.Timer(0.15, ensemble.expire_session, args=(entity, "session-A")).start()

    tA.join()
    tB.join()

    resultA, resultB = results
    log = [r for r in resource.log if r[0] == entity]
    rejected = [r for r in resource.rejected_log if r[0] == entity]

    print(f"[zk-session-expired] worker-A: {resultA}")
    print(f"[zk-session-expired] worker-B: {resultB}")
    print(f"  committed: {log}")
    print(f"  rejected:  {rejected}")

    exactly_one_each = (
        len(log) == 1 and len(rejected) == 1 and
        ((resultA[0] == "COMMITTED" and resultB[0] == "REJECTED_STALE") or
         (resultB[0] == "COMMITTED" and resultA[0] == "REJECTED_STALE"))
    )
    print(f"  => {'PASS (fencing token caught the writer whose session had expired)' if exactly_one_each else 'FAIL'}")
    return exactly_one_each


if __name__ == "__main__":
    results = []
    for _ in range(5):
        results.append(scenario_baseline())
        results.append(scenario_high_contention())
    for _ in range(10):
        results.append(scenario_slow_but_alive())
    for _ in range(10):
        results.append(scenario_session_expired())
    print()
    print("ALL PASSED" if all(results) else "SOME FAILED", f"({sum(results)}/{len(results)})")
