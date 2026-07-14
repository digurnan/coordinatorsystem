# Implementation Guide

DESIGN.md argues *why* ZooKeeper and this particular locking scheme were
chosen. This document is the companion piece: it walks the actual code,
so a reader can open any class and already know where it sits and what
it's responsible for, without re-deriving the design rationale from
scratch.

## The shape of it, in one paragraph

A worker asks an `EntityLockManager` to acquire a lock on an entity key
and gets back an `AcquiredEntityLock` carrying a fencing token. It does
its work, then calls `ProtectedResource.write(entity, token, ...)`,
which accepts the write only if the token is strictly greater than the
last one it accepted for that entity, and rejects it otherwise. The
worker releases the lock either way. Nothing about `ProtectedResource`
or the fencing check knows or cares which backend issued the token —
that boundary is the whole point of the design (DESIGN.md, section 6).

## Package tour

```
com.coordinator.lock       Backend-agnostic contracts
com.coordinator.zklock     The one concrete backend: ZooKeeper
com.coordinator.resource   The thing being protected, and the fencing check
com.coordinator.worker     The reusable acquire/work/write/release cycle
com.coordinator.sim        CLI harness that runs that cycle under contention
com.coordinator.springapp  HTTP wrapper around the same pieces
```

### `com.coordinator.lock` — the contract, not an implementation

- **`EntityLockManager`** — one method, `acquire(entityKey, timeoutMillis)`.
  This is the seam the rest of the system is written against. Nothing
  outside `zklock/` and `springapp/`'s wiring knows a concrete backend
  exists.
- **`AcquiredEntityLock`** — what `acquire()` hands back: `entityKey()`,
  `fencingToken()`, `release()`, `extend()`. `extend()` is a heartbeat
  hook that ZooKeeper's implementation makes a documented no-op (see
  below) — it exists in the interface for backends where it would
  matter.
- **`LockNotAcquiredException`** — thrown when acquisition fails for any
  reason: timeout elapsed, or the backend itself failed in a way that
  makes acquisition impossible.

Nothing in this package imports ZooKeeper. That's deliberate — it's the
line DESIGN.md section 6 draws between "what a lock is" and "how one
particular backend implements it."

### `com.coordinator.zklock` — the one backend this repo ships

- **`ZooKeeperLock`** implements `EntityLockManager`. `acquire()`:
  1. Creates an `EPHEMERAL_SEQUENTIAL` znode under
     `/coordinator/locks/<entityKey>/lock-`. The ensemble assigns the
     sequence number atomically — that number becomes the fencing token
     immediately, before the caller necessarily holds the lock.
  2. Lists the entity's children, sorted. If this znode has the lowest
     sequence number, the lock is held — return an `AcquiredZkLock`.
  3. Otherwise, set a watch on the *immediate predecessor* only (not the
     current holder, not the whole list), and wait for it to be
     deleted or for the timeout to elapse.
  4. On watch fire (or on timeout, re-checking the list first), loop
     back to step 2 — the child list is the only source of truth, never
     an assumption about what the watch event implied.

  `release(path)` and the private `abandon(path)` both call
  `zk.delete(path, -1)` and treat `NoNodeException` /
  `IllegalStateException` as an expected "already gone" outcome rather
  than an error — a lock that's already lost (session expired, or
  closed by a supervisor simulating a crash) is a normal case this
  class has to tolerate, not a bug.

- **`AcquiredZkLock`** is a thin holder: entity key, znode path, the
  fencing token captured at creation. `extend()` always returns `true`
  — see the class Javadoc for why that's correct rather than a stub:
  liveness here is a ZooKeeper session property, heartbeated on a
  background thread the caller never touches.

If you're trying to find where the fencing token actually comes from,
it's `ZooKeeperLock.parseSequence(myNodeName)` — the sequence number
ZooKeeper assigned the znode, not a value this code invents.

### `com.coordinator.resource` — where safety actually lives

- **`ProtectedResource.write(entity, fencingToken, writer, mutate)`** is
  the entire safety mechanism in one method: under a single lock
  (`synchronized (mutex)`), compare `fencingToken` against
  `lastAcceptedToken.get(entity)`. Reject (throw
  `StaleFencingTokenException`, log to `rejectedLog`) if it isn't
  strictly greater; otherwise apply `mutate`, store the new value,
  advance `lastAcceptedToken`, and log to `log`. This method does not
  know or care whether the caller still believes it holds a lock —
  that's exactly the point (DESIGN.md, section 7).
- **`WriteRecord`** — one line of the audit trail: entity, token,
  writer, resulting value (`-1` for a rejected record), and a global
  sequence number so `log()` and `rejected()` can be interleaved back
  into real order if needed.
- **`StaleFencingTokenException`** — the resource's rejection signal.
  Callers (`JobRunner`, `LockController`) catch this specifically to
  distinguish "your write was fenced out" from any other failure.

### `com.coordinator.worker` — the cycle, written once

- **`JobRunner.run(...)`** is deliberately the *only* place the
  acquire → work → write → release sequence is written. It takes an
  `EntityLockManager` as a parameter rather than constructing one, so
  the exact same method runs unmodified in `ZkSimulate` and (indirectly,
  through `LockController`) in the Spring app. It accepts injectable
  `stallMillis` (models a GC pause / blocked syscall mid-job) and
  `preWriteDelayMillis` (models a slow/reordered network before the
  write lands) so callers can construct the failure scenarios from
  DESIGN.md's operating conditions without touching this method.
  Crucially, it does **not** re-check lock possession before calling
  `write()` — a real worker has no reliable way to do that either, which
  is exactly why the resource, not the worker, has to be the one that
  catches a stale write.
- **`WorkerResult`** — `COMMITTED`, `REJECTED_STALE`, or
  `LOCK_UNAVAILABLE`, plus the fencing token and a detail string. What
  every scenario assertion in `ZkSimulate` checks against.

### `com.coordinator.sim` — exercising it under contention

- **`ZkSimulate`** is a CLI entry point with no in-memory fallback —
  ZooKeeper's watches and session semantics are exactly what's under
  test, so faking them with a local stand-in would test the wrong
  thing. Four scenarios, each described in its own Javadoc:
  `scenarioBaseline`, `scenarioHighContention`,
  `scenarioSlowButAlive` (proves automatic session heartbeating — a
  multi-second sleep with zero manual renewal still commits),
  and `scenarioSessionLossCaughtByFencing` (force-closes one worker's
  session mid-job to model a reaped dead worker, and asserts the
  fencing check — not the lock — is what stops the resulting stale
  write).

### `com.coordinator.springapp` — the same pieces, over HTTP

- **`CoordinatorConfig`** wires a `ZooKeeperLock` as a Spring-managed,
  `destroyMethod = "close"` bean (so the ZooKeeper session and any
  ephemeral znodes it still holds are cleaned up on shutdown), exposes
  it as the `EntityLockManager` bean, and separately provides a
  `ProtectedResource` bean. No new locking logic lives here — it's
  wiring, not implementation.
- **`LockController`** — `POST /lock`, `DELETE /lock/{handle}`,
  `POST /writes`, `GET /value`, all under
  `/api/entities/{entityId}`. Translates `LockNotAcquiredException` to
  `423 LOCKED` and `StaleFencingTokenException` to `409 CONFLICT` — the
  latter is the interesting one: it means the lock handle the caller
  holds is stale even though the caller doesn't know it yet, the exact
  failure mode DESIGN.md section 8 describes, now visible as an HTTP
  status instead of a simulation assertion.
- **`LockHandleRegistry`** exists only because HTTP acquire and release
  are two separate requests, so something has to hold the
  `AcquiredEntityLock` object server-side in between (see the class
  Javadoc for why this is a demo-scoped simplification, not a
  production pattern — it's in-memory, per-instance, and doesn't
  survive a restart).

## Tracing one request end to end

`POST /api/entities/acct-1/lock` →
`LockController.acquire` →
`EntityLockManager.acquire` (the `ZooKeeperLock` bean) →
creates the ephemeral sequential znode, becomes lowest sequence (or
waits) →
returns an `AcquiredZkLock` carrying the sequence number as its fencing
token →
`LockHandleRegistry.register` stores it, hands the caller an opaque
handle →
caller responds with `{handle, fencingToken}`.

`POST /api/entities/acct-1/writes` →
`LockController.write` looks up the handle →
`ProtectedResource.write(entity, lock.fencingToken(), ...)` →
either commits and returns the new value, or throws
`StaleFencingTokenException`, which the controller turns into
`409 CONFLICT`.

The CLI path (`ZkSimulate`) runs the identical middle three steps
through `JobRunner.run` instead of two HTTP calls — same
`ZooKeeperLock`, same `ProtectedResource`, same fencing check.

## Where to look for a specific claim in DESIGN.md

| DESIGN.md says... | ...look at |
|---|---|
| "the fencing token is the znode's sequence number" | `ZooKeeperLock.acquire`, the `parseSequence` call |
| "liveness is heartbeated automatically, no manual renewal" | `AcquiredZkLock.extend()` and its Javadoc |
| "only the immediate predecessor is watched" | `ZooKeeperLock.acquire`, the `zk.exists(predecessor, ...)` call |
| "the resource rejects anything not strictly greater" | `ProtectedResource.write`, the `fencingToken <= last` check |
| "a stalled worker still tries to write after being fenced out" | `ZkSimulate.scenarioSessionLossCaughtByFencing` |
| "no manual renewal call anywhere in `JobRunner`" | `JobRunner.run` — search it; there isn't one |

## Running it

See README.md for exact commands. In short: `docker-compose up -d
zookeeper`, then either `java -cp target/the-coordinator.jar
com.coordinator.sim.ZkSimulate` for the CLI scenarios, or `mvn
spring-boot:run` for the HTTP service.
