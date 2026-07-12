# The Coordinator — Design Note

## Assumption: the coordination service

Two coordination services are actually implemented, not just discussed:
a single, non-clustered Redis instance (`RedisLock`, via Jedis), and a
ZooKeeper ensemble (`ZooKeeperLock`, via the raw ZooKeeper client). Each
gets its own concurrency harness (`Simulate`, `ZkSimulate`). This wasn't
necessary to satisfy the assignment — either backend alone would have —
but the two make structurally different trade-offs, and demonstrating
both, honestly, says more about the judgment call than picking one and
asserting the other would also work. See "Redis vs ZooKeeper" below.

Redis is assumed single-instance: not Redis Cluster, not a multi-instance
Redlock deployment. That's a real limitation (a SPOF), named here and
revisited below, not hidden. ZooKeeper is assumed to run as a proper
odd-sized ensemble (3 or 5 nodes); the `docker-compose.yml` here runs a
single node for local development convenience only, which throws away
ZooKeeper's actual fault-tolerance advantage — see "what's cut."

Both locks are written against small, backend-appropriate contracts
rather than against a single interface that pretends the two are
interchangeable underneath: `RedisLock` against `RedisCoordinationClient`
(four TTL-key primitives — conditional-set-with-expiry, conditional-
delete, conditional-extend, atomic increment); `ZooKeeperLock` directly
against the ZooKeeper client, because its primitive (sequential ephemeral
znode + watch) isn't TTL-key-shaped and forcing it through the Redis
interface would hide the differences that matter. What they *do* share is
a thin `EntityLockManager` / `AcquiredEntityLock` contract — acquire,
release, fencing token, best-effort extend — which is exactly the level
at which the two backends can be honestly interchangeable: `JobRunner`
and both harnesses are written once against that contract.

## The guarantee

Precisely: **for a given entity key, the protected resource never applies
a write whose fencing token is not strictly greater than every token it
has already accepted for that entity.**

That is *not* "at most one worker executes the critical section at a
time" — the difference is the point of this exercise, on either backend.
Redis's lock alone only promises that at most one worker holds the lock
key at a given Redis-observed instant (`SET NX PX` to acquire, a
Lua-scripted compare-and-delete to release). ZooKeeper's lock alone only
promises that at most one worker holds the lowest-sequence ephemeral
znode at a given instant. In both cases, a worker can be descheduled —
GC pause, blocked syscall, CPU steal — for an unbounded, *unknowable*
amount of time while it still believes it holds the lock. Lock possession
is never mutual exclusion over the critical section; it's a claim about
the lock's own state, which the worker cannot reliably observe once it's
been paused.

The guarantee actually lives one layer up, at the resource, via the
fencing token — a value that only ever increases for a given entity,
regardless of how many times the lock has been acquired, expired, or
stolen. `ProtectedResource.write()` rejects anything not strictly greater
than the last accepted token. This holds independent of clock behavior,
network delay, or worker stalls, **given two assumptions**:

1. **The fencing token source is available and never silently rolls
   back.** For Redis: if it fails over to a replica that hasn't yet seen
   the latest `INCR`, the new primary can hand out a token lower than one
   already used, defeating the mechanism without anyone knowing — a
   single non-clustered instance sidesteps this by having no failover to
   roll back to, at the cost of being a SPOF. For ZooKeeper: the sequence
   number is assigned as part of the replicated log (Zab), so it survives
   leader failover as long as a quorum of the ensemble is intact — this
   assumption is *structurally* satisfied rather than sidestepped, which
   is the main correctness argument for ZooKeeper over single-instance
   Redis (see below).
2. **The fencing token is the sole authority for whether a write is
   allowed to land** — for every side effect the worker triggers, not
   just the call into `ProtectedResource`. This holds identically on
   either backend, and is exactly where it breaks; see next section.

## The failure you can't prevent at the lock

Worker A acquires the lock and fencing token 41, then stalls past its
lock's liveness window mid-critical-section — say, after it has already
sent a request to an external payment gateway. The lock is reclaimed
(TTL expiry on Redis; session expiry on ZooKeeper). Worker B acquires it,
gets token 42, does its work, writes with 42, releases. Worker A wakes up
with no awareness that time passed and tries to write with 41. The
resource correctly rejects it: `41 ≤ 42`. The ledger stays consistent —
on either backend, by the same mechanism.

But Worker A may already have caused an external side effect — the
payment gateway call — that had already fired and cannot be un-sent
through this mechanism. **That's the failure this design cannot prevent
at the lock: a non-idempotent side effect triggered by a worker who has
since been fenced out.** `ProtectedResource` only ever sees the write
attempt, not whatever the worker did on the way there. It's caught only
if the downstream system is itself idempotent, or if the fencing token is
threaded through to that call as an idempotency key, so the *external*
system does the rejecting instead of ours. Absent either, the only
backstop is reconciliation: an audit process comparing the external
system's log against `ProtectedResource`'s rejected-write log (kept for
exactly this reason) to catch what the lock couldn't. This is entirely
backend-independent — swapping Redis for ZooKeeper does not touch this
gap at all, which is itself worth naming: better coordination primitives
do not make non-idempotent downstream calls safe.

## The TTL decision

Both extremes are real and neither is free. **Too short:** a legitimate
long-running job loses its lock mid-flight; a second worker starts on the
same entity believing it has exclusivity; wasted work, and possibly a
non-idempotent side effect that already fired. **Too long:** a worker
that's actually dead holds the entity hostage for the full TTL/session
window before anyone else can make progress — an availability cost, not
a correctness one, but real.

**On the Redis path**, the fix is to decouple "how long can a legitimate
job run" from "how fast do I detect a dead worker" by making the lock
renewable (`AcquiredLock.extend()` exists for this) and heartbeating
while actively making progress: TTL only needs to cover one heartbeat
interval, not the longest possible job. The catch, stated directly:
heartbeating helps with legitimate long jobs but does nothing for "worker
stalled and doesn't know it," because a stalled worker can't heartbeat
either — that's exactly why fencing at the resource remains the real
backstop regardless. **What's actually wired up:** the Redis harness uses
a fixed TTL and `JobRunner` never calls `extend()` — the wiring exists on
`AcquiredLock` but isn't plugged into the worker loop. A stated,
deliberate cut for time (see "what's cut").

**On the ZooKeeper path, this tension is partially defused rather than
solved.** Liveness is a session property, heartbeated automatically by
the client library on its own thread — a worker that's merely slow
(blocked on I/O, doing CPU work) does not lose its lock, with no
`extend()` call anywhere, because the heartbeat isn't coupled to the
job's own execution. This was verified directly (see "Verification"): a
worker that sleeps 4 seconds with zero manual renewal still commits under
ZooKeeper, where the equivalent shape of stall under the *un-heartbeated*
Redis harness here would have lost the lock. That closes the "I forgot to
wire up heartbeating" failure mode by construction — but it does not
touch the fundamental tension. A true GC-stop-the-world pause freezes the
heartbeat thread too, so a genuinely stalled worker still loses its
session, same as Redis losing a TTL race. And ZooKeeper adds a *new*
version of the "too short" problem that Redis doesn't have: session
timeout is negotiated against ensemble-configured bounds (commonly a
handful of seconds, driven by `tickTime`), so you generally cannot push
it into the low-hundreds-of-milliseconds range this Redis harness uses.
If sub-second dead-worker detection is a hard requirement, ZooKeeper's
session model is a worse fit than a well-tuned Redis TTL, despite being
better on the failover-safety axis. "It depends" — specifically, on
whether the workload's minimum acceptable dead-worker-detection latency
is above or below a few seconds.

## Redis vs ZooKeeper

The two are not interchangeable "pick either" options; they trade
specific, nameable things against each other:

- **Fault tolerance.** Single Redis is a SPOF by this design's own
  assumption. ZooKeeper's ensemble tolerates a minority of node failures
  via quorum consensus (Zab) — but that's only realized if you actually
  run an odd-sized multi-node ensemble; the single-node ZooKeeper in this
  repo's `docker-compose.yml` is a convenience, not a demonstration of
  this property (see "what's cut").
- **Fencing-token durability across failover.** This is the sharpest
  correctness distinction. Redis's `INCR` counter can go backwards if a
  stale replica is promoted after an ungraceful failover — the open risk
  named in "the guarantee," assumption 1, and never actually closed by
  this design on the Redis side. ZooKeeper's sequence number is part of
  the replicated log itself; it cannot be handed out lower than a
  previously-issued value as long as a quorum remains intact. If "the
  fencing token must never roll back" is a hard requirement, ZooKeeper
  structurally satisfies it and single-instance Redis structurally does
  not.
- **Liveness mechanism.** Redis TTL requires an app-level heartbeat that
  someone has to remember to wire up correctly (this repo didn't).
  ZooKeeper session heartbeating is automatic, library-managed, and
  decoupled from the job's own thread — fewer ways for an engineer to get
  this wrong, at the cost of losing fine control over exactly when a
  lock's liveness check happens.
- **Minimum reclaim latency.** Redis TTL can be tuned arbitrarily low
  (this harness uses 300ms) at the cost of clock-skew/jitter risk.
  ZooKeeper session timeout has a practical floor set by the ensemble's
  `tickTime` (commonly a few seconds) — you cannot generally get
  sub-second dead-worker detection out of it.
- **Acquisition mechanism.** Redis here polls (`SET NX`, retried every
  20ms) — a named, deliberate simplification, not a strength. ZooKeeper's
  watch-on-predecessor is push-based by construction: no polling loop, no
  herd effect (only the immediate predecessor is watched, not the holder
  or the full queue).
- **Fencing-token width.** Redis's `INCR` counter is 64-bit and will not
  realistically wrap. ZooKeeper's per-parent sequence number is 32-bit;
  a single entity key that's locked roughly 2^31 times over its lifetime
  would wrap. Unlikely to matter for most workloads, but it's a real,
  nameable limitation the Redis path doesn't share, and "the guarantee"
  should be precise about it rather than silent.
- **Write latency and throughput.** Every ZooKeeper znode create/delete
  is a Zab consensus round requiring a majority ack across the ensemble —
  strictly higher latency than one round-trip to a single Redis instance.
  Under high lock churn, Redis will out-throughput ZooKeeper on the lock
  operations themselves; you're paying that latency for the durability
  guarantee above.
- **Client complexity and operational maturity.** Jedis is a thin,
  hard-to-misuse client; there's nothing to run besides Redis itself.
  Raw ZooKeeper client usage is a known footgun — watch semantics,
  `SyncConnected` vs `Expired` vs `Disconnected` session states, and
  reconnection edge cases are exactly what Curator's `InterProcessMutex`
  exists to handle correctly. `ZooKeeperLock` here hand-rolls the
  standard recipe instead of depending on Curator, specifically to keep
  the algorithm visible for review (this exercise is about reasoning
  about the primitive, not about picking a library) — but that is a real
  production trade-off, not a free simplification; see "what's cut."
- **What it's actually for.** ZooKeeper (like etcd) is a
  coordination/metadata service with correctness as its design center —
  the more "proper" tool for exactly the problem in this exercise, and
  the direction the original design note (Redis-only) pointed to as the
  fix for its own SPOF risk. Redis is an operationally simpler,
  frequently-already-present store being *reused* for coordination —
  faster and cheaper to run, but borrowing a correctness property
  (a linearizable counter) from a system not built to guarantee it.

**If I had to pick one for this specific problem** (a billing
ledger / document store / inventory record, where a rolled-back fencing
token means a silent double charge): ZooKeeper, specifically because of
the failover-durability point above — that's the one gap the Redis
design never closes without adding a second coordination system anyway
(the original design note's own "production" list said as much: "move
the fencing counter to etcd or ZooKeeper"). This repo builds it, rather
than continuing to just recommend it.

## What I'd do with more time / in production

- **Run ZooKeeper as a real multi-node ensemble** (3 or 5 nodes) and
  actually test the failover-durability claim above by killing a
  minority of nodes mid-run and confirming fencing tokens still never go
  backwards. The single-node `docker-compose.yml` here proves the
  algorithm, not the fault-tolerance property that's the whole reason to
  prefer ZooKeeper.
- **Switch to Curator's `InterProcessMutex`** (or at least its recipes)
  for production ZooKeeper usage instead of the hand-rolled
  `ZooKeeperLock` here, specifically for its handling of session
  reconnection edge cases (a `Disconnected` event is not the same as
  `Expired`, and a naive watcher can double-fire or miss events across a
  reconnect) that this simplified version does not handle.
- **Wire heartbeating into the Redis worker loop.** `extend()` exists but
  nothing calls it yet on that path — still the highest-value gap if
  Redis remains in use for latency-sensitive, short-TTL entities.
- **Propagate the fencing token downstream** as an idempotency key on any
  external call a worker makes mid-critical-section, on either backend —
  "the failure you can't prevent at the lock" has no backstop today
  beyond being named in this doc.
- **Add reconciliation.** `ProtectedResource.rejected()` exists so an
  audit job can look for evidence that a rejected writer's side effects
  landed somewhere else anyway, and alert or compensate.
- **Test against real faults, not `Thread.sleep()`/force-closed
  sessions.** Both stalled-worker scenarios fake their failure with
  application-level timing tricks; a more honest suite would use
  `kill -STOP`/`-CONT` on a real worker process and something like
  Toxiproxy for actual network delay/partition between a worker and its
  coordination backend, rather than simulating the failure in the same
  process that's supposed to be failing.
- **Run workers as separate processes,** not threads sharing one JVM
  object as "the resource," on either harness — proves the ordering
  logic but not real network reordering or a real crashed process (as
  opposed to a `.close()` call) losing a ZooKeeper session.

## Verification

Neither the original Redis code (`RedisLock`, `ProtectedResource`,
`JobRunner`, `InMemoryRedisClient`, `JedisRedisClient`) nor the newer
ZooKeeper code (`ZooKeeperLock`, `AcquiredZkLock`) has been compiled or
run directly in the environments this was built in — no JDK compiler was
available for the first, and no route to Maven Central to fetch the
ZooKeeper client jar was available for the second, on top of still having
no `javac`. Both have been reviewed line by line against their respective
client APIs (Jedis 5.x, ZooKeeper 3.9.x) from documentation and memory.

Beyond review, each locking algorithm was independently ported to Python
and actually executed, using the same fencing-check logic and the same
class of scenarios as the Java harnesses:

- `verify/verify_fencing.py` (Redis/TTL model): baseline, high
  contention, and a stalled-worker case where a worker's token is
  rejected after another worker takes over past the TTL. **20/20 runs
  passed**, including 10 runs of the stalled-worker case, each producing
  exactly one commit and one rejection with the expected tokens.
- `verify/verify_zk_fencing.py` (ZooKeeper/session model): baseline, high
  contention, a "slow but alive" worker that keeps its lock through a
  multi-second sleep with zero heartbeat calls, and a "session expired"
  case where a worker's session is force-expired mid-job and a queued
  second worker takes over via a watch. **30/30 runs passed**, including
  10 runs of the session-expiry case, each producing exactly one commit
  and one rejection with the expected sequence-number tokens.

That's real evidence both algorithms behave as claimed; it is not a
substitute for `mvn test` and running both harnesses against live
backends. Please do that before treating either path as final —
`JedisRedisClient` and `ZooKeeperLock` are the two classes whose
wire-level behavior against a live server hasn't been independently
exercised here, and the ZooKeeper session-timeout floor described above
in particular should be confirmed against whatever ensemble config is
actually used, not assumed from documentation.
