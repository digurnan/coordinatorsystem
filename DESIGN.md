# The Coordinator — Design Note

## Assumption: the coordination service

A single, non-clustered Redis instance, reached via Jedis. Not Redis Cluster, not a multi-instance Redlock deployment, not etcd/ZooKeeper. This is the simplest backend that plausibly matches the inherited note's "basically fine for months," and it keeps the exercise focused on what a TTL lock does and doesn't buy you, rather than on building consensus (the prompt explicitly says not to reimplement Raft). A single instance is also, necessarily, a single point of failure — that trade-off is named here, not hidden, and revisited under "production" below.

The lock (`RedisLock`) is written against a narrow `RedisCoordinationClient` interface — four primitives: conditional-set-with-expiry, conditional-delete, conditional-extend, atomic increment — with two implementations: `JedisRedisClient` (real Redis) and `InMemoryRedisClient` (the in-memory stand-in the assignment explicitly allows). Those four primitives are also exactly what you'd need to reproduce on etcd or ZooKeeper if the backend changed later.

## The guarantee

Precisely: **for a given entity key, the protected resource never applies a write whose fencing token is not strictly greater than every token it has already accepted for that entity.**

That is *not* the same statement as "at most one worker executes the critical section at a time" — the difference is the point of this exercise. The lock alone can only promise the weaker thing: that at most one worker holds the lock key at a given Redis-observed instant (`SET NX PX` to acquire, a Lua-scripted compare-and-delete to release, so a worker can never release a lock it no longer owns). A worker can be descheduled — GC pause, blocked syscall, CPU steal — for an unbounded, *unknowable* amount of time while it still believes it holds the lock, so lock possession is not mutual exclusion over the critical section.

The guarantee I can actually stand behind lives one layer up, at the resource: a fencing counter (`INCR`), independent of the lock key and its TTL, that only ever increases for a given entity, regardless of how many times the lock has been acquired, expired, or stolen. `ProtectedResource.write()` rejects anything not strictly greater than the last accepted token. This holds independent of clock behavior, network delay, or worker stalls, **given two assumptions**:

1. **Redis's fencing counter is available and never silently rolls back.** If Redis fails over to a replica that hasn't yet seen the latest `INCR`, the new primary can hand out a token lower than one already used, defeating the mechanism without anyone knowing. A single non-clustered instance sidesteps this by having no failover to roll back to — at the cost of being a SPOF.
2. **The fencing token is the sole authority for whether a write is allowed to land** — for every side effect the worker triggers, not just the call into `ProtectedResource`. This is exactly where it breaks; see below.

## The failure you can't prevent at the lock

Worker A acquires the lock and fencing token 41, then stalls past its TTL mid-critical-section — say, after it has already sent a request to an external payment gateway. The lock expires. Worker B acquires it, gets token 42, does its work, writes with 42, releases. Worker A wakes up with no awareness that time passed and tries to write with 41. The resource correctly rejects it: `41 ≤ 42`. The ledger stays consistent.

But Worker A may already have caused an external side effect — the payment gateway call — that had already fired and cannot be un-sent through this mechanism. **That's the failure this design cannot prevent at the lock: a non-idempotent side effect triggered by a worker who has since been fenced out.** `ProtectedResource` only ever sees the write attempt, not whatever the worker did on the way there. It's caught only if the downstream system is itself idempotent, or if the fencing token is threaded through to that call as an idempotency key, so the *external* system does the rejecting instead of ours. Absent either, the only backstop is reconciliation: an audit process comparing the external system's log against `ProtectedResource`'s rejected-write log (kept for exactly this reason) to catch what the lock couldn't.

Also worth naming: this design does not defend against Redis itself losing the fencing counter's value across an ungraceful failover (assumption 1 above). That's scoped out by assuming a single instance, and flagged as the first thing to address before this goes near multi-node Redis.

## The TTL decision

Both extremes are real and neither is free. **Too short:** a legitimate long-running job loses its lock mid-flight; a second worker starts on the same entity believing it has exclusivity; you pay for wasted work and, per above, possibly a non-idempotent side effect that already fired. **Too long:** a worker that's actually dead — not paused, gone — holds the entity hostage for the full TTL before anyone else can make progress. That's an availability cost, not a correctness one, but it's real and scales badly if customers are waiting on "the entity."

There's no single TTL value that escapes both, because "how long can a legitimate job run" and "how fast do I want to detect a dead worker" are in direct, structural tension. What I'd actually do: **decouple the two questions** by making the lock renewable rather than fixed-TTL-for-the-whole-job (`AcquiredLock.extend()` / `compareAndExpire` exist for this). Set the TTL to something short relative to *normal* job latency — a small multiple of p99, plus scheduler/network jitter headroom — not to the outlier long jobs. Have the worker heartbeat/extend the lock periodically while actively making progress. This turns "TTL must cover the longest possible job" into "TTL must cover one heartbeat interval," a much smaller and more defensible number, and a worker that's *actually* stalled — not heartbeating — still gets reaped promptly.

The catch, stated directly: heartbeating helps with legitimate long jobs but does **nothing** for the "worker stalled and doesn't know it" case, because a stalled worker also can't send a heartbeat. That's exactly why this doesn't remove the need for fencing at the resource — heartbeating narrows the TTL question, it doesn't answer the mutual-exclusion question. "The lock hasn't expired yet" is never treated as proof of exclusivity; lock expiry always means "assume someone else may now also believe they own this."

**What's actually wired up:** the harness uses a fixed TTL (a few hundred ms, for demo speed) and `JobRunner` does not call `extend()` — the heartbeat wiring exists on `AcquiredLock` but isn't plugged into the worker loop. A stated, deliberate cut for time.

## What I'd do with more time / in production

- **Wire heartbeating into the worker loop.** `extend()` exists but nothing calls it yet — the highest-value gap left.
- **Propagate the fencing token downstream** as an idempotency key on any external call a worker makes mid-critical-section, so "the failure you can't prevent at the lock" has an actual backstop instead of just being named in a doc.
- **Remove Redis as a silent SPOF** without breaking the fencing guarantee: either accept the availability hit and require `WAIT`/AOF-fsync acknowledgment before trusting an `INCR`, or move the fencing counter to something with a real linearizable log — etcd or ZooKeeper (ZK's sequential ephemeral znodes give a fencing token natively) — and keep something Redis-shaped, or nothing, for the advisory lock itself. The counter is where correctness actually rests; the lock is a scheduling hint.
- **Add reconciliation.** `ProtectedResource.rejected()` exists so an audit job can look for evidence that a rejected writer's side effects still landed somewhere else, and alert or compensate.
- **Replace polling `acquire()`** with Redis keyspace notifications or a proper wait list — under real contention, N workers polling every 20ms is wasted CPU and network for no benefit.
- **Test against real faults, not `Thread.sleep()`.** The stalled-worker scenario fakes a GC pause with a sleep; a more honest suite would use `kill -STOP`/`-CONT` on a real worker process and something like Toxiproxy for actual network delay/partition between a worker and Redis/the resource, rather than simulating both in one process.
- **Run workers as separate processes,** not threads sharing one JVM object as "the resource." The current harness proves the ordering logic is correct but doesn't exercise real network reordering between a worker and a remote resource.

## Verification

`RedisLock`, `ProtectedResource`, `JobRunner`, and `InMemoryRedisClient` have been hand-reviewed line by line against the Jedis 5.x and JUnit 5 APIs. Beyond that review, the identical algorithm — same four primitives, same strictly-greater-than fencing check, same three scenarios (baseline, high contention, stalled worker) — was independently ported to Python and actually executed: **20/20 runs passed**, including 10 runs of the stalled-worker case, each producing exactly one commit and one rejection with the expected fencing tokens (e.g., token 1 rejected against `last_accepted=2`, token 2 committed). That script lives at `verify/verify_fencing.py` and is real evidence the algorithm behaves as claimed — it is not a substitute for `mvn test`. Please run the Java suite (see README) before treating this as final; `JedisRedisClient` is the one class whose wire-level behavior against a live Redis instance hasn't been independently exercised here.
