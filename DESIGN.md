# The Coordinator — Design Note

## Assumption: what "the coordination service" is

I assumed a **single, non-clustered Redis instance**, reached over a normal
client connection (`JedisRedisClient`, using Jedis). Not Redis Cluster, not a
multi-instance Redlock deployment, not etcd/ZooKeeper.

I picked this because it's the simplest thing that could plausibly match the
inherited note's "we've been running this for months and it's basically
fine" — and because the exercise is explicit that it wants reasoning about
what a TTL lock does and doesn't give you, not a consensus system ("we're
not asking you to reimplement Raft"). A single Redis instance is also,
necessarily, a single point of failure. I'm naming that trade-off here
rather than hiding it; it comes back under "production" below.

The lock/coordination component (`RedisLock`) is written against a small
`RedisCoordinationClient` interface, not directly against Jedis, with two
implementations: `JedisRedisClient` (real Redis — the intended coordination
service) and `InMemoryRedisClient` (the in-memory stand-in the assignment
explicitly allows, used to run the test harness with zero external
infrastructure). Both implement the same four primitives: conditional-set-
with-expiry, conditional-delete, conditional-extend, and an atomic counter.
That list is deliberately short — it's exactly what the guarantee below
depends on, and it's the same four primitives you'd need to reproduce on
etcd or ZooKeeper if you swapped the backend later.

## The guarantee

Precisely: **for a given entity key, the protected resource never applies a
write whose fencing token is not strictly greater than every fencing token
it has already accepted for that entity.**

That is *not* the same statement as "at most one worker executes the
critical section at a time," and the difference is the entire point of this
exercise. The lock alone can promise the weaker thing — that at most one
worker *holds the lock key* at any Redis-observed instant, via `SET NX PX`
to acquire and a Lua-scripted compare-and-delete to release (so a worker
can never release a lock it no longer owns, the classic bug in a naive
"just `DEL` on release" implementation). But lock possession is not
mutual exclusion over the critical section, because a worker can be
descheduled — GC pause, blocked syscall, CPU steal — for an unbounded,
*unknowable* amount of time while it still believes it holds the lock.

So the guarantee I can actually stand behind lives one layer up, at the
resource, via the fencing token: a counter (`INCR`) that is separate from
the lock key, has no TTL of its own, and only ever increases for a given
entity, regardless of how many times the lock on that entity has been
acquired, expired, or stolen. `ProtectedResource.write()` rejects anything
that isn't strictly greater than the last accepted token. This holds
independent of lock correctness, clock behavior, network delay, or worker
stalls, **given two assumptions**:

1. **Redis's fencing counter is available and not silently rolled back.**
   If Redis fails over to a replica that hasn't yet seen the latest `INCR`,
   the new primary can hand out a token that's *lower* than one already
   used, and the whole mechanism is defeated without anyone knowing. A
   single non-clustered instance sidesteps this by having no failover to
   roll back to — at the cost of being a SPOF (see "production").
2. **The fencing token is the sole authority for whether a write is
   allowed to land.** This has to hold for every side effect the worker
   triggers, not just the call into `ProtectedResource`. See the next
   section — this is exactly where it breaks.

## The failure you can't prevent at the lock

Worst case, concretely: Worker A acquires the lock and fencing token 41,
then stalls past its TTL doing "the work" — say, calling an external
payment gateway partway through the critical section. The lock expires.
Worker B acquires the lock, gets token 42, does its work, writes to the
resource with token 42, releases. Worker A now wakes up with **no
awareness that time passed** (per the prompt's own framing) and finishes
what it was doing — including any external call that had *already been
sent* before it woke up. It then tries to write to `ProtectedResource`
with token 41. The resource correctly rejects it: `41 <= 42`.

So the ledger write is safe. But Worker A may have already caused an
external side effect — the payment gateway call — that already fired and
cannot be un-sent through this mechanism. **That's the failure this design
cannot prevent at the lock: a non-idempotent side effect triggered by a
worker who has since been fenced out.** It isn't caught by
`ProtectedResource` at all, because `ProtectedResource` only sees the
*write attempt*, not whatever the worker did on the way there. It's caught
only if:

- the downstream system is itself idempotent, or
- the fencing token (or some other proof-of-freshness) is threaded through
  to that downstream call as an idempotency key, so the *external* system
  does the rejecting instead of ours.

If neither is true, there's no way to prevent this at the lock layer — this
is exactly the "double charge" scenario named in the prompt, and the real
fix is not a better lock, it's pushing the fencing token as far downstream
as the money actually moves. Absent that, the only backstop is
reconciliation: an audit process comparing the external system's log
against `ProtectedResource`'s rejected-write log (which this repo keeps,
for exactly this reason) to catch what the lock couldn't.

Also worth naming directly: this design doesn't defend against Redis
itself losing the fencing counter's value across an ungraceful failover
(see assumption 1 above). I'm not solving that here — I'm scoping it out by
assuming a single instance, and flagging it as the first thing that needs
addressing before this goes anywhere near multi-node Redis.

## The TTL decision

Both extremes are real and neither is free:

- **Too short:** a legitimate long-running job gets its lock stolen
  mid-flight. Another worker sees the entity "available," starts working
  on it, and now two workers believe — at the lock layer — that they have
  exclusive access simultaneously. The fencing token still protects the
  resource, but you've paid for wasted work and, per the section above,
  possibly a non-idempotent side effect that already fired.
- **Too long:** a worker that's actually dead (not paused, gone) holds the
  entity hostage for the full TTL before anyone else can make progress.
  This is an availability cost, not a correctness one — but it's still
  real, and it scales badly if "the entity" is something customers are
  waiting on.

There is no single TTL value that avoids both, because "how long can a
legitimate job run" and "how fast do I want to detect a dead worker" are
in direct, structural tension. Concretely, what I'd do:

**Decouple the two questions** by making the lock renewable rather than
fixed-TTL-for-the-whole-job (`AcquiredLock.extend()` / `compareAndExpire`
exist in this repo for exactly this). Set the TTL to something short
relative to *normal* job latency — a small multiple of p99 for the typical
case, plus scheduler/network jitter headroom — not to the outlier long
jobs. Have the worker heartbeat/extend the lock periodically while it's
actively making progress. This turns "TTL must cover the longest possible
job" into "TTL must cover one heartbeat interval," which is a much smaller
and more defensible number, and it means a worker that's *actually*
stalled — not heartbeating — still gets reaped promptly, which one large
fixed TTL can never give you.

The catch, and I want to be explicit about it rather than wave past it:
heartbeating helps with legitimate long jobs, but it does **nothing** for
the "worker stalled and doesn't know it" case, because a stalled worker
also can't send a heartbeat. That's exactly why this doesn't remove the
need for fencing at the resource — heartbeating narrows the TTL question,
it doesn't answer the mutual-exclusion question. I never treat "the lock
hasn't expired yet" as proof of exclusivity; I treat lock expiry as "assume
someone else may now also believe they own this," always.

**What this repo actually does:** the harness uses a fixed TTL (a few
hundred ms, for demo speed) and `JobRunner` does not call `extend()` — the
heartbeat wiring exists on `AcquiredLock` but isn't plugged into the worker
loop. That's a stated, deliberate cut for time; see below.

## What I'd do with more time / in production

- **Wire heartbeating into the worker loop.** `extend()` exists but nothing
  calls it yet — this is the highest-value thing left undone.
- **Propagate the fencing token downstream** as an idempotency key to any
  external call a worker makes mid-critical-section, so "the failure you
  can't prevent at the lock" has an actual backstop instead of just being
  named in a doc.
- **Remove Redis as a silent SPOF** without breaking the fencing guarantee:
  either accept the availability hit and require `WAIT`/AOF-fsync
  acknowledgment before trusting an `INCR`, or move the fencing counter
  specifically to something with a real linearizable log — I'd lean
  etcd or ZooKeeper for the counter (ZK's sequential ephemeral znodes give
  you a fencing token natively), and keep something Redis-shaped, or
  nothing, for the advisory lock itself. The counter is the piece
  correctness actually rests on; the lock is just a scheduling hint.
- **Add reconciliation.** `ProtectedResource.rejected()` exists so an audit
  job can look for evidence that a rejected writer's side effects still
  landed somewhere else, and alert or compensate.
- **Replace polling `acquire()`** with Redis keyspace notifications or a
  proper wait list — under real contention, N workers all polling every
  20ms is wasted CPU and network for no benefit.
- **Test against real faults, not `Thread.sleep()`.** The stalled-worker
  scenario in `Simulate` fakes a GC pause with a sleep; a more honest test
  suite would use `kill -STOP`/`-CONT` on a real worker process, and
  something like Toxiproxy to inject actual network delay/partition
  between a worker and Redis/the resource, rather than simulating both in
  one process.
- **Run workers as separate processes,** not threads sharing one JVM
  object as "the resource." The current harness proves the ordering logic
  is correct, but it doesn't exercise real network reordering between a
  worker and a remote resource — everything here is in-process for
  speed and simplicity.

## One more thing worth naming: how this was actually built and checked

The environment I built this in had **no outbound network access at all**
(Maven Central, PyPI, npm, and github.com were all blocked at the proxy
level) and **no JDK compiler** — only a JRE. That's why the design leans on
a narrow `RedisCoordinationClient` interface with an in-memory
implementation: it let me actually exercise the algorithm's logic somehow,
even though in the end I couldn't compile *any* of it myself in that
environment (no `javac`). I want to be direct about that rather than imply
a level of verification that didn't happen: this code has been carefully
hand-reviewed against the Jedis and JUnit APIs from memory, but the `mvn
test` / `javac` path has not actually been executed by me. Please run it
before treating this as done — see README for exact commands. If I'd had
a working toolchain, closing that gap would have been the first thing I
did, ahead of everything in the list above.
