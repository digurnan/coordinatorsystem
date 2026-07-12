# The Coordinator

A Redis-backed distributed lock with fencing tokens, a protected resource
that enforces them, and a concurrency simulation harness — built for the
"Coordinator" take-home exercise. **Start with [DESIGN.md](DESIGN.md)** —
that's the part that matters as much as the code.

## The short version

A lock with a TTL (`SET key val NX PX ttl`) gives you *advisory* mutual
exclusion, not a correctness guarantee — a worker can stall past its TTL,
lose the lock, and keep going with no idea time passed. This project treats
the lock as "permission to attempt a write," and adds a **fencing token**: a
monotonic counter, independent of the lock's TTL, that the *protected
resource* uses to reject any write whose token isn't strictly greater than
the last one it accepted. The lock provides liveness (someone always gets
in eventually); the resource, via the fencing token, provides the actual
safety property.

## Layout

```
src/main/java/com/coordinator/
  redisclient/   RedisCoordinationClient interface + JedisRedisClient
                 (real Redis) + InMemoryRedisClient (in-memory stand-in)
  lock/          RedisLock, AcquiredLock, LockNotAcquiredException
  resource/      ProtectedResource (fencing enforcement), WriteRecord,
                 StaleFencingTokenException
  worker/        JobRunner — simulates one worker's acquire/work/write/
                 release cycle, with injectable stall/network-delay
  sim/           Simulate — CLI harness, 3 concurrency scenarios
  selftest/      SelfTest — zero-dependency verification (see below)
src/test/java/...  JUnit 5 suite (mirrors SelfTest, plus a real-Redis
                    integration test)
DESIGN.md        The required design note
docker-compose.yml  Local Redis for the real-Redis path
```

## Running it

**With Maven + real Redis (the intended path):**

```bash
docker-compose up -d
mvn test                                    # unit tests (in-memory) +
                                             #   integration tests if
                                             #   REDIS_HOST=localhost is set
REDIS_HOST=localhost mvn test                # include the real-Redis
                                             #   integration tests

mvn package
java -jar target/the-coordinator.jar --redis-host localhost --redis-port 6379
```

**Zero-dependency path (no Maven, no Redis, no network):**

```bash
mkdir -p out
javac -d out $(find src/main/java -name '*.java')
java -cp out com.coordinator.selftest.SelfTest     # unit-style checks
java -cp out com.coordinator.sim.Simulate          # same 3 scenarios, in-memory
```

## An honest note on verification

This project was built in a sandboxed environment with **no outbound
network access** (Maven Central, npm, PyPI, and github.com were all
blocked at the proxy level) and **no JDK compiler** — only a `java`
runtime, no `javac`. That shaped two decisions:

1. The core logic (`RedisLock`, `ProtectedResource`, `JobRunner`) is
   written against a small `RedisCoordinationClient` interface rather than
   directly against Jedis, with an `InMemoryRedisClient` implementation —
   the "in-memory stand-in" the assignment explicitly allows. This is a
   legitimate design choice on its own merits (testability), independent
   of the environment constraint.
2. `SelfTest` is a hand-rolled, zero-dependency test runner alongside the
   normal JUnit suite, so the algorithm *could* be exercised somehow in
   an environment with no Maven and no `javac` for the primary toolchain.

Even so: **I was not able to compile or run any of this myself**, because
there was no `javac` available at all, not even for the dependency-free
path. Everything here has been carefully hand-reviewed line by line against
the Jedis 5.x and JUnit 5 APIs, but it has not been build-verified. Please
run the commands above on a machine with a real JDK before treating this as
final — if something doesn't compile, it's most likely a small Jedis API
mismatch in `JedisRedisClient`, since that's the one class I couldn't
cross-check against an actual dependency jar.

## Scenarios the harness runs

1. **Baseline** — 20 workers, light contention, sanity check on ordering.
2. **High contention** — 50 workers, short TTL, heavy contention; checks
   the lock still serializes correctly under load.
3. **Stalled worker** — the scenario the inherited design note never
   tested. One worker stalls past its TTL (simulating a GC pause), loses
   the lock to a second worker, then "wakes up" and tries to write anyway.
   Demonstrates that the resource — not the lock — is what actually
   prevents the double write. See DESIGN.md, "The failure you can't
   prevent at the lock," for the case even this doesn't catch.
