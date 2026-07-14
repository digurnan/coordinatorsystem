# The Coordinator

A distributed lock with fencing tokens, and a protected resource that
actually enforces them — built for a worker fleet where two workers
touching the same entity at once (an account, a document, an inventory
record) means real corruption: double charges, lost writes, duplicated
shipments.

- **[DESIGN.md](DESIGN.md)** — why ZooKeeper, not Redis: four candidate
  coordination services weighed against explicit requirements, the
  precise guarantee this design makes, the failure it can't prevent, and
  the timeout trade-off.
- **[IMPLEMENTATION.md](IMPLEMENTATION.md)** — a tour of the code
  itself: what each package does, how one request flows end to end, and
  where in the code each claim in DESIGN.md is actually made true.

## The short version

A lock with a TTL gives you *advisory* mutual exclusion, not a
correctness guarantee — a worker can stall past its TTL, lose the lock,
and keep going with no idea time passed. This project treats the lock
as "permission to attempt a write," and adds a **fencing token**: a
value, independent of the lock's own liveness mechanism, that the
*protected resource* uses to reject any write whose token isn't
strictly greater than the last one it accepted. The lock provides
liveness (someone always gets in eventually); the resource, via the
fencing token, provides the actual safety property.

**ZooKeeper is the implementation** (`ZooKeeperLock`): a session-scoped
ephemeral sequential znode; the fencing token is the sequence number the
ensemble assigns the znode natively — no separate counter needed, and
it cannot roll back on failover, because it's part of ZooKeeper's own
replicated log. Liveness is heartbeated automatically by the client
library rather than requiring an app-level TTL renewal.

Single-instance Redis, Redis Cluster/Sentinel, and Redlock are discussed
in DESIGN.md as evaluated, rejected baselines — see there for why. No
code for those paths lives in this repository.

## Layout

```
src/main/java/com/coordinator/
  lock/          EntityLockManager / AcquiredEntityLock -- backend-
                 agnostic contract ZooKeeperLock implements
                   LockNotAcquiredException
  zklock/        ZooKeeperLock, AcquiredZkLock -- sequential ephemeral
                 znode lock, native fencing via the assigned sequence
                 number
  resource/      ProtectedResource (fencing enforcement), WriteRecord,
                 StaleFencingTokenException
  worker/        JobRunner -- simulates one worker's acquire/work/write/
                 release cycle, with injectable stall/network-delay
  sim/           ZkSimulate -- CLI entry point, several concurrency
                 scenarios against a real ensemble
  springapp/     Small Spring Boot service exposing lock/write over
                 HTTP, backed by ZooKeeperLock -- same
                 EntityLockManager/ProtectedResource underneath
src/main/resources/
  application.yml   Spring config: ZooKeeper connect string and session
                    timeout
src/test/java/...  JUnit 5 suite covering ProtectedResource's fencing
                    enforcement
verify/          Reference Python port of the locking algorithm, used to
                 pressure-test the fencing logic independently of the
                 Java implementation
DESIGN.md          The design note -- why ZooKeeper, the guarantee, the limits
IMPLEMENTATION.md  A tour of the code itself
docker-compose.yml  Local ZooKeeper ensemble for the real-backend paths
```

## Running it

**With Maven, against a real ZooKeeper ensemble:**

```bash
docker-compose up -d zookeeper
mvn test                                     # unit tests, no ensemble needed

mvn package
java -cp target/the-coordinator.jar com.coordinator.sim.ZkSimulate --zk-host localhost:2181
```

**Spring Boot service (HTTP), backed by ZooKeeper:**

```bash
docker-compose up -d zookeeper
mvn spring-boot:run

curl -X POST 'localhost:8080/api/entities/acct-1/lock'
# => {"handle":"...","fencingToken":1}

curl -X POST 'localhost:8080/api/entities/acct-1/writes' \
    -H 'Content-Type: application/json' \
    -d '{"handle":"<handle from above>","amount":100}'
# => {"value":100}

curl -X DELETE 'localhost:8080/api/entities/acct-1/lock/<handle>'
```

## Scenarios the harness runs

**`ZkSimulate`, against a real ensemble:**

1. **Baseline** — 20 workers, light contention, sanity check on
   ordering.
2. **High contention** — 50 workers, heavy contention; checks the lock
   still serializes correctly under load.
3. **Slow but alive** — a worker sleeps well past what would sink a
   TTL-based lock, but keeps its lock the whole time with no manual
   heartbeat call anywhere in the code, because ZooKeeper's session
   heartbeat runs independently of the worker's own thread.
4. **Session loss caught by fencing** — worker A's session is
   force-closed mid-job (modeling the ensemble reaping a dead session);
   worker B, already queued via a watch, takes over; A wakes later and
   is rejected by the fencing check.

See DESIGN.md, "Known limitation," for the case even fencing doesn't
catch.
