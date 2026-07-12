package com.coordinator.sim;

import com.coordinator.redisclient.InMemoryRedisClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the same concurrency scenarios as the {@code Simulate} CLI,
 * asserting each one leaves the protected resource consistent. Uses the
 * in-memory coordination client so this suite runs with no external
 * infrastructure; {@code JedisRedisClientIntegrationTest} exercises the
 * identical RedisLock/ProtectedResource/JobRunner code against a real
 * Redis instance.
 */
class SimulationScenariosTest {

    @Test
    void baselineScenarioStaysConsistent() throws Exception {
        assertTrue(Simulate.scenarioBaseline(new InMemoryRedisClient()));
    }

    @Test
    void highContentionScenarioStaysConsistent() throws Exception {
        assertTrue(Simulate.scenarioHighContention(new InMemoryRedisClient()));
    }

    @Test
    void stalledWorkerIsCaughtByFencingToken() throws Exception {
        assertTrue(Simulate.scenarioStalledWorkerCaughtByFencing(new InMemoryRedisClient()));
    }
}
