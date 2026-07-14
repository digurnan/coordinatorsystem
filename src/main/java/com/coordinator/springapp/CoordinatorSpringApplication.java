package com.coordinator.springapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A small Spring Boot service exposing the coordination library over
 * HTTP, backed by {@link com.coordinator.zklock.ZooKeeperLock} (see
 * {@code application.yml} for the ZooKeeper connect string and session
 * timeout).
 *
 * <p>Same lock/fence/write flow as {@code ZkSimulate}, just driven over
 * HTTP instead of an in-process CLI harness -- see {@link
 * CoordinatorConfig} for how the beans are wired and {@link
 * LockController} for the endpoints.
 *
 * <p>Run with: {@code mvn spring-boot:run} (requires a reachable
 * ZooKeeper ensemble -- {@code docker-compose up -d zookeeper}).
 */
@SpringBootApplication
public class CoordinatorSpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoordinatorSpringApplication.class, args);
    }
}
