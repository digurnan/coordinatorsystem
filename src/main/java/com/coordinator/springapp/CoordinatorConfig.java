package com.coordinator.springapp;

import com.coordinator.lock.EntityLockManager;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.zklock.ZooKeeperLock;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the existing, backend-agnostic coordination classes as Spring
 * beans. Nothing here is new locking logic -- {@link ZooKeeperLock} and
 * {@link ProtectedResource} are the exact same classes {@code
 * ZkSimulate} runs; this just hands them to Spring as singletons instead
 * of constructing them in a {@code main} method.
 *
 * <p>{@link ZooKeeperLock} owns a live ZooKeeper session for the
 * lifetime of the bean, so it's declared as an {@code AutoCloseable}
 * bean -- Spring calls {@code close()} on context shutdown, which
 * releases any ephemeral znodes this instance still holds.
 */
@Configuration
public class CoordinatorConfig {

    @Bean(destroyMethod = "close")
    public ZooKeeperLock zooKeeperLock(
            @Value("${coordinator.zookeeper.connect-string:localhost:2181}") String connectString,
            @Value("${coordinator.zookeeper.session-timeout-millis:6000}") int sessionTimeoutMillis)
            throws Exception {
        return new ZooKeeperLock(connectString, sessionTimeoutMillis);
    }

    @Bean
    public EntityLockManager entityLockManager(ZooKeeperLock zooKeeperLock) {
        return zooKeeperLock;
    }

    @Bean
    public ProtectedResource protectedResource() {
        return new ProtectedResource();
    }
}
