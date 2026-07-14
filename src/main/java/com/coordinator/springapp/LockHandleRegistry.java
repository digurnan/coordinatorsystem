package com.coordinator.springapp;

import com.coordinator.lock.AcquiredEntityLock;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps an opaque handle, returned to the HTTP caller on acquire, to the
 * {@link AcquiredEntityLock} it represents.
 *
 * <p>This exists because acquire and release are two separate HTTP
 * requests, and an {@code AcquiredEntityLock} isn't something you can
 * hand back and forth in a request body -- something has to hold it
 * server-side in between. That's a simplification specific to
 * demonstrating the mechanism over HTTP: a real service built on this
 * library would typically hold the lock for the lifetime of one request
 * or one job, not expose acquire and release as independent calls a
 * caller has to remember to pair up. It's also in-memory and per-
 * instance, so it doesn't survive a restart or work across multiple
 * instances of this service -- fine for a demo, not for production.
 */
@Component
public class LockHandleRegistry {

    private final Map<String, AcquiredEntityLock> handles = new ConcurrentHashMap<>();

    public String register(AcquiredEntityLock lock) {
        String handle = UUID.randomUUID().toString();
        handles.put(handle, lock);
        return handle;
    }

    public AcquiredEntityLock get(String handle) {
        return handles.get(handle);
    }

    public void remove(String handle) {
        handles.remove(handle);
    }
}
