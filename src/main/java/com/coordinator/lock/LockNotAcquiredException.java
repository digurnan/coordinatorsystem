package com.coordinator.lock;

/** Thrown by {@code ZooKeeperLock#acquire} (see the {@code zklock}
 * package) when the lock could not be obtained before the
 * caller-supplied timeout elapsed, or the backend failed in a way that
 * makes acquisition impossible. */
public class LockNotAcquiredException extends RuntimeException {
    public LockNotAcquiredException(String message) {
        super(message);
    }

    public LockNotAcquiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
