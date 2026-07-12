package com.coordinator.lock;

/** Thrown by {@link RedisLock#acquire} when the lock could not be
 * obtained before the caller-supplied timeout elapsed. */
public class LockNotAcquiredException extends RuntimeException {
    public LockNotAcquiredException(String message) {
        super(message);
    }
}
