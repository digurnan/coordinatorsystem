package com.coordinator.resource;

/**
 * Thrown when a write arrives with a fencing token that is not strictly
 * greater than the highest token already accepted for that entity --
 * i.e., this writer's permission to touch the entity was, from the
 * resource's point of view, already superseded by someone else.
 */
public class StaleFencingTokenException extends RuntimeException {
    public StaleFencingTokenException(String message) {
        super(message);
    }
}
