package com.coordinator.resource;

/** One accepted (or rejected) write attempt, kept for the simulation
 * harness / an auditor to inspect. */
public final class WriteRecord {
    public final String entity;
    public final long fencingToken;
    public final String writer;
    public final long newValue; // -1 for rejected records
    public final int seq;

    public WriteRecord(String entity, long fencingToken, String writer, long newValue, int seq) {
        this.entity = entity;
        this.fencingToken = fencingToken;
        this.writer = writer;
        this.newValue = newValue;
        this.seq = seq;
    }

    @Override
    public String toString() {
        return "[" + seq + "] " + writer + " token=" + fencingToken
                + (newValue >= 0 ? " -> " + newValue : " (rejected)");
    }
}
