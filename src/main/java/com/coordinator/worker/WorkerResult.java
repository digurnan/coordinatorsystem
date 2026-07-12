package com.coordinator.worker;

public final class WorkerResult {

    public enum Outcome {
        COMMITTED,
        REJECTED_STALE,
        LOCK_UNAVAILABLE
    }

    public final String workerId;
    public final Outcome outcome;
    public final Long fencingToken;
    public final String detail;

    public WorkerResult(String workerId, Outcome outcome, Long fencingToken, String detail) {
        this.workerId = workerId;
        this.outcome = outcome;
        this.fencingToken = fencingToken;
        this.detail = detail;
    }

    @Override
    public String toString() {
        return workerId + " -> " + outcome
                + (fencingToken != null ? " (token=" + fencingToken + ")" : "")
                + (detail != null && !detail.isEmpty() ? " [" + detail + "]" : "");
    }
}
