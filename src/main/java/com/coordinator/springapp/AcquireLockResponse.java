package com.coordinator.springapp;

public class AcquireLockResponse {

    public String handle;
    public long fencingToken;

    public AcquireLockResponse(String handle, long fencingToken) {
        this.handle = handle;
        this.fencingToken = fencingToken;
    }
}
