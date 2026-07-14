package com.coordinator.springapp;

import com.coordinator.lock.AcquiredEntityLock;
import com.coordinator.lock.EntityLockManager;
import com.coordinator.lock.LockNotAcquiredException;
import com.coordinator.resource.ProtectedResource;
import com.coordinator.resource.StaleFencingTokenException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP surface over the same lock-then-write flow {@code JobRunner}
 * runs in the CLI harnesses: acquire a lock on an entity, get back a
 * fencing token, attempt a write against {@link ProtectedResource} with
 * that token, release. See DESIGN.md for what the fencing token
 * actually guarantees and what it doesn't.
 */
@RestController
@RequestMapping("/api/entities/{entityId}")
public class LockController {

    private final EntityLockManager lockManager;
    private final ProtectedResource protectedResource;
    private final LockHandleRegistry handles;

    public LockController(EntityLockManager lockManager, ProtectedResource protectedResource,
                           LockHandleRegistry handles) {
        this.lockManager = lockManager;
        this.protectedResource = protectedResource;
        this.handles = handles;
    }

    @PostMapping("/lock")
    public ResponseEntity<?> acquire(
            @PathVariable String entityId,
            @RequestParam(defaultValue = "5000") long timeoutMillis) {
        try {
            AcquiredEntityLock lock = lockManager.acquire(entityId, timeoutMillis);
            String handle = handles.register(lock);
            return ResponseEntity.ok(new AcquireLockResponse(handle, lock.fencingToken()));
        } catch (LockNotAcquiredException e) {
            return ResponseEntity.status(HttpStatus.LOCKED).body(new ErrorResponse(e.getMessage()));
        }
    }

    @DeleteMapping("/lock/{handle}")
    public ResponseEntity<?> release(@PathVariable String entityId, @PathVariable String handle) {
        AcquiredEntityLock lock = handles.get(handle);
        if (lock == null || !lock.entityKey().equals(entityId)) {
            return ResponseEntity.notFound().build();
        }
        lock.release();
        handles.remove(handle);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/writes")
    public ResponseEntity<?> write(@PathVariable String entityId, @RequestBody WriteRequest request) {
        AcquiredEntityLock lock = handles.get(request.handle);
        if (lock == null || !lock.entityKey().equals(entityId)) {
            return ResponseEntity.badRequest().body(new ErrorResponse("unknown or mismatched lock handle"));
        }
        try {
            long newValue = protectedResource.write(entityId, lock.fencingToken(), request.handle,
                    old -> old + request.amount);
            return ResponseEntity.ok(new WriteResponse(newValue));
        } catch (StaleFencingTokenException e) {
            // The interesting response: the lock handle is still "held"
            // as far as this registry knows, but the resource has
            // already accepted a fresher token for this entity -- e.g.
            // this handle's session/TTL lapsed and someone else moved
            // on. See DESIGN.md, "consistency guarantee."
            return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(e.getMessage()));
        }
    }

    @GetMapping("/value")
    public ResponseEntity<WriteResponse> value(@PathVariable String entityId) {
        return ResponseEntity.ok(new WriteResponse(protectedResource.value(entityId)));
    }
}
