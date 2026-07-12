package com.coordinator.resource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongUnaryOperator;

/**
 * Stand-in for the shared external resource from the prompt -- a billing
 * ledger, a document store, an inventory record.
 *
 * <p>This is the piece the inherited design note never mentions: the
 * lock does not keep this resource consistent. This resource does, by
 * enforcing that fencing tokens strictly increase per entity and
 * refusing anything else -- regardless of what the caller believes about
 * its own lock status. A worker's lock only ever grants permission to
 * <em>attempt</em> a write; this class is the arbiter that actually
 * enforces ordering.
 */
public class ProtectedResource {

    private final Object mutex = new Object();
    private final Map<String, Long> lastAcceptedToken = new ConcurrentHashMap<>();
    private final Map<String, Long> state = new ConcurrentHashMap<>();
    private final List<WriteRecord> log = new ArrayList<>();
    private final List<WriteRecord> rejectedLog = new ArrayList<>();

    /**
     * Applies {@code mutate} to the current value for {@code entity} and
     * commits it, but only if {@code fencingToken} is strictly greater
     * than the last token accepted for this entity. Otherwise throws
     * {@link StaleFencingTokenException} and leaves state untouched.
     */
    public long write(String entity, long fencingToken, String writer, LongUnaryOperator mutate) {
        synchronized (mutex) {
            long last = lastAcceptedToken.getOrDefault(entity, 0L);
            if (fencingToken <= last) {
                rejectedLog.add(new WriteRecord(entity, fencingToken, writer, -1, log.size() + rejectedLog.size()));
                throw new StaleFencingTokenException(
                        "entity=" + entity + " writer=" + writer + " token=" + fencingToken
                                + " <= last_accepted=" + last);
            }
            long oldValue = state.getOrDefault(entity, 0L);
            long newValue = mutate.applyAsLong(oldValue);
            state.put(entity, newValue);
            lastAcceptedToken.put(entity, fencingToken);
            log.add(new WriteRecord(entity, fencingToken, writer, newValue, log.size() + rejectedLog.size()));
            return newValue;
        }
    }

    public long value(String entity) {
        synchronized (mutex) {
            return state.getOrDefault(entity, 0L);
        }
    }

    public List<WriteRecord> log(String entity) {
        synchronized (mutex) {
            List<WriteRecord> out = new ArrayList<>();
            for (WriteRecord r : log) {
                if (entity == null || r.entity.equals(entity)) out.add(r);
            }
            return out;
        }
    }

    public List<WriteRecord> rejected(String entity) {
        synchronized (mutex) {
            List<WriteRecord> out = new ArrayList<>();
            for (WriteRecord r : rejectedLog) {
                if (entity == null || r.entity.equals(entity)) out.add(r);
            }
            return out;
        }
    }
}
