package com.coordinator.resource;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedResourceTest {

    @Test
    void acceptsIncreasingTokens() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 1, "w1", v -> v + 1);
        res.write("e1", 2, "w2", v -> v + 1);
        assertEquals(2, res.value("e1"));
    }

    @Test
    void rejectsStaleToken() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 5, "w1", v -> v + 100);
        assertThrows(StaleFencingTokenException.class,
                () -> res.write("e1", 3, "w2", v -> v + 1));
        assertEquals(100, res.value("e1"), "stale write must not have applied");
    }

    @Test
    void rejectsExactTokenReplay() {
        ProtectedResource res = new ProtectedResource();
        res.write("e1", 7, "w1", v -> v + 1);
        assertThrows(StaleFencingTokenException.class,
                () -> res.write("e1", 7, "w1-retry", v -> v + 1));
    }
}
