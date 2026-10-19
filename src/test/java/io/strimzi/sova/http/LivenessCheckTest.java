/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LivenessCheck}.
 */
class LivenessCheckTest {

    @Test
    void alwaysReportsUp() throws Exception {
        StubExchange exchange = new StubExchange("GET", "/healthy");
        new LivenessCheck().handle(exchange);

        assertEquals(200, exchange.responseCode);
        assertTrue(exchange.capturedBody().contains("UP"));
    }

    @Test
    void rejectsNonGetMethod() throws Exception {
        StubExchange exchange = new StubExchange("POST", "/healthy");
        new LivenessCheck().handle(exchange);

        assertEquals(405, exchange.responseCode);
    }
}
