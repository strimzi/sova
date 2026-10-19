/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricsRegistry;
import io.strimzi.sova.metrics.collectors.MetricsCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ReadinessCheck}.
 *
 * <p>Tests the HTTP handler directly via a minimal stub — no running server or Mockito required.
 */
class ReadinessCheckTest {

    @Test
    void reportsUpWhenCollectorHealthy() throws Exception {
        StubExchange exchange = new StubExchange("GET", "/ready");
        new ReadinessCheck(new StubCollector(true)).handle(exchange);

        assertEquals(200, exchange.responseCode);
        assertTrue(exchange.capturedBody().contains("UP"));
    }

    @Test
    void reportsDownWhenCollectorUnhealthy() throws Exception {
        StubExchange exchange = new StubExchange("GET", "/ready");
        new ReadinessCheck(new StubCollector(false)).handle(exchange);

        assertEquals(503, exchange.responseCode);
        assertTrue(exchange.capturedBody().contains("DOWN"));
    }

    @Test
    void reportsDownBeforeFirstCollection() throws Exception {
        StubExchange exchange = new StubExchange("GET", "/ready");
        new ReadinessCheck(new MetricsCollector(
                null, new MetricsRegistry(), SovaConfig.forTesting("localhost:9092"))).handle(exchange);

        assertEquals(503, exchange.responseCode);
    }

    @Test
    void rejectsNonGetMethod() throws Exception {
        StubExchange exchange = new StubExchange("POST", "/ready");
        new ReadinessCheck(new StubCollector(true)).handle(exchange);

        assertEquals(405, exchange.responseCode);
    }

    /** Minimal MetricsCollector stand-in that returns a fixed health state. */
    private static final class StubCollector extends MetricsCollector {

        private final boolean healthy;

        StubCollector(boolean healthy) {
            super(null, null, null);
            this.healthy = healthy;
        }

        @Override
        public boolean isHealthy() {
            return healthy;
        }
    }
}
