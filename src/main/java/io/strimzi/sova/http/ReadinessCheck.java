/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.strimzi.sova.metrics.collectors.MetricsCollector;

/**
 * HTTP handler that exposes a readiness health check at {@code GET /ready}.
 *
 * <p>Returns HTTP 200 with {@code {"status":"UP"}} while the last successfully served snapshot
 * is younger than two collection intervals, or HTTP 503 with {@code {"status":"DOWN"}} once it
 * is older than that (including before the first cycle has ever succeeded). This is staleness,
 * not "did the last cycle succeed": a single failed or hung cycle still reports UP as long as
 * the previous good snapshot has not aged out, since stale metrics keep being served either way.
 *
 * <p>Threading: stateless handler backed by a volatile read in {@link MetricsCollector#isHealthy()}.
 */
public class ReadinessCheck implements HttpHandler {
    private static final String CONTENT_TYPE = "application/json";
    private static final byte[] BODY_UP = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BODY_DOWN = "{\"status\":\"DOWN\"}".getBytes(StandardCharsets.UTF_8);

    private final MetricsCollector collector;

    /**
     * Constructs the handler.
     *
     * @param collector the collector whose health state is reported
     */
    public ReadinessCheck(MetricsCollector collector) {
        this.collector = collector;
    }

    /**
     * Handles a readiness probe request. Only {@code GET} is accepted; anything else returns 405.
     *
     * @param exchange the HTTP exchange
     * @throws IOException if writing the response fails
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            boolean healthy = collector.isHealthy();
            byte[] body = healthy ? BODY_UP : BODY_DOWN;

            exchange.getResponseHeaders()
                .set("Content-Type", CONTENT_TYPE);
            exchange.sendResponseHeaders(healthy ? 200 : 503, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }
}
