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

/**
 * HTTP handler that exposes a readiness health check at {@code GET /ready}.
 *
 * <p>Always returns HTTP 200 with {@code {"status":"UP"}} when it is able to answer at all: the
 * operator blocks Kafka reconciliation on Sova's readiness, so a broken collection must not turn
 * it DOWN. How stale the exported numbers are is a metrics concern, read from
 * {@code sova_last_successful_collection_timestamp_seconds}, not a probe (see invariant 9 in
 * {@code AGENTS.md}).
 *
 * <p>Threading: stateless handler, safe to call concurrently.
 */
public class ReadinessCheck implements HttpHandler {
    private static final String CONTENT_TYPE = "application/json";
    private static final byte[] BODY_UP = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);

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

            exchange.getResponseHeaders()
                .set("Content-Type", CONTENT_TYPE);
            exchange.sendResponseHeaders(200, BODY_UP.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(BODY_UP);
            }
        }
    }
}
