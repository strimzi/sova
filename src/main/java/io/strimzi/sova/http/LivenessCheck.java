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
 * HTTP handler that exposes a liveness health check at {@code GET /healthy}.
 *
 * <p>Always returns HTTP 200 with {@code {"status":"UP"}} when it is able to answer at all.
 * This is deliberate: a failed collection cycle must not restart the pod, because Sova keeps
 * serving the last good snapshot while collection is broken. Liveness only catches a wedged
 * JVM or exhausted HTTP threads — conditions under which this handler stops responding.
 * Collector health is reported by {@link ReadinessCheck} instead.
 *
 * <p>Threading: stateless handler — safe to call concurrently.
 */
public class LivenessCheck implements HttpHandler {
    private static final String CONTENT_TYPE = "application/json";
    private static final byte[] BODY_UP = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);

    /**
     * Handles a liveness probe request. Only {@code GET} is accepted; anything else returns 405.
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
