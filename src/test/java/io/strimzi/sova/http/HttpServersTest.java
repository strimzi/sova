/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import io.strimzi.sova.metrics.collectors.MetricsCollector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the two HTTP listeners: resource-limit hardening, endpoint isolation, and probe behaviour.
 */
class HttpServersTest {
    private static final String UP = "{\"status\":\"UP\"}";
    private static final String DOWN = "{\"status\":\"DOWN\"}";
    private static final String MAX_REQ_TIME = "sun.net.httpserver.maxReqTime";

    private HttpServers servers;
    private HttpClient client;
    /** A registry per test, so one test's gauges can never collide with another's. */
    private PrometheusRegistry registry;
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    @BeforeEach
    void startServers() throws Exception {
        registry = new PrometheusRegistry();
        client = HttpClient.newHttpClient();

        MetricsCollector collector = new MetricsCollector(null, null, null) {
            @Override
            public boolean isHealthy() {
                return healthy.get();
            }
        };

        servers = HttpServers.start(0, 0, collector, registry);
    }

    @AfterEach
    void stopServers() {
        if (servers != null) {
            servers.close();
        }
    }

    @Test
    void startAppliesJdkHttpServerResourceLimits() {
        // No explicit hardenJdkHttpServerDefaults() call: @BeforeEach's start() must have done it.
        // This guards the call being dropped from start(). It cannot guard the call being made too
        // late — sun.net.httpserver.ServerConfig latches these in a static initialiser and the
        // effective values are unreadable from outside the jdk.httpserver module.
        assertEquals("60", System.getProperty(MAX_REQ_TIME));
        assertEquals("600", System.getProperty("sun.net.httpserver.maxRspTime"));
        assertEquals("50", System.getProperty("sun.net.httpserver.maxReqHeaders"));
        assertEquals("100", System.getProperty("jdk.httpserver.maxConnections"));
    }

    @Test
    void hardeningDoesNotOverrideOperatorSuppliedValues() {
        // These properties are JVM-global and surefire reuses one fork for every test class, so
        // restore the previous value rather than clearing it: clearing would leave
        // startAppliesJdkHttpServerResourceLimits passing only because @BeforeEach happens to
        // re-harden before every test, and break the moment start() moves to @BeforeAll.
        String previous = System.getProperty(MAX_REQ_TIME);
        System.setProperty(MAX_REQ_TIME, "15");
        try {
            HttpServers.hardenJdkHttpServerDefaults();
            assertEquals("15", System.getProperty(MAX_REQ_TIME));
        } finally {
            if (previous == null) {
                System.clearProperty(MAX_REQ_TIME);
            } else {
                System.setProperty(MAX_REQ_TIME, previous);
            }
        }
    }

    @Test
    void metricsListenerServesMetrics() throws Exception {
        HttpResponse<String> res = get(servers.getMetricsPort(), "/metrics");

        assertEquals(200, res.statusCode());
        assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("text/plain"));
    }

    @Test
    void managementListenerServesBothProbes() throws Exception {
        HttpResponse<String> healthy = get(servers.getManagementPort(), "/healthy");
        HttpResponse<String> ready = get(servers.getManagementPort(), "/ready");

        assertEquals(200, healthy.statusCode());
        assertEquals(UP, healthy.body());
        assertEquals(200, ready.statusCode());
        assertEquals(UP, ready.body());
    }

    @Test
    void readinessIsDownWhileTheCollectorIsUnhealthyButLivenessStaysUp() throws Exception {
        healthy.set(false);

        HttpResponse<String> ready = get(servers.getManagementPort(), "/ready");

        assertEquals(503, ready.statusCode());
        assertEquals(DOWN, ready.body());
        assertEquals(200, get(servers.getManagementPort(), "/healthy").statusCode(),
                "a stale collector must not restart the pod");
    }

    @Test
    void probesOnlyAcceptGet() throws Exception {
        assertEquals(405, post(servers.getManagementPort(), "/healthy").statusCode());
        assertEquals(405, post(servers.getManagementPort(), "/ready").statusCode());
    }

    @Test
    void theTwoListenersDoNotServeEachOthersEndpoints() throws Exception {
        assertEquals(404, get(servers.getMetricsPort(), "/healthy").statusCode());
        assertEquals(404, get(servers.getMetricsPort(), "/ready").statusCode());
        assertEquals(404, get(servers.getManagementPort(), "/metrics").statusCode());
    }

    /**
     * A scrape that blows up must answer 500 without leaking the stack trace, and say why in
     * Sova's own log — {@code HttpErrorHandlingPolicy}'s default error reporter is null, so
     * without the errorReporter wiring in {@link HttpServers} the failure is logged nowhere.
     */
    @Test
    void answersFiveHundredAndLogsWhenACollectorThrows() throws Exception {
        CapturingAppender captured = CapturingAppender.attachedTo(HttpServers.class);
        MultiCollector exploding = explodingCollector();
        registry.register(exploding);
        try {
            HttpResponse<String> res = get(servers.getMetricsPort(), "/metrics");

            assertEquals(500, res.statusCode());
            assertFalse(res.body().contains("IllegalStateException"),
                    "stack traces must not be leaked to scrape clients");
            assertTrue(captured.messages().stream().anyMatch(m -> m.contains("scrape failed")),
                    "scrape failure must be logged by Sova, got: " + captured.messages());
        } finally {
            registry.unregister(exploding);
            captured.detach();
        }
    }

    @Test
    void closeReleasesBothListeners() {
        int metricsPort = servers.getMetricsPort();
        int managementPort = servers.getManagementPort();

        servers.close();
        servers = null;

        // Same hazard as a failed startup: the JDK's HTTP dispatcher thread is non-daemon, and
        // close() deliberately shuts the executors down itself because HttpServer.stop() does
        // not. A listener still running after close() cannot have its port bound again.
        assertDoesNotThrow(() -> new ServerSocket(metricsPort).close(),
                "the metrics listener must be stopped by close()");
        assertDoesNotThrow(() -> new ServerSocket(managementPort).close(),
                "the management listener must be stopped by close()");
    }

    @Test
    void releasesTheMetricsListenerWhenTheManagementPortCannotBind() throws Exception {
        // Occupy a port so the management listener's bind fails after the metrics one succeeded
        try (ServerSocket squatter = new ServerSocket(0)) {
            int metricsPort = freePort();

            assertThrows(IOException.class,
                    () -> HttpServers.start(metricsPort, squatter.getLocalPort(),
                            new MetricsCollector(null, null, null), registry));

            // The JDK's HTTP dispatcher thread is non-daemon: a metrics listener left running
            // keeps the JVM alive forever with no probe endpoint to report the failure. If it is
            // still running, this port cannot be bound again.
            assertDoesNotThrow(() -> new ServerSocket(metricsPort).close(),
                    "metrics listener must be stopped when the management listener fails to start");
        }
    }

    private static MultiCollector explodingCollector() {
        return new MultiCollector() {
            @Override
            public MetricSnapshots collect() {
                throw new IllegalStateException("collector blew up");
            }

            @Override
            public List<String> getPrometheusNames() {
                return List.of("sova_test_exploding");
            }
        };
    }

    private static int freePort() throws IOException {
        try (ServerSocket probe = new ServerSocket(0)) {
            return probe.getLocalPort();
        }
    }

    private HttpResponse<String> get(int port, String path) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(int port, String path) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
