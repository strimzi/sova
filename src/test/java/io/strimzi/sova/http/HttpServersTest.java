/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import io.strimzi.sova.metrics.collectors.MetricsCollector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the two HTTP listeners: resource-limit hardening, endpoint isolation, and probe behaviour.
 */
class HttpServersTest {

    private HttpServers servers;
    private HttpClient client;
    /** A registry per test, so one test's gauges can never collide with another's. */
    private PrometheusRegistry registry;

    @BeforeEach
    void startServers() throws Exception {
        registry = new PrometheusRegistry();

        MetricsCollector healthyCollector = new MetricsCollector(null, null, null) {
            @Override
            public boolean isHealthy() {
                return true;
            }
        };

        // Ephemeral ports (0) to prevent test port conflicts
        servers = HttpServers.start(0, 0, healthyCollector, registry);
        client = HttpClient.newHttpClient();
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
        assertEquals("60", System.getProperty("sun.net.httpserver.maxReqTime"));
        assertEquals("600", System.getProperty("sun.net.httpserver.maxRspTime"));
        assertEquals("50", System.getProperty("sun.net.httpserver.maxReqHeaders"));
        assertEquals("100", System.getProperty("jdk.httpserver.maxConnections"));
    }

    @Test
    void hardeningDoesNotOverrideOperatorSuppliedValues() {
        System.setProperty("sun.net.httpserver.maxReqTime", "15");
        try {
            HttpServers.hardenJdkHttpServerDefaults();
            assertEquals("15", System.getProperty("sun.net.httpserver.maxReqTime"));
        } finally {
            System.clearProperty("sun.net.httpserver.maxReqTime");
        }
    }

    @Test
    void metricsListenerServesMetrics() throws Exception {
        HttpResponse<String> res = get(servers.getMetricsPort(), "/metrics");

        assertEquals(200, res.statusCode());
        assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("text/plain"));
    }

    @Test
    void managementListenerServesLiveness() throws Exception {
        HttpResponse<String> res = get(servers.getManagementPort(), "/healthy");

        assertEquals(200, res.statusCode());
        assertEquals("{\"status\":\"UP\"}", res.body());
    }

    @Test
    void managementListenerServesReadiness() throws Exception {
        HttpResponse<String> res = get(servers.getManagementPort(), "/ready");

        assertEquals(200, res.statusCode());
        assertEquals("{\"status\":\"UP\"}", res.body());
    }

    @Test
    void metricsListenerDoesNotServeManagementEndpoints() throws Exception {
        assertEquals(404, get(servers.getMetricsPort(), "/healthy").statusCode());
        assertEquals(404, get(servers.getMetricsPort(), "/ready").statusCode());
    }

    @Test
    void managementListenerDoesNotServeMetrics() throws Exception {
        assertEquals(404, get(servers.getManagementPort(), "/metrics").statusCode());
    }

    @Test
    void negotiatesOpenMetricsFromAcceptHeader() throws Exception {
        HttpResponse<String> res = send(servers.getMetricsPort(), "/metrics", "GET", "Accept",
                "application/openmetrics-text;version=1.0.0;q=0.75,text/plain;version=0.0.4;q=0.5");

        assertEquals(200, res.statusCode());
        assertTrue(res.headers().firstValue("Content-Type").orElse("")
                        .contains("application/openmetrics-text"),
                "expected OpenMetrics content type, got: "
                        + res.headers().firstValue("Content-Type").orElse("<none>"));
    }

    @Test
    void compressesResponseWhenClientAcceptsGzip() throws Exception {
        Gauge gauge = Gauge.builder().name("sova_test_gzip_gauge").labelNames("slot")
                .register(registry);
        // Enough repetitive data points that compression has something to work with
        for (int i = 0; i < 200; i++) {
            gauge.labelValues("slot-" + i).set(i);
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + servers.getMetricsPort() + "/metrics"))
                .header("Accept-Encoding", "gzip")
                .GET()
                .build();
        // ofByteArray so the JDK client does not transparently decompress
        HttpResponse<byte[]> res = client.send(req, HttpResponse.BodyHandlers.ofByteArray());

        assertEquals(200, res.statusCode());
        assertEquals("gzip", res.headers().firstValue("Content-Encoding").orElse("<none>"));
        byte[] inflated = new GZIPInputStream(new ByteArrayInputStream(res.body())).readAllBytes();
        assertTrue(inflated.length > res.body().length,
                "gzip response should be smaller than its inflated form, got "
                        + res.body().length + " vs " + inflated.length);
    }

    @Test
    void answersHeadRequests() throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + servers.getMetricsPort() + "/metrics"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, res.statusCode());
        assertEquals("", res.body());
    }

    @Test
    void filtersMetricsByNameQueryParameter() throws Exception {
        Gauge.builder().name("sova_test_wanted").register(registry).set(1);
        Gauge.builder().name("sova_test_unwanted").register(registry).set(1);

        // name[] percent-encoded: square brackets are not legal literals in a URI query
        HttpResponse<String> res = get(servers.getMetricsPort(), "/metrics?name%5B%5D=sova_test_wanted");

        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("sova_test_wanted"), "filtered metric should be present");
        assertFalse(res.body().contains("sova_test_unwanted"),
                "name[] filter must exclude other metrics");
    }

    @Test
    void returnsHttpFiveHundredWhenACollectorThrows() throws Exception {
        MultiCollector exploding = explodingCollector();
        registry.register(exploding);
        try {
            HttpResponse<String> res = get(servers.getMetricsPort(), "/metrics");

            assertEquals(500, res.statusCode());
            assertFalse(res.body().contains("IllegalStateException"),
                    "stack traces must not be leaked to scrape clients");
        } finally {
            registry.unregister(exploding);
        }
    }

    @Test
    void reportsScrapeFailuresToSovasOwnLogger() throws Exception {
        // HttpErrorHandlingPolicy's default error reporter is null: without the errorReporter
        // wiring in HttpServers, a failing scrape is answered with a 500 and logged nowhere.
        CapturingAppender captured = CapturingAppender.attachedTo(HttpServers.class);
        MultiCollector exploding = explodingCollector();
        registry.register(exploding);
        try {
            get(servers.getMetricsPort(), "/metrics");

            assertTrue(captured.messages().stream().anyMatch(m -> m.contains("scrape failed")),
                    "scrape failure must be logged by Sova, got: " + captured.messages());
        } finally {
            registry.unregister(exploding);
            captured.detach();
        }
    }

    @Test
    void releasesTheMetricsListenerWhenTheManagementPortCannotBind() throws Exception {
        // freePort() below has an inherent probe-then-bind race: another process (or another
        // test's own ephemeral HttpServer) can grab the probed port before HttpServers.start()
        // binds it, or before the later reconnect attempt runs. Retry a bounded number of times
        // rather than let that race cause an occasional false failure.
        // ponytail: bounded retry over a true reservation, acceptable since the window is a
        // handful of JVM instructions and this is test-only code.
        AssertionError lastFailure = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                attemptReleasesTheMetricsListenerWhenTheManagementPortCannotBind();
                return;
            } catch (AssertionError e) {
                lastFailure = e;
            }
        }
        throw lastFailure;
    }

    private void attemptReleasesTheMetricsListenerWhenTheManagementPortCannotBind() throws Exception {
        // Occupy a port so the management listener's bind fails after the metrics one succeeded
        try (ServerSocket squatter = new ServerSocket(0)) {
            int takenPort = squatter.getLocalPort();
            int metricsPort = freePort();

            assertThrows(IOException.class,
                    () -> HttpServers.start(metricsPort, takenPort,
                            new MetricsCollector(null, null, null), registry));

            // The JDK's HTTP dispatcher thread is non-daemon: a metrics listener left running
            // keeps the JVM alive forever with no probe endpoint to report the failure.
            assertThrows(ConnectException.class, () -> get(metricsPort, "/metrics"),
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

    private HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> send(int port, String path, String method, String headerName,
                                      String headerValue) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .header(headerName, headerValue)
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
