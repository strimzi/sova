/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.strimzi.sova.integration.KafkaTestResourceManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Starts the real {@link Main} in a child JVM, configured only through environment variables as
 * the operator does, and checks what an operator-deployed pod would expose.
 */
class MainSmokeIT {

    private static final Duration WAIT = Duration.ofSeconds(60);

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static KafkaTestResourceManager kafkaManager;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startKafka() {
        kafkaManager = new KafkaTestResourceManager();
        kafkaManager.start();
    }

    @AfterAll
    static void stopKafka() {
        if (kafkaManager != null) {
            kafkaManager.stop();
        }
    }

    @Test
    void servesMetricsAndReportsReadyAgainstARealCluster() throws Exception {
        int metricsPort = freePort();
        int managementPort = freePort();
        Process sova = startSova(KafkaTestResourceManager.getBootstrapServers(), metricsPort, managementPort);
        try {
            awaitStatus(managementPort, "/ready", 200);

            assertEquals(200, get(managementPort, "/healthy").statusCode());
            HttpResponse<String> metrics = get(metricsPort, "/metrics");
            assertEquals(200, metrics.statusCode());
            assertTrue(metrics.body().contains("kafka_brokers 1.0"), metrics.body());
            // The management port must not expose the metrics.
            assertEquals(404, get(managementPort, "/metrics").statusCode());
        } finally {
            sova.destroy();
            sova.waitFor();
        }
    }

    @Test
    void acceptsClientConfigurationAndLogsOnlyItsKeys() throws Exception {
        int metricsPort = freePort();
        int managementPort = freePort();
        Path log = tempDir.resolve("sova.log");
        // sasl.jaas.config stands in for a credential: Sova must log the key, never the value.
        Process sova = startSova(KafkaTestResourceManager.getBootstrapServers(), metricsPort, managementPort,
                Map.of("STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION",
                        "client.id=sova-smoke-it\nsasl.jaas.config=top-secret-credentials\n"),
                log);
        try {
            awaitStatus(managementPort, "/ready", 200);

            assertTrue(get(metricsPort, "/metrics").body().contains("kafka_brokers 1.0"));
            String output = Files.readString(log);
            assertTrue(output.contains("client.id") && output.contains("sasl.jaas.config"),
                    "the keys of the client configuration must be logged, was: " + output);
            assertFalse(output.contains("top-secret-credentials"),
                    "the values of the client configuration must never be logged, was: " + output);
        } finally {
            sova.destroy();
            sova.waitFor();
        }
    }

    @Test
    void clientConfigurationReachesTheAdminClient() throws Exception {
        int metricsPort = freePort();
        int managementPort = freePort();
        // The test broker speaks plaintext. If this setting reaches the Admin client it tries a TLS
        // handshake against it and never collects; if it were ignored, Sova would turn Ready within
        // seconds, as in the test above.
        Process sova = startSova(KafkaTestResourceManager.getBootstrapServers(), metricsPort, managementPort,
                Map.of("STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", "security.protocol=SSL\n"), null);
        try {
            awaitStatus(managementPort, "/healthy", 200);
            Thread.sleep(Duration.ofSeconds(5));

            assertEquals(503, get(managementPort, "/ready").statusCode(),
                    "security.protocol=SSL must have been applied to the Admin client");
        } finally {
            sova.destroy();
            sova.waitFor();
        }
    }

    @Test
    void isLiveButNotReadyWhenTheClusterIsUnreachable() throws Exception {
        int metricsPort = freePort();
        int managementPort = freePort();
        // Nothing listens on this port: the first cycle can never succeed.
        Process sova = startSova("localhost:" + freePort(), metricsPort, managementPort);
        try {
            awaitStatus(managementPort, "/healthy", 200);

            assertEquals(503, get(managementPort, "/ready").statusCode(),
                    "a pod that has never collected anything must not be Ready");
            // Liveness must stay up, otherwise a Kafka outage restarts the pod.
            assertEquals(200, get(managementPort, "/healthy").statusCode());
        } finally {
            sova.destroy();
            sova.waitFor();
        }
    }

    private static Process startSova(String bootstrapServers, int metricsPort, int managementPort)
            throws IOException {
        return startSova(bootstrapServers, metricsPort, managementPort, Map.of(), null);
    }

    /**
     * Starts Sova in a child JVM.
     *
     * @param extraEnv additional environment variables for the child
     * @param logFile  where to write the child's output, or {@code null} to inherit this process's
     */
    private static Process startSova(String bootstrapServers, int metricsPort, int managementPort,
                                     Map<String, String> extraEnv, Path logFile) throws IOException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                Main.class.getName());
        Map<String, String> env = pb.environment();
        env.put("SOVA_KAFKA_BOOTSTRAP_SERVERS", bootstrapServers);
        env.put("SOVA_METRICS_PORT", String.valueOf(metricsPort));
        env.put("SOVA_MANAGEMENT_PORT", String.valueOf(managementPort));
        env.put("SOVA_COLLECT_INTERVAL_MS", "1000");
        env.putAll(extraEnv);
        pb.redirectErrorStream(true);
        pb.redirectOutput(logFile == null
                ? ProcessBuilder.Redirect.INHERIT
                : ProcessBuilder.Redirect.to(logFile.toFile()));
        return pb.start();
    }

    private static void awaitStatus(int port, String path, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT.toNanos();
        int last = -1;
        while (System.nanoTime() < deadline) {
            try {
                last = get(port, path).statusCode();
                if (last == expected) {
                    return;
                }
            } catch (IOException e) {
                // listener not up yet
                last = -1;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("GET " + path + " on port " + port + " did not return " + expected
                + " within " + WAIT + ", last status " + last);
    }

    private static HttpResponse<String> get(int port, String path) throws IOException, InterruptedException {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
