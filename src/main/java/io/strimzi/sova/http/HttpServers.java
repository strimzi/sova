/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.exporter.httpserver.HttpErrorHandlingPolicy;
import io.prometheus.metrics.exporter.httpserver.MetricsHandler;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import io.strimzi.sova.metrics.collectors.MetricsCollector;

/**
 * Owns Sova's two JDK built-in HTTP listeners.
 *
 * <ul>
 *   <li><b>Metrics listener</b> — serves {@code GET /metrics}.</li>
 *   <li><b>Management listener</b> — serves {@code GET /healthy} and {@code GET /ready}.
 *       Never TLS-enabled, so probe behaviour stays stable regardless of the metrics
 *       endpoint's TLS configuration.</li>
 * </ul>
 *
 * <p>Neither listener serves the other's paths.
 *
 * <p>Threading: both listeners use a virtual-thread-per-task executor.
 */
public final class HttpServers implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HttpServers.class);

    private final HttpServer metricsServer;
    private final HttpServer managementServer;
    private final ExecutorService metricsExecutor;
    private final ExecutorService managementExecutor;

    private HttpServers(HttpServer metricsServer, HttpServer managementServer,
                         ExecutorService metricsExecutor, ExecutorService managementExecutor) {
        this.metricsServer = metricsServer;
        this.managementServer = managementServer;
        this.metricsExecutor = metricsExecutor;
        this.managementExecutor = managementExecutor;
    }

    /**
     * Creates and starts both listeners.
     *
     * @param metricsPort    port for {@code /metrics}; {@code 0} binds an ephemeral port
     * @param managementPort port for {@code /healthy} and {@code /ready}; {@code 0} binds an ephemeral port
     * @param collector      collector whose health backs the readiness probe
     * @param registry       the registry a scrape reads
     * @return the started listeners
     * @throws IOException if either port cannot be bound
     */
    public static HttpServers start(int metricsPort, int managementPort, MetricsCollector collector, PrometheusRegistry registry) throws IOException {
        hardenJdkHttpServerDefaults();

        HttpServer metrics = HttpServer.create(new InetSocketAddress(metricsPort), 0);
        metrics.createContext(
            "/metrics",
            new MetricsHandler(
                PrometheusProperties.get(),
                registry,
                // Generic 500 body — never leak stack traces to scrape clients. Details go to
                // Sova's own SLF4J logger rather than the handler's default JUL logger.
                HttpErrorHandlingPolicy.builder()
                    .errorReporter(e -> log.warn("Prometheus scrape failed", e))
                    .build()
            )
        );
        ExecutorService metricsExecutor = Executors.newVirtualThreadPerTaskExecutor();
        metrics.setExecutor(metricsExecutor);
        metrics.start();
        log.info("Metrics HTTP server started on port {}", metrics.getAddress()
            .getPort());

        try {
            HttpServer management = HttpServer.create(new InetSocketAddress(managementPort), 0);
            management.createContext("/healthy", new LivenessCheck());
            management.createContext("/ready", new ReadinessCheck(collector));
            ExecutorService managementExecutor = Executors.newVirtualThreadPerTaskExecutor();
            management.setExecutor(managementExecutor);
            management.start();
            log.info("Management HTTP server started on port {}", management.getAddress()
                .getPort());

            return new HttpServers(metrics, management, metricsExecutor, managementExecutor);
        } catch (IOException | RuntimeException e) {
            // The JDK's HTTP dispatcher thread is not a daemon thread. A metrics listener left
            // running after a failed startup keeps the JVM alive with no management port to
            // report the failure on: the pod would sit NotReady forever instead of restarting.
            metrics.stop(0);
            // HttpServer.stop() does not touch an executor set via setExecutor() — the JDK
            // javadoc is explicit that the caller owns its lifecycle. Leaving it running would
            // leak the virtual-thread-per-task executor's underlying resources.
            metricsExecutor.shutdown();
            throw e;
        }
    }

    /**
     * Applies resource limits to the JDK HTTP server.
     *
     * <p>The JDK ships with no request deadline and no connection cap
     * ({@code maxReqTime = -1}, {@code maxRspTime = -1}, {@code maxConnections = -1}), which is
     * unacceptable on an unauthenticated port. Prometheus' own {@code HTTPServer} sets the first
     * two in a static initialiser, but Sova deliberately does not load that class, so we set them
     * ourselves.
     *
     * <p><b>Must be called before the first {@code HttpServer.create()} in the JVM.</b>
     * {@code sun.net.httpserver.ServerConfig} reads these properties once, in a static
     * initialiser; setting them afterwards is a silent no-op.
     *
     * <p>Existing values are never overwritten, so an operator can still tune them via
     * {@code JAVA_OPTS}.
     */
    static void hardenJdkHttpServerDefaults() {
        setIfAbsent("sun.net.httpserver.maxReqTime", "60");       // JDK default: -1 (forever)
        setIfAbsent("sun.net.httpserver.maxRspTime", "600");      // JDK default: -1 (forever)
        setIfAbsent("sun.net.httpserver.maxReqHeaders", "50");    // JDK default: 200
        setIfAbsent("jdk.httpserver.maxConnections", "100");      // JDK default: -1 (unlimited)
    }

    private static void setIfAbsent(String key, String value) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
        }
    }

    /**
     * @return the bound port of the metrics listener
     */
    public int getMetricsPort() {
        return metricsServer.getAddress()
            .getPort();
    }

    /**
     * @return the bound port of the management listener
     */
    public int getManagementPort() {
        return managementServer.getAddress()
            .getPort();
    }

    /**
     * Stops both listeners, allowing in-flight exchanges up to one second to finish.
     *
     * <p>{@code HttpServer.stop()} does not shut down an executor set via {@code setExecutor()} —
     * the JDK javadoc is explicit that this is the caller's responsibility. The servers are
     * stopped first so in-flight exchanges get their chance to finish before the executors that
     * run them are shut down.
     */
    @Override
    public void close() {
        metricsServer.stop(1);
        managementServer.stop(1);
        metricsExecutor.shutdown();
        managementExecutor.shutdown();
    }
}
