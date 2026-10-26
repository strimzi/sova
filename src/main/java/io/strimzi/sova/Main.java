/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.http.HttpServers;
import io.strimzi.sova.kafka.KafkaAdminClient;
import io.strimzi.sova.metrics.BuildInfo;
import io.strimzi.sova.metrics.collectors.MetricsCollector;
import io.strimzi.sova.metrics.MetricsRegistry;

/**
 * Application entry point.
 *
 * <p>Wires all components together, starts the periodic collection scheduler on a virtual thread,
 * and starts the two JDK built-in HTTP listeners owned by {@link HttpServers}:
 * <ul>
 *   <li>Metrics listener on the configured metrics port serving {@code GET /metrics}</li>
 *   <li>Management listener on the configured management port serving {@code GET /healthy}
 *       and {@code GET /ready}</li>
 * </ul>
 *
 * <p>A JVM shutdown hook stops the listeners, the scheduler, and the Kafka AdminClient — in that
 * order, so no new scrape can arrive while collection is being torn down.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Private constructor — utility class, not to be instantiated.
     */
    private Main() {}

    /**
     * Application entry point.
     *
     * @param args command-line arguments (unused)
     * @throws IOException if binding either listener fails
     */
    public static void main(String[] args) throws IOException {
        BuildInfo buildInfo = BuildInfo.load();
        // Log details about Sova build info
        log.info(buildInfo.toString());

        SovaConfig config = new SovaConfig();
        // Effective config for whoever debugs a pod. Names only: SovaConfig prints the keys of the
        // Kafka client properties, never their values, which can carry credentials.
        log.info(config.toString());

        Admin admin = KafkaAdminClient.create(config);
        MetricsRegistry registry = new MetricsRegistry(buildInfo);
        MetricsCollector collector = new MetricsCollector(admin, registry, config);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        // Fixed delay, not fixed rate: a cycle makes several sequential AdminClient calls, each
        // bounded by default.api.timeout.ms, so a slow cluster can push one cycle past the
        // interval. At a fixed rate the next cycle would then start immediately with no gap,
        // piling AdminClient load onto a cluster that is already struggling.
        scheduler.scheduleWithFixedDelay(
                collector::collect,
                0,
                config.getCollectIntervalMs(),
                TimeUnit.MILLISECONDS);

        HttpServers servers = HttpServers.start(
                config.getMetricsPort(), config.getManagementPort(), collector, registry.prometheusRegistry());

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> shutdown(servers, scheduler, admin), "sova-shutdown"));
    }

    private static void shutdown(HttpServers servers, ScheduledExecutorService scheduler, Admin admin) {
        log.info("Shutting down...");
        servers.close();
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                log.warn("Collection scheduler did not terminate within {}", SHUTDOWN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        admin.close(SHUTDOWN_TIMEOUT);
    }
}
