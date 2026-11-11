/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricsRegistry;
import io.strimzi.sova.metrics.TestSnapshots;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MetricsCollector} readiness state that need no Kafka broker.
 *
 * <p>Readiness is derived from the age of the snapshot in the registry, so these tests drive it by
 * publishing snapshots stamped with chosen collection times.
 */
class MetricsCollectorHealthTest {
    /** The default 30 s interval, so a snapshot goes stale after 60 s. */
    private static final long INTERVAL_MS = 30_000L;

    private final MetricsRegistry registry = new MetricsRegistry();

    /** {@code admin} is null, so every {@link MetricsCollector#collect()} call fails and is caught. */
    private final MetricsCollector collector =
            new MetricsCollector(null, registry, SovaConfig.forTesting("localhost:9092"));

    /**
     * Publishes a snapshot as if a cycle had finished the given time ago.
     *
     * @param ageMs how long ago the snapshot was collected
     */
    private void publishSnapshotAged(long ageMs) {
        registry.update(TestSnapshots.builder()
                .collectedAtMs(System.currentTimeMillis() - ageMs)
                .build());
    }

    @Test
    void isNotHealthyBeforeFirstCollection() {
        assertFalse(collector.isHealthy(),
                "readiness must be DOWN until the first collection cycle has succeeded");
    }

    /**
     * Readiness follows the snapshot age alone. The window is two intervals — 60 s at the default
     * {@link #INTERVAL_MS} — so anything inside it is UP and anything past it is DOWN. A run of
     * failed cycles and a cycle that never returns both surface here, since neither replaces the
     * snapshot.
     *
     * @param ageMs         how long ago the served snapshot was collected
     * @param expectedUp    whether readiness must report UP
     * @param why           what the case stands for, used as the failure message
     */
    @ParameterizedTest(name = "{2}")
    @CsvSource({
        "0,     true,  a cycle that just succeeded must report UP",
        // One second of margin: the clock is read again inside isHealthy().
        "59000, true,  a snapshot still inside the staleness window must report UP",
        "61000, false, a snapshot past the staleness window must report DOWN",
    })
    void readinessFollowsTheAgeOfTheServedSnapshot(long ageMs, boolean expectedUp, String why) {
        publishSnapshotAged(ageMs);

        assertEquals(expectedUp, collector.isHealthy(), why);
    }

    @Test
    void aFailedCycleDoesNotRefreshTheSnapshot() {
        // The failure itself changes nothing: the snapshot keeps its age, and readiness goes DOWN
        // only when that age passes the window. Kubernetes failureThreshold adds the grace period
        // on top.
        publishSnapshotAged(INTERVAL_MS);

        collector.collect();

        assertTrue(collector.isHealthy(),
                "one failed cycle inside the staleness window must not flip the probe");
    }
}
