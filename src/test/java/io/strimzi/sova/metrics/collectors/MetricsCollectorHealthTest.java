/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricSnapshot;
import io.strimzi.sova.metrics.MetricsRegistry;

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
        registry.update(new MetricSnapshot(0, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                System.currentTimeMillis() - ageMs));
    }

    @Test
    void isNotHealthyBeforeFirstCollection() {
        assertFalse(collector.isHealthy(),
                "readiness must be DOWN until the first collection cycle has succeeded");
    }

    @Test
    void aFailedCycleBeforeAnySuccessLeavesReadinessDown() {
        collector.collect();

        assertFalse(collector.isHealthy(),
                "a pod that has never collected anything must not be marked Ready");
    }

    @Test
    void aFreshlyCollectedSnapshotIsHealthy() {
        publishSnapshotAged(0);

        assertTrue(collector.isHealthy(), "a cycle that just succeeded must report UP");
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

    @Test
    void aSuccessfulCycleRecoversReadinessImmediately() {
        publishSnapshotAged(2 * INTERVAL_MS + 1_000);
        assertFalse(collector.isHealthy());

        publishSnapshotAged(0);

        assertTrue(collector.isHealthy(), "recovery must be immediate, not gradual");
    }

    /**
     * Covers both a run of failed cycles and a cycle that never returns: neither replaces the
     * snapshot, so its age is the only signal.
     */
    @Test
    void aSnapshotOlderThanTheStalenessWindowFlipsReadinessDown() {
        publishSnapshotAged(2 * INTERVAL_MS + 1_000);

        assertFalse(collector.isHealthy(),
                "when the snapshot stops being replaced, staleness is the only signal that the "
                        + "exported metrics have stopped tracking the cluster");
    }

    @Test
    void aSnapshotInsideTheStalenessWindowStaysHealthy() {
        publishSnapshotAged(INTERVAL_MS);

        assertTrue(collector.isHealthy(),
                "one missed interval is normal jitter and must not flap the probe");
    }
}
