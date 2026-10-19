/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.util.Map;

import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

/**
 * Holds the most recently collected {@link MetricSnapshot} in a {@code volatile} field and
 * registers a single {@link MultiCollector} with the Prometheus registry.
 *
 * <p>{@link #update(MetricSnapshot)} is one reference swap; every scrape reads that reference
 * once and builds all metric families from it via {@link MetricsSnapshotRenderer}. This makes a scrape
 * atomic — it reflects exactly one collection cycle — and makes series disappear when they
 * leave the cluster, because nothing is carried over from the previous call.
 */
public class MetricsRegistry {

    /**
     * The Prometheus registry this instance collects into, and the one the {@code /metrics}
     * handler scrapes.
     *
     * <p>Deliberately not {@code PrometheusRegistry.defaultRegistry}: a JVM-global registry means
     * construction order decides whether anything else registered there survives, and the only
     * way to make repeated construction safe is to clear it, which silently drops whatever that
     * other party registered. Owning the instance makes both problems go away.
     */
    private final PrometheusRegistry prometheusRegistry = new PrometheusRegistry();

    /**
     * What a scrape serves before the first collection cycle completes: no brokers, no topics,
     * no consumer groups. Readiness stays DOWN until a cycle succeeds, so this is what a scrape
     * that races startup sees.
     */
    private static final MetricSnapshot EMPTY = new MetricSnapshot(
        0,
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of(),
        0L);

    /**
     * The most recently collected cycle. Written by {@link #update(MetricSnapshot)} and read once
     * per scrape; the single volatile reference swap is what makes a scrape atomic.
     */
    private volatile MetricSnapshot current = EMPTY;

    /**
     * The constant build-info family. Built once at construction; nothing about it changes
     * between scrapes.
     */
    private final InfoSnapshot infoSnapshot;

    /**
     * Constructs the registry with the build metadata of the running application.
     *
     * @see #MetricsRegistry(BuildInfo)
     */
    public MetricsRegistry() {
        this(BuildInfo.load());
    }

    /**
     * Constructs the registry, registering the single {@link MultiCollector} plus the JVM and
     * process instrumentation.
     *
     * @param buildInfo the build metadata to expose in {@code sova_build_info}
     */
    public MetricsRegistry(BuildInfo buildInfo) {
        this.infoSnapshot = MetricsSnapshotRenderer.buildInfo(buildInfo);
        prometheusRegistry.register(this::collectSnapshots);
        // process_* and jvm_* families
        JvmMetrics.builder()
            .register(prometheusRegistry);
    }

    /**
     * @return the registry a scrape must read, for the {@code /metrics} handler to bind to
     */
    public PrometheusRegistry prometheusRegistry() {
        return prometheusRegistry;
    }

    /**
     * Publishes a freshly collected cycle.
     *
     * <p>One volatile reference write. A scrape reads that reference once and builds every family
     * from it, so a scrape always reflects exactly one cycle, and a series that left the cluster
     * leaves the scrape with it. Two overlapping cycles resolve to whichever wrote last — whole,
     * never blended — so no lock is needed here.
     *
     * @param snapshot the freshly collected metric values
     */
    public void update(MetricSnapshot snapshot) {
        current = snapshot;
    }

    /**
     * @return wall-clock time the currently served snapshot was collected, epoch millis;
     * {@code 0} until the first cycle has completed
     */
    public long lastCollectedAtMs() {
        return current.collectedAtMs();
    }

    /**
     * Builds the families for one scrape from a single read of {@link #current}.
     *
     * @return the metric families to expose
     */
    private MetricSnapshots collectSnapshots() {
        return MetricsSnapshotRenderer.from(current, infoSnapshot);
    }

}
