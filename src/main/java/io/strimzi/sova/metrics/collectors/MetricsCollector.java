/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.strimzi.sova.metrics.MetricSnapshot;
import io.strimzi.sova.metrics.MetricsRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.collectors.BrokerCollector.BrokerData;
import io.strimzi.sova.metrics.collectors.ConsumerGroupCollector.ConsumerGroupData;
import io.strimzi.sova.metrics.collectors.TopicCollector.TopicData;

/**
 * Drives a periodic Kafka metadata collection cycle and feeds results into {@link MetricsRegistry}.
 *
 * <p>Delegates the actual AdminClient calls to {@link BrokerCollector}, {@link TopicCollector},
 * and {@link ConsumerGroupCollector}, then assembles their results into a {@link MetricSnapshot}
 * and hands it to {@link MetricsRegistry#update(MetricSnapshot)} as an atomic swap.
 *
 * <p>A failed cycle never updates the registry, so the previous snapshot continues to be served to
 * scrapers and ages until readiness reports the staleness. How the failure is logged depends on
 * what it was: {@link InterruptedException} is shutdown rather than failure and logs DEBUG, an
 * {@link Exception} logs WARN, and a {@link Throwable} logs ERROR.
 *
 * <p>Threading: {@link #collect()} is intended to be called from a virtual-thread-per-task
 * executor so the calling thread is never blocked long-term.
 */
public class MetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

    /**
     * How many collection intervals the served snapshot may age before it counts as stale.
     *
     * <p>Two rather than one: collection runs at a fixed delay, so a healthy snapshot is replaced
     * every {@code interval + cycle duration}, which is always a little more than one interval.
     */
    private static final int STALE_AFTER_INTERVALS = 2;

    private final Admin admin;
    private final MetricsRegistry registry;
    private final SovaConfig config;

    /**
     * Constructs a collector with the given dependencies.
     *
     * @param admin    Kafka AdminClient
     * @param registry Prometheus metrics registry
     * @param config   application configuration
     */
    public MetricsCollector(Admin admin, MetricsRegistry registry, SovaConfig config) {
        this.admin = admin;
        this.registry = registry;
        this.config = config;
    }

    /**
     * Readiness is derived from the age of the snapshot being served, and nothing else.
     *
     * <p>Only a successful cycle updates the registry, so a fresh snapshot means a recent success.
     * A failed cycle needs no flag of its own: it stops the snapshot being replaced, and the probe
     * goes DOWN once that snapshot is too old. The same rule covers a cycle that hangs and throws
     * nothing, and the pre-first-cycle state, where the registry still holds its empty snapshot
     * stamped {@code 0}.
     *
     * @return {@code true} when the snapshot being served is no older than
     * {@link #STALE_AFTER_INTERVALS} collection intervals
     */
    public boolean isHealthy() {
        long ageMs = System.currentTimeMillis() - registry.lastCollectedAtMs();
        return ageMs <= STALE_AFTER_INTERVALS * config.getCollectIntervalMs();
    }

    /**
     * Executes one full collection cycle. On failure: logs and exits without updating the
     * registry, so the previous snapshot keeps being served.
     *
     * <p>Never throws. The scheduler runs this at a fixed delay, and a scheduled task that throws
     * is cancelled silently and never runs again, which would leave the pod alive but frozen:
     * liveness is always 200, so Kubernetes would not restart it. That is why {@link Throwable}
     * is caught and not just {@link Exception}.
     */
    public void collect() {
        try {
            collectInternal();
        } catch (InterruptedException e) {
            // Shutdown interrupts the scheduler thread mid-cycle. Restore the flag so the
            // executor can finish tearing down, and do not count this as a collection failure.
            Thread.currentThread().interrupt();
            LOGGER.debug("Collection cycle interrupted");
        } catch (Exception e) {
            LOGGER.warn("Collection cycle failed, stale metrics keep being served", e);
        } catch (Throwable t) {
            // An Error raised on the scheduler thread itself (a branch's Error arrives wrapped in
            // ExecutionException above). Log it and keep the schedule alive: the next cycle may
            // well succeed, and if it does not the snapshot ages out and readiness goes DOWN.
            LOGGER.error("Collection cycle failed with an error, stale metrics keep being served", t);
        }
    }

    /**
     * Core collection logic — separated from {@link #collect()} to simplify error handling.
     *
     * @throws ExecutionException   if an AdminClient future completes exceptionally
     * @throws InterruptedException if the thread is interrupted while waiting for a future
     */
    void collectInternal() throws ExecutionException, InterruptedException {
        long startNs = System.nanoTime();

        BrokerData brokerData;
        TopicData topicData;
        ConsumerGroupData groupData;

        // The three branches share no state and no ordering: describeCluster stands alone, and
        // the topic chain and the group chain never read each other. Only the lag arithmetic
        // below needs both, and that is local. Running them concurrently issues exactly the same
        // requests as running them in sequence did, so it adds no load to the cluster, but makes
        // a cycle as long as its slowest branch instead of the sum of all three.
        // The executor is closed before this method returns, and close() waits for every branch
        // thread to finish. So no branch outlives its cycle: the next cycle, or admin.close() at
        // shutdown, never races a thread left over from this one.
        try (ExecutorService branches = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                Future<BrokerData> broker = branches.submit(() -> BrokerCollector.collect(admin));
                Future<TopicData> topics =
                    branches.submit(() -> TopicCollector.collect(admin, config));
                Future<ConsumerGroupData> groups =
                    branches.submit(() -> ConsumerGroupCollector.collect(admin, config));

                brokerData = broker.get();
                topicData = topics.get();
                groupData = groups.get();
            } finally {
                // Interrupt before close() starts waiting: once one branch has failed the others
                // are of no use this cycle, and their AdminClient calls unwind on interrupt, so
                // the wait is short.
                branches.shutdownNow();
            }
        }

        ConsumerGroupData cgData = ConsumerGroupCollector.computeLag(
            groupData,
            topic -> CollectorUtils.matches(topic, config.getTopicPattern(),
                config.getTopicExcludePattern()),
            topicData);

        registry.update(
            new MetricSnapshot(
                brokerData.brokerCount(),
                brokerData.addresses(),
                topicData.topicPartitionCounts(),
                topicData.partitions(),
                topicData.latestOffsets(),
                topicData.earliestOffsets(),
                cgData.members(),
                cgData.currentOffsets(),
                cgData.currentOffsetSums(),
                cgData.lags(),
                cgData.lagSums(),
                cgData.states(),
                System.currentTimeMillis())
        );

        long durationMs = (System.nanoTime() - startNs) / 1_000_000;
        LOGGER.debug("Collection cycle complete in {} ms: {} brokers, {} topics, {} partitions, {} groups",
            durationMs, brokerData.brokerCount(), topicData.topicPartitionCounts()
                .size(),
            topicData.partitions()
                .size(), groupData.members()
                .size());
    }
}
