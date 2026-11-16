/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import io.strimzi.sova.metrics.MetricSnapshot;
import io.strimzi.sova.metrics.MetricsRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
 * scrapers while the last-success timestamp ages. How the failure is logged depends on
 * what it was: {@link InterruptedException} is shutdown rather than failure and logs DEBUG, an
 * {@link Exception} logs WARN, and a {@link Throwable} logs ERROR. A cycle that runs past
 * {@link SovaConfig#getCollectTimeoutMs()} is abandoned and logs WARN.
 *
 * <p>Threading: {@link #collect()} is intended to be called from a virtual-thread-per-task
 * executor so the calling thread is never blocked long-term.
 */
public class MetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);

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
     * Executes one full collection cycle. On failure: logs and exits without updating the
     * registry, so the previous snapshot keeps being served.
     *
     * <p>Never throws. The scheduler runs this at a fixed delay, and a scheduled task that throws
     * is cancelled silently and never runs again, which would leave the pod alive but frozen:
     * liveness is always 200, so Kubernetes would not restart it. That is why {@link Throwable}
     * is caught and not just {@link Exception}.
     *
     * <p>Every finished cycle is reported to {@link MetricsRegistry#recordCycle}, except one
     * interrupted by shutdown.
     */
    public void collect() {
        long startNs = System.nanoTime();
        try {
            collectInternal();
            recordCycle(startNs, true);
        } catch (InterruptedException e) {
            // Shutdown interrupts the scheduler thread mid-cycle. Restore the flag so the
            // executor can finish tearing down, and do not count this as a collection failure.
            Thread.currentThread().interrupt();
            LOGGER.debug("Collection cycle interrupted");
        } catch (TimeoutException e) {
            // Only the deadline in collectInternal() throws this. A Kafka timeout is a different
            // class and arrives wrapped in ExecutionException, so it goes to the catch below.
            LOGGER.warn("Collection cycle exceeded {} ms, stale metrics keep being served",
                config.getCollectTimeoutMs());
            recordCycle(startNs, false);
        } catch (Exception e) {
            LOGGER.warn("Collection cycle failed, stale metrics keep being served", e);
            recordCycle(startNs, false);
        } catch (Throwable t) {
            // An Error raised on the scheduler thread itself (a branch's Error arrives wrapped in
            // ExecutionException above). Log it and keep the schedule alive: the next cycle may
            // well succeed, and if it does not the last-success timestamp keeps ageing.
            LOGGER.error("Collection cycle failed with an error, stale metrics keep being served", t);
            try {
                recordCycle(startNs, false);
            } catch (Throwable ignored) {
                // Recording must not let a secondary Throwable escape collect() and cancel the schedule.
            }
        }
    }

    private void recordCycle(long startNs, boolean succeeded) {
        registry.recordCycle(Duration.ofNanos(System.nanoTime() - startNs), succeeded);
    }

    /**
     * Core collection logic — separated from {@link #collect()} to simplify error handling.
     *
     * @throws ExecutionException   if an AdminClient future completes exceptionally
     * @throws InterruptedException if the thread is interrupted while waiting for a future
     * @throws TimeoutException     if the branches are not all done within
     *                              {@link SovaConfig#getCollectTimeoutMs()}
     */
    void collectInternal() throws ExecutionException, InterruptedException, TimeoutException {
        long startNs = System.nanoTime();
        long deadlineNs = startNs + MILLISECONDS.toNanos(config.getCollectTimeoutMs());

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

                // Every wait is bounded by one deadline for the whole cycle, so a branch stuck in
                // the AdminClient cannot hold the cycle, and with it the schedule, forever.
                brokerData = broker.get(deadlineNs - System.nanoTime(), NANOSECONDS);
                topicData = topics.get(deadlineNs - System.nanoTime(), NANOSECONDS);
                groupData = groups.get(deadlineNs - System.nanoTime(), NANOSECONDS);
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
