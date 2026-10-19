/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupStateKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that all expected Prometheus metrics are registered and emitted by
 * {@link MetricsRegistry}.
 *
 * <p>Gauges with label names but no observed data points are not emitted by the Prometheus
 * Java client until at least one label-value combination is set. This test therefore calls
 * {@link MetricsRegistry#update(MetricSnapshot)} with a minimal snapshot to
 * populate every metric family before scraping.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
class MetricsRegistryTest {

    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
    }

    /**
     * Populates every metric family with at least one data point via a minimal synthetic
     * snapshot, then verifies the exact set of expected Prometheus metric names is registered.
     *
     * <p>This asserts on exact names collected from the registry rather than substring matches on
     * the scraped text: {@code kafka_consumergroup_lag} is a substring of
     * {@code kafka_consumergroup_lag_sum}, {@code kafka_consumergroup_current_offset} is a
     * substring of {@code kafka_consumergroup_current_offset_sum}, and
     * {@code kafka_topic_partition_leader} is a substring of
     * {@code kafka_topic_partition_leader_is_preferred} — a text-contains check would stay green
     * even if one of the shorter names were deleted from {@link MetricsSnapshotRenderer}.
     *
     * <p>The build-info family is registered under the Prometheus name {@code sova_build}, not
     * {@code sova_build_info}: the {@code _info} suffix is added by the exposition writer, not
     * stored on the metadata.
     */
    @Test
    void allMetricsRegistered() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupTopicPartitionKey cgTpKey =
                new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey cgTKey =
                new ConsumerGroupTopicKey("g", "t");
        ConsumerGroupStateKey cgStateKey =
                new ConsumerGroupStateKey("g", "Stable");

        MetricSnapshot snapshot = new MetricSnapshot(
                1,
                Map.of("0", "broker0:9092"),
                Map.of("t", 1),
                Map.of(tp, new PartitionMetrics(0, 1, 1, true, false)),
                Map.of(tp, 100L),
                Map.of(tp, 0L),
                Map.of("g", 1),
                Map.of(cgTpKey, 80L),
                Map.of(cgTKey, 80L),
                Map.of(cgTpKey, 20L),
                Map.of(cgTKey, 20L),
                Map.of(cgStateKey, true),
                0L);

        registry.update(snapshot);

        Set<String> actualNames = registry.prometheusRegistry().scrape().stream()
                .map(s -> s.getMetadata().getPrometheusName())
                .collect(Collectors.toSet());

        assertRegistered(actualNames, "kafka_brokers");
        assertRegistered(actualNames, "kafka_broker_info");
        assertRegistered(actualNames, "kafka_topic_partitions");
        assertRegistered(actualNames, "kafka_topic_partition_current_offset");
        assertRegistered(actualNames, "kafka_topic_partition_oldest_offset");
        assertRegistered(actualNames, "kafka_topic_partition_leader");
        assertRegistered(actualNames, "kafka_topic_partition_replicas");
        assertRegistered(actualNames, "kafka_topic_partition_in_sync_replica");
        assertRegistered(actualNames, "kafka_topic_partition_leader_is_preferred");
        assertRegistered(actualNames, "kafka_topic_partition_under_replicated_partition");
        assertRegistered(actualNames, "kafka_consumergroup_members");
        assertRegistered(actualNames, "kafka_consumergroup_current_offset");
        assertRegistered(actualNames, "kafka_consumergroup_current_offset_sum");
        assertRegistered(actualNames, "kafka_consumergroup_lag");
        assertRegistered(actualNames, "kafka_consumergroup_lag_sum");
        assertRegistered(actualNames, "kafka_consumergroup_state");
        assertRegistered(actualNames, "sova_last_successful_collection_timestamp_seconds");
        assertRegistered(actualNames, "sova_build");
    }

    /**
     * The {@code process_*} and {@code jvm_*} families must be scraped alongside Sova's own.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void exposesTheProcessAndJvmFamilies() throws IOException {
        String scraped = scrape();

        assertPresent(scraped, "process_cpu_seconds_total");
        assertPresent(scraped, "process_start_time_seconds");
        assertPresent(scraped, "jvm_memory_used_bytes");
    }

    /**
     * The build-info labels must reach the scrape output, with the injected values.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void buildInfoLabelsAreExposed() throws IOException {
        MetricsRegistry custom = new MetricsRegistry(new BuildInfo("1.2.3", "abc1234", "release", "21.0.3"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, custom.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        String scraped = out.toString(StandardCharsets.UTF_8);

        assertPresent(scraped, "version=\"1.2.3\"");
        assertPresent(scraped, "revision=\"abc1234\"");
        assertPresent(scraped, "branch=\"release\"");
        assertPresent(scraped, "javaversion=\"21.0.3\"");
    }

    private void assertPresent(String output, String name) {
        assertTrue(output.contains(name),
                "Expected metric '" + name + "' not found in scrape output");
    }

    private void assertRegistered(Set<String> actualNames, String name) {
        assertTrue(actualNames.contains(name),
                "Expected metric '" + name + "' not found among registered names: " + actualNames);
    }

    /**
     * Builds a snapshot describing exactly one topic with one partition, so that two snapshots
     * built from different topic names share no series at all.
     *
     * @param topic topic name to appear in every topic-keyed family
     * @return a snapshot with every topic family populated for that one topic
     */
    private static MetricSnapshot snapshotForTopic(String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        return new MetricSnapshot(
                1,
                Map.of("0", "broker0:9092"),
                Map.of(topic, 1),
                Map.of(tp, new PartitionMetrics(0, 1, 1, true, false)),
                Map.of(tp, 100L),
                Map.of(tp, 0L),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                0L);
    }

    /**
     * Scrapes this instance's registry into the Prometheus text exposition format.
     *
     * @return the scrape body
     * @throws IOException if the text writer fails
     */
    private String scrape() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, registry.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * A topic that exists in one cycle and not the next must stop being exported. Prometheus'
     * {@code Gauge} caches a data point per label combination and never evicts it, so mutating
     * gauges in place leaks a series for every deleted topic and every dead consumer group —
     * they linger in dashboards and alerts for the lifetime of the pod.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void dropsSeriesForTopicsThatDisappearedSinceTheLastCycle() throws IOException {
        registry.update(snapshotForTopic("gonetopic"));
        registry.update(snapshotForTopic("livetopic"));

        String scraped = scrape();

        assertTrue(scraped.contains("livetopic"), "the current topic must be exported");
        assertFalse(scraped.contains("gonetopic"),
                "a topic absent from the latest cycle must not be exported, got:\n" + scraped);
    }

    /**
     * Before the first collection cycle — and after every topic is deleted — the collector must
     * still produce a valid scrape rather than throwing, which would turn every scrape into an
     * HTTP 500.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void servesAValidScrapeBeforeTheFirstCollectionCycle() throws IOException {
        String scraped = scrape();

        assertTrue(scraped.contains("kafka_brokers 0.0"),
                "broker count must read zero before the first cycle, got:\n" + scraped);
        assertTrue(scraped.contains("sova_build_info"),
                "build info must be exported before the first cycle, got:\n" + scraped);
    }

    /**
     * Builds a snapshot describing exactly one consumer group, so that two snapshots built from
     * different group ids share no series at all.
     *
     * @param group consumer group id
     * @return a snapshot with every consumer group family populated for that one group
     */
    private static MetricSnapshot snapshotForGroup(String group) {
        ConsumerGroupTopicPartitionKey cgtp =
                new ConsumerGroupTopicPartitionKey(group, "t", "0");
        ConsumerGroupTopicKey cgt =
                new ConsumerGroupTopicKey(group, "t");
        ConsumerGroupStateKey state =
                new ConsumerGroupStateKey(group, "Stable");
        return new MetricSnapshot(
                1,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(group, 1),
                Map.of(cgtp, 80L),
                Map.of(cgt, 80L),
                Map.of(cgtp, 20L),
                Map.of(cgt, 20L),
                Map.of(state, true),
                0L);
    }

    /**
     * A consumer group that stops existing must stop being exported, for the same reason a
     * deleted topic must: a dead group left in the scrape keeps firing lag alerts forever.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void dropsSeriesForConsumerGroupsThatDisappearedSinceTheLastCycle() throws IOException {
        registry.update(snapshotForGroup("gonegroup"));
        registry.update(snapshotForGroup("livegroup"));

        String scraped = scrape();

        assertTrue(scraped.contains("livegroup"), "the current group must be exported");
        assertFalse(scraped.contains("gonegroup"),
                "a group absent from the latest cycle must not be exported, got:\n" + scraped);
    }

    /**
     * A committed offset of -1 means the group never committed to that partition. The collector
     * leaves such partitions out of the sum maps entirely; the registry must expose the sums it
     * is given without inventing entries from the per-partition maps.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void exposesSumsExactlyAsCollectedWithoutRederivingThemFromPartitions() throws IOException {
        ConsumerGroupTopicPartitionKey committed =
                new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicPartitionKey neverCommitted =
                new ConsumerGroupTopicPartitionKey("g", "t", "1");
        ConsumerGroupTopicKey cgt = new ConsumerGroupTopicKey("g", "t");

        registry.update(new MetricSnapshot(
                1, Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                Map.of("g", 1),
                Map.of(committed, 80L, neverCommitted, -1L),
                Map.of(cgt, 80L),
                Map.of(committed, 20L),
                Map.of(cgt, 20L),
                Map.of(),
                0L));

        String scraped = scrape();

        assertTrue(scraped.contains("kafka_consumergroup_current_offset_sum{consumergroup=\"g\",topic=\"t\"} 80.0"),
                "the sum must be the collected 80, not 79 with the -1 folded in, got:\n" + scraped);
        assertTrue(scraped.contains("kafka_consumergroup_lag_sum{consumergroup=\"g\",topic=\"t\"} 20.0"),
                "the lag sum must be the collected 20, got:\n" + scraped);
        assertTrue(scraped.contains("kafka_consumergroup_current_offset{consumergroup=\"g\",partition=\"1\",topic=\"t\"} -1.0"),
                "the per-partition -1 must still be exported, got:\n" + scraped);
    }

    /**
     * Consumer group ids are arbitrary strings, unlike topic names. A quote or backslash in one
     * must not throw inside collect() — that would turn every scrape into an HTTP 500, taking
     * out all metrics rather than one series.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void escapesConsumerGroupIdsThatContainQuotesAndBackslashes() throws IOException {
        registry.update(snapshotForGroup("weird\"group\\id"));

        String scraped = scrape();

        assertTrue(scraped.contains("weird\\\"group\\\\id"),
                "group id must be escaped in the exposition text, got:\n" + scraped);
    }

    /**
     * A scrape that races startup — before any collection cycle has run — must read 0, not the
     * current time. That way an alert can tell a never-collected exporter from a fresh one.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void stalenessTimestampIsZeroBeforeFirstCollection() throws IOException {
        // No update() call: this is what a scrape racing startup sees.
        String scraped = scrape();

        assertTrue(scraped.contains("sova_last_successful_collection_timestamp_seconds 0.0"),
                "a never-collected exporter must read 0, not the current time, so an alert can "
                        + "tell it apart from a freshly started one. Scrape was:\n" + scraped);
    }
}
