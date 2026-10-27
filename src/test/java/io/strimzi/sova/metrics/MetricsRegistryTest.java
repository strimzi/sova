/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupStateKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.strimzi.sova.metrics.TestSnapshots.scrape;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies what {@link MetricsRegistry} emits for a scrape: the exact metric names, their label
 * and value rendering, and the one-cycle-per-scrape guarantee.
 *
 * <p>Gauges with label names but no observed data points are not emitted by the Prometheus
 * Java client until at least one label-value combination is set. These tests therefore call
 * {@link MetricsRegistry#update(MetricSnapshot)} with a snapshot that populates the families
 * they look at before scraping.
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
        registry.update(fullSnapshot());

        Set<String> actualNames = registry.prometheusRegistry().scrape().stream()
                .map(s -> s.getMetadata().getPrometheusName())
                .collect(Collectors.toSet());

        for (String name : Set.of(
                "kafka_brokers",
                "kafka_broker_info",
                "kafka_topic_partitions",
                "kafka_topic_partition_current_offset",
                "kafka_topic_partition_oldest_offset",
                "kafka_topic_partition_leader",
                "kafka_topic_partition_replicas",
                "kafka_topic_partition_in_sync_replica",
                "kafka_topic_partition_leader_is_preferred",
                "kafka_topic_partition_under_replicated_partition",
                "kafka_consumergroup_members",
                "kafka_consumergroup_current_offset",
                "kafka_consumergroup_current_offset_sum",
                "kafka_consumergroup_lag",
                "kafka_consumergroup_lag_sum",
                "kafka_consumergroup_state",
                "sova_last_successful_collection_timestamp_seconds",
                "sova_build")) {
            assertTrue(actualNames.contains(name),
                    "Expected metric '" + name + "' not found among registered names: " + actualNames);
        }
    }

    /**
     * The labels and values of the families that are easy to get subtly wrong: the broker info
     * labels come from both key and value of a map, the state metric is one series per
     * {@code true} entry only, and the partition flags are rendered as 1/0.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void rendersLabelsAndValuesOfBrokerInfoGroupStateAndPartitionFlags() throws IOException {
        TopicPartition healthy = new TopicPartition("t", 0);
        TopicPartition degraded = new TopicPartition("t", 1);
        registry.update(TestSnapshots.builder()
                .brokerAddresses(Map.of("0", "broker0:9092"))
                .partitions(Map.of(
                        healthy, new PartitionMetrics(0, 3, 3, true, false),
                        degraded, new PartitionMetrics(-1, 3, 2, false, true)))
                .states(Map.of(
                        new ConsumerGroupStateKey("g", "Stable"), true,
                        new ConsumerGroupStateKey("g", "Empty"), false))
                .build());

        String scraped = scrape(registry);

        assertTrue(scraped.contains("kafka_broker_info{address=\"broker0:9092\",id=\"0\"} 1.0"),
                "broker info must carry id and address, got:\n" + scraped);
        assertTrue(scraped.contains("kafka_consumergroup_state{consumergroup=\"g\",state=\"Stable\"} 1.0"),
                "a present state must be exported, got:\n" + scraped);
        assertFalse(scraped.contains("state=\"Empty\""),
                "a state flagged false must not be exported, got:\n" + scraped);
        assertTrue(scraped.contains("kafka_topic_partition_leader{partition=\"1\",topic=\"t\"} -1.0"),
                "a partition without a leader must read -1, got:\n" + scraped);
        assertTrue(scraped.contains("kafka_topic_partition_leader_is_preferred{partition=\"0\",topic=\"t\"} 1.0"));
        assertTrue(scraped.contains("kafka_topic_partition_leader_is_preferred{partition=\"1\",topic=\"t\"} 0.0"));
        assertTrue(scraped.contains("kafka_topic_partition_under_replicated_partition{partition=\"0\",topic=\"t\"} 0.0"));
        assertTrue(scraped.contains("kafka_topic_partition_under_replicated_partition{partition=\"1\",topic=\"t\"} 1.0"));
        assertTrue(scraped.contains("kafka_topic_partition_in_sync_replica{partition=\"1\",topic=\"t\"} 2.0"));
    }

    /**
     * The {@code process_*} and {@code jvm_*} families must be scraped alongside Sova's own.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void exposesTheProcessAndJvmFamilies() throws IOException {
        String scraped = scrape(registry);

        assertTrue(scraped.contains("process_cpu_seconds_total"));
        assertTrue(scraped.contains("process_start_time_seconds"));
        assertTrue(scraped.contains("jvm_memory_used_bytes"));
    }

    /**
     * The build-info labels must reach the scrape output, with the injected values.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void buildInfoLabelsAreExposed() throws IOException {
        MetricsRegistry custom = new MetricsRegistry(new BuildInfo("1.2.3", "abc1234", "release", "21.0.3"));

        String scraped = scrape(custom);

        assertTrue(scraped.contains("version=\"1.2.3\""));
        assertTrue(scraped.contains("revision=\"abc1234\""));
        assertTrue(scraped.contains("branch=\"release\""));
        assertTrue(scraped.contains("java_info=\"21.0.3\""));
    }

    /**
     * Before the first collection cycle the scrape must still be valid rather than throwing, which
     * would turn every scrape into an HTTP 500, and the staleness timestamp must read 0, not the
     * current time, so an alert can tell a never-collected exporter from a freshly started one.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void servesAValidScrapeBeforeTheFirstCollectionCycle() throws IOException {
        String scraped = scrape(registry);

        assertTrue(scraped.contains("kafka_brokers 0.0"),
                "broker count must read zero before the first cycle, got:\n" + scraped);
        assertTrue(scraped.contains("sova_build_info"),
                "build info must be exported before the first cycle, got:\n" + scraped);
        assertTrue(scraped.contains("sova_last_successful_collection_timestamp_seconds 0.0"),
                "a never-collected exporter must read 0, not the current time. Scrape was:\n" + scraped);
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

        String scraped = scrape(registry);

        assertTrue(scraped.contains("livetopic"), "the current topic must be exported");
        assertFalse(scraped.contains("gonetopic"),
                "a topic absent from the latest cycle must not be exported, got:\n" + scraped);
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

        String scraped = scrape(registry);

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

        registry.update(TestSnapshots.builder()
                .members(Map.of("g", 1))
                .groupOffsets(Map.of(committed, 80L, neverCommitted, -1L))
                .groupOffsetSums(Map.of(cgt, 80L))
                .lags(Map.of(committed, 20L))
                .lagSums(Map.of(cgt, 20L))
                .build());

        String scraped = scrape(registry);

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

        String scraped = scrape(registry);

        assertTrue(scraped.contains("weird\\\"group\\\\id"),
                "group id must be escaped in the exposition text, got:\n" + scraped);
    }

    /**
     * A scrape must reflect exactly one collection cycle. {@code PrometheusRegistry.scrape()}
     * holds no lock on this class, so serialising writers with {@code synchronized} never made
     * readers safe: a scrape landing between two per-gauge mutations read some families from the
     * new cycle and some from the old one, and a derived value such as lag briefly described a
     * cluster state that never existed.
     *
     * @throws Exception if the updater thread cannot be joined
     */
    @Test
    void neverServesAScrapeThatMixesTwoCollectionCycles() throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        Thread updater = Thread.ofVirtual().start(() -> {
            int generation = 1;
            while (!stop.get()) {
                registry.update(snapshotForGeneration(generation));
                generation = generation == 1 ? 2 : 1;
            }
        });

        String mixed = null;
        try {
            for (int i = 0; i < 2_000 && mixed == null; i++) {
                String scraped = scrape(registry);
                boolean brokersFromGenerationOne = scraped.contains("kafka_brokers 1.0");
                boolean partitionsFromGenerationOne =
                        scraped.contains("kafka_topic_partitions{topic=\"t\"} 1.0");
                if (brokersFromGenerationOne != partitionsFromGenerationOne) {
                    mixed = scraped;
                }
            }
        } finally {
            stop.set(true);
            updater.join();
        }

        assertNull(mixed, "scrape mixed two collection cycles:\n" + mixed);
    }

    /**
     * Each scrape rebuilds every family from the snapshot, so scrape cost is proportional to the
     * number of series. A large cluster must still scrape, and the last series must be present.
     *
     * @throws IOException if the text writer fails
     */
    @Test
    void scrapesALargeClusterWithoutFailing() throws IOException {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        Map<String, Integer> partitionCounts = new HashMap<>();
        for (int topic = 0; topic < 100; topic++) {
            partitionCounts.put("topic-" + topic, 100);
            for (int partition = 0; partition < 100; partition++) {
                offsets.put(new TopicPartition("topic-" + topic, partition), 1L);
            }
        }
        registry.update(TestSnapshots.builder()
                .brokerCount(3)
                .topicPartitionCounts(partitionCounts)
                .currentOffsets(offsets)
                .build());

        String scraped = scrape(registry);

        assertTrue(scraped.contains("kafka_topic_partition_current_offset{partition=\"99\",topic=\"topic-99\"} 1.0"),
                "the last of 10,000 partition series must be exported");
    }

    /**
     * @return a snapshot with at least one data point in every family
     */
    private static MetricSnapshot fullSnapshot() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupTopicPartitionKey cgTpKey = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey cgTKey = new ConsumerGroupTopicKey("g", "t");
        return TestSnapshots.builder()
                .brokerCount(1)
                .brokerAddresses(Map.of("0", "broker0:9092"))
                .topicPartitionCounts(Map.of("t", 1))
                .partitions(Map.of(tp, new PartitionMetrics(0, 1, 1, true, false)))
                .currentOffsets(Map.of(tp, 100L))
                .oldestOffsets(Map.of(tp, 0L))
                .members(Map.of("g", 1))
                .groupOffsets(Map.of(cgTpKey, 80L))
                .groupOffsetSums(Map.of(cgTKey, 80L))
                .lags(Map.of(cgTpKey, 20L))
                .lagSums(Map.of(cgTKey, 20L))
                .states(Map.of(new ConsumerGroupStateKey("g", "Stable"), true))
                .build();
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
        return TestSnapshots.builder()
                .brokerCount(1)
                .brokerAddresses(Map.of("0", "broker0:9092"))
                .topicPartitionCounts(Map.of(topic, 1))
                .partitions(Map.of(tp, new PartitionMetrics(0, 1, 1, true, false)))
                .currentOffsets(Map.of(tp, 100L))
                .oldestOffsets(Map.of(tp, 0L))
                .build();
    }

    /**
     * Builds a snapshot describing exactly one consumer group, so that two snapshots built from
     * different group ids share no series at all.
     *
     * @param group consumer group id
     * @return a snapshot with every consumer group family populated for that one group
     */
    private static MetricSnapshot snapshotForGroup(String group) {
        ConsumerGroupTopicPartitionKey cgtp = new ConsumerGroupTopicPartitionKey(group, "t", "0");
        ConsumerGroupTopicKey cgt = new ConsumerGroupTopicKey(group, "t");
        return TestSnapshots.builder()
                .brokerCount(1)
                .members(Map.of(group, 1))
                .groupOffsets(Map.of(cgtp, 80L))
                .groupOffsetSums(Map.of(cgt, 80L))
                .lags(Map.of(cgtp, 20L))
                .lagSums(Map.of(cgt, 20L))
                .states(Map.of(new ConsumerGroupStateKey(group, "Stable"), true))
                .build();
    }

    /**
     * Builds a snapshot in which every family encodes the same generation number, so any scrape
     * that mixes two cycles is detectable from the exposition text alone.
     *
     * @param generation the value written into every family
     * @return a self-consistent snapshot
     */
    private static MetricSnapshot snapshotForGeneration(int generation) {
        TopicPartition tp = new TopicPartition("t", 0);
        return TestSnapshots.builder()
                .brokerCount(generation)
                .topicPartitionCounts(Map.of("t", generation))
                .partitions(Map.of(tp, new PartitionMetrics(generation, generation, generation, true, false)))
                .currentOffsets(Map.of(tp, (long) generation))
                .oldestOffsets(Map.of(tp, (long) generation))
                .build();
    }
}
