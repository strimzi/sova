/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency and scale tests for {@link MetricsRegistry} that are in their own class to avoid
 * pushing {@code MetricsRegistryTest} past Checkstyle's {@code ClassDataAbstractionCoupling}
 * limit of 7 distinct instantiated types.
 */
class MetricsRegistryConcurrencyTest {

    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
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
            for (int i = 0; i < 20_000 && mixed == null; i++) {
                String scraped = scrape();
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
     * Each scrape now rebuilds every family rather than reading pre-mutated gauges, so scrape
     * cost is proportional to the number of series. A large cluster must still scrape.
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

        registry.update(new MetricSnapshot(
                3, Map.of(), partitionCounts, Map.of(), offsets, Map.of(), Map.of(), Map.of(),
                Map.of(), Map.of(), Map.of(), Map.of(), 0L));

        String scraped = scrape();

        assertTrue(scraped.contains("kafka_topic_partition_current_offset{partition=\"99\",topic=\"topic-99\"} 1.0"),
                "the last of 10,000 partition series must be exported");
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
        return new MetricSnapshot(
                generation,
                Map.of(),
                Map.of("t", generation),
                Map.of(tp, new PartitionMetrics(generation, generation, generation, true, false)),
                Map.of(tp, (long) generation),
                Map.of(tp, (long) generation),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                0L);
    }

    private String scrape() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, registry.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        return out.toString(StandardCharsets.UTF_8);
    }
}
