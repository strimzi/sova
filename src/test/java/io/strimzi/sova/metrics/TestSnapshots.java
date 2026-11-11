/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupStateKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;
import org.apache.kafka.common.TopicPartition;

/**
 * Test helpers shared by the registry, collector and integration tests: a {@link MetricSnapshot}
 * builder that defaults every family to empty, and a scrape to exposition text.
 */
public final class TestSnapshots {
    private TestSnapshots() {
    }

    /**
     * @return a builder whose every family is empty and whose collection time is 0
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Scrapes a registry into the Prometheus text exposition format.
     *
     * @param registry the registry to scrape
     * @return the scrape body
     * @throws IOException if the text writer fails
     */
    public static String scrape(MetricsRegistry registry) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, registry.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * Fluent builder for {@link MetricSnapshot}; unset families stay empty.
     */
    public static final class Builder {
        private int brokerCount;
        private Map<String, String> brokerAddresses = Map.of();
        private Map<String, Integer> topicPartitionCounts = Map.of();
        private Map<TopicPartition, PartitionMetrics> partitions = Map.of();
        private Map<TopicPartition, Long> currentOffsets = Map.of();
        private Map<TopicPartition, Long> oldestOffsets = Map.of();
        private Map<String, Integer> members = Map.of();
        private Map<ConsumerGroupTopicPartitionKey, Long> groupOffsets = Map.of();
        private Map<ConsumerGroupTopicKey, Long> groupOffsetSums = Map.of();
        private Map<ConsumerGroupTopicPartitionKey, Long> lags = Map.of();
        private Map<ConsumerGroupTopicKey, Long> lagSums = Map.of();
        private Map<ConsumerGroupStateKey, Boolean> states = Map.of();
        private long collectedAtMs;

        private Builder() {
        }

        public Builder brokerCount(int value) {
            brokerCount = value;
            return this;
        }

        public Builder brokerAddresses(Map<String, String> value) {
            brokerAddresses = value;
            return this;
        }

        public Builder topicPartitionCounts(Map<String, Integer> value) {
            topicPartitionCounts = value;
            return this;
        }

        public Builder partitions(Map<TopicPartition, PartitionMetrics> value) {
            partitions = value;
            return this;
        }

        public Builder currentOffsets(Map<TopicPartition, Long> value) {
            currentOffsets = value;
            return this;
        }

        public Builder oldestOffsets(Map<TopicPartition, Long> value) {
            oldestOffsets = value;
            return this;
        }

        public Builder members(Map<String, Integer> value) {
            members = value;
            return this;
        }

        public Builder groupOffsets(Map<ConsumerGroupTopicPartitionKey, Long> value) {
            groupOffsets = value;
            return this;
        }

        public Builder groupOffsetSums(Map<ConsumerGroupTopicKey, Long> value) {
            groupOffsetSums = value;
            return this;
        }

        public Builder lags(Map<ConsumerGroupTopicPartitionKey, Long> value) {
            lags = value;
            return this;
        }

        public Builder lagSums(Map<ConsumerGroupTopicKey, Long> value) {
            lagSums = value;
            return this;
        }

        public Builder states(Map<ConsumerGroupStateKey, Boolean> value) {
            states = value;
            return this;
        }

        /**
         * @param value epoch millis the cycle completed
         * @return this builder
         */
        public Builder collectedAtMs(long value) {
            collectedAtMs = value;
            return this;
        }

        /**
         * @return the snapshot
         */
        public MetricSnapshot build() {
            return new MetricSnapshot(brokerCount, brokerAddresses, topicPartitionCounts, partitions,
                    currentOffsets, oldestOffsets, members, groupOffsets, groupOffsetSums, lags,
                    lagSums, states, collectedAtMs);
        }
    }
}
