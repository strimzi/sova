/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * Immutable value object carrying all data collected in one collection cycle.
 *
 * <p>All maps are expected to be unmodifiable after construction. The snapshot is produced by
 * {@link io.strimzi.sova.metrics.collectors.MetricsCollector} and consumed by
 * {@link MetricsRegistry#update(MetricSnapshot)}.
 *
 * <p>Partition-keyed maps use the Kafka client's own {@link TopicPartition} rather than a local
 * key type with a pre-stringified partition number. The lag calculation needs the number, the
 * labels need the string, and keeping both meant carrying the same offsets in two maps.
 *
 * @param brokerCount                      total number of brokers
 * @param brokerAddresses                  map of broker-id → host:port
 * @param topicPartitionCounts             map of topic → partition count
 * @param partitions                       map of topic-partition → topology of that partition
 * @param currentOffsets                   map of topic-partition → latest offset
 * @param oldestOffsets                    map of topic-partition → earliest offset
 * @param consumerGroupMembers             map of group-id → member count
 * @param consumerGroupCurrentOffsets      map of group+topic+partition → committed offset
 * @param consumerGroupCurrentOffsetSums   map of group+topic → sum of committed offsets
 * @param consumerGroupLags                map of group+topic+partition → lag
 * @param consumerGroupLagSums             map of group+topic → sum of lags
 * @param consumerGroupStates              map of group+state → true (presence indicator)
 * @param collectedAtMs                    wall-clock time the cycle completed, epoch millis,
 *                                         or 0 before the first successful cycle
 */
public record MetricSnapshot(
        int brokerCount,
        Map<String, String> brokerAddresses,
        Map<String, Integer> topicPartitionCounts,
        Map<TopicPartition, PartitionMetrics> partitions,
        Map<TopicPartition, Long> currentOffsets,
        Map<TopicPartition, Long> oldestOffsets,
        Map<String, Integer> consumerGroupMembers,
        Map<ConsumerGroupTopicPartitionKey, Long> consumerGroupCurrentOffsets,
        Map<ConsumerGroupTopicKey, Long> consumerGroupCurrentOffsetSums,
        Map<ConsumerGroupTopicPartitionKey, Long> consumerGroupLags,
        Map<ConsumerGroupTopicKey, Long> consumerGroupLagSums,
        Map<ConsumerGroupStateKey, Boolean> consumerGroupStates,
        long collectedAtMs) {

    /**
     * The topology of one partition, as reported by {@code describeTopics}. These five values are
     * always collected together and always present together, so they travel as one value rather
     * than as five maps sharing a key.
     *
     * @param leader            leader broker id, or -1 when the partition has no leader
     * @param replicas          total replica count
     * @param isr               in-sync replica count
     * @param leaderIsPreferred whether the leader is the first replica in the assignment
     * @param underReplicated   whether fewer replicas are in sync than are assigned
     */
    public record PartitionMetrics(int leader, int replicas, int isr,
            boolean leaderIsPreferred, boolean underReplicated) { }

    /**
     * Composite key for consumer-group + topic + partition.
     *
     * @param group     consumer group id
     * @param topic     topic name
     * @param partition partition number as a string label
     */
    public record ConsumerGroupTopicPartitionKey(String group, String topic, String partition) { }

    /**
     * Composite key for consumer-group + topic (used for sum metrics).
     *
     * @param group consumer group id
     * @param topic topic name
     */
    public record ConsumerGroupTopicKey(String group, String topic) { }

    /**
     * Composite key for consumer-group + state (used for state metric).
     *
     * @param group consumer group id
     * @param state consumer group state string
     */
    public record ConsumerGroupStateKey(String group, String state) { }
}
