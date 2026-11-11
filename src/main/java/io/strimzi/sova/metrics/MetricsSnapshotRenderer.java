/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.apache.kafka.common.TopicPartition;

import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupStateKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;

/**
 * Converts one collected {@link MetricSnapshot} into the Prometheus metric families served by a
 * scrape.
 *
 * <p>Stateless and side-effect free: everything a scrape exposes is derived from the snapshot
 * handed in. That is what makes a scrape atomic — the caller reads the snapshot reference once
 * and every family below comes from that one read — and it is also what makes series disappear
 * when they leave the cluster, since nothing is carried over from the previous call.
 *
 * <p>The metric names and help strings here are a fixed contract with existing Prometheus
 * dashboards and alerts. Never change one.
 *
 * <p>This is a separate class rather than part of {@link MetricsRegistry} because the Prometheus
 * snapshot model types push {@code MetricsRegistry} past Checkstyle's {@code ClassFanOutComplexity}
 * limit of 20.
 */
final class MetricsSnapshotRenderer {
    // Label names are part of the same contract as the metric names: dashboards query by them.
    private static final String LABEL_TOPIC = "topic";
    private static final String LABEL_PARTITION = "partition";
    private static final String LABEL_CONSUMERGROUP = "consumergroup";
    private static final String LABEL_STATE = "state";
    private static final String LABEL_ID = "id";
    private static final String LABEL_ADDRESS = "address";

    private MetricsSnapshotRenderer() {
    }

    /**
     * Builds every metric family from a single snapshot.
     *
     * @param s         the collected values for one cycle
     * @param buildInfo the constant build-info family, built once at startup
     * @return the families to expose for this scrape
     */
    static MetricSnapshots from(MetricSnapshot s, InfoSnapshot buildInfo) {
        return MetricSnapshots.builder()
            .metricSnapshot(scalar("kafka_brokers",
                "Number of Brokers in the Kafka Cluster.",
                s.brokerCount()))
            .metricSnapshot(brokerInfo(s.brokerAddresses()))
            .metricSnapshot(family("kafka_topic_partitions",
                "Number of partitions for this Topic.",
                s.topicPartitionCounts(),
                topic -> Labels.of(LABEL_TOPIC, topic),
                Integer::doubleValue))
            .metricSnapshot(family("kafka_topic_partition_current_offset",
                "Current Offset of a Broker at Topic/Partition.",
                s.currentOffsets(), MetricsSnapshotRenderer::topicPartitionLabels, Long::doubleValue))
            .metricSnapshot(family("kafka_topic_partition_oldest_offset",
                "Oldest Offset of a Broker at Topic/Partition.",
                s.oldestOffsets(), MetricsSnapshotRenderer::topicPartitionLabels, Long::doubleValue))
            .metricSnapshot(family("kafka_topic_partition_leader",
                "Leader Broker ID of this Topic/Partition.",
                s.partitions(), MetricsSnapshotRenderer::topicPartitionLabels, PartitionMetrics::leader))
            .metricSnapshot(family("kafka_topic_partition_replicas",
                "Number of Replicas for this Topic/Partition.",
                s.partitions(), MetricsSnapshotRenderer::topicPartitionLabels, PartitionMetrics::replicas))
            .metricSnapshot(family("kafka_topic_partition_in_sync_replica",
                "Number of In-Sync Replicas for this Topic/Partition.",
                s.partitions(), MetricsSnapshotRenderer::topicPartitionLabels, PartitionMetrics::isr))
            .metricSnapshot(family("kafka_topic_partition_leader_is_preferred",
                "1 if Topic/Partition is using the Preferred Broker.",
                s.partitions(), MetricsSnapshotRenderer::topicPartitionLabels,
                pm -> flag(pm.leaderIsPreferred())))
            .metricSnapshot(family("kafka_topic_partition_under_replicated_partition",
                "1 if Topic/Partition is under Replicated.",
                s.partitions(), MetricsSnapshotRenderer::topicPartitionLabels,
                pm -> flag(pm.underReplicated())))
            .metricSnapshot(family("kafka_consumergroup_members",
                "Amount of members in a consumer group.",
                s.consumerGroupMembers(),
                group -> Labels.of(LABEL_CONSUMERGROUP, group),
                Integer::doubleValue))
            .metricSnapshot(family("kafka_consumergroup_current_offset",
                "Current Offset of a ConsumerGroup at Topic/Partition.",
                s.consumerGroupCurrentOffsets(), MetricsSnapshotRenderer::consumerGroupTopicPartitionLabels, Long::doubleValue))
            .metricSnapshot(family("kafka_consumergroup_current_offset_sum",
                "Current Offset of a ConsumerGroup at Topic for all partitions.",
                s.consumerGroupCurrentOffsetSums(), MetricsSnapshotRenderer::consumerGroupTopicLabels, Long::doubleValue))
            .metricSnapshot(family("kafka_consumergroup_lag",
                "Current Approximate Lag of a ConsumerGroup at Topic/Partition.",
                s.consumerGroupLags(), MetricsSnapshotRenderer::consumerGroupTopicPartitionLabels, Long::doubleValue))
            .metricSnapshot(family("kafka_consumergroup_lag_sum",
                "Current Approximate Lag of a ConsumerGroup at Topic for all partitions.",
                s.consumerGroupLagSums(), MetricsSnapshotRenderer::consumerGroupTopicLabels, Long::doubleValue))
            .metricSnapshot(consumerGroupStates(s.consumerGroupStates()))
            .metricSnapshot(scalar("sova_last_successful_collection_timestamp_seconds",
                "Unix timestamp of the last successful metrics collection cycle, "
                    + "or 0 if none has completed.",
                s.collectedAtMs() / 1000.0))
            .metricSnapshot(buildInfo)
            .build();
    }

    /**
     * Builds a gauge family with one unlabelled data point.
     *
     * @param name  metric name
     * @param help  help string
     * @param value the value
     * @return the family
     */
    private static GaugeSnapshot scalar(String name, String help, double value) {
        return GaugeSnapshot.builder()
            .name(name)
            .help(help)
            .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                .value(value)
                .build())
            .build();
    }

    /**
     * Builds a gauge family with one data point per map entry.
     *
     * <p>An empty map yields a family with no data points, which the exposition writer omits
     * entirely — the same as an unused label combination on a {@code Gauge}.
     *
     * @param name     metric name
     * @param help     help string
     * @param source   the snapshot map backing this family
     * @param labelsOf turns a map key into the data point's labels
     * @param valueOf  turns a map value into the data point's value
     * @param <K>      map key type
     * @param <V>      map value type
     * @return the family
     */
    private static <K, V> GaugeSnapshot family(String name, String help, Map<K, V> source,
                                               Function<K, Labels> labelsOf,
                                               ToDoubleFunction<V> valueOf) {
        GaugeSnapshot.Builder builder = GaugeSnapshot.builder()
            .name(name)
            .help(help);
        for (Map.Entry<K, V> entry : source.entrySet()) {
            builder.dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                .labels(labelsOf.apply(entry.getKey()))
                .value(valueOf.applyAsDouble(entry.getValue()))
                .build());
        }
        return builder.build();
    }

    /**
     * Builds {@code kafka_broker_info}, whose labels come from both the key and the value of the
     * map and whose value is always 1.
     *
     * @param brokerAddresses map of broker id to host:port
     * @return the family
     */
    private static GaugeSnapshot brokerInfo(Map<String, String> brokerAddresses) {
        GaugeSnapshot.Builder builder = GaugeSnapshot.builder()
            .name("kafka_broker_info")
            .help("Information about the Kafka Broker.");
        for (Map.Entry<String, String> entry : brokerAddresses.entrySet()) {
            builder.dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                .labels(Labels.of(LABEL_ID, entry.getKey(), LABEL_ADDRESS, entry.getValue()))
                .value(1)
                .build());
        }
        return builder.build();
    }

    /**
     * Builds {@code kafka_consumergroup_state}. Only states flagged true get a data point — the
     * metric is a presence indicator whose value is always 1.
     *
     * @param states map of group and state to presence
     * @return the family
     */
    private static GaugeSnapshot consumerGroupStates(Map<ConsumerGroupStateKey, Boolean> states) {
        GaugeSnapshot.Builder builder = GaugeSnapshot.builder()
            .name("kafka_consumergroup_state")
            .help("The state of a consumer group.");
        for (Map.Entry<ConsumerGroupStateKey, Boolean> entry : states.entrySet()) {
            if (Boolean.TRUE.equals(entry.getValue())) {
                builder.dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                    .labels(Labels.of(LABEL_CONSUMERGROUP, entry.getKey()
                            .group(),
                        LABEL_STATE, entry.getKey()
                            .state()))
                    .value(1)
                    .build());
            }
        }
        return builder.build();
    }

    /**
     * Builds the constant {@code sova_build_info} family.
     *
     * <p>The metadata name deliberately omits the {@code _info} suffix: the exposition writer
     * appends it, and passing the full name here would expose
     * {@code sova_build_info_info}.
     *
     * @param info the build metadata
     * @return the family
     */
    static InfoSnapshot buildInfo(BuildInfo info) {
        return InfoSnapshot.builder()
            .name("sova_build")
            .help("A metric with a constant value of 1 labeled with build information.")
            .dataPoint(InfoSnapshot.InfoDataPointSnapshot.builder()
                .labels(Labels.of(
                    "version", info.version(),
                    "revision", info.revision(),
                    "branch", info.branch(),
                    "java_info", info.javaVersion()))
                .build())
            .build();
    }

    /**
     * @param tp topic-partition
     * @return the {@code topic} and {@code partition} labels for it
     */
    private static Labels topicPartitionLabels(TopicPartition tp) {
        return Labels.of(LABEL_TOPIC, tp.topic(), LABEL_PARTITION, String.valueOf(tp.partition()));
    }

    /**
     * @param key consumer group, topic and partition key
     * @return the {@code consumergroup}, {@code topic} and {@code partition} labels
     */
    private static Labels consumerGroupTopicPartitionLabels(ConsumerGroupTopicPartitionKey key) {
        return Labels.of(LABEL_CONSUMERGROUP, key.group(), LABEL_TOPIC, key.topic(),
            LABEL_PARTITION, key.partition());
    }

    /**
     * @param key consumer group and topic key
     * @return the {@code consumergroup} and {@code topic} labels
     */
    private static Labels consumerGroupTopicLabels(ConsumerGroupTopicKey key) {
        return Labels.of(LABEL_CONSUMERGROUP, key.group(), LABEL_TOPIC, key.topic());
    }

    /**
     * @param value a boolean partition property
     * @return 1 when true, 0 otherwise
     */
    private static double flag(boolean value) {
        return value ? 1 : 0;
    }
}
