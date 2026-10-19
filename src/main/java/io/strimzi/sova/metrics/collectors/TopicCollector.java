/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;

/**
 * Collects topic partition topology and offset metadata via {@code AdminClient}.
 */
final class TopicCollector {

    private static final Logger log = LoggerFactory.getLogger(TopicCollector.class);

    private TopicCollector() {
    }

    /**
     * Holds all topic-level collected data.
     *
     * <p>Topology is always present for every partition described this cycle; offsets are not,
     * because a partition whose {@code listOffsets} call failed is dropped individually. Hence
     * the separate maps rather than one wider value.
     *
     * @param topicPartitionCounts partition counts per topic
     * @param partitions           topology per partition
     * @param latestOffsets        latest (high-water-mark) offset per partition
     * @param earliestOffsets      earliest available offset per partition
     */
    record TopicData(Map<String, Integer> topicPartitionCounts,
                     Map<TopicPartition, PartitionMetrics> partitions,
                     Map<TopicPartition, Long> latestOffsets,
                     Map<TopicPartition, Long> earliestOffsets) {
    }

    private record OffsetData(Map<TopicPartition, Long> latest, Map<TopicPartition, Long> earliest) {
    }

    /**
     * Lists, describes, and collects offset data for filtered topics.
     *
     * @param admin  Kafka AdminClient
     * @param config application configuration, carrying the compiled topic filters
     * @return topic partition topology and offset data
     * @throws ExecutionException        on AdminClient error
     * @throws InterruptedException      on thread interrupt
     * @throws CollectionFailedException if no topic or offset lookup in the cycle succeeded
     */
    static TopicData collect(Admin admin, SovaConfig config)
        throws ExecutionException, InterruptedException, CollectionFailedException {
        Set<String> allTopics = admin.listTopics()
            .names()
            .get();
        List<String> filtered = new ArrayList<>();
        for (String topic : allTopics) {
            if (CollectorUtils.matches(topic, config.getTopicPattern(),
                config.getTopicExcludePattern())) {
                filtered.add(topic);
            }
        }
        log.debug("Topics: {} total, {} after filtering", allTopics.size(), filtered.size());

        // Per-topic futures rather than allTopicNames(): a topic deleted between listTopics and
        // describeTopics must cost us that topic, not the whole cycle.
        Map<String, TopicDescription> descriptions =
            CollectorUtils.resolveAll(admin.describeTopics(filtered)
                .topicNameValues(), "topic");
        return buildTopicData(admin, descriptions);
    }

    private static TopicData buildTopicData(Admin admin, Map<String, TopicDescription> descriptions)
        throws ExecutionException, InterruptedException, CollectionFailedException {
        Map<String, Integer> partitionCounts = new HashMap<>();
        Map<TopicPartition, PartitionMetrics> partitions = new HashMap<>();

        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            String topic = entry.getKey();
            List<TopicPartitionInfo> infos = entry.getValue()
                .partitions();
            partitionCounts.put(topic, infos.size());
            for (TopicPartitionInfo info : infos) {
                partitions.put(new TopicPartition(topic, info.partition()), partitionMetrics(info));
            }
        }

        OffsetData offsets = collectOffsets(admin, partitions.keySet());
        return new TopicData(partitionCounts, partitions, offsets.latest(), offsets.earliest());
    }

    private static PartitionMetrics partitionMetrics(TopicPartitionInfo info) {
        int leaderId = info.leader() != null ? info.leader()
            .id() : -1;
        boolean preferred = !info.replicas()
            .isEmpty()
            && info.leader() != null
            && info.replicas()
            .get(0)
            .id() == info.leader()
            .id();
        boolean underReplicated = info.isr()
            .size() < info.replicas()
            .size();
        return new PartitionMetrics(leaderId, info.replicas()
            .size(), info.isr()
            .size(),
            preferred, underReplicated);
    }

    private static OffsetData collectOffsets(Admin admin, Set<TopicPartition> tps)
        throws InterruptedException, CollectionFailedException {
        Map<TopicPartition, OffsetSpec> latestReq = new HashMap<>();
        Map<TopicPartition, OffsetSpec> earliestReq = new HashMap<>();
        for (TopicPartition tp : tps) {
            latestReq.put(tp, OffsetSpec.latest());
            earliestReq.put(tp, OffsetSpec.earliest());
        }

        // Dispatch both requests before resolving either, so they overlap on the wire.
        ListOffsetsResult latest = admin.listOffsets(latestReq);
        ListOffsetsResult earliest = admin.listOffsets(earliestReq);

        return new OffsetData(resolveOffsets(latest, tps, "latest offset for partition"),
            resolveOffsets(earliest, tps, "earliest offset for partition"));
    }

    /**
     * Resolves one {@code listOffsets} result per partition, dropping the partitions that failed.
     *
     * @param result the dispatched request
     * @param tps    the partitions it was asked about
     * @param kind   what failed, for the debug log
     * @return offset per partition, missing the ones that failed
     * @throws InterruptedException      on thread interrupt
     * @throws CollectionFailedException if no partition's offset could be resolved
     */
    private static Map<TopicPartition, Long> resolveOffsets(ListOffsetsResult result,
                                                            Set<TopicPartition> tps, String kind)
        throws InterruptedException, CollectionFailedException {
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures =
            new HashMap<>(tps.size());
        for (TopicPartition tp : tps) {
            futures.put(tp, result.partitionResult(tp));
        }

        Map<TopicPartition, Long> offsets = new HashMap<>(tps.size());
        CollectorUtils.resolveAll(futures, kind)
            .forEach((tp, info) -> offsets.put(tp, info.offset()));
        return offsets;
    }
}
