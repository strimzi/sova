/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupStateKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.collectors.TopicCollector.TopicData;

/**
 * Collects consumer group membership, state, committed offsets, and lag via {@code AdminClient}.
 */
final class ConsumerGroupCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupCollector.class);

    /** Sentinel value emitted when a group has not committed to a partition of a consumed topic. */
    private static final long NO_COMMITTED_OFFSET = -1L;

    private ConsumerGroupCollector() { }

    /**
     * Holds all consumer-group-level collected data.
     *
     * <p>{@link #collect(Admin, SovaConfig)} fills only {@code members}, {@code states} and
     * {@code committedOffsets}, so that branch of a cycle needs no topic data and can run
     * concurrently with the topic branch.
     * {@link #computeLag(ConsumerGroupData, Predicate<String>, TopicData)} then fills the offset and lag
     * maps and drops {@code committedOffsets}.
     *
     * @param members           member counts per group
     * @param states            state presence map
     * @param committedOffsets  committed offsets per group, exactly as the broker reported them;
     *                          empty once lag has been computed
     * @param currentOffsets    committed offsets per group+topic+partition
     * @param currentOffsetSums sum of committed offsets per group+topic
     * @param lags              lag per group+topic+partition
     * @param lagSums           lag sum per group+topic
     */
    record ConsumerGroupData(Map<String, Integer> members,
            Map<ConsumerGroupStateKey, Boolean> states,
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committedOffsets,
            Map<ConsumerGroupTopicPartitionKey, Long> currentOffsets,
            Map<ConsumerGroupTopicKey, Long> currentOffsetSums,
            Map<ConsumerGroupTopicPartitionKey, Long> lags,
            Map<ConsumerGroupTopicKey, Long> lagSums) { }

    /**
     * Lists consumer groups and filters them by the configured include/exclude patterns.
     *
     * @param admin  Kafka AdminClient
     * @param config application configuration, carrying the compiled group filters
     * @return filtered list of group IDs
     * @throws java.util.concurrent.ExecutionException   on AdminClient error
     * @throws InterruptedException on thread interrupt
     */
    private static List<String> listGroupIds(Admin admin, SovaConfig config)
            throws java.util.concurrent.ExecutionException, InterruptedException {
        // valid() rather than all(): all() completes exceptionally if any single broker could not
        // be queried, so one slow or unreachable broker in a cluster of three would abort this
        // branch and cost the whole cycle, while two thirds of the groups were right there. The
        // per-element handling below exists for exactly that reason; this call was the last one
        // still failing wholesale.
        ListGroupsResult result = admin.listGroups(ListGroupsOptions.forConsumerGroups());
        Collection<GroupListing> allGroups = result.valid().get();
        Collection<Throwable> errors = result.errors().get();
        if (!errors.isEmpty()) {
            LOGGER.warn("Listing consumer groups failed on {} broker(s); groups they host are missing "
                    + "from this cycle. First failure: {}", errors.size(),
                    errors.iterator().next().toString());
        }
        List<String> groupIds = new ArrayList<>();
        for (GroupListing listing : allGroups) {
            if (CollectorUtils.matches(listing.groupId(), config.getGroupPattern(),
                    config.getGroupExcludePattern())) {
                groupIds.add(listing.groupId());
            }
        }
        LOGGER.debug("Consumer groups: {} total, {} after filtering", allGroups.size(), groupIds.size());
        return groupIds;
    }

    /**
     * Lists, describes, and fetches committed offsets for the filtered consumer groups.
     *
     * @param admin  Kafka AdminClient
     * @param config application configuration
     * @return group membership, state, and committed offsets
     * @throws java.util.concurrent.ExecutionException   on AdminClient error
     * @throws InterruptedException on thread interrupt
     * @throws CollectionFailedException if no group description or offset lookup succeeded
     */
    static ConsumerGroupData collect(Admin admin, SovaConfig config)
            throws java.util.concurrent.ExecutionException, InterruptedException,
            CollectionFailedException {
        List<String> groupIds = listGroupIds(admin, config);
        Map<String, Integer> members = new HashMap<>();
        Map<ConsumerGroupStateKey, Boolean> states = new HashMap<>();

        if (groupIds.isEmpty()) {
            return withCommitted(members, states, Map.of());
        }

        // Per-group futures rather than all(): a group deleted between listGroups and
        // describeConsumerGroups must cost us that group, not the whole cycle.
        Map<String, ConsumerGroupDescription> descriptions = CollectorUtils.resolveAll(
                admin.describeConsumerGroups(groupIds).describedGroups(), "consumer group");
        for (Map.Entry<String, ConsumerGroupDescription> e : descriptions.entrySet()) {
            members.put(e.getKey(), e.getValue().members().size());
            states.put(new ConsumerGroupStateKey(
                    e.getKey(), e.getValue().groupState().toString()), true);
        }

        Map<String, ListConsumerGroupOffsetsSpec> req = new HashMap<>();
        for (String gid : groupIds) {
            req.put(gid, new ListConsumerGroupOffsetsSpec());
        }

        // Per-group futures rather than all(), for the same reason as describeConsumerGroups.
        ListConsumerGroupOffsetsResult result = admin.listConsumerGroupOffsets(req);
        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures =
                new HashMap<>(groupIds.size());
        for (String gid : groupIds) {
            futures.put(gid, result.partitionsToOffsetAndMetadata(gid));
        }

        return withCommitted(members, states, CollectorUtils.resolveAll(futures, "committed offsets for group"));
    }

    private static ConsumerGroupData withCommitted(Map<String, Integer> members,
            Map<ConsumerGroupStateKey, Boolean> states,
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committedOffsets) {
        return new ConsumerGroupData(members, states, committedOffsets,
                Map.of(), Map.of(), Map.of(), Map.of());
    }

    /**
     * Computes lag from committed offsets and the topic high-water marks. Pure arithmetic — no
     * AdminClient calls — which is why it can wait until both branches of the cycle are in.
     *
     * <p>Takes a topic filter rather than the whole {@link SovaConfig}: deciding whether a topic
     * is reported is all the arithmetic needs from it, and a narrower parameter is one a test can
     * supply without a back door into the config object.
     *
     * @param groups      what {@link #collect(Admin, SovaConfig)} returned
     * @param topicFilter {@code true} for topics that should be reported
     * @param topicData   topic data from the same cycle, used for high-water marks
     * @return assembled consumer group data
     */
    static ConsumerGroupData computeLag(ConsumerGroupData groups, Predicate<String> topicFilter, TopicData topicData) {
        Map<ConsumerGroupTopicPartitionKey, Long> currentOffsets = new HashMap<>();
        Map<ConsumerGroupTopicKey, Long> currentOffsetSums = new HashMap<>();
        Map<ConsumerGroupTopicPartitionKey, Long> lags = new HashMap<>();
        Map<ConsumerGroupTopicKey, Long> lagSums = new HashMap<>();

        for (Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> g : groups.committedOffsets().entrySet()) {
            collectGroup(g.getKey(), g.getValue(), topicFilter, topicData,
                    currentOffsets, currentOffsetSums, lags, lagSums);
        }
        return new ConsumerGroupData(groups.members(), groups.states(), Map.of(),
                currentOffsets, currentOffsetSums, lags, lagSums);
    }

    /**
     * Accumulates one group's committed offsets and lag into the shared maps.
     *
     * <p>Offsets are grouped by topic first, then every partition of each such topic is walked,
     * not just the committed ones. A topic only appears in the committed map if the group has at
     * least one real commit on it, so a topic the group has never committed to produces no series.
     * Within such a topic, a series is emitted for every partition, carrying {@code -1} where the
     * group has not committed. The broker does not tell us about those gaps, because a spec with
     * no partition list returns only committed partitions, so we reconstruct them from the
     * partition count {@link TopicCollector} already collected, with no extra AdminClient round
     * trip.
     */
    private static void collectGroup(String gid,
            Map<TopicPartition, OffsetAndMetadata> offsets, Predicate<String> topicFilter,
            TopicData topicData,
            Map<ConsumerGroupTopicPartitionKey, Long> currentOffsets,
            Map<ConsumerGroupTopicKey, Long> currentOffsetSums,
            Map<ConsumerGroupTopicPartitionKey, Long> lags,
            Map<ConsumerGroupTopicKey, Long> lagSums) {

        Map<String, Map<Integer, Long>> committedByTopic = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            String topic = e.getKey().topic();
            // A null value means the group has no committed offset for this partition. With our
            // spec that should not occur, but the admin client can produce it, so skip it rather
            // than record a phantom commit.
            if (e.getValue() == null) {
                LOGGER.debug("Skipping null OffsetAndMetadata for group '{}' topic '{}' partition {}",
                        gid, topic, e.getKey().partition());
                continue;
            }
            if (topicFilter.test(topic)) {
                committedByTopic.computeIfAbsent(topic, t -> new HashMap<>())
                        .put(e.getKey().partition(), e.getValue().offset());
            }
        }

        for (Map.Entry<String, Map<Integer, Long>> tEntry : committedByTopic.entrySet()) {
            String topic = tEntry.getKey();
            Map<Integer, Long> committed = tEntry.getValue();
            Integer partitionCount = topicData.topicPartitionCounts().get(topic);
            if (partitionCount == null) {
                // The group commits to a topic we did not collect this cycle (e.g. deleted between
                // listTopics and describeTopics), so we have no partition count and no high-water
                // marks. Nothing useful to emit.
                LOGGER.debug("Skipping group '{}' topic '{}': no partition count in this cycle's "
                        + "topic data (topic may have been deleted mid-cycle)", gid, topic);
                continue;
            }

            ConsumerGroupTopicKey cgTKey = new ConsumerGroupTopicKey(gid, topic);
            for (int partition = 0; partition < partitionCount; partition++) {
                ConsumerGroupTopicPartitionKey cgTpKey = new ConsumerGroupTopicPartitionKey(
                        gid, topic, String.valueOf(partition));
                Long committedOff = committed.get(partition);

                if (committedOff == null) {
                    currentOffsets.put(cgTpKey, NO_COMMITTED_OFFSET);
                    lags.put(cgTpKey, NO_COMMITTED_OFFSET);

                    continue;
                }

                currentOffsets.put(cgTpKey, committedOff);
                currentOffsetSums.merge(cgTKey, committedOff, Long::sum);

                Long latest = topicData.latestOffsets().get(new TopicPartition(topic, partition));
                if (latest == null) {
                    // No high-water mark this cycle, so no lag can be computed.
                    continue;
                }
                // Negative lag is reported as it comes out, never clamped to zero: a committed
                // offset ahead of the high-water mark is a real anomaly, and rounding it away
                // only hides it. One caveat — the high-water mark and the committed offset are
                // read by two branches running concurrently, so they are never quite of the same
                // instant and a busy partition can briefly show a small negative that means
                // nothing. Alert on sustained negatives, not on a single sample.
                long lag = latest - committedOff;
                lags.put(cgTpKey, lag);
                lagSums.merge(cgTKey, lag, Long::sum);
            }
        }
    }
}
