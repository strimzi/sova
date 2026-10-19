/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.collectors.ConsumerGroupCollector.ConsumerGroupData;
import io.strimzi.sova.metrics.collectors.TopicCollector.TopicData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@code ConsumerGroupCollector.computeLag}.
 *
 * <p>Pure arithmetic over plain records, with no {@code Admin} dependency, so every scenario is
 * built directly rather than through a stubbed AdminClient. Expected values below are derived
 * from the invariants documented in {@code AGENTS.md} (negative lag is never clamped, {@code -1}
 * is synthesised for uncommitted partitions and excluded from the sums, a topic with no commits
 * produces no series, and so on) — not from running the collector and recording its output.
 */
class ConsumerGroupCollectorTest {

    private static final Predicate<String> ALL_TOPICS = topic -> true;

    @Test
    void negativeLagIsReportedAsIsNeverClamped() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(100L))));
        TopicData topicData = topicData(Map.of("t", 1), Map.of(tp, 80L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        assertEquals(-20L, result.lags().get(key),
                "a committed offset ahead of the high-water mark must be reported as a negative "
                        + "lag, not clamped to zero");
        assertEquals(-20L, result.lagSums().get(topicKey),
                "the sum of a single negative lag must stay negative");
        assertEquals(100L, result.currentOffsets().get(key));
    }

    @Test
    void uncommittedPartitionsAreSynthesisedAsMinusOne() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        TopicPartition tp2 = new TopicPartition("t", 2);
        // Only partition 0 is committed; the broker never reports 1 and 2 at all.
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp0, new OffsetAndMetadata(50L))));
        TopicData topicData = topicData(Map.of("t", 3),
                Map.of(tp0, 200L, tp1, 200L, tp2, 200L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key1 = new ConsumerGroupTopicPartitionKey("g", "t", "1");
        ConsumerGroupTopicPartitionKey key2 = new ConsumerGroupTopicPartitionKey("g", "t", "2");
        assertEquals(-1L, result.currentOffsets().get(key1),
                "an uncommitted partition must be synthesised as -1 in currentOffsets");
        assertEquals(-1L, result.currentOffsets().get(key2));
        assertEquals(-1L, result.lags().get(key1),
                "an uncommitted partition must be synthesised as -1 in lags");
        assertEquals(-1L, result.lags().get(key2));
    }

    @Test
    void minusOneIsExcludedFromBothSums() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        TopicPartition tp2 = new TopicPartition("t", 2);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp0, new OffsetAndMetadata(50L))));
        TopicData topicData = topicData(Map.of("t", 3),
                Map.of(tp0, 200L, tp1, 200L, tp2, 200L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        // Only partition 0 was really committed: offset 50, lag 200 - 50 = 150. Summing the two
        // synthesised -1s in would wrongly drag the sums down to 48 and 148.
        assertEquals(50L, result.currentOffsetSums().get(topicKey),
                "the -1s of uncommitted partitions must not be folded into the offset sum");
        assertEquals(150L, result.lagSums().get(topicKey),
                "the -1s of uncommitted partitions must not be folded into the lag sum");
    }

    @Test
    void topicTheGroupNeverCommittedToProducesNoSeriesAtAll() {
        TopicPartition committedTp = new TopicPartition("t1", 0);
        ConsumerGroupData groups = withCommitted(
                Map.of("g", Map.of(committedTp, new OffsetAndMetadata(10L))));
        // t2 exists in this cycle's topic data (5 partitions) but the group has never committed
        // to it at all.
        TopicData topicData = topicData(Map.of("t1", 1, "t2", 5), Map.of(committedTp, 20L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        assertTrue(result.currentOffsets().keySet().stream().noneMatch(k -> "t2".equals(k.topic())),
                "a topic with no commits must produce no currentOffsets series, got: "
                        + result.currentOffsets());
        assertTrue(result.lags().keySet().stream().noneMatch(k -> "t2".equals(k.topic())),
                "a topic with no commits must produce no lag series, got: " + result.lags());
        assertFalse(result.currentOffsetSums().containsKey(new ConsumerGroupTopicKey("g", "t2")));
        assertFalse(result.lagSums().containsKey(new ConsumerGroupTopicKey("g", "t2")));
        // The topic the group did commit to must still be reported normally.
        assertTrue(result.currentOffsets().containsKey(new ConsumerGroupTopicPartitionKey("g", "t1", "0")));
    }

    @Test
    void missingHighWaterMarkLeavesCurrentOffsetButNoLag() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(30L))));
        // No latest-offset entry for this partition at all this cycle.
        TopicData topicData = topicData(Map.of("t", 1), Map.of());

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        assertEquals(30L, result.currentOffsets().get(key),
                "the committed value must still be recorded even without a high-water mark");
        assertFalse(result.lags().containsKey(key),
                "no lag entry must be produced when the high-water mark is missing");
        assertFalse(result.lagSums().containsKey(topicKey),
                "when every committed partition of a topic is missing its high-water mark, the "
                        + "lag sum entry must be absent entirely, not zero");
        assertEquals(30L, result.currentOffsetSums().get(topicKey),
                "the offset sum is unaffected by a missing high-water mark");
    }

    @Test
    void groupCommittedToATopicMissingFromThisCyclesTopicDataIsSkippedEntirely() {
        TopicPartition tp = new TopicPartition("ghost", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(40L))));
        // "ghost" is absent from topicPartitionCounts, as if deleted between listTopics and
        // describeTopics in this same cycle.
        TopicData topicData = topicData(Map.of(), Map.of());

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        assertTrue(result.currentOffsets().isEmpty(), "no partition count means nothing to emit");
        assertTrue(result.lags().isEmpty());
        assertTrue(result.currentOffsetSums().isEmpty());
        assertTrue(result.lagSums().isEmpty());
    }

    @Test
    void nullOffsetAndMetadataIsSkippedRatherThanRecordedAsAPhantomCommit() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        // Map.of() rejects null values, so a mutable map is needed to reproduce what the admin
        // client can hand back.
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(tp0, new OffsetAndMetadata(15L));
        committed.put(tp1, null);
        ConsumerGroupData groups = withCommitted(Map.of("g", committed));
        TopicData topicData = topicData(Map.of("t", 2), Map.of(tp0, 100L, tp1, 100L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key0 = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicPartitionKey key1 = new ConsumerGroupTopicPartitionKey("g", "t", "1");
        assertEquals(15L, result.currentOffsets().get(key0));
        assertEquals(-1L, result.currentOffsets().get(key1),
                "a null OffsetAndMetadata must be skipped and the partition synthesised as -1, "
                        + "never recorded as a real commit");
        assertEquals(-1L, result.lags().get(key1));
        assertEquals(15L, result.currentOffsetSums().get(new ConsumerGroupTopicKey("g", "t")),
                "the sum must not include a value derived from the null entry");
    }

    @Test
    void topicExcludePatternAppliesToCommittedOffsets() {
        TopicPartition tp = new TopicPartition("excluded-topic", 0);
        ConsumerGroupData groups = withCommitted(
                Map.of("g", Map.of(tp, new OffsetAndMetadata(99L))));
        TopicData topicData = topicData(Map.of("excluded-topic", 1), Map.of(tp, 200L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(
                groups, topic -> !"excluded-topic".equals(topic), topicData);

        assertTrue(result.currentOffsets().isEmpty(),
                "commits on an excluded topic must produce no currentOffsets series, got: "
                        + result.currentOffsets());
        assertTrue(result.lags().isEmpty(),
                "commits on an excluded topic must produce no lag series, got: " + result.lags());
        assertTrue(result.currentOffsetSums().isEmpty());
        assertTrue(result.lagSums().isEmpty());
    }

    private static ConsumerGroupData withCommitted(
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committedOffsets) {
        return new ConsumerGroupData(Map.of(), Map.of(), committedOffsets,
                Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static TopicData topicData(Map<String, Integer> partitionCounts,
            Map<TopicPartition, Long> latestOffsets) {
        return new TopicData(partitionCounts, Map.of(), latestOffsets, Map.of());
    }

}
