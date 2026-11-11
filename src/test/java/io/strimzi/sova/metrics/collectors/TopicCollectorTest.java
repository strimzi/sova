/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.strimzi.sova.metrics.MetricSnapshot.PartitionMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for how {@link TopicCollector} turns a described partition into its metrics.
 *
 * <p>A single-broker integration cluster can only ever produce the healthy case: the leader is
 * always set and always the first replica, and the ISR always equals the replica set. The cases
 * that drive alerts need a described partition built by hand.
 */
class TopicCollectorTest {
    private static final Node N0 = new Node(0, "b0", 9092);
    private static final Node N1 = new Node(1, "b1", 9092);
    private static final Node N2 = new Node(2, "b2", 9092);

    static Stream<Arguments> partitions() {
        return Stream.of(
                Arguments.of("healthy: preferred leader and full ISR",
                        new TopicPartitionInfo(0, N0, List.of(N0, N1, N2), List.of(N0, N1, N2)),
                        new PartitionMetrics(0, 3, 3, true, false)),
                Arguments.of("no leader reports -1 and cannot be the preferred leader",
                        new TopicPartitionInfo(0, null, List.of(N0, N1, N2), List.of(N1, N2)),
                        new PartitionMetrics(-1, 3, 2, false, true)),
                Arguments.of("leader that is not the first replica is not preferred",
                        new TopicPartitionInfo(0, N1, List.of(N0, N1, N2), List.of(N0, N1, N2)),
                        new PartitionMetrics(1, 3, 3, false, false)),
                Arguments.of("ISR smaller than the replica set is under-replicated",
                        new TopicPartitionInfo(0, N0, List.of(N0, N1, N2), List.of(N0, N1)),
                        new PartitionMetrics(0, 3, 2, true, true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("partitions")
    void partitionMetricsAreDerivedFromTheDescribedPartition(
            String scenario, TopicPartitionInfo info, PartitionMetrics expected) {
        assertEquals(expected, TopicCollector.partitionMetrics(info), scenario);
    }
}
