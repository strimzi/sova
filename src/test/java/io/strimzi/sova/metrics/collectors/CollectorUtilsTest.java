/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CollectorUtils}. No Kafka broker needed.
 */
class CollectorUtilsTest {

    private static final Pattern ALL = Pattern.compile(".*");

    @Test
    void includeMustMatchWholeValue() {
        assertTrue(CollectorUtils.matches("my-topic", Pattern.compile("my-.*"), Optional.empty()));
        assertFalse(CollectorUtils.matches("other", Pattern.compile("my-.*"), Optional.empty()));
    }

    @Test
    void excludeWinsOverInclude() {
        assertFalse(CollectorUtils.matches("__consumer_offsets", ALL,
                Optional.of(Pattern.compile("__.*"))));
    }

    @Test
    void absentExcludeExcludesNothing() {
        assertTrue(CollectorUtils.matches("__consumer_offsets", ALL, Optional.empty()));
    }

    @Test
    void resolveAllKeepsSuccessesAndDropsFailures() throws Exception {
        KafkaFutureImpl<String> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new UnknownTopicOrPartitionException("gone"));

        Map<String, KafkaFuture<String>> futures = new LinkedHashMap<>();
        futures.put("alive", KafkaFuture.completedFuture("value"));
        futures.put("deleted", failed);

        Map<String, String> resolved = CollectorUtils.resolveAll(futures, "topic");

        assertEquals(Map.of("alive", "value"), resolved,
                "a single failed element must not cost us the rest of the batch");
    }

    @Test
    void resolveAllThrowsWhenEveryElementOfTheBatchFailed() {
        KafkaFutureImpl<String> one = new KafkaFutureImpl<>();
        one.completeExceptionally(new TopicAuthorizationException("denied"));
        KafkaFutureImpl<String> two = new KafkaFutureImpl<>();
        two.completeExceptionally(new TopicAuthorizationException("denied"));

        Map<String, KafkaFuture<String>> futures = new LinkedHashMap<>();
        futures.put("a", one);
        futures.put("b", two);

        CollectionFailedException thrown = assertThrows(CollectionFailedException.class,
                () -> CollectorUtils.resolveAll(futures, "topic"),
                "a batch in which nothing resolved must not look like an empty cluster: returning "
                        + "an empty map would be published as a successful cycle, advancing the "
                        + "staleness timestamp and leaving readiness UP with no series at all");

        assertInstanceOf(TopicAuthorizationException.class, thrown.getCause(),
                "the underlying failure must survive so the log says why");
    }

    @Test
    void resolveAllReturnsEmptyForAnEmptyBatchWithoutThrowing() throws Exception {
        // A cluster that genuinely has no topics is not a failure, and must stay distinguishable
        // from one we were not allowed to ask about.
        assertTrue(CollectorUtils.resolveAll(Map.<String, KafkaFuture<String>>of(), "topic")
                .isEmpty());
    }
}
