/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.integration.KafkaTestResourceManager;
import io.strimzi.sova.metrics.MetricsRegistry;
import io.strimzi.sova.metrics.TestSnapshots;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
class MetricsCollectorIT {
    private static KafkaTestResourceManager kafkaManager;
    private static Admin admin;
    private static MetricsCollector collector;
    private static MetricsRegistry registry;
    private static String bootstrapServers;

    @BeforeAll
    static void startKafka() {
        kafkaManager = new KafkaTestResourceManager();
        kafkaManager.start();
        bootstrapServers = kafkaManager.getBootstrapServers();

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        admin = Admin.create(adminProps);

        SovaConfig config = SovaConfig.forTesting(bootstrapServers);
        registry = new MetricsRegistry();
        collector = new MetricsCollector(admin, registry, config);
    }

    @AfterAll
    static void stopKafka() {
        if (admin != null) {
            admin.close();
        }
        if (kafkaManager != null) {
            kafkaManager.stop();
        }
    }

    /**
     * The broker, topic and offset families all come out of the same cycle, so one topic and one
     * collection covers them: separate tests would only buy separate round trips to the broker.
     */
    @Test
    void brokerTopicAndOffsetMetricsReflectTheCluster() throws Exception {
        String topic = "test-partitions";
        admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1)))
                .all().get();
        produce(topic, 0, 10);

        collector.collectInternal();

        String scraped = scrape();
        assertMetricValue(scraped, "kafka_brokers", "1.0");
        // The broker id and address are not known up front, so check the shape and the value.
        assertMetricWithLabels(scraped, "kafka_broker_info", "address=\"", null);
        assertMetricWithLabels(scraped, "kafka_topic_partitions", "topic=\"" + topic + "\"} ", "3.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_leader",
                "partition=\"0\",topic=\"" + topic + "\"} ", null);
        assertMetricWithLabels(scraped, "kafka_topic_partition_replicas",
                "partition=\"1\",topic=\"" + topic + "\"} ", "1.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_in_sync_replica",
                "partition=\"2\",topic=\"" + topic + "\"} ", "1.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_current_offset",
                "partition=\"0\",topic=\"" + topic + "\"} ", "10.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_oldest_offset",
                "partition=\"0\",topic=\"" + topic + "\"} ", "0.0");
    }

    @Test
    void consumerGroupLagCalculated() throws Exception {
        String topic = "test-lag-topic";
        String group = "test-lag-group";
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get();

        produce(topic, 0, 20);
        commit(topic, 0, group, 15);
        collector.collectInternal();

        String scraped = scrape();
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset",
                "consumergroup=\"test-lag-group\",partition=\"0\",topic=\"test-lag-topic\"} ", "15.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-lag-group\",partition=\"0\",topic=\"test-lag-topic\"} ", "5.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag_sum",
                "consumergroup=\"test-lag-group\",topic=\"test-lag-topic\"} ", "5.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_members",
                "consumergroup=\"test-lag-group\"} ", "0.0");
        // A group that only commits offsets, with no live member, is Empty
        assertMetricWithLabels(scraped, "kafka_consumergroup_state",
                "consumergroup=\"test-lag-group\",state=\"Empty\"} ", "1.0");
    }

    @Test
    void uncommittedPartitionExcludedFromLagSum() throws Exception {
        String topic = "test-uncommitted";
        String group = "test-uncommitted-group";
        admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1)))
                .all().get();

        produce(topic, 0, 10);
        produce(topic, 1, 10);

        // Only commit on partition 0
        commit(topic, 0, group, 5);

        collector.collectInternal();

        String scraped = scrape();
        // Partition 0: committed=5, latest=10, lag=5
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-uncommitted-group\",partition=\"0\",topic=\"test-uncommitted\"} ", "5.0");
        // lag_sum counts only partition 0: the -1 sentinel is deliberately not summed
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag_sum",
                "consumergroup=\"test-uncommitted-group\",topic=\"test-uncommitted\"} ", "5.0");

        // Partition 1 has no commit, but the group does consume this topic, so it reports -1
        // rather than dropping the series. The broker never tells us about this partition:
        // it is reconstructed from the topic's partition count.
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset",
                "consumergroup=\"test-uncommitted-group\",partition=\"1\",topic=\"test-uncommitted\"} ", "-1.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-uncommitted-group\",partition=\"1\",topic=\"test-uncommitted\"} ", "-1.0");
        // current_offset_sum excludes the -1, so it is partition 0's offset alone
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset_sum",
                "consumergroup=\"test-uncommitted-group\",topic=\"test-uncommitted\"} ", "5.0");
    }

    @Test
    void topicTheGroupNeverCommittedToProducesNoSeries() throws Exception {
        String consumed = "gate-consumed";
        String untouched = "gate-untouched";
        String group = "gate-group";
        admin.createTopics(List.of(
                new NewTopic(consumed, 1, (short) 1),
                new NewTopic(untouched, 1, (short) 1))).all().get();
        produce(consumed, 0, 5);
        produce(untouched, 0, 5);
        commit(consumed, 0, group, 2);

        collector.collectInternal();

        String scraped = scrape();
        assertTrue(scraped.contains("consumergroup=\"" + group + "\",partition=\"0\",topic=\"" + consumed + "\""),
                "the consumed topic must be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("consumergroup=\"" + group + "\",partition=\"0\",topic=\"" + untouched + "\""),
                "a topic the group has never committed to must produce no series at all. Scrape was:\n" + scraped);
    }

    @Test
    void topicIncludeAndExcludeRegexAreApplied() throws Exception {
        admin.createTopics(List.of(
                new NewTopic("filter-keep", 1, (short) 1),
                new NewTopic("filter-drop", 1, (short) 1),
                new NewTopic("filter-other-app", 1, (short) 1),
                new NewTopic("decoy-topic", 1, (short) 1))).all().get();

        MetricsRegistry filtered = collectWith(Map.of(
                "STRIMZI_TOPIC_REGEX", "filter-.*",
                "STRIMZI_TOPIC_EXCLUDE_REGEX", "filter-drop|filter-other-.*"));

        String scraped = TestSnapshots.scrape(filtered);
        assertTrue(scraped.contains("kafka_topic_partitions{topic=\"filter-keep\"}"),
                "an included topic must be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"filter-drop\""),
                "an excluded topic must not be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"filter-other-app\""),
                "an excluded topic must not be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"decoy-topic\""),
                "a topic outside the include pattern must not be reported. Scrape was:\n" + scraped);
    }

    @Test
    void groupIncludeAndExcludeRegexAreApplied() throws Exception {
        String topic = "group-filter-topic";
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        produce(topic, 0, 5);
        commit(topic, 0, "grp-keep", 1);
        commit(topic, 0, "grp-drop", 1);
        commit(topic, 0, "decoy-group", 1);

        MetricsRegistry filtered = collectWith(Map.of(
                "STRIMZI_GROUP_REGEX", "grp-.*",
                "STRIMZI_GROUP_EXCLUDE_REGEX", "grp-drop"));

        String scraped = TestSnapshots.scrape(filtered);
        assertTrue(scraped.contains("kafka_consumergroup_members{consumergroup=\"grp-keep\"}"),
                "an included group must be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("consumergroup=\"grp-drop\""),
                "an excluded group must not be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("consumergroup=\"decoy-group\""),
                "a group outside the include pattern must not be reported. Scrape was:\n" + scraped);
    }

    // -- helpers --

    /**
     * Runs one collection cycle with extra filter variables and returns the registry it filled.
     */
    private static MetricsRegistry collectWith(Map<String, String> filters) throws Exception {
        Map<String, String> env = new HashMap<>(filters);
        env.put("STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", "bootstrap.servers=" + bootstrapServers);
        MetricsRegistry filteredRegistry = new MetricsRegistry();
        new MetricsCollector(admin, filteredRegistry, new SovaConfig(env::get)).collectInternal();
        return filteredRegistry;
    }

    private static void produce(String topic, int partition, int count)
            throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, partition, "key-" + i, "val-" + i)).get();
            }
        }
    }

    /**
     * Commits an offset for a group without consuming anything or joining the group.
     */
    private static void commit(String topic, int partition, String group, int offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
        }
    }

    private static String scrape() throws IOException {
        return TestSnapshots.scrape(registry);
    }

    /**
     * Asserts that an unlabelled metric has exactly the given value.
     */
    private static void assertMetricValue(String scraped, String metric, String value) {
        assertMetricWithLabels(scraped, metric, null, value);
    }

    /**
     * Asserts that a line exists for the metric whose labels start with {@code labels} and, when
     * {@code value} is not null, whose value equals it exactly. Comparing the whole last token
     * matters: {@code endsWith("5.0")} would also accept {@code 15.0} and {@code -5.0}.
     *
     * <p>End {@code labels} with the closing brace and a space to pin the full label set, so that
     * {@code topic="a"} cannot match {@code topic="a-b"}; pass {@code null} for an unlabelled
     * metric.
     */
    private static void assertMetricWithLabels(String scraped, String metric, String labels, String value) {
        String prefix = labels == null ? metric + " " : metric + "{" + labels;
        for (String line : scraped.split("\n")) {
            if (line.startsWith(prefix)
                    && (value == null || line.substring(line.lastIndexOf(' ') + 1).equals(value))) {
                return;
            }
        }
        throw new AssertionError("Expected metric '" + prefix + "' with value "
                + (value == null ? "<any>" : value) + "\n\nScraped output:\n" + scraped);
    }
}
