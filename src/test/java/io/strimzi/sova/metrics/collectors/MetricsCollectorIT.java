/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.integration.KafkaTestResourceManager;
import io.strimzi.sova.metrics.MetricsRegistry;

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
        bootstrapServers = KafkaTestResourceManager.getBootstrapServers();

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

    @Test
    void brokerMetricsReflectClusterSize() throws Exception {
        collector.collectInternal();

        String scraped = scrape();
        assertMetricValue(scraped, "kafka_brokers", "1.0");
    }

    @Test
    void topicPartitionMetrics() throws Exception {
        admin.createTopics(List.of(new NewTopic("test-partitions", 3, (short) 1)))
                .all().get();

        collector.collectInternal();

        String scraped = scrape();
        assertMetricWithLabels(scraped, "kafka_topic_partitions", "topic=\"test-partitions\"", "3.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_leader",
                "partition=\"0\",topic=\"test-partitions\"");
        assertMetricWithLabels(scraped, "kafka_topic_partition_replicas",
                "partition=\"1\",topic=\"test-partitions\"", "1.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_in_sync_replica",
                "partition=\"2\",topic=\"test-partitions\"", "1.0");
    }

    @Test
    void producerOffsetsTracked() throws Exception {
        String topic = "test-offsets";
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get();

        produce(topic, 10);
        collector.collectInternal();

        String scraped = scrape();
        assertMetricWithLabels(scraped, "kafka_topic_partition_current_offset",
                "partition=\"0\",topic=\"test-offsets\"", "10.0");
        assertMetricWithLabels(scraped, "kafka_topic_partition_oldest_offset",
                "partition=\"0\",topic=\"test-offsets\"", "0.0");
    }

    @Test
    void consumerGroupLagCalculated() throws Exception {
        String topic = "test-lag-topic";
        String group = "test-lag-group";
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                .all().get();

        produce(topic, 20);
        consumeAndCommit(topic, group, 15);
        collector.collectInternal();

        String scraped = scrape();
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset",
                "consumergroup=\"test-lag-group\",partition=\"0\",topic=\"test-lag-topic\"", "15.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-lag-group\",partition=\"0\",topic=\"test-lag-topic\"", "5.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag_sum",
                "consumergroup=\"test-lag-group\",topic=\"test-lag-topic\"", "5.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_members",
                "consumergroup=\"test-lag-group\"", "0.0");
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
        consumeAndCommit(topic, group, 0, 5);

        collector.collectInternal();

        String scraped = scrape();
        // Partition 0: committed=5, latest=10, lag=5
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-uncommitted-group\",partition=\"0\",topic=\"test-uncommitted\"", "5.0");
        // lag_sum counts only partition 0: the -1 sentinel is deliberately not summed
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag_sum",
                "consumergroup=\"test-uncommitted-group\",topic=\"test-uncommitted\"", "5.0");

        // Partition 1 has no commit, but the group does consume this topic, so it reports -1
        // rather than dropping the series. The broker never tells us about this partition:
        // it is reconstructed from the topic's partition count.
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset",
                "consumergroup=\"test-uncommitted-group\",partition=\"1\",topic=\"test-uncommitted\"", "-1.0");
        assertMetricWithLabels(scraped, "kafka_consumergroup_lag",
                "consumergroup=\"test-uncommitted-group\",partition=\"1\",topic=\"test-uncommitted\"", "-1.0");
        // current_offset_sum excludes the -1, so it is partition 0's offset alone
        assertMetricWithLabels(scraped, "kafka_consumergroup_current_offset_sum",
                "consumergroup=\"test-uncommitted-group\",topic=\"test-uncommitted\"", "5.0");
    }

    @Test
    void topicTheGroupNeverCommittedToProducesNoSeries() throws Exception {
        String consumed = "gate-consumed";
        String untouched = "gate-untouched";
        String group = "gate-group";
        admin.createTopics(List.of(
                new NewTopic(consumed, 1, (short) 1),
                new NewTopic(untouched, 1, (short) 1))).all().get();
        produce(consumed, 5);
        produce(untouched, 5);
        consumeAndCommit(consumed, group, 2);

        collector.collectInternal();

        String scraped = scrape();
        assertTrue(scraped.contains("consumergroup=\"" + group + "\",partition=\"0\",topic=\"" + consumed + "\""),
                "the consumed topic must be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("consumergroup=\"" + group + "\",partition=\"0\",topic=\"" + untouched + "\""),
                "a topic the group has never committed to must produce no series at all. " + "Scrape was:\n" + scraped);
    }

    @Test
    void topicIncludeAndExcludeRegexAreApplied() throws Exception {
        admin.createTopics(List.of(
                new NewTopic("filter-keep", 1, (short) 1),
                new NewTopic("filter-drop", 1, (short) 1),
                new NewTopic("filter-other-app", 1, (short) 1))).all().get();

        Map<String, String> env = Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", bootstrapServers,
                "SOVA_TOPIC_REGEX", "filter-.*",
                "SOVA_TOPIC_EXCLUDE_REGEX", "filter-drop|filter-other-.*");
        MetricsRegistry filteredRegistry = new MetricsRegistry();
        new MetricsCollector(admin, filteredRegistry, new SovaConfig(env::get)).collectInternal();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, filteredRegistry.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        String scraped = out.toString(StandardCharsets.UTF_8);

        assertTrue(scraped.contains("kafka_topic_partitions{topic=\"filter-keep\"}"),
                "an included topic must be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"filter-drop\""),
                "an excluded topic must not be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"filter-other-app\""),
                "an excluded topic must not be reported. Scrape was:\n" + scraped);
        assertFalse(scraped.contains("topic=\"test-"),
                "a topic outside the include pattern must not be reported. Scrape was:\n" + scraped);
    }

    // -- helpers --

    private void produce(String topic, int count) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "val-" + i)).get();
            }
        }
    }

    private void produce(String topic, int partition, int count)
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

    private void consumeAndCommit(String topic, String group, int commitOffset) {
        TopicPartition tp = new TopicPartition(topic, 0);
        commitOffset(group, tp, commitOffset);
    }

    private void consumeAndCommit(String topic, String group, int partition, int commitOffset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        commitOffset(group, tp, commitOffset);
    }

    private void commitOffset(String group, TopicPartition tp, int offset) {
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

    private String scrape() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(
                out, registry.prometheusRegistry().scrape(), EscapingScheme.DEFAULT);
        return out.toString(StandardCharsets.UTF_8);
    }

    private void assertMetricValue(String scraped, String metric, String value) {
        for (String line : scraped.split("\n")) {
            if (!line.startsWith("#") && line.startsWith(metric) && line.endsWith(value)) {
                return;
            }
        }
        throw new AssertionError("Expected metric '" + metric + "' with value " + value
                + "\n\nScraped output:\n" + scraped);
    }

    private void assertMetricWithLabels(String scraped, String metric, String labels, String value) {
        String pattern = metric + "{" + labels;
        for (String line : scraped.split("\n")) {
            if (!line.startsWith("#") && line.contains(pattern) && line.endsWith(value)) {
                return;
            }
        }
        throw new AssertionError("Expected metric '" + metric + "{" + labels + "}' with value "
                + value + "\n\nScraped output:\n" + scraped);
    }

    private void assertMetricWithLabels(String scraped, String metric, String labels) {
        String pattern = metric + "{" + labels;
        for (String line : scraped.split("\n")) {
            if (!line.startsWith("#") && line.contains(pattern)) {
                return;
            }
        }
        throw new AssertionError("Expected metric '" + metric + "{" + labels + "}' to be present"
                + "\n\nScraped output:\n" + scraped);
    }
}
