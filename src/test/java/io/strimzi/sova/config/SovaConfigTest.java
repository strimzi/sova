/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.config;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SovaConfig}.
 */
class SovaConfigTest {

    @Test
    void testingConfigDefaults() {
        SovaConfig config = SovaConfig.forTesting("localhost:9092");

        assertEquals("localhost:9092", config.getKafkaBootstrapServers());
        assertEquals(".*", config.getGroupPattern().pattern());
        assertEquals(".*", config.getTopicPattern().pattern());
        assertTrue(config.getGroupExcludePattern().isEmpty());
        assertTrue(config.getTopicExcludePattern().isEmpty());
        assertTrue(config.getKafkaAdminClientConfiguration().isEmpty());
        // 9404 == MetricsModel.METRICS_PORT in strimzi-kafka-operator ("tcp-prometheus")
        assertEquals(9404, config.getMetricsPort());
        assertEquals(8080, config.getManagementPort());
        assertEquals(30000L, config.getCollectIntervalMs());
    }

    private static SovaConfig fromEnv(Map<String, String> env) {
        return new SovaConfig(env::get);
    }

    @Test
    void missingBootstrapServersNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> fromEnv(Map.of()));

        assertTrue(e.getMessage().contains("SOVA_KAFKA_BOOTSTRAP_SERVERS"), "was: " + e.getMessage());
    }

    @Test
    void environmentDefaults() {
        SovaConfig config = fromEnv(Map.of("SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093"));

        assertEquals("kafka:9093", config.getKafkaBootstrapServers());
        assertTrue(config.getKafkaAdminClientConfiguration().isEmpty());
        assertEquals(9404, config.getMetricsPort());
        assertEquals(8080, config.getManagementPort());
        assertEquals(30000L, config.getCollectIntervalMs());
    }

    @Test
    void kafkaAdminClientConfigurationIsParsedWhenSet() {
        SovaConfig config = fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION",
                "security.protocol=SSL\nrequest.timeout.ms=130000\n"));

        assertEquals("SSL", config.getKafkaAdminClientConfiguration().getProperty("security.protocol"));
        assertEquals("130000", config.getKafkaAdminClientConfiguration().getProperty("request.timeout.ms"));
    }

    @Test
    void kafkaAdminClientConfigurationIsReturnedAsACopy() {
        SovaConfig config = fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", "security.protocol=SSL"));

        config.getKafkaAdminClientConfiguration().setProperty("security.protocol", "PLAINTEXT");

        assertEquals("SSL", config.getKafkaAdminClientConfiguration().getProperty("security.protocol"));
    }

    @Test
    void malformedKafkaAdminClientConfigurationNamesTheVariableButNotTheValue() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", "password=\\uZZZZsecret")));

        assertTrue(e.getMessage().contains("STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION"), "was: " + e.getMessage());
        assertFalse(e.getMessage().contains("secret"), "the value may hold credentials, was: " + e.getMessage());
    }

    @Test
    void toStringListsTheClientConfigurationKeysButNotTheValues() {
        SovaConfig config = fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION", "sasl.jaas.config=top-secret-credentials"));

        assertTrue(config.toString().contains("sasl.jaas.config"));
        assertFalse(config.toString().contains("top-secret-credentials"));
    }

    @Test
    void environmentValuesAreValidatedAtConstructionAndNameTheVariable() {
        assertNamesVariable("SOVA_METRICS_PORT", "70000");
        assertNamesVariable("SOVA_MANAGEMENT_PORT", "-1");
        assertNamesVariable("SOVA_TOPIC_REGEX", "[unclosed");
        assertNamesVariable("SOVA_GROUP_EXCLUDE_REGEX", "(");
        assertNamesVariable("SOVA_COLLECT_INTERVAL_MS", "soon");
    }

    private static void assertNamesVariable(String variable, String value) {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                variable, value)));

        assertTrue(e.getMessage().contains(variable), "was: " + e.getMessage());
    }

    @Test
    void invalidRegexNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.compilePattern("SOVA_TOPIC_REGEX", "[unclosed"));

        assertTrue(e.getMessage().contains("SOVA_TOPIC_REGEX"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void validRegexCompiles() {
        assertEquals("my-.*", SovaConfig.compilePattern("SOVA_TOPIC_REGEX", "my-.*").pattern());
    }

    @Test
    void invalidPortNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parsePortOrThrow("SOVA_METRICS_PORT", "not-a-port"));

        assertTrue(e.getMessage().contains("SOVA_METRICS_PORT"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void outOfRangePortIsRejectedAtStartupRatherThanAtBindTime() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parsePortOrThrow("SOVA_METRICS_PORT", "70000"));

        assertTrue(e.getMessage().contains("SOVA_METRICS_PORT"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void invalidLongNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parseLongOrThrow("SOVA_COLLECT_INTERVAL_MS", "soon"));

        assertTrue(e.getMessage().contains("SOVA_COLLECT_INTERVAL_MS"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void validPositiveLongsAreAccepted() {
        assertEquals(30000L,
                SovaConfig.parsePositiveLongOrThrow("SOVA_COLLECT_INTERVAL_MS", "30000"));
    }

    @Test
    void zeroCollectIntervalNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parsePositiveLongOrThrow("SOVA_COLLECT_INTERVAL_MS", "0"));

        assertTrue(e.getMessage().contains("SOVA_COLLECT_INTERVAL_MS"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void negativeCollectIntervalNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parsePositiveLongOrThrow("SOVA_COLLECT_INTERVAL_MS", "-1"));

        assertTrue(e.getMessage().contains("SOVA_COLLECT_INTERVAL_MS"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }

    @Test
    void collectIntervalThatWouldOverflowWhenDoubledIsRejected() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parsePositiveLongOrThrow("SOVA_COLLECT_INTERVAL_MS",
                        String.valueOf(Long.MAX_VALUE)));

        assertTrue(e.getMessage().contains("SOVA_COLLECT_INTERVAL_MS"),
                "the message must name the variable the operator has to fix, was: " + e.getMessage());
    }
}
