/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.config;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SovaConfig}.
 */
class SovaConfigTest {
    private static final String CLIENT_CONFIG_ENV = "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION";
    private static final String BOOTSTRAP = "bootstrap.servers=kafka:9093\n";

    private static SovaConfig fromEnv(Map<String, String> env) {
        return new SovaConfig(env::get);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "security.protocol=SSL"})
    void missingBootstrapServersNamesTheEnvironmentVariable(String clientConfig) {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new SovaConfig(name -> CLIENT_CONFIG_ENV.equals(name) ? clientConfig : null));

        assertTrue(e.getMessage().contains(CLIENT_CONFIG_ENV), "was: " + e.getMessage());
        assertTrue(e.getMessage().contains("bootstrap.servers"), "was: " + e.getMessage());
    }

    @Test
    void environmentDefaults() {
        SovaConfig config = fromEnv(Map.of(CLIENT_CONFIG_ENV, BOOTSTRAP));

        assertEquals("kafka:9093", config.getKafkaAdminClientConfiguration().getProperty("bootstrap.servers"));
        assertEquals(1, config.getKafkaAdminClientConfiguration().size());
        assertEquals(".*", config.getGroupPattern().pattern());
        assertEquals(".*", config.getTopicPattern().pattern());
        assertTrue(config.getGroupExcludePattern().isEmpty());
        assertTrue(config.getTopicExcludePattern().isEmpty());
        // 9404 == MetricsModel.METRICS_PORT in strimzi-kafka-operator ("tcp-prometheus")
        assertEquals(9404, config.getMetricsPort());
        assertEquals(8080, config.getManagementPort());
        assertEquals(30000L, config.getCollectIntervalMs());
    }

    @Test
    void kafkaAdminClientConfigurationIsParsedAndReturnedAsACopy() {
        SovaConfig config = fromEnv(Map.of(
                CLIENT_CONFIG_ENV,
                BOOTSTRAP + "security.protocol=SSL\nrequest.timeout.ms=130000\n"));

        assertEquals("SSL", config.getKafkaAdminClientConfiguration().getProperty("security.protocol"));
        assertEquals("130000", config.getKafkaAdminClientConfiguration().getProperty("request.timeout.ms"));

        config.getKafkaAdminClientConfiguration().setProperty("security.protocol", "PLAINTEXT");

        assertEquals("SSL", config.getKafkaAdminClientConfiguration().getProperty("security.protocol"));
    }

    @Test
    void malformedKafkaAdminClientConfigurationNamesTheVariableButNotTheValue() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> fromEnv(Map.of(
                CLIENT_CONFIG_ENV, "password=\\uZZZZsecret")));

        assertTrue(e.getMessage().contains(CLIENT_CONFIG_ENV), "was: " + e.getMessage());
        assertFalse(e.getMessage().contains("secret"), "the value may hold credentials, was: " + e.getMessage());
    }

    @Test
    void toStringListsTheClientConfigurationKeysButNotTheValues() {
        SovaConfig config = fromEnv(Map.of(
                CLIENT_CONFIG_ENV, BOOTSTRAP + "sasl.jaas.config=top-secret-credentials"));

        assertTrue(config.toString().contains("sasl.jaas.config"));
        assertFalse(config.toString().contains("top-secret-credentials"));
    }

    @Test
    void environmentValuesAreValidatedAtConstructionAndNameTheVariable() {
        assertNamesVariable("STRIMZI_METRICS_PORT", "70000");
        assertNamesVariable("STRIMZI_MANAGEMENT_PORT", "-1");
        assertNamesVariable("STRIMZI_TOPIC_REGEX", "[unclosed");
        assertNamesVariable("STRIMZI_GROUP_EXCLUDE_REGEX", "(");
        assertNamesVariable("STRIMZI_METRICS_PORT", "not-a-port");
        assertNamesVariable("STRIMZI_COLLECT_INTERVAL_MS", "soon");
        assertNamesVariable("STRIMZI_COLLECT_INTERVAL_MS", "0");
        assertNamesVariable("STRIMZI_COLLECT_INTERVAL_MS", "-1");
        // Doubled by MetricsCollector.isHealthy(), so this would overflow to a negative window
        assertNamesVariable("STRIMZI_COLLECT_INTERVAL_MS", String.valueOf(Long.MAX_VALUE));
    }

    private static void assertNamesVariable(String variable, String value) {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> fromEnv(Map.of(
                CLIENT_CONFIG_ENV, BOOTSTRAP,
                variable, value)));

        assertTrue(e.getMessage().contains(variable), "was: " + e.getMessage());
    }

    @Test
    void environmentOverridesAreReadIntoTheMatchingField() {
        SovaConfig config = fromEnv(Map.of(
                CLIENT_CONFIG_ENV, BOOTSTRAP,
                "STRIMZI_GROUP_REGEX", "group-.*",
                "STRIMZI_GROUP_EXCLUDE_REGEX", "group-internal",
                "STRIMZI_TOPIC_REGEX", "topic-.*",
                "STRIMZI_TOPIC_EXCLUDE_REGEX", "topic-internal",
                "STRIMZI_METRICS_PORT", "19404",
                "STRIMZI_MANAGEMENT_PORT", "18080",
                "STRIMZI_COLLECT_INTERVAL_MS", "5000"));

        assertEquals("group-.*", config.getGroupPattern().pattern());
        assertEquals("group-internal", config.getGroupExcludePattern().orElseThrow().pattern());
        assertEquals("topic-.*", config.getTopicPattern().pattern());
        assertEquals("topic-internal", config.getTopicExcludePattern().orElseThrow().pattern());
        assertEquals(19404, config.getMetricsPort());
        assertEquals(18080, config.getManagementPort());
        assertEquals(5000L, config.getCollectIntervalMs());
    }
}
