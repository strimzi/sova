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
        assertFalse(config.isTlsEnabled());
        // 9404 == MetricsModel.METRICS_PORT in strimzi-kafka-operator ("tcp-prometheus")
        assertEquals(9404, config.getMetricsPort());
        assertEquals(8080, config.getManagementPort());
        assertEquals(30000L, config.getCollectIntervalMs());
        assertTrue(config.getSaslMechanism().isEmpty());
        assertTrue(config.getSaslJaasConfig().isEmpty());
        assertTrue(config.getSaslLoginCallbackHandlerClass().isEmpty());
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
        assertTrue(config.isTlsEnabled());
        assertEquals("/etc/sova/cluster-ca-certs/cluster-ca.crt", config.getTlsCaCertFile());
        assertEquals(9404, config.getMetricsPort());
        assertEquals(8080, config.getManagementPort());
        assertEquals(30000L, config.getCollectIntervalMs());
    }

    @Test
    void tlsEnabledWithOnlyTheCaHasNoClientCertOrKey() {
        SovaConfig config = fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "SOVA_TLS_ENABLED", "true",
                "SOVA_TLS_CA_CERT_FILE", "/ca.crt"));

        assertEquals("/ca.crt", config.getTlsCaCertFile());
        assertTrue(config.getTlsCertFile().isEmpty());
        assertTrue(config.getTlsKeyFile().isEmpty());
    }

    @Test
    void clientCertAndKeyAreReadWhenSet() {
        SovaConfig config = fromEnv(Map.of(
                "SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "SOVA_TLS_CERT_FILE", "/tls.crt",
                "SOVA_TLS_KEY_FILE", "/tls.key"));

        assertEquals("/tls.crt", config.getTlsCertFile().orElseThrow());
        assertEquals("/tls.key", config.getTlsKeyFile().orElseThrow());
    }

    @Test
    void environmentValuesAreValidatedAtConstructionAndNameTheVariable() {
        assertNamesVariable("SOVA_METRICS_PORT", "70000");
        assertNamesVariable("SOVA_MANAGEMENT_PORT", "-1");
        assertNamesVariable("SOVA_TOPIC_REGEX", "[unclosed");
        assertNamesVariable("SOVA_GROUP_EXCLUDE_REGEX", "(");
        assertNamesVariable("SOVA_COLLECT_INTERVAL_MS", "soon");
        assertNamesVariable("SOVA_TLS_ENABLED", "yes");
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
    void validBooleansAreAccepted() {
        assertTrue(SovaConfig.parseBooleanOrThrow("SOVA_TLS_ENABLED", "true"));
        assertTrue(SovaConfig.parseBooleanOrThrow("SOVA_TLS_ENABLED", "TRUE"));
        assertFalse(SovaConfig.parseBooleanOrThrow("SOVA_TLS_ENABLED", "false"));
        assertFalse(SovaConfig.parseBooleanOrThrow("SOVA_TLS_ENABLED", "FALSE"));
    }

    @Test
    void invalidBooleanNamesTheEnvironmentVariable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SovaConfig.parseBooleanOrThrow("SOVA_TLS_ENABLED", "1"));

        assertTrue(e.getMessage().contains("SOVA_TLS_ENABLED"),
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
