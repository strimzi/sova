/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link KafkaAdminClient#buildProperties(SovaConfig)}.
 */
class KafkaAdminClientTest {
    private Map<String, String> env;

    private static final String CLIENT_CONFIG_ENV = "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION";

    @BeforeEach
    void setUp() {
        env = new HashMap<>();
        env.put(CLIENT_CONFIG_ENV, "bootstrap.servers=kafka:9092\n");
    }

    private Properties build() {
        return KafkaAdminClient.buildProperties(new SovaConfig(env::get));
    }

    @Test
    void minimalClientConfigurationAddsOnlyTheTimeoutDefault() {
        Properties props = build();

        assertEquals("kafka:9092", props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("30000", props.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
        assertNull(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals(2, props.size());
    }

    @Test
    void clientConfigurationEntriesAreMergedIntoTheClientProperties() {
        env.put(CLIENT_CONFIG_ENV, """
                bootstrap.servers=kafka:9092
                security.protocol=SASL_SSL
                ssl.truststore.type=PEM
                ssl.truststore.certificates=CA
                sasl.mechanism=OAUTHBEARER
                sasl.jaas.config=org.example.Jaas required;
                """);

        Properties props = build();

        assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("PEM", props.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        assertEquals("CA", props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertEquals("OAUTHBEARER", props.getProperty(SaslConfigs.SASL_MECHANISM));
        assertEquals("org.example.Jaas required;", props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("kafka:9092", props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void defaultApiTimeoutCanBeOverriddenByClientConfiguration() {
        env.put(CLIENT_CONFIG_ENV, """
                bootstrap.servers=kafka:9092
                default.api.timeout.ms=60000
                """);

        Properties props = build();

        assertEquals("60000", props.getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}
