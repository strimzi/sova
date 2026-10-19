/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.strimzi.sova.config.SovaConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link KafkaAdminClient#buildProperties(SovaConfig)}.
 */
class KafkaAdminClientTest {

    @TempDir
    Path tempDir;

    private Map<String, String> env;

    @BeforeEach
    void setUp() throws Exception {
        env = new HashMap<>();
        env.put("SOVA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        Path ca = tempDir.resolve("ca.crt");
        Files.writeString(ca, "CA");
        env.put("SOVA_TLS_CA_CERT_FILE", ca.toString());
    }

    private Properties build() throws Exception {
        return KafkaAdminClient.buildProperties(new SovaConfig(env::get));
    }

    private void withClientCert() throws Exception {
        Path cert = tempDir.resolve("tls.crt");
        Path key = tempDir.resolve("tls.key");
        Files.writeString(cert, "CERT");
        Files.writeString(key, "KEY");
        env.put("SOVA_TLS_CERT_FILE", cert.toString());
        env.put("SOVA_TLS_KEY_FILE", key.toString());
    }

    @Test
    void plaintextSetsOnlyBootstrapAndTimeout() throws Exception {
        env.put("SOVA_TLS_ENABLED", "false");

        Properties props = build();

        assertEquals("kafka:9092", props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertNull(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertNull(props.getProperty(SaslConfigs.SASL_MECHANISM));
    }

    @Test
    void apiTimeoutIsThirtySeconds() throws Exception {
        env.put("SOVA_TLS_ENABLED", "false");

        assertEquals("30000", build().getProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    @Test
    void tlsOnlyTrustsTheCaAndVerifiesHostnames() throws Exception {
        Properties props = build();

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("CA", props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertEquals("HTTPS", props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG));
    }

    @Test
    void mtlsAddsTheClientKeystore() throws Exception {
        withClientCert();

        Properties props = build();

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("CERT", props.getProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
        assertEquals("KEY", props.getProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG));
    }

    @Test
    void tlsWithOauthbearerUsesSaslSsl() throws Exception {
        env.put("SOVA_SASL_MECHANISM", "oauthbearer");
        env.put("SOVA_SASL_JAAS_CONFIG", "org.example.Jaas required;");
        env.put("SOVA_SASL_LOGIN_CALLBACK_HANDLER_CLASS", "org.example.Handler");

        Properties props = build();

        assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("OAUTHBEARER", props.getProperty(SaslConfigs.SASL_MECHANISM));
        assertEquals("org.example.Jaas required;", props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals("org.example.Handler", props.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        // The operator's ServiceAccount mode: CA only, no client certificate.
        assertEquals("CA", props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
    }

    @Test
    void plaintextWithOauthbearerUsesSaslPlaintext() throws Exception {
        env.put("SOVA_TLS_ENABLED", "false");
        env.put("SOVA_SASL_MECHANISM", "oauthbearer");

        Properties props = build();

        assertEquals("SASL_PLAINTEXT", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("OAUTHBEARER", props.getProperty(SaslConfigs.SASL_MECHANISM));
    }

    @Test
    void unsupportedSaslMechanismIsRejected() {
        env.put("SOVA_SASL_MECHANISM", "plain");

        assertThrows(IllegalArgumentException.class, this::build);
    }

    @Test
    void certWithoutKeyIsRejected() throws Exception {
        withClientCert();
        env.remove("SOVA_TLS_KEY_FILE");

        assertThrows(IllegalStateException.class, this::build);
    }
}
