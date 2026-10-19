/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TlsConfigBuilder}.
 */
class TlsConfigBuilderTest {

    @TempDir
    Path tempDir;

    @Test
    void configuresTlsPropertiesFromFiles() throws Exception {
        Path caCert = tempDir.resolve("ca.crt");
        Path clientCert = tempDir.resolve("tls.crt");
        Path clientKey = tempDir.resolve("tls.key");

        Files.writeString(caCert, "-----BEGIN CERTIFICATE-----\nCA_CERT\n-----END CERTIFICATE-----");
        Files.writeString(clientCert, "-----BEGIN CERTIFICATE-----\nCLIENT_CERT\n-----END CERTIFICATE-----");
        Files.writeString(clientKey, "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----");

        Properties props = new Properties();
        TlsConfigBuilder.configure(props, caCert.toString(),
                Optional.of(clientCert.toString()), Optional.of(clientKey.toString()));

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("PEM", props.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        assertEquals("PEM", props.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        // "HTTPS" = RFC 2818 hostname verification on the Kafka connection. An empty string here
        // would disable it, which is a weaker trust model than the Go exporter Sova replaces.
        assertEquals("HTTPS", props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertEquals("-----BEGIN CERTIFICATE-----\nCA_CERT\n-----END CERTIFICATE-----",
                props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertEquals("-----BEGIN CERTIFICATE-----\nCLIENT_CERT\n-----END CERTIFICATE-----",
                props.getProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
        assertEquals("-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
                props.getProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG));
    }

    @Test
    void caOnlyConfiguresTruststoreAndLeavesKeystoreUntouched() throws Exception {
        Path caCert = tempDir.resolve("ca.crt");
        Files.writeString(caCert, "-----BEGIN CERTIFICATE-----\nCA_CERT\n-----END CERTIFICATE-----");

        Properties props = new Properties();
        TlsConfigBuilder.configure(props, caCert.toString(), Optional.empty(), Optional.empty());

        assertEquals("SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("PEM", props.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
        assertEquals("HTTPS", props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));
        assertEquals("-----BEGIN CERTIFICATE-----\nCA_CERT\n-----END CERTIFICATE-----",
                props.getProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
        assertNull(props.getProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG));
    }

    @Test
    void throwsWhenFileNotFound() {
        Properties props = new Properties();
        assertThrows(IOException.class, () ->
                TlsConfigBuilder.configure(props,
                        tempDir.resolve("nonexistent-ca.crt").toString(),
                        Optional.of(tempDir.resolve("nonexistent-cert.crt").toString()),
                        Optional.of(tempDir.resolve("nonexistent-key.key").toString())));
    }

    @Test
    void missingCaFileErrorNamesThePath() {
        String missing = tempDir.resolve("nonexistent-ca.crt").toString();

        IOException e = assertThrows(IOException.class, () ->
                TlsConfigBuilder.configure(new Properties(), missing, Optional.empty(), Optional.empty()));

        assertTrue(e.getMessage().contains(missing), "was: " + e.getMessage());
    }

    @Test
    void certWithoutKeyIsRejectedNamingBothVariables() throws Exception {
        Path caCert = tempDir.resolve("ca.crt");
        Files.writeString(caCert, "CA");

        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                TlsConfigBuilder.configure(new Properties(), caCert.toString(),
                        Optional.of("/some/tls.crt"), Optional.empty()));

        assertTrue(e.getMessage().contains("SOVA_TLS_CERT_FILE") && e.getMessage().contains("SOVA_TLS_KEY_FILE"),
                "was: " + e.getMessage());
    }

    @Test
    void keyWithoutCertIsRejectedNamingBothVariables() throws Exception {
        Path caCert = tempDir.resolve("ca.crt");
        Files.writeString(caCert, "CA");

        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                TlsConfigBuilder.configure(new Properties(), caCert.toString(),
                        Optional.empty(), Optional.of("/some/tls.key")));

        assertTrue(e.getMessage().contains("SOVA_TLS_CERT_FILE") && e.getMessage().contains("SOVA_TLS_KEY_FILE"),
                "was: " + e.getMessage());
    }
}
