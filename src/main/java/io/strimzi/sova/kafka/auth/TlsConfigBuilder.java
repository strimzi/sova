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
import org.apache.kafka.common.security.auth.SecurityProtocol;

import io.strimzi.sova.config.SovaConfig;

/**
 * Builds the TLS-related Kafka client {@link Properties} entries from PEM files on disk.
 *
 * <p>The cluster CA certificate is always required. The client certificate and key are needed only
 * for mutual TLS: when both are absent the client only verifies the brokers (for example when it
 * authenticates with SASL OAUTHBEARER instead), and when exactly one is present the configuration
 * is rejected as incomplete.
 *
 * <p>Threading: stateless utility class — thread-safe.
 */
public class TlsConfigBuilder {

    /**
     * Private constructor — utility class, not to be instantiated.
     */
    private TlsConfigBuilder() {
    }

    /**
     * Populates {@code props} with TLS properties read from the PEM files at the given paths.
     *
     * @param props      the {@link Properties} to mutate
     * @param caCertFile path to the PEM-encoded CA certificate
     * @param certFile   path to the PEM-encoded client certificate chain, if mutual TLS is used
     * @param keyFile    path to the PEM-encoded client private key, if mutual TLS is used
     * @throws IOException           if any file cannot be read
     * @throws IllegalStateException if only one of {@code certFile} and {@code keyFile} is present
     */
    public static void configure(Properties props, String caCertFile, Optional<String> certFile,
                                 Optional<String> keyFile) throws IOException {
        if (certFile.isPresent() != keyFile.isPresent()) {
            throw new IllegalStateException(SovaConfig.ENV_TLS_CERT_FILE + " and " + SovaConfig.ENV_TLS_KEY_FILE
                    + " must be set together (mutual TLS) or both left unset (server verification only)");
        }
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, read(caCertFile));
        if (certFile.isPresent()) {
            props.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            props.setProperty(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, read(certFile.get()));
            props.setProperty(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, read(keyFile.get()));
        }
        // Hostname verification on. "HTTPS" names the RFC 2818 verification rules; it has nothing
        // to do with the protocol of Sova's own /metrics listener, which stays plain HTTP.
        // Without this, trust degrades to "any certificate the cluster CA signed" — which is every
        // other Strimzi component certificate in the namespace, not just the brokers we mean.
        props.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
    }

    private static String read(String file) throws IOException {
        try {
            return Files.readString(Path.of(file));
        } catch (IOException e) {
            throw new IOException("Cannot read TLS file " + file, e);
        }
    }
}
