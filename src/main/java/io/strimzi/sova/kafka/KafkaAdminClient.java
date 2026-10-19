/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.kafka.auth.SaslConfigBuilder;
import io.strimzi.sova.kafka.auth.TlsConfigBuilder;

/**
 * Factory that creates the {@link Admin} client used for all Kafka metadata queries.
 *
 * <p>TLS and SASL configuration is delegated to {@link TlsConfigBuilder} and
 * {@link SaslConfigBuilder} respectively. Both are applied only when the corresponding feature
 * is enabled / present in {@link SovaConfig}.
 *
 * <p>Threading: the produced {@code Admin} instance is a thread-safe Kafka client.
 */
public class KafkaAdminClient {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClient.class);

    /**
     * Per-call deadline for every AdminClient request.
     */
    private static final Duration API_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Private constructor — utility class, not to be instantiated.
     */
    private KafkaAdminClient() {
    }

    /**
     * Creates the {@link Admin} instance from the given config.
     *
     * @param config application configuration
     * @return configured {@link Admin} client
     * @throws IOException if TLS PEM files cannot be read
     */
    public static Admin create(SovaConfig config) throws IOException {
        Properties props = buildProperties(config);
        log.info("Creating Kafka AdminClient for bootstrap: {} (topic.regex={}, group.regex={}, collect.interval={}ms)",
            config.getKafkaBootstrapServers(), config.getTopicPattern(),
            config.getGroupPattern(), config.getCollectIntervalMs());
        return Admin.create(props);
    }

    /**
     * Assembles the Kafka client properties from the given config.
     *
     * @param config application configuration
     * @return client properties
     * @throws IOException if TLS PEM files cannot be read
     */
    static Properties buildProperties(SovaConfig config) throws IOException {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        // Halve the 60 s client default. A cycle makes several sequential calls, so this bounds a
        // call, not a cycle — the cycle is covered by the staleness window in MetricsCollector —
        // but it stops one unresponsive coordinator from stretching every cycle past the interval.
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(API_TIMEOUT.toMillis()));

        if (config.isTlsEnabled()) {
            log.info("TLS enabled, loading CA cert from {} (mTLS: {})", config.getTlsCaCertFile(),
                config.getTlsCertFile().isPresent() ? "enabled" : "disabled");
            TlsConfigBuilder.configure(props,
                config.getTlsCaCertFile(),
                config.getTlsCertFile(),
                config.getTlsKeyFile());
        } else {
            log.info("TLS disabled");
        }

        config.getSaslMechanism()
            .ifPresent(mechanism -> {
                log.info("SASL enabled with mechanism: {}", mechanism);
                SaslConfigBuilder.configure(props, mechanism,
                    config.getSaslJaasConfig()
                        .orElse(""),
                    config.getSaslLoginCallbackHandlerClass()
                        .orElse(""),
                    config.isTlsEnabled());
            });

        return props;
    }
}
