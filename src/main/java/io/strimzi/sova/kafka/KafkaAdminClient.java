/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.strimzi.sova.config.SovaConfig;

/**
 * Factory that creates the {@link Admin} client used for all Kafka metadata queries.
 *
 * <p>Bootstrap servers, security (TLS, SASL) and any other client setting come from the
 * {@code STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION} environment variable (Kafka client properties,
 * one {@code key=value} per line); its entries are passed to the client as they are.
 *
 * <p>Threading: the produced {@code Admin} instance is a thread-safe Kafka client.
 */
public class KafkaAdminClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClient.class);

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
     */
    public static Admin create(SovaConfig config) {
        Properties props = buildProperties(config);
        LOGGER.info("Creating Kafka AdminClient for bootstrap: {} (topic.regex={}, group.regex={}, collect.interval={}ms)",
            props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), config.getTopicPattern(),
            config.getGroupPattern(), config.getCollectIntervalMs());
        return Admin.create(props);
    }

    /**
     * Assembles the Kafka client properties from the given config.
     *
     * <p>Default settings are applied first, followed by the configured client properties, so
     * user-specified settings in {@code STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION} can override defaults.
     *
     * @param config application configuration
     * @return client properties
     */
    static Properties buildProperties(SovaConfig config) {
        Properties props = new Properties();
        // Halve the 60 s client default. A cycle makes several sequential calls, so this bounds a
        // call, not a cycle — the cycle is covered by the staleness window in MetricsCollector —
        // but it stops one unresponsive coordinator from stretching every cycle past the interval.
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(API_TIMEOUT.toMillis()));
        props.putAll(config.getKafkaAdminClientConfiguration());

        return props;
    }
}
