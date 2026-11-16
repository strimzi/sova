/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
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
     * <p>The configured client properties are passed as they are. Sova adds no defaults: in
     * particular it does not set {@code default.api.timeout.ms}, so when the user leaves it out
     * Kafka's own default applies, including the automatic raise to {@code request.timeout.ms}.
     *
     * @param config application configuration
     * @return client properties
     */
    static Properties buildProperties(SovaConfig config) {
        // default.api.timeout.ms bounds one call; the cycle is bounded by the deadline in
        // MetricsCollector, derived from SovaConfig.getAdminApiTimeoutMs().
        return config.getKafkaAdminClientConfiguration();
    }
}
