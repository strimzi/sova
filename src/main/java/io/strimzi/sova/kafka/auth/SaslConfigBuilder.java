/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka.auth;

import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the SASL-related Kafka client {@link Properties} entries for the configured mechanism.
 *
 * <p>This is a transparent passthrough: the mechanism, JAAS config string, and callback handler
 * class name are copied directly from the caller into the {@link Properties} object that is
 * passed to {@code Admin.create()}. No SASL assembly logic lives here — that responsibility
 * belongs to the operator (or the user for standalone deployments), consistent with how
 * Bridge, Topic Operator, and other Strimzi components handle projected SA token authentication.
 *
 * <p>Only {@code oauthbearer} is a supported mechanism. Other SASL mechanisms and Kerberos
 * (GSSAPI) are out of scope.
 *
 * <p>Threading: stateless utility class — thread-safe.
 */
public class SaslConfigBuilder {

    private static final Logger log = LoggerFactory.getLogger(SaslConfigBuilder.class);

    /**
     * Private constructor — utility class, not to be instantiated.
     */
    private SaslConfigBuilder() {
    }

    /**
     * Populates {@code props} with SASL properties for the given mechanism.
     *
     * <p>The {@code jaasConfig} and {@code loginCallbackHandlerClass} values are written verbatim
     * to {@code sasl.jaas.config} and {@code sasl.login.callback.handler.class} respectively.
     * The security protocol is set to {@code SASL_SSL} when TLS is also enabled, or
     * {@code SASL_PLAINTEXT} otherwise.
     *
     * @param props                     the {@link Properties} to mutate
     * @param mechanism                 SASL mechanism identifier (case-insensitive, must be {@code oauthbearer})
     * @param jaasConfig                full SASL JAAS config string
     * @param loginCallbackHandlerClass SASL login callback handler class name
     * @param tlsEnabled                whether TLS is also active (determines security protocol)
     * @throws IllegalArgumentException if the mechanism is unsupported
     */
    public static void configure(Properties props, String mechanism,
                                 String jaasConfig, String loginCallbackHandlerClass, boolean tlsEnabled) {
        String normalized = mechanism.toUpperCase(Locale.ENGLISH);
        if (!"OAUTHBEARER".equals(normalized)) {
            throw new IllegalArgumentException("Unsupported SASL mechanism: '" + mechanism + "'. Only 'oauthbearer' is supported.");
        }

        props.setProperty(SaslConfigs.SASL_MECHANISM, normalized);

        if (!jaasConfig.isBlank()) {
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }

        if (!loginCallbackHandlerClass.isBlank()) {
            props.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, loginCallbackHandlerClass);
        }

        if (!tlsEnabled) {
            // OAUTHBEARER puts the bearer token in the initial client response, so on a plaintext
            // connection the token crosses the wire in the clear and anything on the path can
            // replay it. Allowed, because a local broker with no TLS is a legitimate way to run
            // Sova standalone, but never silently: under the operator this combination means
            // SOVA_TLS_ENABLED was misconfigured.
            log.warn("SASL {} is configured without TLS: the connection will use {} and the "
                    + "bearer token will be sent unencrypted. Set SOVA_TLS_ENABLED=true unless "
                    + "this is a local deployment on a trusted network.",
                normalized, SecurityProtocol.SASL_PLAINTEXT.name);
        }

        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            (tlsEnabled ? SecurityProtocol.SASL_SSL : SecurityProtocol.SASL_PLAINTEXT).name);
    }
}
