/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.kafka.auth;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link SaslConfigBuilder}.
 */
class SaslConfigBuilderTest {

    private static final String JAAS_CONFIG =
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
            + "oauth.access.token.location=\"/var/run/secrets/strimzi.io/token\";";

    private static final String CALLBACK_HANDLER =
            "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";

    @Test
    void passesMechanismVerbatimAsUpperCase() {
        Properties props = new Properties();
        SaslConfigBuilder.configure(props, "oauthbearer", JAAS_CONFIG, CALLBACK_HANDLER, false);
        assertEquals("OAUTHBEARER", props.getProperty(SaslConfigs.SASL_MECHANISM));
    }

    @Test
    void passesJaasConfigVerbatim() {
        Properties props = new Properties();
        SaslConfigBuilder.configure(props, "OAUTHBEARER", JAAS_CONFIG, CALLBACK_HANDLER, false);
        assertEquals(JAAS_CONFIG, props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
    }

    @Test
    void passesLoginCallbackHandlerClassVerbatim() {
        Properties props = new Properties();
        SaslConfigBuilder.configure(props, "oauthbearer", JAAS_CONFIG, CALLBACK_HANDLER, false);
        assertEquals(CALLBACK_HANDLER,
                props.getProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
    }

    @Test
    void securityProtocolIsSaslSslWhenTlsEnabled() {
        Properties props = new Properties();
        SaslConfigBuilder.configure(props, "oauthbearer", JAAS_CONFIG, CALLBACK_HANDLER, true);
        assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    void securityProtocolIsSaslPlaintextWhenTlsDisabled() {
        Properties props = new Properties();
        SaslConfigBuilder.configure(props, "oauthbearer", JAAS_CONFIG, CALLBACK_HANDLER, false);
        assertEquals("SASL_PLAINTEXT",
                props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    void throwsForUnsupportedMechanisms() {
        Properties props = new Properties();
        assertThrows(IllegalArgumentException.class, () ->
                SaslConfigBuilder.configure(props, "plain", JAAS_CONFIG, CALLBACK_HANDLER, false));
        assertThrows(IllegalArgumentException.class, () ->
                SaslConfigBuilder.configure(props, "scram-sha-256", JAAS_CONFIG, CALLBACK_HANDLER, false));
        assertThrows(IllegalArgumentException.class, () ->
                SaslConfigBuilder.configure(props, "gssapi", JAAS_CONFIG, CALLBACK_HANDLER, false));
    }
}
