/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks the shipped {@code log4j2.properties}. There is no test-scoped override, so the
 * configuration loaded here is the one Sova runs with.
 */
class LoggingConfigTest {
    private static LoggerConfig kafkaLoggerConfig() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        return ctx.getConfiguration().getLoggerConfig("org.apache.kafka");
    }

    /**
     * A non-additive logger with no appender of its own silently drops every event, which would
     * hide the Kafka client's own WARN and ERROR lines (authentication, TLS, bootstrap failures).
     */
    @Test
    void kafkaLoggerOutputReachesAnAppender() {
        LoggerConfig kafka = kafkaLoggerConfig();
        assertEquals("org.apache.kafka", kafka.getName(), "a dedicated org.apache.kafka logger must exist");
        assertTrue(kafka.isAdditive() || !kafka.getAppenders().isEmpty(),
                "org.apache.kafka must either inherit the root appender or have its own");
    }

    /**
     * The org.apache.kafka level comes from STRIMZI_AC_LOG_LEVEL, defaulting to WARN, so the
     * expectation reads the same variable rather than assuming it is unset.
     */
    @Test
    void kafkaLoggerLevelFollowsStrimziAcLogLevel() {
        Level expected = Level.toLevel(System.getenv().getOrDefault("STRIMZI_AC_LOG_LEVEL", "WARN"));
        assertEquals(expected, kafkaLoggerConfig().getLevel(),
                "the org.apache.kafka level must follow STRIMZI_AC_LOG_LEVEL, defaulting to WARN");
    }
}
