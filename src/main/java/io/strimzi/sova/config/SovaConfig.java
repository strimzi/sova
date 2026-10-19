/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.config;

import java.util.Locale;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Holds all configuration properties for Sova, read from environment variables.
 *
 * <p>Every value is parsed and validated here, so a malformed environment fails at startup with
 * a message naming the variable. That matters because a collection failure is survivable by
 * design: it logs a WARN and keeps serving the previous snapshot. A regex compiled lazily in the
 * collector would therefore produce a pod that looks alive but can never collect anything.
 *
 * <p>Threading: effectively immutable after construction; safe to read from any thread.
 */
public final class SovaConfig {

    // Environment variable names. The operator injects these; they must never change.
    private static final String ENV_KAFKA_BOOTSTRAP_SERVERS = "SOVA_KAFKA_BOOTSTRAP_SERVERS";
    private static final String ENV_GROUP_REGEX = "SOVA_GROUP_REGEX";
    private static final String ENV_GROUP_EXCLUDE_REGEX = "SOVA_GROUP_EXCLUDE_REGEX";
    private static final String ENV_TOPIC_REGEX = "SOVA_TOPIC_REGEX";
    private static final String ENV_TOPIC_EXCLUDE_REGEX = "SOVA_TOPIC_EXCLUDE_REGEX";
    private static final String ENV_TLS_ENABLED = "SOVA_TLS_ENABLED";
    private static final String ENV_TLS_CA_CERT_FILE = "SOVA_TLS_CA_CERT_FILE";
    public static final String ENV_TLS_CERT_FILE = "SOVA_TLS_CERT_FILE";
    public static final String ENV_TLS_KEY_FILE = "SOVA_TLS_KEY_FILE";
    private static final String ENV_SASL_MECHANISM = "SOVA_SASL_MECHANISM";
    private static final String ENV_SASL_JAAS_CONFIG = "SOVA_SASL_JAAS_CONFIG";
    private static final String ENV_SASL_LOGIN_CALLBACK_HANDLER_CLASS = "SOVA_SASL_LOGIN_CALLBACK_HANDLER_CLASS";
    private static final String ENV_METRICS_PORT = "SOVA_METRICS_PORT";
    private static final String ENV_MANAGEMENT_PORT = "SOVA_MANAGEMENT_PORT";
    private static final String ENV_COLLECT_INTERVAL_MS = "SOVA_COLLECT_INTERVAL_MS";

    // Defaults, shared by the environment constructor and the test constructor.
    private static final String DEFAULT_REGEX = ".*";
    private static final String DEFAULT_TLS_CA_CERT_FILE = "/etc/sova/cluster-ca-certs/cluster-ca.crt";
    private static final int DEFAULT_METRICS_PORT = 9404;
    private static final int DEFAULT_MANAGEMENT_PORT = 8080;
    private static final long DEFAULT_COLLECT_INTERVAL_MS = 30000L;

    /**
     * Bootstrap servers of the Kafka cluster to monitor.
     * Env var: {@code SOVA_KAFKA_BOOTSTRAP_SERVERS}. Required — no default.
     */
    private final String kafkaBootstrapServers;

    /**
     * Compiled regex selecting which consumer groups are reported.
     * Env var: {@code SOVA_GROUP_REGEX}. Default: {@code .*} (all groups).
     */
    private final Pattern groupRegex;

    /**
     * Optional compiled regex excluding consumer groups from reports.
     * Env var: {@code SOVA_GROUP_EXCLUDE_REGEX}. Absent by default (nothing excluded).
     */
    private final Optional<Pattern> groupExcludeRegex;

    /**
     * Compiled regex selecting which topics are reported.
     * Env var: {@code SOVA_TOPIC_REGEX}. Default: {@code .*} (all topics).
     */
    private final Pattern topicRegex;

    /**
     * Optional compiled regex excluding topics from reports.
     * Env var: {@code SOVA_TOPIC_EXCLUDE_REGEX}. Absent by default (nothing excluded).
     */
    private final Optional<Pattern> topicExcludeRegex;

    /**
     * Whether TLS is enabled on the Kafka connection.
     * Env var: {@code SOVA_TLS_ENABLED}. Default: {@code true}.
     */
    private final boolean tlsEnabled;

    /**
     * Path to the CA certificate PEM file used for TLS.
     * Env var: {@code SOVA_TLS_CA_CERT_FILE}.
     * Default: {@code /etc/sova/cluster-ca-certs/cluster-ca.crt}.
     */
    private final String tlsCaCertFile;

    /**
     * Path to the client certificate PEM file used for mutual TLS.
     * Env var: {@code SOVA_TLS_CERT_FILE}. Optional, no default: must be set together with
     * {@code SOVA_TLS_KEY_FILE} or not at all.
     */
    private final Optional<String> tlsCertFile;

    /**
     * Path to the client private-key PEM file used for mutual TLS.
     * Env var: {@code SOVA_TLS_KEY_FILE}. Optional, no default: must be set together with
     * {@code SOVA_TLS_CERT_FILE} or not at all.
     */
    private final Optional<String> tlsKeyFile;

    /**
     * SASL mechanism to use (e.g. {@code OAUTHBEARER} if set).
     * Env var: {@code SOVA_SASL_MECHANISM}. Absent means SASL is disabled.
     */
    private final Optional<String> saslMechanism;

    /**
     * Full SASL JAAS config string passed verbatim to the Kafka client.
     * Env var: {@code SOVA_SASL_JAAS_CONFIG}. Optional.
     */
    private final Optional<String> saslJaasConfig;

    /**
     * SASL login callback handler class name passed verbatim to the Kafka client.
     * Env var: {@code SOVA_SASL_LOGIN_CALLBACK_HANDLER_CLASS}. Optional.
     */
    private final Optional<String> saslLoginCallbackHandlerClass;

    /**
     * Metrics HTTP port.
     * Env var: {@code SOVA_METRICS_PORT}. Default: {@code 9404} — matches
     * {@code MetricsModel.METRICS_PORT} ("tcp-prometheus") in strimzi-kafka-operator, so the
     * operator's container port and PodMonitor need no special-casing for Sova.
     */
    private final int metricsPort;

    /**
     * Management HTTP port (for health and readiness probes).
     * Env var: {@code SOVA_MANAGEMENT_PORT}. Default: {@code 8080}.
     */
    private final int managementPort;

    /**
     * Interval between metric collection cycles in milliseconds.
     * Env var: {@code SOVA_COLLECT_INTERVAL_MS}. Default: {@code 30000}.
     */
    private final long collectIntervalMs;

    /**
     * Package-private constructor for tests — allows direct field injection without reading
     * environment variables.
     */
    SovaConfig(String kafkaBootstrapServers, boolean tlsEnabled, String groupRegex,
               String topicRegex) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.tlsEnabled = tlsEnabled;
        this.groupRegex = compilePattern(ENV_GROUP_REGEX, groupRegex);
        this.topicRegex = compilePattern(ENV_TOPIC_REGEX, topicRegex);
        this.groupExcludeRegex = Optional.empty();
        this.topicExcludeRegex = Optional.empty();
        this.tlsCaCertFile = "";
        this.tlsCertFile = Optional.empty();
        this.tlsKeyFile = Optional.empty();
        this.saslMechanism = Optional.empty();
        this.saslJaasConfig = Optional.empty();
        this.saslLoginCallbackHandlerClass = Optional.empty();
        this.metricsPort = DEFAULT_METRICS_PORT;
        this.managementPort = DEFAULT_MANAGEMENT_PORT;
        this.collectIntervalMs = DEFAULT_COLLECT_INTERVAL_MS;
    }

    /**
     * Creates a minimal config suitable for tests against a plain (no-TLS) Kafka broker.
     *
     * @param bootstrapServers Kafka bootstrap address
     * @return config instance
     */
    public static SovaConfig forTesting(String bootstrapServers) {
        return new SovaConfig(bootstrapServers, false, DEFAULT_REGEX, DEFAULT_REGEX);
    }

    /**
     * Reads all configuration from environment variables.
     *
     * @throws IllegalStateException if a required variable is missing or a value is malformed
     */
    public SovaConfig() {
        this(System::getenv);
    }

    /**
     * Reads all configuration from the given environment lookup. Exists so tests can inject an
     * environment instead of depending on the process one.
     *
     * @param env lookup returning the value of a variable, or {@code null} when it is not set
     * @throws IllegalStateException if a required variable is missing or a value is malformed
     */
    public SovaConfig(UnaryOperator<String> env) {
        kafkaBootstrapServers = requireEnv(env, ENV_KAFKA_BOOTSTRAP_SERVERS);
        groupRegex = compilePattern(ENV_GROUP_REGEX, getEnv(env, ENV_GROUP_REGEX, DEFAULT_REGEX));
        groupExcludeRegex = optionalEnv(env, ENV_GROUP_EXCLUDE_REGEX)
                .map(v -> compilePattern(ENV_GROUP_EXCLUDE_REGEX, v));
        topicRegex = compilePattern(ENV_TOPIC_REGEX, getEnv(env, ENV_TOPIC_REGEX, DEFAULT_REGEX));
        topicExcludeRegex = optionalEnv(env, ENV_TOPIC_EXCLUDE_REGEX)
                .map(v -> compilePattern(ENV_TOPIC_EXCLUDE_REGEX, v));
        tlsEnabled = parseBooleanOrThrow(ENV_TLS_ENABLED, getEnv(env, ENV_TLS_ENABLED, "true"));
        tlsCaCertFile = getEnv(env, ENV_TLS_CA_CERT_FILE, DEFAULT_TLS_CA_CERT_FILE);
        tlsCertFile = optionalEnv(env, ENV_TLS_CERT_FILE);
        tlsKeyFile = optionalEnv(env, ENV_TLS_KEY_FILE);
        saslMechanism = optionalEnv(env, ENV_SASL_MECHANISM);
        saslJaasConfig = optionalEnv(env, ENV_SASL_JAAS_CONFIG);
        saslLoginCallbackHandlerClass = optionalEnv(env, ENV_SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        metricsPort = parsePortOrThrow(ENV_METRICS_PORT,
                getEnv(env, ENV_METRICS_PORT, String.valueOf(DEFAULT_METRICS_PORT)));
        managementPort = parsePortOrThrow(ENV_MANAGEMENT_PORT,
                getEnv(env, ENV_MANAGEMENT_PORT, String.valueOf(DEFAULT_MANAGEMENT_PORT)));
        collectIntervalMs = parsePositiveLongOrThrow(ENV_COLLECT_INTERVAL_MS,
                getEnv(env, ENV_COLLECT_INTERVAL_MS, String.valueOf(DEFAULT_COLLECT_INTERVAL_MS)));
    }

    private static String requireEnv(UnaryOperator<String> env, String name) {
        String value = env.apply(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required environment variable not set: " + name);
        }
        return value;
    }

    private static String getEnv(UnaryOperator<String> env, String name, String defaultValue) {
        String value = env.apply(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static Optional<String> optionalEnv(UnaryOperator<String> env, String name) {
        String value = env.apply(name);
        return (value == null || value.isBlank()) ? Optional.empty() : Optional.of(value);
    }

    /**
     * Compiles a regex, reporting the environment variable that carried it.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param regex      the regex source
     * @return the compiled pattern
     * @throws IllegalStateException if the regex is malformed
     */
    static Pattern compilePattern(String envVarName, String regex) {
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalStateException(
                    "Invalid regular expression in " + envVarName + ": " + regex, e);
        }
    }

    /**
     * Parses a TCP port, reporting the environment variable that carried it.
     *
     * <p>Out-of-range values are rejected here rather than at bind time, so the failure names the
     * variable to fix instead of surfacing as an opaque {@code IllegalArgumentException} from the
     * socket layer. {@code 0} is allowed and binds an ephemeral port.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value
     * @return the parsed port
     * @throws IllegalStateException if the value is not a number or is not a valid port
     */
    static int parsePortOrThrow(String envVarName, String value) {
        long port = parseLongOrThrow(envVarName, value);
        if (port < 0 || port > 65535) {
            throw new IllegalStateException(
                    "Invalid port in " + envVarName + ": " + value + " (must be 0-65535)");
        }
        return (int) port;
    }

    /**
     * Parses a long, reporting the environment variable that carried it.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value
     * @return the parsed long
     * @throws IllegalStateException if the value is not a long
     */
    static long parseLongOrThrow(String envVarName, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Invalid long in " + envVarName + ": " + value, e);
        }
    }

    /**
     * Parses a strict boolean, reporting the environment variable that carried it.
     *
     * <p>{@link Boolean#parseBoolean(String)} accepts only case-insensitive {@code "true"} and
     * silently treats everything else — {@code "1"}, {@code "yes"}, a typo — as {@code false}.
     * For a flag that turns a security control on or off, that fails open instead of failing
     * loudly, so this accepts only case-insensitive {@code true}/{@code false} and rejects
     * anything else.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value
     * @return the parsed boolean
     * @throws IllegalStateException if the value is not {@code true} or {@code false}
     */
    static boolean parseBooleanOrThrow(String envVarName, String value) {
        String normalized = value.toLowerCase(Locale.ENGLISH);
        if ("true".equals(normalized)) {
            return true;
        }
        if ("false".equals(normalized)) {
            return false;
        }
        throw new IllegalStateException(
                "Invalid boolean in " + envVarName + ": " + value + " (must be true or false)");
    }

    /**
     * Parses a positive long, reporting the environment variable that carried it.
     *
     * <p>A value {@code <= 0} passed to {@code ScheduledExecutorService.scheduleWithFixedDelay}
     * throws a bare, message-less {@code IllegalArgumentException} at startup instead of naming
     * the variable to fix, so it is rejected here instead. Values large enough to overflow when
     * doubled are rejected too, since {@code MetricsCollector.isHealthy()} multiplies this value
     * by two to decide staleness, and an overflow to negative would pin readiness DOWN forever.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value
     * @return the parsed positive long
     * @throws IllegalStateException if the value is not a long, is not positive, or would
     *                               overflow when doubled
     */
    static long parsePositiveLongOrThrow(String envVarName, String value) {
        long parsed = parseLongOrThrow(envVarName, value);
        if (parsed <= 0 || parsed > Long.MAX_VALUE / 4) {
            throw new IllegalStateException(
                    "Invalid value in " + envVarName + ": " + value
                            + " (must be positive and no greater than " + (Long.MAX_VALUE / 4) + ")");
        }
        return parsed;
    }

    /** @return bootstrap server list */
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    /** @return compiled consumer-group include pattern */
    public Pattern getGroupPattern() {
        return groupRegex;
    }

    /** @return compiled consumer-group exclude pattern, if configured */
    public Optional<Pattern> getGroupExcludePattern() {
        return groupExcludeRegex;
    }

    /** @return compiled topic include pattern */
    public Pattern getTopicPattern() {
        return topicRegex;
    }

    /** @return compiled topic exclude pattern, if configured */
    public Optional<Pattern> getTopicExcludePattern() {
        return topicExcludeRegex;
    }

    /** @return whether TLS is enabled */
    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    /** @return CA certificate PEM file path */
    public String getTlsCaCertFile() {
        return tlsCaCertFile;
    }

    /** @return client certificate PEM file path, if mutual TLS is configured */
    public Optional<String> getTlsCertFile() {
        return tlsCertFile;
    }

    /** @return client private-key PEM file path, if mutual TLS is configured */
    public Optional<String> getTlsKeyFile() {
        return tlsKeyFile;
    }

    /** @return SASL mechanism, if configured */
    public Optional<String> getSaslMechanism() {
        return saslMechanism;
    }

    /** @return SASL JAAS config string, if configured */
    public Optional<String> getSaslJaasConfig() {
        return saslJaasConfig;
    }

    /** @return SASL login callback handler class name, if configured */
    public Optional<String> getSaslLoginCallbackHandlerClass() {
        return saslLoginCallbackHandlerClass;
    }

    /** @return metrics HTTP port */
    public int getMetricsPort() {
        return metricsPort;
    }

    /** @return management HTTP port */
    public int getManagementPort() {
        return managementPort;
    }

    /** @return collection interval in milliseconds */
    public long getCollectIntervalMs() {
        return collectIntervalMs;
    }

    @Override
    public String toString() {
        return "SovaConfig{" +
            "\n\tkafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
            "\n\tgroupRegex=" + groupRegex +
            "\n\tgroupExcludeRegex=" + groupExcludeRegex +
            "\n\ttopicRegex=" + topicRegex +
            "\n\ttopicExcludeRegex=" + topicExcludeRegex +
            "\n\ttlsEnabled=" + tlsEnabled +
            "\n\ttlsCaCertFile='" + tlsCaCertFile + '\'' +
            "\n\ttlsCertFile=" + tlsCertFile +
            "\n\ttlsKeyFile=" + tlsKeyFile +
            "\n\tsaslMechanism=" + saslMechanism +
            "\n\tsaslJaasConfig=" + saslJaasConfig.map(v -> "<set>").orElse("<unset>") +
            "\n\tsaslLoginCallbackHandlerClass=" + saslLoginCallbackHandlerClass +
            "\n\tmetricsPort=" + metricsPort +
            "\n\tmanagementPort=" + managementPort +
            "\n\tcollectIntervalMs=" + collectIntervalMs +
            '}';
    }
}
