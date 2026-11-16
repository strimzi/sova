/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;

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
    private static final String ENV_GROUP_REGEX = "STRIMZI_GROUP_REGEX";
    private static final String ENV_GROUP_EXCLUDE_REGEX = "STRIMZI_GROUP_EXCLUDE_REGEX";
    private static final String ENV_TOPIC_REGEX = "STRIMZI_TOPIC_REGEX";
    private static final String ENV_TOPIC_EXCLUDE_REGEX = "STRIMZI_TOPIC_EXCLUDE_REGEX";
    private static final String ENV_KAFKA_ADMIN_CLIENT_CONFIGURATION = "STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION";
    private static final String ENV_METRICS_PORT = "STRIMZI_METRICS_PORT";
    private static final String ENV_MANAGEMENT_PORT = "STRIMZI_MANAGEMENT_PORT";
    private static final String ENV_COLLECT_INTERVAL_MS = "STRIMZI_COLLECT_INTERVAL_MS";

    // Defaults, shared by the environment constructor and the test constructor.
    private static final String DEFAULT_REGEX = ".*";
    private static final int DEFAULT_METRICS_PORT = 9404;
    private static final int DEFAULT_MANAGEMENT_PORT = 8080;
    private static final long DEFAULT_COLLECT_INTERVAL_MS = 30000L;

    /**
     * Number of {@code default.api.timeout.ms} periods one collection cycle may take.
     *
     * <p>The Kafka client already bounds each Admin call by {@code default.api.timeout.ms}, so a
     * dead or unreachable cluster fails the cycle after about one timeout, because the first call
     * in each branch fails. Detecting a down cluster is not this deadline's job. It is a backstop
     * for a branch that never returns (an Admin call or code that ignores its timeout). Without
     * it, a stuck cycle would freeze the fixed-delay schedule while the probes stay 200.
     *
     * <p>The timeout multiplied is {@link #getAdminApiTimeoutMs()}, the client's effective
     * per-call timeout, so the deadline follows the client when {@code request.timeout.ms} is
     * raised. That value is a Kafka INT, so 5 times it cannot overflow a long.
     *
     * <p>Why 5: the longest branch makes 4 sequential Admin calls (groups: {@code listGroups},
     * the rare {@code describeCluster}, {@code describeConsumerGroups},
     * {@code listConsumerGroupOffsets}). Topics make 3 ({@code listTopics},
     * {@code describeTopics}, then both {@code listOffsets} dispatched together), and the broker
     * branch makes 1. One more call is added as margin. So a cycle that is slow but healthy,
     * with every call succeeding just under the timeout, is never cut off.
     *
     * <p>If a collector gains another sequential Admin call, bump this constant.
     */
    private static final long CYCLE_API_CALLS = 5;

    /**
     * Compiled regex selecting which consumer groups are reported.
     * Env var: {@code STRIMZI_GROUP_REGEX}. Default: {@code .*} (all groups).
     */
    private final Pattern groupRegex;

    /**
     * Optional compiled regex excluding consumer groups from reports.
     * Env var: {@code STRIMZI_GROUP_EXCLUDE_REGEX}. Absent by default (nothing excluded).
     */
    private final Optional<Pattern> groupExcludeRegex;

    /**
     * Compiled regex selecting which topics are reported.
     * Env var: {@code STRIMZI_TOPIC_REGEX}. Default: {@code .*} (all topics).
     */
    private final Pattern topicRegex;

    /**
     * Optional compiled regex excluding topics from reports.
     * Env var: {@code STRIMZI_TOPIC_EXCLUDE_REGEX}. Absent by default (nothing excluded).
     */
    private final Optional<Pattern> topicExcludeRegex;

    /**
     * Kafka client properties passed to the {@code Admin} client (bootstrap servers, TLS, SASL and
     * any other client setting), given as {@code key=value} lines.
     * Env var: {@code STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION}. Required: it must at least
     * contain {@code bootstrap.servers}.
     *
     * <p>Not exposed directly: {@link #getKafkaAdminClientConfiguration()} returns a copy, so the
     * config stays immutable. Values may be sensitive, so {@link #toString()} prints keys only.
     */
    private final Properties kafkaAdminClientConfiguration;

    /**
     * Metrics HTTP port.
     * Env var: {@code STRIMZI_METRICS_PORT}. Default: {@code 9404} — matches
     * {@code MetricsModel.METRICS_PORT} ("tcp-prometheus") in strimzi-kafka-operator, so the
     * operator's container port and PodMonitor need no special-casing for Sova.
     */
    private final int metricsPort;

    /**
     * Management HTTP port (for the liveness and readiness probes).
     * Env var: {@code STRIMZI_MANAGEMENT_PORT}. Default: {@code 8080}.
     */
    private final int managementPort;

    /**
     * Interval between metric collection cycles in milliseconds.
     * Env var: {@code STRIMZI_COLLECT_INTERVAL_MS}. Default: {@code 30000}.
     */
    private final long collectIntervalMs;

    /**
     * Effective per-call timeout of the {@code Admin} client, see {@link #getAdminApiTimeoutMs()}.
     */
    private final long adminApiTimeoutMs;

    /**
     * Package-private constructor for tests — allows direct field injection without reading
     * environment variables.
     */
    /* test */ SovaConfig(String kafkaBootstrapServers, String groupRegex, String topicRegex) {
        this.groupRegex = compilePattern(ENV_GROUP_REGEX, groupRegex);
        this.topicRegex = compilePattern(ENV_TOPIC_REGEX, topicRegex);
        this.groupExcludeRegex = Optional.empty();
        this.topicExcludeRegex = Optional.empty();
        this.kafkaAdminClientConfiguration = new Properties();
        this.kafkaAdminClientConfiguration.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        this.metricsPort = DEFAULT_METRICS_PORT;
        this.managementPort = DEFAULT_MANAGEMENT_PORT;
        this.collectIntervalMs = DEFAULT_COLLECT_INTERVAL_MS;
        this.adminApiTimeoutMs = effectiveAdminApiTimeoutMs(kafkaAdminClientConfiguration);
    }

    /**
     * Creates a minimal config suitable for tests against a plain Kafka broker.
     *
     * @param bootstrapServers Kafka bootstrap address
     * @return config instance
     */
    public static SovaConfig forTesting(String bootstrapServers) {
        return new SovaConfig(bootstrapServers, DEFAULT_REGEX, DEFAULT_REGEX);
    }

    /**
     * Reads all configuration from environment variables.
     *
     * @throws IllegalStateException if a required variable is missing or a value is malformed
     * @throws org.apache.kafka.common.config.ConfigException if an Admin client property in
     *         {@code STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION} is invalid
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
     * @throws org.apache.kafka.common.config.ConfigException if an Admin client property in
     *         {@code STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION} is invalid
     */
    public SovaConfig(UnaryOperator<String> env) {
        kafkaAdminClientConfiguration = parseProperties(ENV_KAFKA_ADMIN_CLIENT_CONFIGURATION,
                env.apply(ENV_KAFKA_ADMIN_CLIENT_CONFIGURATION));
        requireBootstrapServers(kafkaAdminClientConfiguration);
        adminApiTimeoutMs = effectiveAdminApiTimeoutMs(kafkaAdminClientConfiguration);
        groupRegex = compilePattern(ENV_GROUP_REGEX, getEnv(env, ENV_GROUP_REGEX, DEFAULT_REGEX));
        groupExcludeRegex = optionalEnv(env, ENV_GROUP_EXCLUDE_REGEX)
                .map(v -> compilePattern(ENV_GROUP_EXCLUDE_REGEX, v));
        topicRegex = compilePattern(ENV_TOPIC_REGEX, getEnv(env, ENV_TOPIC_REGEX, DEFAULT_REGEX));
        topicExcludeRegex = optionalEnv(env, ENV_TOPIC_EXCLUDE_REGEX)
                .map(v -> compilePattern(ENV_TOPIC_EXCLUDE_REGEX, v));
        metricsPort = parsePortOrThrow(ENV_METRICS_PORT,
                getEnv(env, ENV_METRICS_PORT, String.valueOf(DEFAULT_METRICS_PORT)));
        managementPort = parsePortOrThrow(ENV_MANAGEMENT_PORT,
                getEnv(env, ENV_MANAGEMENT_PORT, String.valueOf(DEFAULT_MANAGEMENT_PORT)));
        collectIntervalMs = parsePositiveLongOrThrow(ENV_COLLECT_INTERVAL_MS,
                getEnv(env, ENV_COLLECT_INTERVAL_MS, String.valueOf(DEFAULT_COLLECT_INTERVAL_MS)));
    }

    private static void requireBootstrapServers(Properties adminClientConfig) {
        String value = adminClientConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Required property " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
                    + " not set in " + ENV_KAFKA_ADMIN_CLIENT_CONFIGURATION);
        }
    }

    /**
     * Computes the Admin client's effective per-call timeout with Kafka's own
     * {@link AdminClientConfig}, which supplies the defaults and validates the values (an invalid
     * one throws its {@code ConfigException}, naming the key). The rule is the one in
     * {@code KafkaAdminClient.configureDefaultApiTimeoutMs}: an explicit
     * {@code default.api.timeout.ms} is used as-is, otherwise the larger of its default and
     * {@code request.timeout.ms}.
     *
     * <p>The public {@code AdminClientConfig(Map)} constructor does not log the values, which may
     * hold credentials.
     */
    private static long effectiveAdminApiTimeoutMs(Properties adminClientConfig) {
        AdminClientConfig kafka = new AdminClientConfig(adminClientConfig);
        int request = kafka.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int api = kafka.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        return kafka.originals().containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG) ? api : Math.max(api, request);
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
     * Parses {@code key=value} lines in {@link Properties} format, reporting the environment
     * variable that carried them.
     *
     * <p>The value is deliberately left out of the failure message: it may hold credentials.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value, or {@code null} when the variable is not set
     * @return the parsed properties, empty when the value is {@code null}
     * @throws IllegalStateException if the value is not valid properties syntax
     */
    static Properties parseProperties(String envVarName, String value) {
        Properties properties = new Properties();
        if (value != null) {
            try {
                properties.load(new StringReader(value));
            } catch (IOException | IllegalArgumentException e) {
                throw new IllegalStateException("Invalid properties in " + envVarName, e);
            }
        }
        return properties;
    }

    /**
     * Parses a positive long, reporting the environment variable that carried it.
     *
     * <p>A value {@code <= 0} passed to {@code ScheduledExecutorService.scheduleWithFixedDelay}
     * throws a bare, message-less {@code IllegalArgumentException} at startup instead of naming
     * the variable to fix, so it is rejected here instead. Values above a quarter of
     * {@link Long#MAX_VALUE} are rejected too, so arithmetic on the interval cannot overflow.
     *
     * @param envVarName the variable the value came from, used in the failure message
     * @param value      the raw value
     * @return the parsed positive long
     * @throws IllegalStateException if the value is not a long, is not positive, or is too
     *                               large
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

    /** @return a copy of the Kafka client properties to merge into the {@code Admin} configuration */
    public Properties getKafkaAdminClientConfiguration() {
        Properties copy = new Properties();
        copy.putAll(kafkaAdminClientConfiguration);
        return copy;
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

    /**
     * @return the {@code Admin} client's effective per-call timeout, read through Kafka's
     *         {@code AdminClientConfig}: {@code default.api.timeout.ms} when set, otherwise
     *         {@code max(default.api.timeout.ms default, request.timeout.ms)}
     */
    public long getAdminApiTimeoutMs() {
        return adminApiTimeoutMs;
    }

    /**
     * @return deadline for one collection cycle in milliseconds: {@link #CYCLE_API_CALLS} ×
     *         {@link #getAdminApiTimeoutMs()}, a backstop for a branch that never returns
     */
    public long getCollectTimeoutMs() {
        return CYCLE_API_CALLS * adminApiTimeoutMs;
    }

    @Override
    public String toString() {
        return "SovaConfig{" +
            "\n\tgroupRegex=" + groupRegex +
            "\n\tgroupExcludeRegex=" + groupExcludeRegex +
            "\n\ttopicRegex=" + topicRegex +
            "\n\ttopicExcludeRegex=" + topicExcludeRegex +
            "\n\tkafkaAdminClientConfigurationKeys=" + new TreeSet<>(kafkaAdminClientConfiguration.stringPropertyNames()) +
            "\n\tmetricsPort=" + metricsPort +
            "\n\tmanagementPort=" + managementPort +
            "\n\tcollectIntervalMs=" + collectIntervalMs +
            "\n\tadminApiTimeoutMs=" + getAdminApiTimeoutMs() +
            "\n\tcollectTimeoutMs=" + getCollectTimeoutMs() +
            '}';
    }
}
