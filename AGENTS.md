# AGENTS.md — AI assistant guidance for Sova

## Project overview

Sova is a **Kafka consumer group metrics exporter for Prometheus**, written in pure Java 21.
It is a **standalone project** — it produces only a ZIP artifact.
It does not build container images.

**Hard constraints (never violate):**
- Metric names are a fixed contract shared with Prometheus dashboards and alerts. **Never rename a metric.**
- Kafka client settings, including security (TLS, SASL), are configured only through `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION`.
- Values of `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION` can be credentials: never log them or put them in an exception message — keys only.
- The endpoints are served via JDK built-in HTTP servers on two ports (metrics port 9404, management port 8080).

---

## Architecture

**`Main`** — Application entry point.
Wires together `SovaConfig`, `Admin`, `MetricsRegistry`, `MetricsCollector`, schedules background metrics collection, and starts the two listeners via `HttpServers` (metrics port on 9404, management port on 8080).
Registers a shutdown hook that stops the listeners, the scheduler, and the `Admin` client.

**`config/SovaConfig`** — Class holding all configuration options read from environment variables.
Validates and compiles all regexes, numeric values, and the Kafka client properties at construction time so a malformed or missing environment variable (including a missing `bootstrap.servers`) fails at startup with a message naming the variable; an invalid Admin client property fails with Kafka's own `ConfigException`, naming the key.
The public `SovaConfig(UnaryOperator<String> env)` constructor takes the environment lookup so tests can inject an environment; the no-argument constructor passes `System::getenv`.
Effectively immutable after creation; safe to read from any thread.

**`kafka/KafkaAdminClient`** — Factory that creates and configures the `Admin` client.
Passes the Kafka client properties from `SovaConfig.getKafkaAdminClientConfiguration()` (`bootstrap.servers`, TLS, SASL and any other client setting) as they are and adds nothing: without a user `default.api.timeout.ms`, Kafka's own default (60 s) applies.
`SovaConfig.getAdminApiTimeoutMs()` mirrors the client's effective per-call timeout, read through Kafka's own `AdminClientConfig` (which supplies the defaults and validates, throwing `ConfigException`): the user's `default.api.timeout.ms`, otherwise the larger of its default and `request.timeout.ms`, as `KafkaAdminClient.configureDefaultApiTimeoutMs` does. It is used only to compute the cycle deadline.
There is no Sova-side security logic: whoever sets the variable decides the mechanism.

**`metrics/MetricsRegistry`** — Holds the most recently collected `MetricSnapshot` in a `volatile` field and registers a single `MultiCollector`, plus the JVM/process instrumentation, with a `PrometheusRegistry` it owns (deliberately not `defaultRegistry` — see invariant 8).
`sova_collection_failures_total` and `sova_collection_duration_seconds` are process-level series registered directly on the registry through `recordCycle(Duration, boolean)`, not part of `MetricSnapshot`; invariant 6 applies only to the Kafka series.
`update(MetricSnapshot)` is one reference swap; every scrape reads that reference once and builds all metric families from it via `MetricsSnapshotRenderer`.

**`metrics/MetricSnapshot`** — The record carrying one collection cycle, plus the `PartitionMetrics` value and the three composite-key records used as map keys.
Partition-keyed maps use the Kafka client's `TopicPartition`, so the lag calculation and the Prometheus labels read the same map.

**`metrics/MetricsSnapshotRenderer`** — Stateless conversion from a `MetricSnapshot` to Prometheus' `MetricSnapshots`.
Owns the exact metric names, help strings, and label mappings.
Separate from `MetricsRegistry` because the Prometheus snapshot model types would push that class past Checkstyle's `ClassFanOutComplexity` limit.

**`metrics/BuildInfo`** — Record holding the `sova_build_info` labels (version, revision, branch, Java version).
`load()` reads the `git.properties` file that `git-commit-id-maven-plugin` writes into `target/classes` (restricted to three keys in `pom.xml`) plus the running JVM; any field that is missing or blank becomes `unknown`.
Without a `.git` directory at build time the plugin omits the revision and branch keys, so those two are `unknown` while the version is still known.
There is no resource filtering.
`MetricsRegistry` has a public constructor taking a `BuildInfo` so `Main` and tests can inject one.

**`metrics/collectors/MetricsCollector`** — Class scheduled periodically on a virtual thread.
Orchestrates one collection cycle by running `BrokerCollector`, `TopicCollector`, and `ConsumerGroupCollector` concurrently on virtual threads, then computes lag from their results, assembles a `MetricSnapshot` and calls `registry.update(snapshot)`.
`collect()` never throws — a scheduled task that throws is cancelled silently and never runs again, which would freeze the pod without ever failing liveness.
Every wait on a branch is bounded by one deadline for the whole cycle, `SovaConfig.getCollectTimeoutMs()` (5 × `getAdminApiTimeoutMs()`; the reasoning is on `SovaConfig.CYCLE_API_CALLS`); missing it throws `java.util.concurrent.TimeoutException`, the branches are interrupted, and the cycle fails.
Failures are swallowed by level: `InterruptedException` restores the interrupt flag and logs DEBUG (shutdown, not a failure), the deadline's `TimeoutException` logs WARN, any other `Exception` logs WARN, `Throwable` logs ERROR.
None of them update the registry's snapshot, so stale metrics continue to be served.
Every finished cycle calls `registry.recordCycle(elapsed, succeeded)`; the `TimeoutException`, `Exception` and `Throwable` paths count as failures, the `InterruptedException` path records nothing.
Staleness is exposed as the `sova_last_successful_collection_timestamp_seconds` metric, not through a probe.

**`metrics/collectors/BrokerCollector`** — Stateless collector.
Calls `describeCluster()` and returns broker count and address map as a `BrokerData` record.

**`metrics/collectors/TopicCollector`** — Stateless collector.
Calls `listTopics()` + `describeTopics()` for topic/partition topology, then `listOffsets()` (latest+earliest) for offsets.
Uses per-element futures so a single deleted topic does not abort the whole cycle.
Returns a `TopicData` record.

**`metrics/collectors/ConsumerGroupCollector`** — Stateless collector.
`collect()` calls `listGroups()` to list and filter consumer groups, `describeConsumerGroups()` for membership/state, and `listConsumerGroupOffsets()` for committed offsets; it needs no topic data, so it runs concurrently with `TopicCollector`.
`computeLag()` is a separate pure step, run by `MetricsCollector` once both branches are in: it computes offsets and lag against the `TopicData`.
Uses per-element futures so a single deleted group does not abort the whole cycle.
`listGroups()` follows the same rule through `requireUsableListing()`: its `valid()` future never fails, so an empty listing with an error from every broker (counted with `describeCluster()`, only on that path) throws `CollectionFailedException` rather than publishing a cycle with no group series.
Returns a `ConsumerGroupData` record.

**`metrics/collectors/CollectorUtils`** — Stateless helpers shared by all three collectors: `matches(value, include, exclude)` applies the include/exclude pattern pair, and `resolveAll(futures, kind)` resolves a map of per-element `KafkaFuture` values, dropping failures individually instead of failing the whole batch.
If every element of a non-empty batch fails, `resolveAll` throws `CollectionFailedException` instead, so a total failure (for example a revoked ACL) is not published as an empty successful cycle.

**`http/HttpServers`** — Owns both JDK `HttpServer` listeners: applies the JDK resource limits the platform leaves unset, registers the contexts, starts them on virtual-thread executors, and stops them on close.
`/metrics` is served by Prometheus' own `MetricsHandler`; `/healthy` and `/ready` by Sova's handlers.

**`http/LivenessCheck`** — JDK `HttpHandler` that exposes `GET /healthy`.
Always 200 — a failed collection must not restart the pod, since stale metrics keep being served.

**`http/ReadinessCheck`** — JDK `HttpHandler` that exposes `GET /ready`.
Always 200, like `/healthy`: it reports that the HTTP server answers, not how fresh the snapshot is.

---

## Build & test commands

```bash
# Full build + unit tests (no Docker required)
mvn verify

# Including broker-backed integration tests (requires Docker)
mvn verify -Pfailsafe

# Checkstyle only
mvn checkstyle:check@validate

# SpotBugs only (needs compiled classes first)
make spotbugs

# Make target (runs mvn install)
make java_install

# Everything CI gates on: install + SpotBugs
make all

# Run a single test class
mvn test -Dtest=MetricsRegistryTest
```

---

## Key invariants

1. **Metric names never change.**
   Every metric name in `MetricsSnapshotRenderer` must exactly match the list in `README.md`.
   Rename = breaking change for all existing dashboards.
2. **Uncommitted partitions are reported as -1.**
   A partition of a consumed topic that the group has not committed to is reported as `-1` in both `kafka_consumergroup_current_offset` and `kafka_consumergroup_lag`.
   It is excluded from both `kafka_consumergroup_lag_sum` and `kafka_consumergroup_current_offset_sum`: summing the -1 in only drags a sum of offsets down by one per uncommitted partition and helps no query.
3. **The -1 is synthesised locally, never received.**
   A `ListConsumerGroupOffsetsSpec` with no partition list returns only the partitions the group has actually committed to.
   Uncommitted partitions are absent from the response entirely.
   The -1 is reconstructed from the topic's partition count already collected by `TopicCollector`.
   Never switch to naming every partition explicitly in the OffsetFetch request just to get the -1 back from the broker — that multiplies the request size by the partition count for every group.
4. **Lag is never clamped at zero.**
   Negative lag is a real signal (committed offset ahead of HWM).
   Hiding it helps nobody.
5. **A topic the group has never committed to produces no series at all.**
   A topic only appears in the committed-offset response if the group has at least one real commit on it.
   This needs no explicit check.
6. **A scrape serves exactly one collection cycle.**
   `MetricsRegistry` holds the last `MetricSnapshot` in a `volatile` field and builds every family from a single read of it behind one `MultiCollector`, so a scrape can never mix two cycles and a series that leaves the cluster leaves the next scrape with it.
   Never go back to mutating per-metric `Gauge` objects: `Gauge.labelValues(...)` caches a data point per label combination and never removes it, so deleted topics and dead consumer groups would be exported for the lifetime of the pod.
   Note that `PrometheusRegistry.scrape()` takes no lock on `MetricsRegistry` — atomicity comes from the reference swap, not from a lock, so do not add one back expecting it to protect readers.
7. **No separate HTTP server daemon.**
   `prometheus-metrics-exporter-httpserver` is on the classpath so we can mount its `MetricsHandler` — a plain `com.sun.net.httpserver.HttpHandler` — on our own JDK listener.
   Never call `HTTPServer.builder()` or `new HTTPServer(...)`: that class force-registers `/` and `/metrics` on every listener it creates, cannot be extended (its only constructor is private), and would put an unprotected `/metrics` on the management port.
8. **The scrape registry is owned, not global.**
   `MetricsRegistry` constructs its own `PrometheusRegistry` and `HttpServers.start` is handed that instance.
   Never go back to `PrometheusRegistry.defaultRegistry`: a JVM-global registry makes construction order decide what survives, and the only way to make repeated construction safe is `clear()`, which silently drops whatever else registered — the JVM instrumentation included.
9. **Readiness is the HTTP server, nothing else.**
   `/ready` is 200 whenever the management listener answers.
   Staleness is a metrics concern: read `sova_last_successful_collection_timestamp_seconds` and `sova_collection_failures_total`.
10. **Every wait in a cycle is bounded.**
    `MetricsCollector` never calls `Future.get()` without a timeout: each wait uses the time left until the cycle deadline, `SovaConfig.getCollectTimeoutMs()`.
    A branch stuck in the AdminClient would otherwise hold the single scheduler thread forever, and since `/healthy` and `/ready` are always 200, nothing would restart the pod.
    Branches must block only in interruptible waits (today every branch wait is `KafkaFuture.get()`), because after the deadline `close()` waits for the interrupted branches to unwind.

---

## What to never do

- **Never rename a metric** — breaking change.
- **Never start a separate Prometheus HTTP server daemon (`HTTPServer`)** — mount `MetricsHandler` on our own JDK `HttpServer` instead.
- **Never name every partition explicitly in a `ListConsumerGroupOffsetsSpec`** — use the default (no partition list) and synthesise -1 locally from the partition count.

---

## Environment variables

Sova is configured only through environment variables.

| Variable                                   | Purpose                                                                                                                         |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `STRIMZI_GROUP_REGEX`                      | Consumer group include pattern (default `.*`)                                                                                   |
| `STRIMZI_GROUP_EXCLUDE_REGEX`              | Consumer group exclude pattern (optional)                                                                                       |
| `STRIMZI_TOPIC_REGEX`                      | Topic include pattern (default `.*`)                                                                                            |
| `STRIMZI_TOPIC_EXCLUDE_REGEX`              | Topic exclude pattern (optional)                                                                                                |
| `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION` | Kafka client properties, one `key=value` per line, holding `bootstrap.servers`, TLS, SASL and other client config; required, must contain `bootstrap.servers` |
| `STRIMZI_METRICS_PORT`                     | Metrics port (default 9404)                                                                                                     |
| `STRIMZI_MANAGEMENT_PORT`                  | Management port (default 8080)                                                                                                  |
| `STRIMZI_COLLECT_INTERVAL_MS`              | Collection interval in milliseconds (default 30000)                                                                             |

`STRIMZI_LOG_LEVEL` and `STRIMZI_AC_LOG_LEVEL` are read by `log4j2.properties`.

---

## Code style

- Checkstyle is enforced by `mvn checkstyle:check@validate` and runs on every `mvn verify`.
- SpotBugs (`effort=Max`, `threshold=Low`) is enforced by `make spotbugs`, which CI runs via the shared `build-binaries` action.
  It is deliberately not bound to a Maven phase, so `mvn verify` does **not** run it — use `make all`.
  Exclusions go in `.spotbugs/spotbugs-exclude.xml` with a comment saying why; prefer fixing the code or a targeted `@SuppressFBWarnings`.
  Keep SpotBugs at 4.10.5 or newer — earlier releases cannot read Java 25 class files and fail with `Unsupported class file major version 69`.
- Every `.java` file **must** start with the Apache 2.0 copyright header (see `.checkstyle/checkstyle.xml`).
- No `assert` statements (`IllegalToken` rule).
- No star imports (`AvoidStarImport` rule).
- No `toLowerCase()`/`toUpperCase()` without `Locale` (use `Locale.ENGLISH`).
- DCO sign-off is required on every commit: `git commit -s`.

---

## Adding a new metric (step by step)

1. Add the field to the `MetricSnapshot` record.
   If it is another per-partition property, add it to `PartitionMetrics` instead of adding a map — those five values travel together.
2. Populate it in the relevant collector's result record (`BrokerCollector.BrokerData`, `TopicCollector.TopicData`, or `ConsumerGroupCollector.ConsumerGroupData`), then thread it through `MetricsCollector.collectInternal()`, which builds the `MetricSnapshot` inline from the three collectors' results and passes it to `MetricsRegistry.update(...)`.
3. Add one `.metricSnapshot(...)` line to `MetricsSnapshotRenderer.from()`, with the exact metric name, help string, and label names documented in `README.md`.
   Reuse `scalar` or `family`; add a dedicated method only if the labels do not come from the map key alone.
4. Add an assertion in `MetricsRegistryTest.allMetricsRegistered()` that the metric name appears in the scrape output, and populate its map in the test's snapshot — a family with no data points is not emitted at all.
5. Add an assertion in `MetricsCollectorIT` that the value is populated correctly.
