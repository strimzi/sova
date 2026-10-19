# AGENTS.md — AI assistant guidance for Sova

## Project overview

Sova is a **Kafka consumer group metrics exporter for Prometheus**, written in pure Java 21.
It is a **standalone library project** — it produces only a ZIP artifact.
Docker images are built and pushed in the `strimzi-kafka-operator` repository.

**Hard constraints (never violate):**
- Metric names are a fixed contract shared with Prometheus dashboards and alerts. **Never rename a metric.**
- GSSAPI (Kerberos) and non-OAUTHBEARER SASL mechanisms are intentionally unsupported.
- ZooKeeper support is intentionally absent; Kafka 4.x is KRaft-only.
- The endpoints are served via JDK built-in HTTP servers on two ports (metrics port 9404, management port 8080).

---

## Architecture — one paragraph per class

**`Main`** — Application entry point.
Wires together `SovaConfig`, `Admin`, `MetricsRegistry`, `MetricsCollector`, schedules background metrics collection, and starts the two listeners via `HttpServers` (metrics port on 9404, management port on 8080).
Registers a shutdown hook that stops the listeners, the scheduler, and the `Admin` client.

**`config/SovaConfig`** — Class holding all configuration options read from environment variables.
Validates and compiles all regexes and numeric values at construction time so a malformed environment variable fails at startup with a message naming the variable.
The public `SovaConfig(UnaryOperator<String> env)` constructor takes the environment lookup so tests can inject an environment; the no-argument constructor passes `System::getenv`.
Effectively immutable after creation; safe to read from any thread.

**`kafka/KafkaAdminClient`** — Factory that creates and configures the `Admin` client.
Delegates TLS setup to `TlsConfigBuilder` and SASL setup to `SaslConfigBuilder`.

**`metrics/MetricsRegistry`** — Holds the most recently collected `MetricSnapshot` in a `volatile` field and registers a single `MultiCollector`, plus the JVM/process instrumentation, with a `PrometheusRegistry` it owns (deliberately not `defaultRegistry` — see invariant 8).
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
Orchestrates one collection cycle by delegating to `BrokerCollector`, `TopicCollector`, and `ConsumerGroupCollector`, then assembles their results into a `MetricSnapshot` and calls `registry.update(snapshot)`.
`collect()` never throws — a scheduled task that throws is cancelled silently and never runs again, which would freeze the pod without ever failing liveness.
Failures are swallowed by level: `InterruptedException` restores the interrupt flag and logs DEBUG (shutdown, not a failure), `Exception` logs WARN, `Throwable` logs ERROR.
None of them update the registry, so stale metrics continue to be served.
Readiness is derived solely from the age of the snapshot in `MetricsRegistry`: DOWN until the first cycle succeeds, and DOWN again once the last good snapshot is older than two collection intervals.

**`metrics/collectors/BrokerCollector`** — Stateless collector.
Calls `describeCluster()` and returns broker count and address map as a `BrokerData` record.

**`metrics/collectors/TopicCollector`** — Stateless collector.
Calls `listTopics()` + `describeTopics()` for topic/partition topology, then `listOffsets()` (latest+earliest) for offsets.
Uses per-element futures so a single deleted topic does not abort the whole cycle.
Returns a `TopicData` record.

**`metrics/collectors/ConsumerGroupCollector`** — Stateless collector.
Calls `listGroups()` to list and filter consumer groups, `describeConsumerGroups()` for membership/state, and `listConsumerGroupOffsets()` for committed offsets, computing lag against the `TopicData` passed in.
Uses per-element futures so a single deleted group does not abort the whole cycle.
Returns a `ConsumerGroupData` record.

**`metrics/collectors/CollectorUtils`** — Stateless helpers shared by all three collectors: `matches(value, include, exclude)` applies the include/exclude pattern pair, and `resolveAll(futures, kind)` resolves a map of per-element `KafkaFuture` values, dropping failures individually instead of failing the whole batch.

**`http/HttpServers`** — Owns both JDK `HttpServer` listeners: applies the JDK resource limits the platform leaves unset, registers the contexts, starts them on virtual-thread executors, and stops them on close.
`/metrics` is served by Prometheus' own `MetricsHandler`; `/healthy` and `/ready` by Sova's handlers.

**`http/LivenessCheck`** — JDK `HttpHandler` that exposes `GET /healthy`.
Always 200 — a failed collection must not restart the pod, since stale metrics keep being served.

**`http/ReadinessCheck`** — JDK `HttpHandler` that exposes `GET /ready`.
Reports UP (200) or DOWN (503) from `MetricsCollector.isHealthy()`, which is false until the first cycle succeeds and false again once the last good snapshot is older than two collection intervals.

**`kafka/auth/TlsConfigBuilder`** — Stateless utility class.
Always reads the CA cert and sets the truststore properties, the `HTTPS` endpoint identification algorithm (hostname verification on), and `security.protocol=SSL`.
The client cert and key are optional: when both are present it also sets the keystore type, certificate chain and key (mTLS); when both are absent the client only verifies the brokers; when exactly one is present it throws `IllegalStateException` naming `SOVA_TLS_CERT_FILE` and `SOVA_TLS_KEY_FILE`.
When SASL is also configured, `SaslConfigBuilder` runs afterwards and overwrites that protocol.

**`kafka/auth/SaslConfigBuilder`** — Stateless utility class.
Uppercases `SASL_MECHANISM` (`Locale.ENGLISH`) and validates it is `OAUTHBEARER`, throwing otherwise; passes `SASL_JAAS_CONFIG` and `SASL_LOGIN_CALLBACK_HANDLER_CLASS` through to the Kafka client `Properties` verbatim, skipping the blank ones; then sets `security.protocol` to `SASL_SSL` or `SASL_PLAINTEXT` depending on whether TLS is enabled.
No token-file handling — that is delegated to whichever callback handler class the operator configures.

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
9. **Readiness is staleness, nothing else.**
   `isHealthy()` is true while the snapshot in `MetricsRegistry` is younger than two collection intervals.
   Only a successful cycle replaces that snapshot, so failed cycles and cycles that hang (which throw nothing) both surface the same way.
   Do not add a separate "last cycle ok" flag: it cannot see a hung cycle, and it duplicates what the snapshot age already says.

---

## What to never do

- **Never add ZooKeeper** — Kafka 4.x is KRaft-only.
- **Never rename a metric** — breaking change.
- **Never start a separate Prometheus HTTP server daemon (`HTTPServer`)** — mount `MetricsHandler` on our own JDK `HttpServer` instead.
- **Never name every partition explicitly in a `ListConsumerGroupOffsetsSpec`** — use the default (no partition list) and synthesise -1 locally from the partition count.

---

## Operator compatibility — env vars and mount paths

The Strimzi operator injects these environment variables (names must never change):

| Variable                                 | Purpose                                                                 |
|------------------------------------------|-------------------------------------------------------------------------|
| `SOVA_KAFKA_BOOTSTRAP_SERVERS`           | Bootstrap servers                                                       |
| `SOVA_GROUP_REGEX`                       | Consumer group include pattern (always set)                             |
| `SOVA_GROUP_EXCLUDE_REGEX`               | Consumer group exclude pattern (only set when configured)               |
| `SOVA_TOPIC_REGEX`                       | Topic include pattern (always set)                                      |
| `SOVA_TOPIC_EXCLUDE_REGEX`               | Topic exclude pattern (only set when configured)                        |
| `SOVA_TLS_ENABLED`                       | TLS on/off                                                              |
| `SOVA_TLS_CA_CERT_FILE`                  | CA cert path                                                            |
| `SOVA_TLS_CERT_FILE`                     | Client cert path (optional, only set for mTLS; no Sova default)         |
| `SOVA_TLS_KEY_FILE`                      | Client key path (optional, only set for mTLS; no Sova default)          |
| `SOVA_METRICS_PORT`                      | Metrics port (default 9404 — matches `MetricsModel.METRICS_PORT`)       |
| `SOVA_MANAGEMENT_PORT`                   | Management port (default 8080)                                          |
| `SOVA_SASL_MECHANISM`                    | SASL mechanism (`oauthbearer`)                                          |
| `SOVA_SASL_JAAS_CONFIG`                  | Full `sasl.jaas.config` string passed verbatim to the Kafka client      |
| `SOVA_SASL_LOGIN_CALLBACK_HANDLER_CLASS` | `sasl.login.callback.handler.class` passed verbatim to the Kafka client |

The full list the operator sets lives in `cluster-operator/src/main/java/io/strimzi/operator/cluster/model/Sova.java`.
`SOVA_COLLECT_INTERVAL_MS` is read by `SovaConfig` but is **not** injected by the operator.
In ServiceAccount (OAUTHBEARER) mode the operator sets only the CA variable; Sova must not require a client cert or key then.

Default CA mount path (operator-managed):
- `/etc/sova/cluster-ca-certs/cluster-ca.crt`

The operator mounts the client cert and key at `/etc/sova/sova-certs/sova.crt` and `/etc/sova/sova-certs/sova.key` in mTLS mode and passes them explicitly; Sova has no default for them.

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
- `kafka-clients` version must be kept in sync with `strimzi-kafka-operator/pom.xml`.

---

## Adding a new metric (step by step)

1. Add the field to the `MetricSnapshot` record.
   If it is another per-partition property, add it to `PartitionMetrics` instead of adding a map — those five values travel together.
2. Populate it in the relevant collector's result record (`BrokerCollector.BrokerData`, `TopicCollector.TopicData`, or `ConsumerGroupCollector.ConsumerGroupData`), then thread it through `MetricsCollector.collectInternal()`, which builds the `MetricSnapshot` inline from the three collectors' results and passes it to `MetricsRegistry.update(...)`.
3. Add one `.metricSnapshot(...)` line to `MetricsSnapshotRenderer.from()`, with the exact metric name, help string, and label names documented in `README.md`.
   Reuse `scalar` or `family`; add a dedicated method only if the labels do not come from the map key alone.
4. Add an assertion in `MetricsRegistryTest.allMetricsRegistered()` that the metric name appears in the scrape output, and populate its map in the test's snapshot — a family with no data points is not emitted at all.
5. Add an assertion in `MetricsCollectorIT` that the value is populated correctly.
