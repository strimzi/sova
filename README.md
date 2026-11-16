# Sova

[![Build Status](https://github.com/strimzi/sova/actions/workflows/build.yml/badge.svg)](https://github.com/strimzi/sova/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio?style=social)](https://twitter.com/strimziio)

Sova is a Kafka consumer group metrics exporter for Prometheus, written in pure Java.

---

## Running

This repository builds a ZIP distribution, not a container image. 
Images are built in the `strimzi-kafka-operator` repository. 
Unpack the ZIP and run the `Main` class on the classpath, with configuration supplied as environment variables:

```bash
unzip sova-*.zip && cd sova-*/
export STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION='bootstrap.servers=kafka-bootstrap:9093
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/path/to/ca.crt'
java -classpath "libs/*" io.strimzi.sova.Main
```

Metrics are available at `http://localhost:9404/metrics`.
The liveness probe is at `http://localhost:8080/healthy` and the readiness probe at `http://localhost:8080/ready`.

### Authentication

Sova connects to Kafka in plaintext unless told otherwise.
The bootstrap servers, TLS, mutual TLS, SASL and any other Kafka client setting are configured through `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION`.
Its value is Kafka client properties, one `key=value` per line.
`bootstrap.servers` is required; Sova fails at startup without it.
Every entry is passed to the Kafka `Admin` client as it is, for example:

```properties
bootstrap.servers=my-cluster-kafka-bootstrap:9093
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.location=/path/to/ca.crt
```

A value that is not valid properties syntax fails Sova at startup.

Because the properties are passed to the Kafka client unchanged, Kafka config providers (`config.providers=...`) work as usual.
Use them to read credentials from files, environment variables, or Kubernetes Secrets, for example with the [Strimzi Kubernetes config provider](https://github.com/strimzi/kafka-kubernetes-config-provider).
The provider's JAR must be on the classpath.

> **Note:** 
> At startup, Sova logs only the keys of this variable, never the values to keep credentials out of the log.

The `/metrics` endpoint is served over plain HTTP.

---

## Metrics

| Metric name                                         | Labels                                       | Description                                                                                                                                                                             |
|-----------------------------------------------------|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka_brokers`                                     | —                                            | Total number of brokers in the cluster                                                                                                                                                  |
| `kafka_broker_info`                                 | `id`, `address`                              | Broker metadata (always 1)                                                                                                                                                              |
| `kafka_topic_partitions`                            | `topic`                                      | Number of partitions for the topic                                                                                                                                                      |
| `kafka_topic_partition_current_offset`              | `topic`, `partition`                         | Latest (high-water-mark) offset                                                                                                                                                         |
| `kafka_topic_partition_oldest_offset`               | `topic`, `partition`                         | Earliest available offset                                                                                                                                                               |
| `kafka_topic_partition_leader`                      | `topic`, `partition`                         | Leader broker ID                                                                                                                                                                        |
| `kafka_topic_partition_replicas`                    | `topic`, `partition`                         | Total replica count                                                                                                                                                                     |
| `kafka_topic_partition_in_sync_replica`             | `topic`, `partition`                         | In-sync replica count                                                                                                                                                                   |
| `kafka_topic_partition_leader_is_preferred`         | `topic`, `partition`                         | 1 if preferred leader is elected                                                                                                                                                        |
| `kafka_topic_partition_under_replicated_partition`  | `topic`, `partition`                         | 1 if under-replicated                                                                                                                                                                   |
| `kafka_consumergroup_members`                       | `consumergroup`                              | Active member count                                                                                                                                                                     |
| `kafka_consumergroup_current_offset`                | `consumergroup`, `topic`, `partition`        | Committed offset                                                                                                                                                                        |
| `kafka_consumergroup_current_offset_sum`            | `consumergroup`, `topic`                     | Sum of committed offsets                                                                                                                                                                |
| `kafka_consumergroup_lag`                           | `consumergroup`, `topic`, `partition`        | Lag per partition                                                                                                                                                                       |
| `kafka_consumergroup_lag_sum`                       | `consumergroup`, `topic`                     | Sum of lags across partitions                                                                                                                                                           |
| `kafka_consumergroup_state`                         | `consumergroup`, `state`                     | Presence indicator: emits `1` for the group's current state (the `state` label), including `Empty` and `Dead`. Query a specific state, e.g. `kafka_consumergroup_state{state="Stable"}` |
| `sova_build_info`                                   | `version`, `revision`, `branch`, `java_info` | Build information                                                                                                                                                                       |
| `sova_last_successful_collection_timestamp_seconds` | —                                            | Unix timestamp of the last successful collection cycle, 0 if none has completed                                                                                                         |
| `sova_collection_failures_total` | — | Collection cycles that ended without publishing a snapshot |
| `sova_collection_duration_seconds` | — | Duration of the last finished collection cycle, successful or failed |

The `process_*` and `jvm_*` families are exported too.

### Notes for dashboard authors

- **`-1` means "no commit", and it is a real sample.** If a consumer group has committed to a topic but not to every one of its partitions, the uncommitted partitions report `-1` in both `kafka_consumergroup_current_offset` and `kafka_consumergroup_lag`. Any `avg()`, `min()`, or `topk()` over raw `kafka_consumergroup_lag` must filter `> -1` first, or the uncommitted partitions will skew the result. `lag > N` threshold alerts are unaffected, since `-1` never trips a positive threshold.
- **Lag is never clamped and can be negative.** A committed offset ahead of the high-water mark is a real anomaly worth seeing, not a glitch, so it is reported as-is. Because the broker, topic, and consumer-group data is collected concurrently within a cycle, a busy partition can also show a small transient negative lag that means nothing — do not alert on `lag < 0` without a `for:` duration.
- **The `_sum` metrics are not the sum of the visible per-partition series.** Uncommitted partitions (the `-1` ones above) are excluded from both `kafka_consumergroup_lag_sum` and `kafka_consumergroup_current_offset_sum`, since folding a `-1` into a sum only drags it down for no useful meaning. A panel showing per-partition series next to the `_sum` will not reconcile — this is deliberate, not a bug.
- **`kafka_consumergroup_lag_sum` can be absent rather than `0`** if offset fetching fails for a whole topic during a cycle. Prefer `absent_over_time(...[5m])` over a bare `absent()` so a single bad collection cycle does not page on its own.
- **Internal topics are not reported.** `__consumer_offsets` and `__transaction_state` have no `kafka_topic_*` series, unlike in Kafka Exporter.
- **Only consumer groups are reported**, both classic and KIP-848 (`group.protocol=consumer`) ones. Kafka Connect worker groups, share groups and KIP-1071 streams-protocol groups have no `kafka_consumergroup_*` series. Sink connectors and Kafka Streams applications on the classic protocol use ordinary consumer groups, so their lag is reported.
- **`sova_last_successful_collection_timestamp_seconds` is the staleness signal**, reading `0` before the first successful cycle. `time() - sova_last_successful_collection_timestamp_seconds` is the "is this exporter actually working" panel.
- **`sova_collection_failures_total` counts cycles that ended without publishing a snapshot.** Alert on `increase(sova_collection_failures_total[5m]) > 0`. Cycles interrupted by shutdown are not counted. `sova_collection_duration_seconds` is the duration of the last finished cycle, successful or failed.

---

## Configuration

| Environment variable                       | Default      | Description                                                                                                               |
|--------------------------------------------|--------------|---------------------------------------------------------------------------------------------------------------------------|
| `STRIMZI_GROUP_REGEX`                      | `.*`         | Consumer group include regex                                                                                              |
| `STRIMZI_GROUP_EXCLUDE_REGEX`              | *(none)*     | Consumer group exclude regex                                                                                              |
| `STRIMZI_TOPIC_REGEX`                      | `.*`         | Topic include regex                                                                                                       |
| `STRIMZI_TOPIC_EXCLUDE_REGEX`              | *(none)*     | Topic exclude regex                                                                                                       |
| `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION` | *(required)* | Kafka client properties, one `key=value` per line; must contain `bootstrap.servers` (plus TLS, SASL, other client config) |
| `STRIMZI_METRICS_PORT`                     | `9404`       | Port for `/metrics` endpoint                                                                                              |
| `STRIMZI_MANAGEMENT_PORT`                  | `8080`       | Port for `/healthy` and `/ready` endpoints                                                                                |
| `STRIMZI_COLLECT_INTERVAL_MS`              | `30000`      | Metrics collection interval in milliseconds                                                                               |
| `STRIMZI_LOG_LEVEL`                        | `INFO`       | Root log level                                                                                                            |
| `STRIMZI_AC_LOG_LEVEL`                     | `WARN`       | Log level for the `org.apache.kafka` logger                                                                               |

### Timeouts

Each Admin call is bounded by `default.api.timeout.ms`. Sova does not set it, so the Kafka Admin client default of 60 s applies, raised to `request.timeout.ms` when that is larger.
To change it, put `default.api.timeout.ms=<ms>` in `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION`; the value is validated by the Kafka client, and an invalid one fails Sova at startup.
If you set `default.api.timeout.ms` explicitly, keep it ≥ `request.timeout.ms`, otherwise the Admin client refuses to start.

A whole collection cycle is bounded by a deadline of 5 × that effective per-call timeout (300 s by default).
The deadline is a backstop for a cycle that never returns, not a way to detect a down cluster.
It is sized so that a slow but healthy cycle is never cut off: the longest branch makes 4 sequential Admin calls, and one more call is added as margin.
A cycle that misses the deadline is abandoned, logs a WARN, and counts in `sova_collection_failures_total`; the previous snapshot keeps being served.
A dead cluster is still counted as a failed cycle after about one API timeout, not the full deadline, because the first call in each branch fails on its own.

### Health endpoints

`/healthy` is always 200 while the JVM can answer: a broken collection must not restart a pod that is still serving its last good snapshot.

`/ready` is also 200 while the JVM can answer.
Staleness is exposed as metrics instead: alert on `time() - sova_last_successful_collection_timestamp_seconds`.
Before the first cycle completes, `/metrics` serves an empty snapshot and that timestamp reads `0`.

---

## Development guide

See [development-docs/DEV_GUIDE.md](development-docs/DEV_GUIDE.md).

## Artifact attestations

Sova release archives are published with [GitHub artifact attestations](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations/using-artifact-attestations-to-establish-provenance-for-builds) containing [SLSA build provenance](https://slsa.dev/provenance/v1).
The attestations are signed keylessly with [Sigstore](https://www.sigstore.dev/) using the GitHub Actions OIDC identity, so no public key is needed to verify them.
They are created by the shared [Strimzi GitHub Actions](https://github.com/strimzi/github-actions) workflows, so the `--signer-repo strimzi/github-actions` option is required when verifying them.
You can verify them with the [GitHub CLI](https://cli.github.com/).

To verify a release archive downloaded from the GitHub release page:

```
gh attestation verify <release-archive> --repo strimzi/sova --signer-repo strimzi/github-actions
```

Each GitHub release also contains the attestation bundle `release-<version>.intoto.jsonl`.
You can use it to verify the archive without querying the GitHub Attestation API:

```
gh attestation verify <release-archive> --bundle release-<version>.intoto.jsonl --repo strimzi/sova --signer-repo strimzi/github-actions
```
