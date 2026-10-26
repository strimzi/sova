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
export SOVA_KAFKA_BOOTSTRAP_SERVERS=kafka-bootstrap:9093
export STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION='security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.certificates=${strimzisecrets:my-ns/my-cluster-cluster-ca-cert:ca.crt}'
java -classpath "libs/*" io.strimzi.sova.Main
```

Metrics are available at `http://localhost:9404/metrics`.
The liveness probe is at `http://localhost:8080/healthy` and the readiness probe at `http://localhost:8080/ready`.

### Authentication

Sova connects to Kafka in plaintext unless told otherwise.
TLS, mutual TLS, SASL and any other Kafka client setting are configured through `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION`.
Its value is Kafka client properties, one `key=value` per line, in the same format as the User Operator variable of the same name.
Every entry is passed to the Kafka `Admin` client as it is, for example:

```properties
security.protocol=SSL
ssl.truststore.type=PEM
ssl.truststore.certificates=${strimzisecrets:my-ns/my-cluster-cluster-ca-cert:ca.crt}
```

`SOVA_KAFKA_BOOTSTRAP_SERVERS` always sets `bootstrap.servers` and takes precedence over the same key in this variable, as does the Admin client API timeout.
A value that is not valid properties syntax fails Sova at startup.

> **Note:** Environment variables are visible in the Pod spec.
> Do not put credentials in this variable.
> Reference secrets through a Kafka configuration provider, as in the example above.
> At startup, Sova logs only the keys of this variable, never the values.

The `/metrics` endpoint is served over plain HTTP.

---

## Metrics

| Metric name                                         | Labels                                         | Description                                                                                                                                                                             |
|-----------------------------------------------------|------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafka_brokers`                                     | —                                              | Total number of brokers in the cluster                                                                                                                                                  |
| `kafka_broker_info`                                 | `id`, `address`                                | Broker metadata (always 1)                                                                                                                                                              |
| `kafka_topic_partitions`                            | `topic`                                        | Number of partitions for the topic                                                                                                                                                      |
| `kafka_topic_partition_current_offset`              | `topic`, `partition`                           | Latest (high-water-mark) offset                                                                                                                                                         |
| `kafka_topic_partition_oldest_offset`               | `topic`, `partition`                           | Earliest available offset                                                                                                                                                               |
| `kafka_topic_partition_leader`                      | `topic`, `partition`                           | Leader broker ID                                                                                                                                                                        |
| `kafka_topic_partition_replicas`                    | `topic`, `partition`                           | Total replica count                                                                                                                                                                     |
| `kafka_topic_partition_in_sync_replica`             | `topic`, `partition`                           | In-sync replica count                                                                                                                                                                   |
| `kafka_topic_partition_leader_is_preferred`         | `topic`, `partition`                           | 1 if preferred leader is elected                                                                                                                                                        |
| `kafka_topic_partition_under_replicated_partition`  | `topic`, `partition`                           | 1 if under-replicated                                                                                                                                                                   |
| `kafka_consumergroup_members`                       | `consumergroup`                                | Active member count                                                                                                                                                                     |
| `kafka_consumergroup_current_offset`                | `consumergroup`, `topic`, `partition`          | Committed offset                                                                                                                                                                        |
| `kafka_consumergroup_current_offset_sum`            | `consumergroup`, `topic`                       | Sum of committed offsets                                                                                                                                                                |
| `kafka_consumergroup_lag`                           | `consumergroup`, `topic`, `partition`          | Lag per partition                                                                                                                                                                       |
| `kafka_consumergroup_lag_sum`                       | `consumergroup`, `topic`                       | Sum of lags across partitions                                                                                                                                                           |
| `kafka_consumergroup_state`                         | `consumergroup`, `state`                       | Presence indicator: emits `1` for the group's current state (the `state` label), including `Empty` and `Dead`. Query a specific state, e.g. `kafka_consumergroup_state{state="Stable"}` |
| `sova_build_info`                                   | `version`, `revision`, `branch`, `javaversion` | Build information                                                                                                                                                                       |
| `sova_last_successful_collection_timestamp_seconds` | —                                              | Unix timestamp of the last successful collection cycle, 0 if none has completed                                                                                                         |

The `process_*` and `jvm_*` families are exported too.

### Notes for dashboard authors

- **`-1` means "no commit", and it is a real sample.** If a consumer group has committed to a topic but not to every one of its partitions, the uncommitted partitions report `-1` in both `kafka_consumergroup_current_offset` and `kafka_consumergroup_lag`. Any `avg()`, `min()`, or `topk()` over raw `kafka_consumergroup_lag` must filter `> -1` first, or the uncommitted partitions will skew the result. `lag > N` threshold alerts are unaffected, since `-1` never trips a positive threshold.
- **Lag is never clamped and can be negative.** A committed offset ahead of the high-water mark is a real anomaly worth seeing, not a glitch, so it is reported as-is. Because the broker, topic, and consumer-group data is collected concurrently within a cycle, a busy partition can also show a small transient negative lag that means nothing — do not alert on `lag < 0` without a `for:` duration.
- **The `_sum` metrics are not the sum of the visible per-partition series.** Uncommitted partitions (the `-1` ones above) are excluded from both `kafka_consumergroup_lag_sum` and `kafka_consumergroup_current_offset_sum`, since folding a `-1` into a sum only drags it down for no useful meaning. A panel showing per-partition series next to the `_sum` will not reconcile — this is deliberate, not a bug.
- **`kafka_consumergroup_lag_sum` can be absent rather than `0`** if offset fetching fails for a whole topic during a cycle. Prefer `absent_over_time(...[5m])` over a bare `absent()` so a single bad collection cycle does not page on its own.
- **`sova_last_successful_collection_timestamp_seconds` is the staleness signal**, reading `0` before the first successful cycle. `time() - sova_last_successful_collection_timestamp_seconds` is the "is this exporter actually working" panel.

---

## Configuration

| Environment variable                       | Default      | Description                                                                        |
|--------------------------------------------|--------------|------------------------------------------------------------------------------------|
| `SOVA_KAFKA_BOOTSTRAP_SERVERS`             | *(required)* | Kafka bootstrap server(s), e.g. `kafka:9093`                                       |
| `SOVA_GROUP_REGEX`                         | `.*`         | Consumer group include regex                                                       |
| `SOVA_GROUP_EXCLUDE_REGEX`                 | *(none)*     | Consumer group exclude regex                                                       |
| `SOVA_TOPIC_REGEX`                         | `.*`         | Topic include regex                                                                |
| `SOVA_TOPIC_EXCLUDE_REGEX`                 | *(none)*     | Topic exclude regex                                                                |
| `STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION` | *(none)*     | Kafka client properties, one `key=value` per line (TLS, SASL, other client config) |
| `SOVA_METRICS_PORT`                        | `9404`       | Port for `/metrics` endpoint                                                       |
| `SOVA_MANAGEMENT_PORT`                     | `8080`       | Port for `/healthy` and `/ready` endpoints                                         |
| `SOVA_COLLECT_INTERVAL_MS`                 | `30000`      | Metrics collection interval in milliseconds                                        |
| `STRIMZI_LOG_LEVEL`                        | `INFO`       | Root log level                                                                     |
| `STRIMZI_AC_LOG_LEVEL`                     | `WARN`       | Log level for the `org.apache.kafka` logger                                        |

### Health endpoints

`/healthy` is always 200 while the JVM can answer: a broken collection must not restart a pod that is still serving its last good snapshot.

`/ready` is 200 only while the snapshot being served is younger than two collection intervals.
Only a successful collection cycle replaces the snapshot, so failing cycles and cycles that hang - which throw nothing - both turn the probe DOWN once the exported numbers are frozen for that long. 
Before the first cycle completes there is no snapshot, so the probe is DOWN.


---

## Development guide

See [development-docs/DEV_GUIDE.md](development-docs/DEV_GUIDE.md).
