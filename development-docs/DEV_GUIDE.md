# Developer Guide

## Prerequisites

- Java 21 (JDK)
- Maven 3.8+
- Docker (required for integration tests — `StrimziKafkaCluster` uses Testcontainers)
- GNU Make

## Quick start

```bash
# Clone the repository
git clone https://github.com/strimzi/sova.git
cd sova

# Build (no Docker needed)
mvn clean install -DskipTests

# Build and run unit tests (Docker not required)
mvn verify
```

Integration tests (`*IT.java`) are excluded by Surefire and only run by Maven Failsafe inside
the `failsafe` profile (see the `failsafe` profile in `pom.xml`). Plain `mvn verify` runs zero
ITs — see "Running integration tests only" below.

## Running locally with environment variables

```bash
mvn clean package
cd target && unzip sova-*.zip && cd sova-*/
export STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION='bootstrap.servers=localhost:9092'
java -classpath "libs/*" io.strimzi.sova.Main
```

There is no runnable JAR — `maven-jar-plugin` does not set a `Main-Class`, so
`java -jar target/sova-*.jar` fails. The ZIP produced by `maven-assembly-plugin`
(`src/assembly/dist.xml`) only bundles `libs/` plus `LICENSE`/`README`/`NOTICE`; run the
unpacked distribution the same way the README does.

Metrics are available at `http://localhost:9404/metrics`, the liveness probe at `http://localhost:8080/healthy` and the readiness probe at `http://localhost:8080/ready`.

## Running integration tests only

```bash
mvn verify -Pfailsafe -Dskip.surefire=true
```

`-Dskip.surefire=true` skips the unit tests; `-DskipTests` would skip the integration tests too. In CI, pass it through `MVN_ARGS` (for example `make java_verify MVN_ARGS="-Pfailsafe -Dskip.surefire=true"`).

Docker must be running. The tests start a `StrimziKafkaCluster` (Testcontainers) automatically.

## Checkstyle

Checkstyle runs automatically during build. To run it standalone:

```bash
mvn checkstyle:check@validate
```

## SpotBugs

SpotBugs is not bound to a Maven phase, so `mvn verify` does not run it. It needs compiled
classes first:

```bash
mvn install -DskipTests
make spotbugs
```

Keep SpotBugs at 4.10.5 or newer — earlier releases cannot read Java 25 class files and fail
with `Unsupported class file major version 69`.

`make all` runs the install and SpotBugs together — that is what CI gates on. Suppress a
false positive with `@SuppressFBWarnings` on the member, or, if the pattern is wrong for the
whole project, add a `<Match>` to `.spotbugs/spotbugs-exclude.xml` with a comment saying why.

## Published distributions

Every push to `main` publishes `sova-<version>-dist.zip` to `ghcr.io/strimzi/dist-sova` as an OCI
artifact, and a release publishes it again under the release version. The package name follows the
`strimzi/binaries-operators` convention: flat under the org, `<kind>-<component>`.

| Tag                       | Pushed by                | Contains                      |
|---------------------------|--------------------------|-------------------------------|
| `0.1.0-SNAPSHOT`          | push to `main`           | `sova-0.1.0-SNAPSHOT-dist.zip` |
| `0.1.0`, `0.1.0-RC1`, …   | `release.yml`            | `sova-0.1.0-dist.zip`         |

The snapshot tag is mutable and always points at the tip of `main`. The tag and the file name
inside it always agree, so a consumer can use one version string for both a snapshot and a
release: `strimzi-kafka-operator` puts `0.1.0-SNAPSHOT` or `0.1.0` in its `sova.version` file and
the ZIP lands under the name its Makefile looks for.

The package is public, so pulling needs no credentials — GHCR issues anonymous tokens for public
packages and ORAS does that handshake itself:

```bash
oras pull ghcr.io/strimzi/dist-sova:0.1.0-SNAPSHOT -o ./binaries
```

Release artifacts are attested, and the attestation covers the ZIP's digest, so it verifies on the
copy pulled from GHCR as well as on the GitHub release asset:

```bash
gh attestation verify sova-0.1.0-dist.zip --repo strimzi/sova
```

## DCO sign-off

All commits require a Developer Certificate of Origin sign-off:

```bash
git commit -s -m "Your message"
```

## kafka-clients version alignment

The `kafka-clients` version in `pom.xml` **must** be kept in sync with the version used in `strimzi-kafka-operator/pom.xml`. Check the operator's `pom.xml` before bumping.

## Adding a new metric

1. Add the field to the `MetricSnapshot` record, its own top-level file at
   `src/main/java/io/strimzi/sova/metrics/MetricSnapshot.java`.
2. Populate it in the relevant collector's result record and thread it through
   `MetricsCollector.collectInternal()`, which builds the `MetricSnapshot` inline and passes it
   to `MetricsRegistry.update(...)`.
3. Add one `.metricSnapshot(...)` line to `MetricsSnapshotRenderer.from()` with the exact metric name, help string, and label names from `README.md`. Reuse `scalar` or `family`; add a dedicated method only if the labels do not come from the map key alone.
4. Add an assertion in `MetricsRegistryTest.allMetricsRegistered()` that the metric name appears in scrape output, and populate its map in the snapshot.
5. Add an assertion in `MetricsCollectorIT` checking the value.
6. Document the new metric in `README.md`.
