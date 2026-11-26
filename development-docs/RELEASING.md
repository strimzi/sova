# Releasing Sova

This document describes how to release a new version of Strimzi Sova.

Sova ships no container image and no Helm Chart.
A release consists of:

* the distribution archive `sova-<version>-dist.zip` (the JARs in `libs/` plus `LICENSE` and `README`), attached to the GitHub release
* the attestation bundle `release-<version>.intoto.jsonl`, attached to the GitHub release
* the same distribution archive published as an OCI artifact to `ghcr.io/strimzi/dist-sova:<version>`, which is what `strimzi-kafka-operator` consumes

The Java artifacts are not deployed to Maven Central.

## Regular releases

### Create the release branch

Before releasing a new major or minor version, create the release branch.
The release branch is named `release-<Major>.<Minor>.x`.
For example, for release 0.1.0, the branch is named `release-0.1.x`.
The release branch is normally created locally from the `main` branch and pushed to the GitHub repository.

After creating the release branch, bump the project version in the `main` branch to the next minor / major version with the `-SNAPSHOT` suffix:

```bash
RELEASE_VERSION=0.2.0-SNAPSHOT make release_maven
```

In the same PR, add a header for the next release to `CHANGELOG.md`.

Once this is merged, the next push to `main` publishes `ghcr.io/strimzi/dist-sova:0.2.0-SNAPSHOT`.
The previous snapshot tag (`0.1.0-SNAPSHOT`) stays in GHCR but is no longer updated.

When releasing a new patch version, the release branch already exists and you do not need to bump the version in the `main` branch.
Cherry-pick the bug fixes into the release branch or add them through PRs.

### Prepare the release

For any release - major, minor or patch - prepare the release branch:

1. Make sure `CHANGELOG.md` in the release branch has an up-to-date section for the version being released.
2. Check that the `kafka-clients` version in `pom.xml` matches the one used by `strimzi-kafka-operator` (see [DEV_GUIDE.md](DEV_GUIDE.md#kafka-clients-version-alignment)).
3. Set the Maven project version to the release version:
   ```bash
   RELEASE_VERSION=0.1.0 make release_maven
   ```
   Always use the GA version here (e.g. `0.1.0`), not the RC version (e.g. `0.1.0-rc1`).
4. Review and commit the changes, and push them to the release branch.

The build pipeline starts automatically for every commit pushed to the release branch.

### Run the release pipeline

Wait until the [Build workflow](https://github.com/strimzi/sova/actions/workflows/build.yml) has finished successfully for the last commit in the release branch.
Then run the [Release workflow](https://github.com/strimzi/sova/actions/workflows/release.yml) manually from the GitHub Actions UI:

* Select the release branch (e.g. `release-0.1.x`) in the `Use workflow from` field.
  The workflow refuses to build from any branch that does not start with `release-`.
* Set the release version (e.g. `0.1.0` for GA or `0.1.0-rc1` for an RC).

Unlike the operators and Drain Cleaner, Sova's release workflow builds the distribution itself from the selected branch, so it does not take a source build ID or a release suffix.

The workflow runs the following jobs:

* `prepare-release-artifacts` - runs `make release` and uploads the `release-sova-<version>.tar` workflow artifact containing `sova-<VERSION>-dist.zip`
* `attest` - signs the ZIP with Sigstore (SLSA build provenance) and adds `release-<version>.intoto.jsonl` to the `release-sova-<version>.tar` artifact
* `publish-dist` - pushes the attested ZIP to `ghcr.io/strimzi/dist-sova:<VERSION>`
* `snyk-scan` and `fossa-scan` - dependency vulnerability and license scans of the release

> [!NOTE]
> `make release` upper-cases the version in the POM.
> An RC started as `0.1.0-rc1` therefore produces `sova-0.1.0-RC1-dist.zip` and the GHCR tag `0.1.0-RC1`, while the workflow artifact and the attestation bundle keep the lower-case input (`release-sova-0.1.0-rc1.tar`, `release-0.1.0-rc1.intoto.jsonl`).
> GA versions are unaffected.

### Verify the release artifacts

After the release pipeline has finished:

1. Download the `release-sova-<version>.tar` artifact from the workflow run and unpack it.
2. Verify the attestation of the ZIP:
   ```bash
   gh attestation verify sova-0.1.0-dist.zip --repo strimzi/sova --signer-repo strimzi/github-actions
   gh attestation verify sova-0.1.0-dist.zip --bundle release-0.1.0.intoto.jsonl --repo strimzi/sova --signer-repo strimzi/github-actions
   ```
3. Check that the distribution was published to GHCR and matches the attested ZIP:
   ```bash
   oras pull ghcr.io/strimzi/dist-sova:0.1.0 -o ./ghcr-check
   gh attestation verify ./ghcr-check/sova-0.1.0-dist.zip --repo strimzi/sova --signer-repo strimzi/github-actions
   ```
4. Smoke test the distribution against a Kafka cluster as described in [DEV_GUIDE.md](DEV_GUIDE.md#running-locally-with-environment-variables) and check that `/metrics`, `/healthy` and `/ready` respond.
5. Check the results of the Snyk and FOSSA scans.

### Create the GitHub release

1. Tag the commit in the release branch that the release workflow built, using the release version (e.g. `git tag 0.1.0`, or `git tag 0.1.0-rc1` for an RC), and push the tag to GitHub.
2. On GitHub, create the release from that tag:
   * Use the `CHANGELOG.md` section of the release as the release notes.
   * Mark release candidates as pre-releases.
   * Attach the following files from the `release-sova-<version>.tar` artifact **without changing their file names**:
     * `sova-<VERSION>-dist.zip`
     * `release-<version>.intoto.jsonl` - the `.intoto.jsonl` name is what the OpenSSF Scorecard `Signed-Releases` check looks for, and the [README](../README.md#artifact-attestations) tells users to verify the release archive with `gh attestation verify <archive> --bundle release-<version>.intoto.jsonl`.

### Update the Strimzi operators

_(only for GA, optionally for RCs that need testing in the operators)_

Update the `sova.version` file in the [strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator) repository from the snapshot to the released version (e.g. `0.1.0`) and open a PR.
The operators build pulls the ZIP from `ghcr.io/strimzi/dist-sova:<version>`, so no files need to be copied.

### Post-release tasks

* Make sure `CHANGELOG.md` in the `main` branch contains the released section as it was released.

### Announcements

Announce the release, both GA and RCs, on the following channels:

* `cncf-strimzi-users` and `cncf-strimzi-dev` mailing lists
* Slack

Announce only GA releases on the social accounts, if the release is significant enough.

## Release candidates

Release candidates are built with the same release workflow as the final releases.
When starting the workflow, use the RC name as the release version, for example `0.1.0-rc1` or `0.1.0-rc2`.

When building release candidates, the release branch should already be prepared for the final release.
E.g. when building `0.1.0-rc1`, the release branch should already have the version `0.1.0` set.
The RC version (e.g. `0.1.0-rc1`) is used for the GitHub tag and the GitHub release.

## CVEs

Sova has no container image of its own, so there is no CVE rebuild workflow.
A CVE in Sova's code or in one of its Java dependencies requires a new patch (or minor) release.
CVEs in the operand container image that runs Sova are handled by the `strimzi-kafka-operator` CVE rebuild process.
