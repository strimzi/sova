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

Once this is merged, the build within `main` branch publishes `ghcr.io/strimzi/dist-sova:0.2.0-SNAPSHOT`.
The previous snapshot tag (`0.1.0-SNAPSHOT`) stays in GHCR but is no longer updated.

When releasing a new patch version, the release branch already exists and you do not need to bump the version in the `main` branch.
Cherry-pick the bug fixes into the release branch or add them through PRs.

### Prepare the release

For any release - major, minor or patch - prepare the release branch:

1. Make sure `CHANGELOG.md` in the release branch has an up-to-date section for the version being released.
2. Check that the `kafka-clients` version in `pom.xml` matches the one used by `strimzi-kafka-operator` (see [DEV_GUIDE.md](DEV_GUIDE.md#kafka-clients-version-alignment)).
3. Prepare the release with `make release`, which sets the Maven project version to the release version:
   ```bash
   RELEASE_VERSION=0.1.0 make release
   ```
   Always use the GA version here (e.g. `0.1.0`), not the RC version (e.g. `0.1.0-rc1`).
4. Review and commit the changes (only `pom.xml`), and push them to the release branch.

The build pipeline starts automatically for every commit pushed to the release branch.
It builds the distribution `sova-<GA version>-dist.zip` from the committed version, and the release pipeline takes it from there.
That is why the GA version has to be set in the release branch before the build the release is made from, also when releasing an RC.

### Run the release pipeline

Wait until the [Build workflow](https://github.com/strimzi/sova/actions/workflows/build.yml) has finished successfully for the last commit in the release branch.
Note its Run ID from the URL (e.g. `https://github.com/strimzi/sova/actions/runs/1234567890` - the Run ID is `1234567890`).
Then run the [Release workflow](https://github.com/strimzi/sova/actions/workflows/release.yml) manually from the GitHub Actions UI:

* Select the release branch (e.g. `release-0.1.x`) in the `Use workflow from` field.
  The workflow refuses to build from any branch that does not start with `release-`.
* Set the release version (e.g. `0.1.0` for GA or `0.1.0-rc1` for an RC).
* Set the GitHub Actions Run ID of the source build to the Run ID noted above.
  For a GA release, use the Run ID used for the last RC, since there should be no changes after it.

The release workflow does not build anything.
It takes `sova-<GA version>-dist.zip` from the source build and only names it after the release version.
The workflow fails if the source build does not contain the ZIP for the GA part of the release version, for example when it was built from a snapshot.

The workflow runs the following jobs:

* `prepare-release-artifacts` - downloads `binaries-sova.tar` from the source build and uploads the `release-sova-<version>.tar` workflow artifact containing the ZIP renamed to `sova-<version>-dist.zip`
* `attest` - signs the ZIP with Sigstore (SLSA build provenance) and adds `release-<version>.intoto.jsonl` to the `release-sova-<version>.tar` artifact
* `publish-dist` - pushes the attested ZIP to `ghcr.io/strimzi/dist-sova:<version>`
* `snyk-scan` and `fossa-scan` - dependency vulnerability and license scans of the release

For example, for `0.1.0-rc1` the workflow produces `sova-0.1.0-rc1-dist.zip` and `ghcr.io/strimzi/dist-sova:0.1.0-rc1`.
The content of the ZIP is the GA build: it unpacks into `sova-0.1.0/` and contains `io.strimzi.sova-0.1.0.jar`, and a running RC reports `version="0.1.0"` in `sova_build_info`.
The RC version is only in the file name and in the tags.

> [!NOTE]
> The Build workflow keeps `binaries-sova.tar` for only 7 days.
> If the GA release does not happen within 7 days of the source build, re-run the Build workflow for the same commit and release a new RC from it before the GA.

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
     * `sova-<version>-dist.zip` - the distribution from the source build, named after the release version
     * `release-<version>.intoto.jsonl` - the `.intoto.jsonl` name is what the OpenSSF Scorecard `Signed-Releases` check looks for, and the [README](../README.md#artifact-attestations) tells users to verify the release archive with `gh attestation verify <archive> --bundle release-<version>.intoto.jsonl`.

### Update the Strimzi operators

_(only for GA, optionally for RCs that need testing in the operators)_

Update the `sova.version` file in the [strimzi-kafka-operator](https://github.com/strimzi/strimzi-kafka-operator) repository from the snapshot to the released version (e.g. `0.1.0`, or `0.1.0-rc1` to test an RC) and open a PR.
The operators build pulls the ZIP from `ghcr.io/strimzi/dist-sova:<version>`, so no files need to be copied.

### Post-release tasks

* Make sure `CHANGELOG.md` in the `main` branch contains the released section as it was released.

### Announcements

Announce the release, both GA and RCs, on the following channels:

* `cncf-strimzi-users` and `cncf-strimzi-dev` mailing lists
* Slack

Announce only GA releases on the social accounts, if the release is significant enough.

## Release candidates

Release candidates are released with the same release workflow as the final releases.
When starting the workflow, use the RC name as the release version, for example `0.1.0-rc1` or `0.1.0-rc2`.

When releasing release candidates, the release branch should already be prepared for the final release.
E.g. when releasing `0.1.0-rc1`, the release branch should already have the version `0.1.0` set before the source build.
The RC version (e.g. `0.1.0-rc1`) is used for the GitHub tag and release, the ZIP file name and the GHCR tag, but not inside the ZIP.

When the last RC is accepted, release the GA with the same source build Run ID as that RC.
The GA ZIP then has the same digest as the RC ZIP, just under the GA name.

## CVEs

Sova has no container image of its own, so there is no CVE rebuild workflow.
A CVE in Sova's code or in one of its Java dependencies requires a new patch (or minor) release.
CVEs in the operand container image that runs Sova are handled by the `strimzi-kafka-operator` CVE rebuild process.
