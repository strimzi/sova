include ./Makefile.maven

PROJECT_NAME ?= sova

.PHONY: all
all: java_install spotbugs

.PHONY: clean
clean: java_clean

# Prepares the release branch for a release and packages the distribution. The release workflow
# does not use this package, it takes the distribution from the Build workflow run of the prepared
# release branch. The package is kept because the shared Strimzi GitHub Actions integration tests
# expect `make release` to produce the release archives.
.PHONY: release
release: release_maven release_package

.PHONY: release_maven
release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn $(MVN_ARGS) versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn $(MVN_ARGS) versions:commit

.PHONY: release_package
release_package: java_package
