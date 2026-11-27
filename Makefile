include ./Makefile.maven

PROJECT_NAME ?= sova

.PHONY: all
all: java_install spotbugs

.PHONY: clean
clean: java_clean

# Prepares the release branch for a release by setting the Maven project version. Nothing is
# built here: the release workflow takes the distribution from the Build workflow run of the
# prepared release branch.
.PHONY: release
release: release_maven

.PHONY: release_maven
release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn $(MVN_ARGS) versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn $(MVN_ARGS) versions:commit
