include ./Makefile.maven

PROJECT_NAME ?= sova

.PHONY: all
all: java_install spotbugs

.PHONY: clean
clean: java_clean
