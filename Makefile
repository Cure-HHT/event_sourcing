# Local checks: analyzers, every package's tests, the browser and desktop
# suites, the Postgres-gated files on throwaway Postgres containers, the
# throughput guard and elspais. Every recipe calls tools/run-checks.sh, which
# holds the logic and the help text; `make` alone prints it.
#
# Variables (see `make help`): SHARDS, JOBS, UNIT_PIECES, PG_IMAGE,
# ELSPAIS_STRICT, PG_TEST_URL, PG_TEST_URL_OTHER_SERVER. Works with GNU make
# 3.81 and later.

SHELL := /bin/sh
.DEFAULT_GOAL := help

RUN_CHECKS := ./tools/run-checks.sh

export SHARDS JOBS UNIT_PIECES PG_IMAGE ELSPAIS_STRICT PG_TEST_URL PG_TEST_URL_OTHER_SERVER

TARGETS := help analyze test-unit test-web test-desktop test-postgres \
	test-demos test-throughput elspais test-all test-all-parallel \
	pg-up pg-down

.PHONY: $(TARGETS)

$(TARGETS):
	@$(RUN_CHECKS) $@
