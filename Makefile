# Local checks: the analyzers, the test suites and elspais. The test suites
# are the [[scanning.test.targets]] of .elspais.toml, run through
# `elspais checks --run-tests`; every recipe calls tools/run-checks.sh, which
# asks elspais for a group's targets and holds the help text. `make` alone
# prints it.
#
# Variables (see `make help`): SHARDS, JOBS, UNIT_PIECES, PG_IMAGE,
# ELSPAIS_STRICT, PG_TEST_URL, PG_TEST_URLS, PG_TEST_URL_OTHER_SERVER, PG_PART.
# Works with GNU make 3.81 and later.

SHELL := /bin/sh
.DEFAULT_GOAL := help

RUN_CHECKS := ./tools/run-checks.sh

export SHARDS JOBS UNIT_PIECES PG_IMAGE ELSPAIS_STRICT PG_TEST_URL PG_TEST_URLS \
	PG_TEST_URL_OTHER_SERVER PG_PART

TARGETS := help analyze test-unit test-web test-desktop test-postgres \
	test-demos test-throughput elspais test-all test-all-parallel \
	pg-up pg-down

.PHONY: $(TARGETS)

$(TARGETS):
	@$(RUN_CHECKS) $@
