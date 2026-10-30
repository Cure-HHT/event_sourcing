#!/usr/bin/env python3
"""Regenerates tools/postgres-ci-durations.tsv from CI runs.

Usage: tools/postgres_ci_durations.py RUN_ID [RUN_ID ...] > tools/postgres-ci-durations.tsv

Reads the "Postgres suites (part i of n)" jobs of each event-sourcing-tests
run through the GitHub CLI (`gh`, authenticated for Cure-HHT/event_sourcing),
adds up each file's seconds across its pieces within a run, and writes the
median over the runs, one "<seconds>\t<repository-relative path>" line per
file. run-checks.sh weighs the Postgres slices with this table, so give it
several recent runs: one run's times swing by minutes for the measured files.
"""
import re
import statistics
import subprocess
import sys
from collections import defaultdict

REPO = "Cure-HHT/event_sourcing"
JOB = re.compile(r"Postgres suites \(part \d+ of \d+\)")
LINE = re.compile(r"\b(?:PASS|FAIL)\s+(\S+_test\.dart)(?: \(tests piece \d+/\d+\))?\s+(\d+)s\s*$")


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def file_seconds(run_id: str) -> dict[str, int]:
    jobs = gh("run", "view", run_id, "-R", REPO, "--json", "jobs",
              "--jq", '.jobs[] | "\\(.databaseId)\\t\\(.name)"')
    totals: dict[str, int] = defaultdict(int)
    for row in jobs.splitlines():
        job_id, _, name = row.partition("\t")
        if not JOB.fullmatch(name):
            continue
        for line in gh("run", "view", run_id, "-R", REPO, "--log", "--job", job_id).splitlines():
            m = LINE.search(line)
            if m:
                totals[m.group(1)] += int(m.group(2))
    if not totals:
        sys.exit(f"run {run_id}: no Postgres slice timings found")
    return totals


def main(run_ids: list[str]) -> None:
    if not run_ids:
        sys.exit(__doc__)
    per_file: dict[str, list[int]] = defaultdict(list)
    for run_id in run_ids:
        for path, secs in file_seconds(run_id).items():
            per_file[path].append(secs)
    print("# CI seconds per Postgres-gated test file, all its pieces together: the")
    print("# median over the event-sourcing-tests runs below. Regenerate with")
    print("# tools/postgres_ci_durations.py <run id>...; run-checks.sh reads it")
    print("# (file_weight).")
    print("# runs: " + " ".join(run_ids))
    for path in sorted(per_file):
        print(f"{int(round(statistics.median(per_file[path])))}\t{path}")


if __name__ == "__main__":
    main(sys.argv[1:])
