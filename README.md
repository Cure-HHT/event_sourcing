# event_sourcing

Reactive, append-only event-sourcing primitives for regulated Dart and
Flutter applications, with companion libraries for canonical JSON
serialization and provenance tracking. Built for FDA 21 CFR Part 11
compliant audit trails.

## Start here

After cloning, run once:

```sh
./tools/setup-repo.sh          # activate this clone's git hooks
./tools/setup-repo.sh --check  # verify the clone is set up (reports via exit status)
(cd event_sourcing && flutter test)  # run the core library's tests
```

Each package under this repo (`event_sourcing/`, `canonical_json_jcs/`,
`provenance/`, `reaction/`, `reaction_widgets/`,
`reaction_widgets_testing/`) carries its own `pubspec.yaml` and test
suite; run `flutter test` from inside any package directory to run
that package's tests.

## Running the checks locally

The repository's test suites are defined once, as the
`[[scanning.test.targets]]` of `.elspais.toml`: each target names its
command, working directory, reporter and the results and coverage files it
writes, and `[scanning.test.groups]` gathers them into groups (`unit`, `web`,
`desktop`, `postgres`, `demos`, `throughput`; the reserved `default` group is
every target). `elspais checks --run-tests --targets <group or target>` runs
them, ingests their results and credits the requirements their tests verify;
a bare `elspais checks --run-tests` runs the `default` group, the full
verification. The target commands pass `--no-pub`, so resolve each package's
dependencies first (`make` does).

The root `Makefile` runs the targets through `tools/run-checks.sh`, which asks
elspais for a group's targets and runs one `elspais checks --run-tests
--targets <target>` per target, `JOBS` at a time. CI runs the same targets the
same way. `make` alone lists the make targets and variables.

| Target | What it runs |
| --- | --- |
| `make analyze` | `flutter analyze` / `dart analyze` in every package a target runs in (infos fatal) |
| `make test-unit` | the `unit` group: every package's tests without Postgres |
| `make test-web` | the `web` group: `event_sourcing/test/web/` in Chrome |
| `make test-desktop` | the `desktop` group: `event_sourcing/example`'s integration test under `xvfb-run` (Linux) |
| `make test-postgres` | the `postgres` group: every Postgres-gated file, sharded across throwaway containers |
| `make test-demos` | the `demos` group: the example packages' unit, Postgres and desktop tests |
| `make test-throughput` | the `throughput` group: the throughput guard against the baseline build |
| `make elspais` | `elspais checks --lenient` over the results on disk |
| `make test-all-parallel` | analyze and every target at once, then one `elspais checks` over all the results as the gate: the fast full verification |
| `make test-all` | analyze, then `elspais checks --run-tests`: every target one after another in one invocation, one Postgres container per Postgres target, then the gate: the slow reference run |
| `make pg-up` / `make pg-down` | start / remove throwaway Postgres containers by hand |

Each target writes its machine-JSON results (and lcov coverage) under its
package's `coverage/`, to paths no other target writes, so targets run side by
side. The Postgres targets run every `_test.dart` file that names
`PG_TEST_URL` (or uses the test URL helper); they take servers from
`PG_TEST_URLS` (space-separated, one shard each) or `PG_TEST_URL`, and without
them start their own `postgres:16` containers on free ports (`SHARDS` of
them) and remove them when they end, interrupted or not. A target that
splits its files (the Postgres targets and event_sourcing's unit suite, in
`UNIT_PIECES` pieces) writes one results file per shard or piece and merges
their coverage. The gate is `elspais checks --lenient` (the pre-push hook's
flag; `ELSPAIS_STRICT=1` drops it) plus a strict check that results were
ingested and every results and coverage file a target names was read
(`tools/run-checks.sh results-ingested`); it builds the graph locally, since
the elspais daemon does not watch the results files. Output is one line per target,
shard or piece; full logs go to `.check-logs/<run id>/`, and a run ends with
every failing test. Per-file Postgres durations and per-target durations are
kept in `.check-durations` and balance the next run. Runs on Linux and macOS
(bash 3.2 or later, GNU make 3.81 or later, python3 and Docker).

## Related repositories

- `hht_diary` — the core application that consumes this library.
- `hht_workflows` — shared CI checks used across Cure-HHT repos.
- `hht_admin` — org infrastructure and the authoritative `HHT-OPS-*` spec.

## Packages

| Package | Purpose |
| --- | --- |
| [`event_sourcing/`](event_sourcing/) | Core library: storage, sync, ingest, materialization, action dispatch, permissions |
| [`canonical_json_jcs/`](canonical_json_jcs/) | JCS (RFC 8785) JSON canonicalization |
| [`provenance/`](provenance/) | Append-only provenance chain types |

`event_sourcing` depends on the other two via path-deps. The packages are
intentionally small and pure-Dart so they can be reused by server-side
and web deployments as well as the mobile client.

### Demos

`event_sourcing/example/`, `event_sourcing/example_action_permissions/`,
and `event_sourcing/example_clinical_scopes/` are intra-lib worked
examples that exercise the public API. `example_action_permissions/`
hosts a Flutter dual-pane shell + shelf-based server demonstrating the
action-dispatch + permission-snapshot flow end to end.
`example_clinical_scopes/` is a Flutter + shelf demo of hierarchy-scoped
reads — a `region → site → participant` model with Investigator (site-
scoped), Overseer (region-scoped, two-hop), and Admin roles, where each
user's reactive participant list is narrowed by the read-path
`ScopeDescendantExpander`.

## Roadmap

Deliberate future work is recorded in `spec/roadmap/` — the only place
in the repo where unbuilt capability is described. The headline item
is multi-source (multi-user) editing:
`spec/roadmap/multi-source-editing.md`. The
single-source-per-aggregate-type invariant holds today; the dormant
multi-source machinery activates under that roadmap item.

## Setup

`tools/setup-repo.sh` sets `core.hooksPath = .githooks` (shared across
all worktrees of the clone) and pre-populates the hook environments.
Pre-commit framework runs hooks listed in `.pre-commit-config.yaml` on
every commit; gitleaks runs additionally on push. Hooks include
trailing-whitespace / EOF / merge-conflict checks, gitleaks (secret
scanning), markdownlint, and `dart format`.

Requires `pre-commit` on PATH; if absent, the script prints install
instructions (`pipx install pre-commit`, `brew install pre-commit`, or
`pip install --user pre-commit`) and exits.

## License

AGPLv3 — see [LICENSE](LICENSE).
