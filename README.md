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
`desktop`, `postgres`, `demos`, `evidence`, `throughput`; the reserved
`default` group is every target). `elspais test --targets <group or target>`
runs them and records their results; a bare `elspais test` runs the `default`
group, the full verification. `elspais checks` then ingests the results,
credits the requirements their tests verify and judges them. The target
commands pass `--no-pub`, so resolve each package's dependencies first (`make`
does).

The root `Makefile` runs the targets through `tools/run-checks.sh`, which asks
elspais for a group's targets and runs one `elspais test --targets <target>`
per target, `JOBS` at a time. CI runs the same targets the same way. `make`
alone lists the make targets and variables.

A run is capped at `MEMORY_MAX`, 60% of the memory available when it starts
unless set. On Linux with systemd the run starts inside a scope with that cap
and no swap: if the run exceeds the cap, then the kernel kills processes in
the run, not the desktop, and the summary says so. The defaults of `JOBS`,
`SHARDS` and `UNIT_PIECES` shrink to fit the cap, and the summary reports the
run's peak memory. `MEMORY_MAX=off` removes the cap.

| Target | What it runs |
| --- | --- |
| `make analyze` | `flutter analyze` / `dart analyze` in every package a target runs in (infos fatal) |
| `make test-unit` | the `unit` group: every package's tests without Postgres |
| `make test-web` | the `web` group: `event_sourcing/test/web/` in Chrome |
| `make test-desktop` | the `desktop` group: `event_sourcing/example`'s integration test under `xvfb-run` (Linux) |
| `make test-postgres` | the `postgres` group: every Postgres-gated file, sharded across throwaway containers |
| `make test-demos` | the `demos` group: the example packages' unit, Postgres and desktop tests |
| `make test-throughput` | the `throughput` group: the throughput guard against the baseline build |
| `make elspais` | `elspais checks` (strict) over the results on disk |
| `make evidence` | the `evidence` group, then `elspais evidence write`: the Evidence Snapshot (see [Test evidence](#test-evidence)) |
| `make test-all-parallel` | analyze and every target at once, then the gate: the fast full verification |
| `make test-all` | analyze, then `elspais test`: every target one after another in one invocation, one Postgres container per Postgres target, then the gate: the slow reference run |
| `make pg-up` / `make pg-down` | start / remove throwaway Postgres containers by hand |

Each target writes its machine-JSON results, its lcov coverage and the
fingerprint of the inputs it ran against into its own folder,
`.results/<target>/`, so targets run side by side. The Postgres targets run
every `_test.dart` file that names `PG_TEST_URL` (or uses the test URL
helper); they take servers from `PG_TEST_URLS` (space-separated, one shard
each) or `PG_TEST_URL`, and without them start their own containers of the
pinned Postgres image on free ports (`SHARDS` of them, and the second server
one file compares against) and remove them when they end, interrupted or not.
A target that splits its files (the Postgres targets and event_sourcing's unit
suite, in `UNIT_PIECES` pieces) writes one results file per shard or piece and
merges their coverage. The gate is one strict `elspais checks --expect
default` over every result: a warning fails it, and so does a target with no
results. It builds the graph locally, since the elspais daemon does not watch
the results files. Output is one line per target, shard or piece; full logs
go to `.check-logs/<run id>/`, and a run ends with every failing test.
Per-file Postgres durations and per-target durations are kept in
`.check-durations` and balance the next run. Runs on Linux and macOS (bash
3.2 or later, GNU make 3.81 or later, python3 and Docker).

The tool versions are pinned in `.github/versions.env`: elspais, the Flutter
SDK and the Postgres image. CI installs them, and `tools/run-checks.sh` reads
them.

## Test evidence

`test-evidence/` holds the Evidence Snapshot: the results of every test suite
except the throughput guard, for the commit whose tree digest it records,
and the traceability report derived from them. A consumer that pins a commit
of this repository can cite that report without running the suites.

- `results.jsonl` holds the outcome of each test, by the file and line that
  declare it.
- `snapshot.json` holds the digest of the tree the run tested, the targets of
  the `evidence` group with the digest of each target's inputs, and the facts
  about the run.
- `TRACEABILITY.md` names, for each assertion, the code that implements it and
  the tests that verify it, with each test's outcome.
- `timings.jsonl` holds each test's duration and printed output. No check
  compares it.

The facts state what the run used:

| Fact | Meaning |
| --- | --- |
| `backends=vm,chrome,desktop,postgres` | The suites ran on the Dart VM, in Chrome, as a Linux desktop build under a virtual display, and against Postgres servers |
| `flutter=<version>` | The Flutter SDK that ran them: `FLUTTER_VERSION` of `.github/versions.env` |
| `postgres=<image>` | The Postgres image the servers ran: `POSTGRES_IMAGE` of `.github/versions.env` |

The throughput guard is not part of the snapshot: it measures the machine that
runs it, not the tree. CI runs it as a blocking job of its own.

CI runs every suite again on each pull request and push to `main`, and its
gate verifies the committed snapshot against that run byte for byte, with the
same facts. On a pull request CI tests GitHub's merge of the branch into
`main`, so the snapshot matches only when the branch holds the tip of `main`:
bring the branch up to date before writing the snapshot. A difference in any test's outcome, in the set of results, in a
fact, in the tree digest or in the report fails the gate. So the snapshot on
`main` describes the commit that holds it.

Write the snapshot as the last step before committing a change, and commit
`test-evidence/` with it:

```sh
make evidence           # run the `evidence` group, then write test-evidence/
git add test-evidence
```

`make evidence` refuses to start, and names what to install, when Docker, a
Chrome that flutter can use (`CHROME_EXECUTABLE` names one), `xvfb-run` or
the pinned Flutter and elspais versions are missing: the snapshot's facts
would then not hold. It starts its own Postgres containers, so it refuses
`PG_TEST_URL`, `PG_TEST_URLS`, another `PG_IMAGE` and a `PG_PART` slice. A
test that skips off CI when a tool prerequisite is missing (an offline
`flutter pub get`, a package that does not resolve, a process that does not
become ready) fails in `make evidence`, as it does on CI, so both runs record
the same outcome. It writes nothing when a target fails. A snapshot of another
tree fails CI's gate, so write it again after every change that a pull request
carries.

The pre-push `elspais checks` is strict, and it judges the spec, the code
citations and the defined terms. It does not judge test results. CI's gate
judges them, so a push of work in progress needs no full suite run, and a pull
request merges only with a current snapshot.

To check a snapshot without writing one, run the `evidence` group again and
compare:

```sh
tools/run-checks.sh evidence-verify --run
```

It runs the `evidence` group as `make evidence` does, then
`elspais evidence verify --targets evidence` with the snapshot's facts, which
lists each difference. Without `--run` it compares the snapshot with the
results already in `.results/`, as CI's gate does.

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
