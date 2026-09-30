#!/usr/bin/env bash
# Runs the repository's checks locally: the analyzers, the test suites and
# elspais.
#
# The test suites are defined in one place: the [[scanning.test.targets]] of
# .elspais.toml, grouped by [scanning.test.groups]. This script lists none of
# them. Each make target asks elspais for the targets of a group and runs them
# through `elspais checks --run-tests --targets <target>`, one invocation per
# target, JOBS at a time; `make test-all` is one plain
# `elspais checks --run-tests`. CI (.github/workflows/event-sourcing-tests.yml)
# runs the same targets the same way.
#
# The script has two halves:
#   - the make targets (`help` lists them), run from the repository root;
#   - `target <unit|postgres|throughput|desktop>`, which the .elspais.toml
#     commands of the targets that shard, split into pieces or may skip call
#     from their package directory. It writes one machine-JSON file per piece
#     or shard under the target's results glob, merges the pieces' lcov into
#     the target's coverage file, and prints the pieces' JSON on stdout at the
#     end, one after another (elspais parses a flutter-machine target's
#     stdout; interleaved streams would cross their test ids). Progress goes
#     to stderr.
#
# Portable to macOS (bash 3.2, BSD userland) and Linux: no associative
# arrays, no mapfile, no `wait -n`, no GNU-only tool flags. Needs python3
# (elspais runs on it) to read elspais's JSON.
#
# Output is one line per target, suite or shard; full output goes to
# .check-logs/<run id>/ and a run ends with a summary of every failing test.
# The exit status is non-zero when anything failed.

# SC2016: the single-quoted `sh -c` scripts expand their own positional
# arguments. SC2001: sed stands in for bash-4 pattern substitution idioms.
# SC2317: the trap handlers are reached through `trap`.
# shellcheck disable=SC2016,SC2001,SC2317

set -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SELF="$ROOT/tools/run-checks.sh"
DURATIONS_FILE="$ROOT/.check-durations"

# event_sourcing's throughput guard: gated on Postgres like the other files,
# run by its own target (the postgres targets leave it out).
THROUGHPUT_FILE="test/storage/postgres/postgres_throughput_guard_test.dart"
# The file that compares a lock session against a second server.
OTHER_SERVER_FILE="test/storage/postgres/postgres_generation_guard_test.dart"

PG_USER=evs
PG_PASSWORD=evs
PG_DB=evs_test

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

cpu_count() {
  local n
  n="$(getconf _NPROCESSORS_ONLN 2>/dev/null)" || n=""
  if [ -z "$n" ]; then n="$(sysctl -n hw.ncpu 2>/dev/null)" || n=""; fi
  if [ -z "$n" ]; then n="$(nproc 2>/dev/null)" || n=""; fi
  case "$n" in '' | *[!0-9]*) n=4 ;; esac
  echo "$n"
}

CORES="$(cpu_count)"
default_shards=$((CORES / 3))
if [ "$default_shards" -lt 1 ]; then default_shards=1; fi
if [ "$default_shards" -gt 8 ]; then default_shards=8; fi
default_jobs=$((CORES / 3))
if [ "$default_jobs" -lt 2 ]; then default_jobs=2; fi
if [ "$default_jobs" -gt 8 ]; then default_jobs=8; fi
default_unit_pieces=$((CORES / 6))
if [ "$default_unit_pieces" -lt 1 ]; then default_unit_pieces=1; fi
if [ "$default_unit_pieces" -gt 4 ]; then default_unit_pieces=4; fi

SHARDS="${SHARDS:-$default_shards}"
UNIT_PIECES="${UNIT_PIECES:-$default_unit_pieces}"
JOBS="${JOBS:-$default_jobs}"
PG_IMAGE="${PG_IMAGE:-postgres:16}"
PG_PART="${PG_PART:-1/1}"
# elspais checks run with the pre-push hook's flag (.pre-commit-config.yaml):
# warnings are reported without failing. ELSPAIS_STRICT=1 drops --lenient.
ELSPAIS_ARGS="--lenient"
if [ "${ELSPAIS_STRICT:-}" = 1 ]; then ELSPAIS_ARGS=""; fi
case "$SHARDS" in '' | *[!0-9]* | 0) echo "SHARDS must be a positive integer (got '$SHARDS')" >&2; exit 2 ;; esac
case "$UNIT_PIECES" in '' | *[!0-9]* | 0) echo "UNIT_PIECES must be a positive integer (got '$UNIT_PIECES')" >&2; exit 2 ;; esac
case "$JOBS" in '' | *[!0-9]* | 0) echo "JOBS must be a positive integer (got '$JOBS')" >&2; exit 2 ;; esac
PART_I="${PG_PART%/*}"
PART_N="${PG_PART#*/}"
case "$PART_I/$PART_N" in
  *[!0-9/]* | /* | */ | 0/* | */0) echo "PG_PART must be <i>/<n> with 1 <= i <= n (got '$PG_PART')" >&2; exit 2 ;;
esac
if [ "$PART_I" -gt "$PART_N" ]; then
  echo "PG_PART must be <i>/<n> with 1 <= i <= n (got '$PG_PART')" >&2
  exit 2
fi
# Every child process re-reads these, so they carry the resolved values.
export SHARDS JOBS UNIT_PIECES PG_IMAGE PG_PART ELSPAIS_ARGS

usage() {
  cat <<EOF
Usage: make <target> [VAR=value ...]    (or: tools/run-checks.sh <target>)

The test suites are the [[scanning.test.targets]] of .elspais.toml; each make
target runs a group of them (\`elspais config get scanning.test.groups\` lists
the groups), one \`elspais checks --run-tests --targets <target>\` per target,
JOBS at a time.

Targets:
  help               This list (the default target).
  analyze            flutter analyze --no-pub (dart analyze in pure Dart
                     packages) in every package a test target runs in, infos
                     fatal, as CI.
  test-unit          The \`unit\` group: every package's tests without
                     Postgres (the targets remove PG_TEST_URL).
  test-web           The \`web\` group: event_sourcing/test/web/ in Chrome.
  test-desktop       The \`desktop\` group: event_sourcing/example's
                     integration test under xvfb-run (Linux; skipped without
                     it).
  test-postgres      The \`postgres\` group: every Postgres-gated file,
                     sharded across SHARDS throwaway Postgres containers (the
                     throughput guard excluded).
  test-demos         The \`demos\` group: the example packages' unit,
                     Postgres and desktop tests.
  test-throughput    The \`throughput\` group: the throughput guard against
                     the baseline build.
  elspais            elspais checks --lenient (as the pre-push hook) over the
                     results on disk, built locally (not by the daemon).
  test-all           \`elspais checks --run-tests\`: the \`default\` group
                     (every target) one after another in one invocation, one
                     Postgres container per Postgres target, after analyze:
                     the slow reference run.
  test-all-parallel  analyze and every target of the \`default\` group, JOBS
                     at a time, the Postgres files sharded, then one
                     \`elspais checks\` over all the results as the gate: the
                     fast full verification.
  pg-up              Start SHARDS throwaway Postgres containers and print
                     their URLs (left running until pg-down).
  pg-down            Remove the containers pg-up started.

Both full runs end with the gate \`elspais checks\` (lenient) and a strict
check that every target's results were ingested.

Variables (current value in brackets):
  SHARDS    Postgres servers a Postgres target shards its files across when
            it starts its own containers [$SHARDS; default cores/3, 1..8].
  JOBS      Targets (and analyzers) run at once [$JOBS; default cores/3,
            2..8].
  UNIT_PIECES
            Pieces event_sourcing's one-file-at-a-time unit suite is split
            into, run side by side [$UNIT_PIECES; default cores/6, 1..4].
  PG_IMAGE  Postgres image for the containers [$PG_IMAGE].
  ELSPAIS_STRICT
            1 runs elspais checks without --lenient, so its warnings fail
            too [${ELSPAIS_STRICT:-unset}].
  PG_TEST_URL
            Reuse this existing server instead of starting containers: the
            Postgres targets then run one after another on it, unsharded. Its
            role must be able to create roles. PG_TEST_URL_OTHER_SERVER, if
            set, is the second server one file compares against.
  PG_TEST_URLS
            Several existing servers (space-separated, e.g. from pg-up), one
            shard each; takes precedence over PG_TEST_URL.
  PG_PART   <i>/<n>: run slice i of the Postgres shard plan cut n ways (CI's
            matrix) [$PG_PART].

Logs: .check-logs/<run id>/ (one file per target, analyzer and Postgres file).
Per-file Postgres durations and per-target durations: .check-durations
(balances the shards and orders the queue).
EOF
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

strip_ansi() {
  local esc
  esc="$(printf '\033')"
  sed "s/${esc}\[[0-9;]*[A-Za-z]//g"
}

# Kills a process and all its descendants. Each process is stopped before
# its children are listed, so it cannot start another one meanwhile.
kill_tree() {
  local pid="$1" child children
  if ! kill -STOP "$pid" 2>/dev/null; then return 0; fi
  children="$(pgrep -P "$pid" 2>/dev/null)" || children=""
  for child in $children; do kill_tree "$child"; done
  if kill -TERM "$pid" 2>/dev/null; then
    if ! kill -CONT "$pid" 2>/dev/null; then return 0; fi
  fi
}

safe_name() { echo "$1" | sed 's#[^A-Za-z0-9_-]#_#g'; }

rel() { echo "${1#"$ROOT"/}"; }

new_runid() { echo "$(date +%Y%m%d-%H%M%S)-$$-$RANDOM"; }

find_elspais() {
  if command -v elspais >/dev/null 2>&1; then
    command -v elspais
  elif [ -x "$ROOT/.venv/bin/elspais" ]; then
    echo "$ROOT/.venv/bin/elspais"
  else
    return 1
  fi
}

# ---------------------------------------------------------------------------
# The test targets, as .elspais.toml defines them
# ---------------------------------------------------------------------------

TARGETS_JSON=""

# Loads the configured targets once per process.
load_targets() {
  [ -n "$TARGETS_JSON" ] && return 0
  local bin
  if ! bin="$(find_elspais)"; then
    echo "elspais not found on PATH or in .venv/bin (pip install elspais; see .github/versions.env)" >&2
    exit 2
  fi
  ELSPAIS_BIN="$bin"
  export ELSPAIS_BIN
  if ! TARGETS_JSON="$("$bin" -C "$ROOT" config get scanning.test.targets)"; then
    echo "could not read the test targets from .elspais.toml" >&2
    exit 2
  fi
}

# The targets of the given groups (or target names), in declaration order,
# one per line. A target that claims no group is in `default`; every target
# is in `all`.
targets_of() {
  load_targets
  printf '%s' "$TARGETS_JSON" | python3 -c '
import json, sys
wanted = {w.lower() for w in sys.argv[1:]}
for t in json.load(sys.stdin):
    groups = {g.lower() for g in (t.get("groups") or ["default"])} | {"all"}
    if groups & wanted or t["name"] in sys.argv[1:]:
        print(t["name"])
' "$@"
}

# A field of a target (name, cwd, command, results, coverage, ...).
target_field() {
  load_targets
  printf '%s' "$TARGETS_JSON" | python3 -c '
import json, sys
for t in json.load(sys.stdin):
    if t["name"] == sys.argv[1]:
        print(t.get(sys.argv[2]) or "")
' "$1" "$2"
}

# Whether a target needs Postgres (it is in the postgres or throughput group).
target_needs_postgres() {
  targets_of postgres throughput | grep -qxF "$1"
}

# Every package directory a target runs in, in declaration order.
target_packages() {
  load_targets
  printf '%s' "$TARGETS_JSON" | python3 -c '
import json, sys
seen = []
for t in json.load(sys.stdin):
    cwd = t.get("cwd") or "."
    if cwd not in seen:
        seen.append(cwd)
print("\n".join(seen))
'
}

is_flutter_package() { grep -q 'sdk: flutter' "$ROOT/$1/pubspec.yaml"; }

# The failing tests a target's results files record, as "file: test name".
failing_results() {
  local name="$1" cwd results
  cwd="$(target_field "$name" cwd)"
  results="$(target_field "$name" results)"
  [ -n "$results" ] || return 0
  python3 - "$ROOT/$cwd" "$results" "$ROOT" <<'PY'
import glob, json, os, sys
base, pattern, root = sys.argv[1:4]
seen = set()
for path in sorted(glob.glob(os.path.join(base, pattern))):
    suites, tests = {}, {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if not isinstance(ev, dict):
                continue
            kind = ev.get("type")
            if kind == "suite":
                suites[ev["suite"]["id"]] = ev["suite"].get("path") or ""
            elif kind == "testStart":
                t = ev["test"]
                tests[t["id"]] = (t.get("name", ""), t.get("suiteID"))
            elif kind == "testDone" and not ev.get("hidden") and not ev.get("skipped"):
                if ev.get("result") in ("failure", "error") and ev.get("testID") in tests:
                    name, suite = tests[ev["testID"]]
                    where = os.path.relpath(suites.get(suite, "") or "?", root)
                    seen.add(f"{where}: {name}")
for entry in sorted(seen):
    print(entry)
PY
}

# ---------------------------------------------------------------------------
# Postgres containers
# ---------------------------------------------------------------------------

CONTAINER_LABEL=""

remove_containers() {
  local ids
  if [ -n "$CONTAINER_LABEL" ]; then
    ids="$(docker ps -aq --filter "label=$CONTAINER_LABEL" 2>/dev/null)" || ids=""
    if [ -n "$ids" ]; then
      # shellcheck disable=SC2086 # one id per word
      if ! docker rm -f -v $ids >/dev/null 2>&1; then
        echo "warning: could not remove containers labelled $CONTAINER_LABEL" >&2
      fi
    fi
    CONTAINER_LABEL=""
  fi
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found: install Docker, or set PG_TEST_URL (or PG_TEST_URLS) to existing servers" >&2
    exit 2
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "docker is not running (docker info failed): start it, or set PG_TEST_URL" >&2
    exit 2
  fi
  if ! docker image inspect "$PG_IMAGE" >/dev/null 2>&1; then
    echo "pulling $PG_IMAGE ..." >&2
    if ! docker pull -q "$PG_IMAGE" >/dev/null; then
      echo "could not pull $PG_IMAGE" >&2
      exit 2
    fi
  fi
}

# Starts a container with a free host port Docker chooses; prints nothing.
pg_start() {
  local name="$1" label="$2"
  docker run -d --rm --name "$name" --label "$label" \
    -e POSTGRES_USER="$PG_USER" -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DB" -p 127.0.0.1::5432 "$PG_IMAGE" \
    -c fsync=off -c synchronous_commit=off -c full_page_writes=off >/dev/null
}

# Waits for the container's server (over TCP, so not the image's
# initialization-time server) and prints its URL.
pg_ready_url() {
  local name="$1" tries=0 port
  while ! docker exec "$name" pg_isready -q -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -gt 120 ]; then
      echo "container $name did not become ready" >&2
      return 1
    fi
    sleep 1
  done
  port="$(docker port "$name" 5432/tcp | head -1 | sed 's/.*://')"
  if [ -z "$port" ]; then
    echo "container $name has no published port" >&2
    return 1
  fi
  echo "postgres://$PG_USER:$PG_PASSWORD@127.0.0.1:$port/$PG_DB"
}

# ---------------------------------------------------------------------------
# Coverage: merges lcov fragments (one per piece or shard)
# ---------------------------------------------------------------------------

# Merges the *.info fragments in <dir>/lcov/ into <dir>/lcov.info, summing
# the hits of each (file, line); elspais reads one lcov file per target and
# keeps only the last record of a file it sees twice. With no fragment (no run
# covered a line of the package) the file is empty: it measured nothing.
merge_lcov_dir() {
  local dir="$1"
  set -- "$dir"/lcov/*.info
  if [ ! -f "$1" ]; then
    : >"$dir/lcov.info"
    return 0
  fi
  cat "$@" | awk '
    /^SF:/ { sf = substr($0, 4); if (!(sf in seen)) { seen[sf] = 1; order[++n] = sf }; next }
    /^DA:/ {
      split(substr($0, 4), a, ",")
      key = sf SUBSEP a[1]
      if (!(key in hits)) { lines[sf] = lines[sf] " " a[1]; hits[key] = 0 }
      hits[key] += a[2]
      next
    }
    END {
      for (i = 1; i <= n; i++) {
        sf = order[i]
        print "SF:" sf
        m = split(lines[sf], ls, " ")
        lf = 0; lh = 0
        for (j = 1; j <= m; j++) {
          if (ls[j] == "") continue
          h = hits[sf SUBSEP ls[j]]
          print "DA:" ls[j] "," h
          lf++
          if (h > 0) lh++
        }
        print "LF:" lf
        print "LH:" lh
        print "end_of_record"
      }
    }' >"$dir/lcov.info.tmp.$$"
  mv "$dir/lcov.info.tmp.$$" "$dir/lcov.info"
}

# Every package's coverage/<suite>/lcov/ directory, merged (CI's gate job runs
# this after downloading every job's fragments).
merge_all_coverage() {
  local d
  find "$ROOT" -type d -path '*/coverage/*/lcov' ! -path '*/.dart_tool/*' ! -path '*/build/*' |
    while IFS= read -r d; do
      merge_lcov_dir "$(dirname "$d")"
      echo "merged $(rel "$(dirname "$d")")/lcov.info"
    done
}

# ---------------------------------------------------------------------------
# `target <kind>`: the commands of the targets that shard, split or skip.
# Run from the target's package directory.
# ---------------------------------------------------------------------------

TGT_PIDS=""

target_cleanup() {
  local pid
  for pid in $TGT_PIDS; do kill_tree "$pid"; done
  TGT_PIDS=""
  remove_containers
}

target_signal() {
  echo "interrupted; stopping and removing containers" >&2
  target_cleanup
  exit 130
}

# Sets TLOG (this target's log directory) and PKG (the package, relative to
# the repository root).
target_setup() {
  local kind="$1"
  PKG="${PWD#"$ROOT"/}"
  if [ "$PKG" = "$PWD" ]; then
    echo "target $kind must run from a package directory under $ROOT (got $PWD)" >&2
    exit 2
  fi
  local base="${LOGDIR:-$ROOT/.check-logs/$(new_runid)}"
  TLOG="$base/$(safe_name "$PKG-$kind")"
  mkdir -p "$TLOG"
  trap target_cleanup EXIT
  trap target_signal INT TERM
}

# Prints the given machine-JSON files on stdout, one after another.
emit_results() {
  local f
  for f in "$@"; do
    [ -f "$f" ] || continue
    cat "$f"
  done
}

# The test files of piece $1 of $2 of this package's unit suite: every
# *_test.dart under test/ outside test/web/, the largest first onto the piece
# with the fewest bytes so far.
unit_piece_files() {
  find test -name '*_test.dart' ! -path 'test/web/*' -exec wc -c {} + |
    awk '$2 != "total" { print $1 "\t" $2 }' | sort -t "$(printf '\t')" -k1,1nr -k2,2 |
    awk -F'\t' -v want="$1" -v n="$2" '
      BEGIN { for (i = 1; i <= n; i++) load[i] = 0 }
      {
        best = 1
        for (i = 2; i <= n; i++) if (load[i] < load[best]) best = i
        load[best] += $1
        if (best == want) print $2
      }'
}

target_unit() {
  target_setup unit
  local out="coverage/unit" n="$UNIT_PIECES" i rc failed=0 pid
  rm -rf "$out"
  mkdir -p "$out/lcov"
  local nfiles
  nfiles="$(find test -name '*_test.dart' ! -path 'test/web/*' | wc -l | tr -d ' ')"
  if [ "$n" -gt "$nfiles" ]; then n="$nfiles"; fi
  if [ "$n" -lt 1 ]; then
    echo "no test files under $PKG/test" >&2
    return 1
  fi
  local pids="" start=$SECONDS
  i=1
  while [ "$i" -le "$n" ]; do
    (
      files=()
      while IFS= read -r f; do files+=("$f"); done < <(unit_piece_files "$i" "$n")
      [ "${#files[@]}" -gt 0 ] || exit 0
      s=$SECONDS
      prc=0
      env -u PG_TEST_URL -u PG_TEST_URLS -u PG_TEST_URL_OTHER_SERVER -u EVS_THROUGHPUT_TEST \
        flutter test --no-pub --no-test-assets --concurrency=1 -r failures-only \
        --file-reporter="json:$out/piece-$i-of-$n.jsonl" \
        --coverage --coverage-path="$out/lcov/piece-$i-of-$n.info" \
        "${files[@]}" >"$TLOG/piece-$i-of-$n.log" 2>&1 </dev/null || prc=$?
      # 79: a piece in which no test ran (every test of its files gated off).
      if [ "$prc" -eq 79 ]; then prc=0; fi
      if [ "$prc" -eq 0 ]; then tag=PASS; else tag=FAIL; fi
      printf '%-4s  %s unit piece %s/%s  %ss  (log: %s)\n' "$tag" "$PKG" "$i" "$n" \
        $((SECONDS - s)) "$(rel "$TLOG/piece-$i-of-$n.log")" >&2
      exit "$prc"
    ) &
    pids="$pids $!"
    TGT_PIDS="$TGT_PIDS $!"
    i=$((i + 1))
  done
  for pid in $pids; do
    rc=0
    wait "$pid" || rc=$?
    if [ "$rc" -ne 0 ]; then failed=1; fi
  done
  TGT_PIDS=""
  merge_lcov_dir "$out"
  emit_results "$out"/piece-*.jsonl
  echo "$PKG unit: $n piece(s), $((SECONDS - start))s" >&2
  return "$failed"
}

target_desktop() {
  target_setup desktop
  local out="coverage/desktop" rc=0
  rm -rf "$out"
  mkdir -p "$out"
  if [ "$(uname -s)" != "Linux" ]; then
    echo "SKIP  $PKG desktop: the desktop integration test runs on Linux only" >&2
    return 0
  fi
  if ! command -v xvfb-run >/dev/null 2>&1; then
    echo "SKIP  $PKG desktop: xvfb-run not found (install xvfb)" >&2
    return 0
  fi
  # GDK_BACKEND=x11: without it the app opens on a Wayland session instead
  # of the virtual display.
  GDK_BACKEND=x11 xvfb-run -a flutter test --no-pub -r failures-only \
    --file-reporter="json:$out/dual_pane.jsonl" \
    integration_test/dual_pane_test.dart -d linux >"$TLOG/desktop.log" 2>&1 </dev/null || rc=$?
  emit_results "$out/dual_pane.jsonl"
  if [ "$rc" -ne 0 ]; then
    echo "FAIL  $PKG desktop (log: $(rel "$TLOG/desktop.log"))" >&2
  fi
  return "$rc"
}

# The Postgres-gated test files of this package, one per line: a _test.dart
# file under test/ that names PG_TEST_URL, calls testPostgresUrl( or imports
# test_postgres_url.dart (event_sourcing/test/ci/postgres_ci_listing_test.dart
# holds the same rule and checks the two agree).
postgres_gated_files() {
  find test -name '*_test.dart' | sort | while IFS= read -r f; do
    if grep -q -e 'PG_TEST_URL' -e 'testPostgresUrl(' -e 'test_postgres_url.dart' "$f"; then
      echo "$f"
    fi
  done
}

# The files a Postgres target runs: `postgres` leaves the throughput guard
# out, `throughput` runs only it.
postgres_target_files() {
  local kind="$1" f
  postgres_gated_files | while IFS= read -r f; do
    case "$kind" in
      postgres) [ "$f" = "$THROUGHPUT_FILE" ] && continue ;;
      throughput) [ "$f" = "$THROUGHPUT_FILE" ] || continue ;;
    esac
    echo "$f"
  done
}

# Estimated seconds for a Postgres file: the last recorded duration, else a
# heuristic (the known long files first, then source size). The heuristic is
# what CI uses (it keeps no durations), so every job of a PG_PART matrix
# computes the same plan.
file_weight() {
  local key="$1" w=""
  if [ -f "$DURATIONS_FILE" ] && [ -z "${CI:-}" ]; then
    w="$(awk -F'\t' -v k="$key" '$2 == k { v = $1 } END { if (v != "") print v }' "$DURATIONS_FILE")"
  fi
  if [ -n "$w" ]; then
    echo "$w"
    return
  fi
  case "$key" in
    *postgres_throughput_guard_test.dart) echo 900 ;;
    *postgres_view_convergence_measured_test.dart) echo 300 ;;
    *postgres_runtime_role_test.dart) echo 320 ;;
    *)
      local bytes
      bytes="$(wc -c <"$ROOT/$key" | tr -d ' ')"
      echo $((15 + bytes / 2000))
      ;;
  esac
}

# Splits the files listed in $1 across $2 buckets; writes $3/<n>.list
# ("<path>\t<pieces>\t<piece index>" lines). A file estimated at more than
# half a bucket's share is cut into pieces of whole tests (the test runner's
# --total-shards and --shard-index), at most one per test and one per bucket.
# The pieces then go, longest first, onto the least loaded bucket.
plan_buckets() {
  local listfile="$1" want="$2" dir="$3" i best path w k tests total=0 target
  local loads=()
  local tab
  tab="$(printf '\t')"
  : >"$dir/weights"
  while IFS= read -r path; do
    w="$(file_weight "$PKG/$path")"
    total=$((total + w))
    printf '%s\t%s\n' "$w" "$path" >>"$dir/weights"
  done <"$listfile"
  target=$((total / want))
  if [ "$target" -lt 1 ]; then target=1; fi
  : >"$dir/units"
  while IFS="$tab" read -r w path; do
    k=1
    if [ "$want" -gt 1 ] && [ $((2 * w)) -gt "$target" ]; then
      tests="$(grep -cE '^[[:space:]]*test\(' "$path")" || tests=1
      k=$(((2 * w + target - 1) / target))
      if [ "$k" -gt "$tests" ]; then k="$tests"; fi
      if [ "$k" -gt "$want" ]; then k="$want"; fi
      if [ "$k" -lt 1 ]; then k=1; fi
    fi
    i=0
    while [ "$i" -lt "$k" ]; do
      printf '%s\t%s\t%s\t%s\n' $((w / k)) "$path" "$k" "$i" >>"$dir/units"
      i=$((i + 1))
    done
  done <"$dir/weights"
  sort -t "$tab" -k1,1nr -k2,2 -k4,4n "$dir/units" >"$dir/units.sorted"
  i=1
  while [ "$i" -le "$want" ]; do
    loads[i]=0
    : >"$dir/$i.list"
    i=$((i + 1))
  done
  while IFS="$tab" read -r w path k idx; do
    best=1
    i=2
    while [ "$i" -le "$want" ]; do
      if [ "${loads[i]}" -lt "${loads[best]}" ]; then best=$i; fi
      i=$((i + 1))
    done
    loads[best]=$((loads[best] + w))
    printf '%s\t%s\t%s\n' "$path" "$k" "$idx" >>"$dir/$best.list"
  done <"$dir/units.sorted"
  PLAN_LOADS=""
  i=1
  while [ "$i" -le "$want" ]; do
    PLAN_LOADS="$PLAN_LOADS ${i}:~${loads[i]}s"
    i=$((i + 1))
  done
}

# Runs one shard's list one file (or piece of a file) at a time against its
# server; appends the machine JSON of each run to the shard's results file.
run_shard() {
  local list="$1" url="$2" other="$3" kind="$4" results="$5" lcovdir="$6" tag="$7"
  local path k idx key log s rc failed=0 seq=0 extra piece
  local tab
  tab="$(printf '\t')"
  : >"$results"
  while IFS="$tab" read -r path k idx; do
    [ -n "$path" ] || continue
    seq=$((seq + 1))
    key="$PKG/$path"
    piece=""
    extra=()
    if [ "$k" -gt 1 ]; then
      piece=" (tests piece $((idx + 1))/$k)"
      extra=(--total-shards "$k" --shard-index "$idx")
    fi
    if [ "$kind" != throughput ]; then
      extra+=(--coverage --coverage-path="$lcovdir/$tag-$seq.info")
    fi
    log="$TLOG/$(safe_name "$path")$([ "$k" -gt 1 ] && echo ".piece$((idx + 1))of$k").log"
    s=$SECONDS
    rc=0
    env -u EVS_THROUGHPUT_TEST -u PG_TEST_URLS PG_TEST_URL="$url" PG_TEST_URL_OTHER_SERVER="$other" \
      sh -c 'if [ "$0" = throughput ]; then EVS_THROUGHPUT_TEST=1; export EVS_THROUGHPUT_TEST; fi
             exec flutter test --no-pub --no-test-assets -r failures-only "$@"' \
      "$kind" --file-reporter="json:$TLOG/run-$tag-$seq.jsonl" "${extra[@]}" "$path" \
      >"$log" 2>&1 </dev/null || rc=$?
    # 79: a piece of a split file that no test fell into.
    if [ "$rc" -eq 79 ] && [ "$k" -gt 1 ]; then rc=0; fi
    if [ -f "$TLOG/run-$tag-$seq.jsonl" ]; then
      cat "$TLOG/run-$tag-$seq.jsonl" >>"$results"
    fi
    printf '%s\t%s\n' $((SECONDS - s)) "$key" >>"$TLOG/durations"
    if [ "$rc" -eq 0 ]; then
      printf 'PASS  %s%s  %ss\n' "$key" "$piece" $((SECONDS - s)) >&2
    else
      failed=1
      printf 'FAIL  %s%s  %ss  (log: %s)\n' "$key" "$piece" $((SECONDS - s)) "$(rel "$log")" >&2
    fi
  done <"$list"
  return "$failed"
}

# Merges this run's per-file durations into .check-durations.
record_durations() {
  local new="$1"
  [ -s "$new" ] || return 0
  # A split file's pieces add up to the file's duration.
  awk -F'\t' 'NF == 2 { d[$2] += $1 } END { for (k in d) print d[k] "\t" k }' "$new" >"$new.sum"
  if [ -f "$DURATIONS_FILE" ]; then
    cat "$DURATIONS_FILE" "$new.sum"
  else
    cat "$new.sum"
  fi | awk -F'\t' 'NF == 2 { d[$2] = $1 } END { for (k in d) print d[k] "\t" k }' |
    sort -t "$(printf '\t')" -k2,2 >"$DURATIONS_FILE.tmp.$$"
  mv "$DURATIONS_FILE.tmp.$$" "$DURATIONS_FILE"
}

target_postgres() {
  local kind="$1"
  target_setup "$kind"
  local out="coverage/$kind" start=$SECONDS
  rm -rf "$out"
  mkdir -p "$out/lcov" "$TLOG/plan"
  postgres_target_files "$kind" >"$TLOG/files"
  if [ ! -s "$TLOG/files" ]; then
    echo "no Postgres-gated files for the $kind target in $PKG" >&2
    return 1
  fi
  : >"$TLOG/durations"

  # The servers: given (PG_TEST_URLS, else PG_TEST_URL), else containers.
  local urls="" nservers own=0
  if [ -n "${PG_TEST_URLS:-}" ]; then
    urls="$PG_TEST_URLS"
  elif [ -n "${PG_TEST_URL:-}" ]; then
    urls="$PG_TEST_URL"
  fi
  if [ -n "$urls" ]; then
    # shellcheck disable=SC2086 # one URL per word
    set -- $urls
    nservers=$#
  else
    own=1
    nservers="$SHARDS"
    if [ "$kind" = throughput ]; then nservers=1; fi
  fi

  # The plan: PART_N slices of nservers buckets each; this run takes slice
  # PART_I.
  local nbuckets=$((PART_N * nservers)) first last b j
  plan_buckets "$TLOG/files" "$nbuckets" "$TLOG/plan"
  first=$(((PART_I - 1) * nservers + 1))
  last=$((PART_I * nservers))
  local mine=() need_other=0
  b=$first
  while [ "$b" -le "$last" ]; do
    if [ -s "$TLOG/plan/$b.list" ]; then
      mine+=("$b")
      if grep -q "^$OTHER_SERVER_FILE	" "$TLOG/plan/$b.list"; then need_other=1; fi
    fi
    b=$((b + 1))
  done
  echo "$PKG $kind: $(wc -l <"$TLOG/files" | tr -d ' ') files, part $PART_I/$PART_N, ${#mine[@]} shard(s), estimated load:$PLAN_LOADS" >&2
  if [ "${#mine[@]}" -eq 0 ]; then
    echo "$PKG $kind: nothing falls in part $PART_I/$PART_N" >&2
    return 0
  fi

  local other="${PG_TEST_URL_OTHER_SERVER:-}" shard_urls=()
  if [ "$own" -eq 1 ]; then
    require_docker
    local runid
    runid="$(new_runid)"
    CONTAINER_LABEL="evs-checks.run=$runid"
    local t0=$SECONDS name ok=1
    j=1
    while [ "$j" -le "${#mine[@]}" ]; do
      name="evs-checks-$runid-$j"
      if ! pg_start "$name" "$CONTAINER_LABEL"; then
        echo "could not start container $name" >&2
        return 1
      fi
      j=$((j + 1))
    done
    if [ "$need_other" -eq 1 ] && [ -z "$other" ]; then
      if ! pg_start "evs-checks-$runid-other" "$CONTAINER_LABEL"; then
        echo "could not start container evs-checks-$runid-other" >&2
        return 1
      fi
    fi
    j=1
    while [ "$j" -le "${#mine[@]}" ]; do
      if ! shard_urls[j]="$(pg_ready_url "evs-checks-$runid-$j")"; then ok=0; fi
      j=$((j + 1))
    done
    if [ "$need_other" -eq 1 ] && [ -z "$other" ]; then
      if ! other="$(pg_ready_url "evs-checks-$runid-other")"; then ok=0; fi
    fi
    if [ "$ok" -ne 1 ]; then
      echo "Postgres containers failed to start" >&2
      return 1
    fi
    echo "$PKG $kind: ${#mine[@]} container(s) ready ($PG_IMAGE, $((SECONDS - t0))s)" >&2
  else
    # shellcheck disable=SC2086 # one URL per word
    set -- $urls
    j=1
    for b in "${mine[@]}"; do
      shard_urls[j]="$1"
      shift
      j=$((j + 1))
    done
  fi
  if [ "$need_other" -eq 1 ] && [ -z "$other" ]; then
    echo "note: PG_TEST_URL_OTHER_SERVER unset; the second-server check skips" >&2
  fi

  local pids="" pid rc failed=0 tag files_out=()
  j=1
  for b in "${mine[@]}"; do
    tag="part${PART_I}of${PART_N}-shard$j"
    files_out+=("$out/$tag.jsonl")
    run_shard "$TLOG/plan/$b.list" "${shard_urls[j]}" "$other" "$kind" \
      "$out/$tag.jsonl" "$out/lcov" "$tag" &
    pids="$pids $!"
    TGT_PIDS="$TGT_PIDS $!"
    j=$((j + 1))
  done
  for pid in $pids; do
    rc=0
    wait "$pid" || rc=$?
    if [ "$rc" -ne 0 ]; then failed=1; fi
  done
  TGT_PIDS=""
  remove_containers
  record_durations "$TLOG/durations"
  if [ "$kind" != throughput ]; then merge_lcov_dir "$out"; fi
  emit_results "${files_out[@]}"
  echo "$PKG $kind: done in $((SECONDS - start))s" >&2
  return "$failed"
}

run_target_kind() {
  case "$1" in
    unit) target_unit ;;
    desktop) target_desktop ;;
    postgres | throughput) target_postgres "$1" ;;
    *)
      echo "unknown target kind: $1 (unit, postgres, throughput, desktop)" >&2
      return 2
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Tasks of a make run (each its own process: `run-checks.sh _task <task>`)
#   analyze:<package>      the package's analyzer
#   targets:<t1>,<t2>,...  one `elspais checks --run-tests` over the targets
# ---------------------------------------------------------------------------

task_label() {
  case "$1" in
    analyze:*) echo "analyze ${1#analyze:}" ;;
    targets:*) echo "${1#targets:}" | tr ',' ' ' ;;
    gate) echo "elspais checks (gate)" ;;
    *) echo "$1" ;;
  esac
}

# Writes a task's status record and prints its one-line result.
#   report <result> <secs> <label> <log> [note]
report() {
  local result="$1" secs="$2" label="$3" log="$4" note="${5:-}" tag
  if [ -f "$LOGDIR/interrupted" ]; then return 0; fi
  case "$result" in
    pass) tag="PASS" ;;
    fail) tag="FAIL" ;;
    *) tag="SKIP" ;;
  esac
  printf '%s|%s|%s|%s|%s\n' "$result" "$secs" "$label" "$log" "$note" \
    >"$LOGDIR/status/$(safe_name "$label")"
  if [ -n "$note" ]; then
    printf '%-4s  %-52s %5ss  %s\n' "$tag" "$label" "$secs" "$note"
  else
    printf '%-4s  %-52s %5ss\n' "$tag" "$label" "$secs"
  fi
}

CHILD=""
on_task_signal() {
  if [ -n "$CHILD" ]; then kill_tree "$CHILD"; fi
  exit 143
}
run_logged() {
  local log="$1"
  shift
  "$@" >>"$log" 2>&1 </dev/null &
  CHILD=$!
  local rc=0
  wait "$CHILD" || rc=$?
  CHILD=""
  return "$rc"
}

task_analyze() {
  local pkg="$1" label="analyze $1" log start rc=0
  log="$LOGDIR/$(safe_name "analyze $pkg").log"
  start=$SECONDS
  if is_flutter_package "$pkg"; then
    run_logged "$log" sh -c 'cd "$0" && flutter analyze --no-pub' "$ROOT/$pkg" || rc=$?
  else
    run_logged "$log" sh -c 'cd "$0" && dart analyze' "$ROOT/$pkg" || rc=$?
  fi
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

# Records one status line per target from an elspais log's runner lines
# ("<<< name: passed (12.3s)" / "<<< name: FAILED ..."). A target's status is
# its runner's: the checks each invocation runs afterwards read every results
# file on disk, including those of targets still running or left by earlier
# runs, so they are advisory here and a full run's gate is the verdict. With
# a fifth argument of 1 (test-all's one invocation over every target), a
# failed check counts too.
report_elspais_run() {
  local log="$1" rc="$2" secs="$3" names="$4" count_checks="${5:-0}"
  local name line result tsecs note any_fail=0
  for name in $names; do
    line="$(strip_ansi <"$log" | grep -F "<<< $name: " | tail -1)" || line=""
    note=""
    case "$line" in
      *": passed "*) result=pass ;;
      *": FAILED"*) result=fail; any_fail=1 ;;
      *) result=fail; any_fail=1; note="no runner result (crashed or interrupted)" ;;
    esac
    tsecs="$(echo "$line" | sed -n 's/.*(\([0-9]*\)\.[0-9]*s)[[:space:]]*$/\1/p')"
    [ -n "$tsecs" ] || tsecs="$secs"
    if [ -n "$tsecs" ] && [ -z "$note" ] && [ -z "${CI:-}" ]; then
      printf '%s\ttarget:%s\n' "$tsecs" "$name" >>"$LOGDIR/target-durations"
    fi
    report "$result" "$tsecs" "$name" "$log" "$note"
  done
  if [ "$rc" -ne 0 ] && [ "$any_fail" -eq 0 ] && [ "$count_checks" = 1 ]; then
    report fail "$secs" "elspais checks after $(echo "$names" | tr ' ' ',')" "$log" "the checks failed"
  fi
}

task_targets() {
  local names start rc=0 log
  names="$(echo "$1" | tr ',' ' ')"
  log="$LOGDIR/$(safe_name "targets $1").log"
  start=$SECONDS
  local args=()
  local n
  for n in $names; do args+=("$n"); done
  # shellcheck disable=SC2086 # ELSPAIS_ARGS is a flag list
  run_logged "$log" sh -c 'd="$0"; b="$1"; shift; cd "$d" && exec "$b" checks --run-tests "$@"' \
    "$ROOT" "$ELSPAIS_BIN" $ELSPAIS_ARGS --targets "${args[@]}" || rc=$?
  report_elspais_run "$log" "$rc" $((SECONDS - start)) "$names"
}

run_task() {
  trap on_task_signal TERM INT
  case "$1" in
    analyze:*) task_analyze "${1#analyze:}" ;;
    targets:*) task_targets "${1#targets:}" ;;
    *)
      echo "unknown task: $1" >&2
      return 2
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Orchestration (top-level process of a make run)
# ---------------------------------------------------------------------------

POOL_PID=""

cleanup() {
  if [ -n "$POOL_PID" ]; then kill_tree "$POOL_PID"; fi
  POOL_PID=""
}

on_signal() {
  if [ -n "${LOGDIR:-}" ] && [ -d "$LOGDIR" ]; then : >"$LOGDIR/interrupted"; fi
  echo "" >&2
  echo "interrupted; stopping targets (each removes its own containers)" >&2
  cleanup
  exit 130
}

new_run() {
  RUNID="$(new_runid)"
  LOGDIR="$ROOT/.check-logs/$RUNID"
  mkdir -p "$LOGDIR/status"
  : >"$LOGDIR/order"
  export LOGDIR RUNID
  RUN_START=$SECONDS
  trap cleanup EXIT
  trap on_signal INT TERM
  load_targets
  echo "run $RUNID  (logs: $(rel "$LOGDIR"))"
}

# `pub get` in each package the selected targets (or analyzers) run in, before
# anything runs: the target commands pass --no-pub, so targets sharing a
# package never resolve it under each other.
pub_get() {
  local pkg log="$LOGDIR/pub_get.log"
  for pkg in "$@"; do
    echo "== $pkg" >>"$log"
    if is_flutter_package "$pkg"; then
      if ! (cd "$ROOT/$pkg" && flutter pub get) >>"$log" 2>&1; then
        echo "FAIL  pub get $pkg (see $(rel "$log"))"
        exit 1
      fi
    else
      if ! (cd "$ROOT/$pkg" && dart pub get) >>"$log" 2>&1; then
        echo "FAIL  pub get $pkg (see $(rel "$log"))"
        exit 1
      fi
    fi
  done
}

# The packages the given targets run in.
packages_of() {
  local t
  for t in "$@"; do target_field "$t" cwd; done | awk '!seen[$0]++'
}

# Estimated seconds for a target: its last recorded duration, else a guess.
target_weight() {
  local w=""
  if [ -f "$DURATIONS_FILE" ]; then
    w="$(awk -F'\t' -v k="target:$1" '$2 == k { v = $1 } END { if (v != "") print v }' "$DURATIONS_FILE")"
  fi
  if [ -n "$w" ]; then
    echo "$w"
    return
  fi
  case "$1" in
    */postgres) echo 600 ;;
    event_sourcing) echo 400 ;;
    */throughput) echo 300 ;;
    */desktop) echo 120 ;;
    */example*) echo 80 ;;
    *) echo 20 ;;
  esac
}

# Builds the task list for the given targets into TASKS, longest first. With
# existing servers given (PG_TEST_URL, PG_TEST_URLS), the targets that need
# Postgres share them, so they run in one task, one after another.
TASKS=()
target_tasks() {
  local t pg=""
  local tab
  tab="$(printf '\t')"
  for t in "$@"; do
    if { [ -n "${PG_TEST_URL:-}" ] || [ -n "${PG_TEST_URLS:-}" ]; } && target_needs_postgres "$t"; then
      pg="$pg,$t"
    else
      printf '%s\t%s\n' "$(target_weight "$t")" "targets:$t"
    fi
  done >"$LOGDIR/tasks"
  if [ -n "$pg" ]; then
    echo "note: existing Postgres servers given, so the Postgres targets run one after another on them"
    local w=0
    for t in $(echo "${pg#,}" | tr ',' ' '); do w=$((w + $(target_weight "$t"))); done
    printf '%s\t%s\n' "$w" "targets:${pg#,}" >>"$LOGDIR/tasks"
  fi
  while IFS="$tab" read -r _ t; do TASKS+=("$t"); done < <(sort -t "$tab" -k1,1nr "$LOGDIR/tasks")
}

# Runs the given tasks, at most $1 at a time, in the foreground.
run_pool() {
  local jobs="$1" t
  shift
  [ "$#" -gt 0 ] || return 0
  for t in "$@"; do echo "$t" >>"$LOGDIR/order"; done
  printf '%s\n' "$@" | xargs -P "$jobs" -I{} "$SELF" _task {} &
  POOL_PID=$!
  if ! wait "$POOL_PID"; then
    echo "some pool task failed" >/dev/null
  fi
  POOL_PID=""
}

# The final gate: one `elspais checks` over every result on disk, built
# locally (--spec-dir forces a local graph; the daemon does not watch the
# results files), then results_ingested (a lenient gate passes the "no
# results ingested" warning).
run_gate() {
  local log="$LOGDIR/gate.log" start=$SECONDS rc=0
  echo "gate" >>"$LOGDIR/order"
  # shellcheck disable=SC2086 # ELSPAIS_ARGS is a flag list
  run_logged "$log" sh -c 'cd "$0" && exec "$1" --spec-dir spec checks $2' "$ROOT" "$ELSPAIS_BIN" "$ELSPAIS_ARGS" || rc=$?
  echo "--- results ingested (strict)" >>"$log"
  run_logged "$log" "$SELF" results-ingested || rc=$?
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "elspais checks (gate)" "$log"
  else
    report fail $((SECONDS - start)) "elspais checks (gate)" "$log"
  fi
}

# Strict about result ingestion alone: the checks tests.results (some result
# ingested, none failed), tests.ingestion_fault (every results and coverage
# file a target names was read) and tests.partial_read must pass. A lenient
# gate only reports them, and `elspais checks --check` narrows the report but
# not the exit status, so this reads elspais's JSON report.
results_ingested() {
  local bin json rc=0
  if ! bin="$(find_elspais)"; then
    echo "elspais not found on PATH or in .venv/bin" >&2
    return 2
  fi
  json="$(mktemp "${TMPDIR:-/tmp}/evs-checks.XXXXXX")"
  (cd "$ROOT" && "$bin" --spec-dir spec checks --lenient --format json -o "$json") >/dev/null || rc=$?
  if [ "$rc" -ne 0 ]; then
    rm -f "$json"
    echo "elspais checks failed (exit $rc) before the ingestion could be read" >&2
    return "$rc"
  fi
  rc=0
  python3 - "$json" <<'PY2' || rc=$?
import json, sys
checks = {c["name"]: c for c in json.load(open(sys.argv[1]))["checks"]}
bad = 0
for name in ("tests.results", "tests.ingestion_fault", "tests.partial_read"):
    c = checks.get(name)
    if c is None:
        print(f"FAIL  {name}: not in the report")
        bad = 1
        continue
    ok = c.get("passed") and not (name == "tests.results" and not (c.get("details") or {}).get("passed"))
    print(f"{'ok  ' if ok else 'FAIL'}  {name}: {c.get('message')}")
    if not ok:
        bad = 1
sys.exit(bad)
PY2
  rm -f "$json"
  return "$rc"
}

# Merges this run's target durations into .check-durations.
record_target_durations() {
  [ -s "$LOGDIR/target-durations" ] || return 0
  record_durations "$LOGDIR/target-durations"
}

summarize() {
  local task label sf result secs log note pass=0 fail=0 skip=0 t
  record_target_durations
  echo ""
  echo "---- summary (wall $((SECONDS - RUN_START))s, logs: $(rel "$LOGDIR"))"
  while IFS= read -r task; do
    case "$task" in
      targets:*)
        for t in $(echo "${task#targets:}" | tr ',' ' '); do echo "target:$t"; done
        if [ -f "$LOGDIR/status/$(safe_name "elspais checks after ${task#targets:}")" ]; then
          echo "checks:${task#targets:}"
        fi
        ;;
      *) echo "$task" ;;
    esac
  done <"$LOGDIR/order" >"$LOGDIR/order.expanded"
  while IFS= read -r task; do
    case "$task" in
      target:*) label="${task#target:}" ;;
      checks:*) label="elspais checks after ${task#checks:}" ;;
      *) label="$(task_label "$task")" ;;
    esac
    sf="$LOGDIR/status/$(safe_name "$label")"
    if [ ! -f "$sf" ]; then
      fail=$((fail + 1))
      echo "FAIL  $label: no result recorded (crashed or interrupted)"
      continue
    fi
    IFS='|' read -r result secs label log note <"$sf"
    case "$result" in
      pass) pass=$((pass + 1)) ;;
      skip) skip=$((skip + 1)) ;;
      *)
        fail=$((fail + 1))
        echo "FAIL  $label  (log: $(rel "$log"))"
        case "$task" in
          target:*)
            failing_results "$label" | head -40 | sed 's/^/      - /'
            ;;
          *)
            strip_ansi <"$log" | grep -E '^[[:space:]]*(error|warning|info|ERROR|WARN|✗|⚠)' | head -20 | sed 's/^/      /'
            ;;
        esac
        ;;
    esac
  done <"$LOGDIR/order.expanded"
  echo "---- $pass passed, $fail failed, $skip skipped"
  [ "$fail" -eq 0 ]
}

analyze_tasks() {
  local p
  target_packages | while IFS= read -r p; do echo "analyze:$p"; done
}

# Runs the targets of the given groups, one elspais invocation per target,
# JOBS at a time.
run_groups() {
  local t
  local names=()
  while IFS= read -r t; do names+=("$t"); done < <(targets_of "$@")
  if [ "${#names[@]}" -eq 0 ]; then
    echo "no targets in group(s) $*" >&2
    exit 2
  fi
  # shellcheck disable=SC2046 # one package per word
  pub_get $(packages_of "${names[@]}")
  target_tasks "${names[@]}"
  run_pool "$JOBS" "${TASKS[@]}"
}

main() {
  local cmd="${1:-help}"
  case "$cmd" in
    _task)
      run_task "$2"
      return
      ;;
    target)
      run_target_kind "${2:-}"
      return
      ;;
    target-files)
      # The files a Postgres target runs, from its package directory
      # (event_sourcing/test/ci/postgres_ci_listing_test.dart reads this).
      case "${2:-}" in
        postgres | throughput) postgres_target_files "$2" ;;
        *)
          echo "usage: run-checks.sh target-files <postgres|throughput>" >&2
          return 2
          ;;
      esac
      return
      ;;
    merge-coverage)
      merge_all_coverage
      return
      ;;
    results-ingested)
      results_ingested
      return
      ;;
    help | -h | --help)
      usage
      return 0
      ;;
    pg-up)
      require_docker
      local i url urls=""
      i=1
      mkdir -p "$ROOT/.check-logs"
      : >"$ROOT/.check-logs/pg-up.env"
      while [ "$i" -le "$SHARDS" ]; do
        if ! pg_start "evs-checks-pgup-$i" "evs-checks.run=pg-up"; then
          echo "could not start evs-checks-pgup-$i (already up? run make pg-down)" >&2
          return 1
        fi
        i=$((i + 1))
      done
      i=1
      while [ "$i" -le "$SHARDS" ]; do
        if ! url="$(pg_ready_url "evs-checks-pgup-$i")"; then return 1; fi
        echo "evs-checks-pgup-$i  $url"
        urls="$urls $url"
        i=$((i + 1))
      done
      echo "PG_TEST_URLS='${urls# }'" >>"$ROOT/.check-logs/pg-up.env"
      echo "(PG_TEST_URLS also in .check-logs/pg-up.env; remove with: make pg-down)"
      return 0
      ;;
    pg-down)
      local ids
      ids="$(docker ps -aq --filter "label=evs-checks.run=pg-up")" || ids=""
      if [ -z "$ids" ]; then
        echo "no pg-up containers running"
        return 0
      fi
      # shellcheck disable=SC2086 # one id per word
      docker rm -f -v $ids >/dev/null
      echo "removed $(echo "$ids" | wc -w | tr -d ' ') container(s)"
      rm -f "$ROOT/.check-logs/pg-up.env"
      return 0
      ;;
  esac

  new_run
  TASKS=()
  case "$cmd" in
    analyze)
      local pkgs
      pkgs="$(target_packages)"
      # shellcheck disable=SC2086 # one package per word
      pub_get $pkgs
      local a=()
      while IFS= read -r t; do a+=("$t"); done < <(analyze_tasks)
      run_pool "$JOBS" "${a[@]}"
      ;;
    test-unit) run_groups unit ;;
    test-web) run_groups web ;;
    test-desktop) run_groups desktop ;;
    test-postgres) run_groups postgres ;;
    test-demos) run_groups demos ;;
    test-throughput) run_groups throughput ;;
    elspais)
      echo "elspais" >>"$LOGDIR/order"
      local log="$LOGDIR/elspais.log" start=$SECONDS rc=0
      # shellcheck disable=SC2086 # ELSPAIS_ARGS is a flag list
      run_logged "$log" sh -c 'cd "$0" && exec "$1" --spec-dir spec checks $2' "$ROOT" "$ELSPAIS_BIN" "$ELSPAIS_ARGS" || rc=$?
      if [ "$rc" -eq 0 ]; then
        report pass $((SECONDS - start)) "elspais" "$log"
      else
        report fail $((SECONDS - start)) "elspais" "$log"
      fi
      ;;
    test-all)
      # The reference run: analyze, then one `elspais checks --run-tests` of
      # the default group, every target one after another, one Postgres
      # server per Postgres target and one unit piece.
      local pkgs a=() names
      pkgs="$(target_packages)"
      # shellcheck disable=SC2086 # one package per word
      pub_get $pkgs
      while IFS= read -r t; do a+=("$t"); done < <(analyze_tasks)
      run_pool 1 "${a[@]}"
      names="$(targets_of default | tr '\n' ' ')"
      echo "targets:$(echo "$names" | sed 's/ *$//' | tr ' ' ',')" >>"$LOGDIR/order"
      local log="$LOGDIR/elspais-run-tests.log" start=$SECONDS rc=0
      echo "elspais checks --run-tests (default group, one target at a time; log: $(rel "$log"))"
      # shellcheck disable=SC2086 # ELSPAIS_ARGS is a flag list
      run_logged "$log" env SHARDS=1 UNIT_PIECES=1 \
        sh -c 'cd "$0" && exec "$1" checks --run-tests $2' "$ROOT" "$ELSPAIS_BIN" "$ELSPAIS_ARGS" || rc=$?
      report_elspais_run "$log" "$rc" $((SECONDS - start)) "$names" 1
      run_gate
      ;;
    test-all-parallel)
      local pkgs names=() a=()
      pkgs="$(target_packages)"
      # shellcheck disable=SC2086 # one package per word
      pub_get $pkgs
      while IFS= read -r t; do names+=("$t"); done < <(targets_of default)
      target_tasks "${names[@]}"
      while IFS= read -r t; do a+=("$t"); done < <(analyze_tasks)
      # Targets longest first, then the analyzers, so the pool's tail is short.
      run_pool "$JOBS" "${TASKS[@]}" "${a[@]}"
      run_gate
      ;;
    *)
      echo "unknown target: $cmd" >&2
      echo "" >&2
      usage >&2
      return 2
      ;;
  esac
  summarize
}

# One line, so bash has read the whole script before main runs: an edit to
# this file during a run cannot change what the run executes next.
main "$@"; exit $?
