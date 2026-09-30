#!/usr/bin/env bash
# Runs the repository's checks locally: analyzers, every package's tests,
# the browser and desktop suites, the Postgres-gated files against
# throwaway Postgres containers, the throughput guard and elspais.
#
# The root Makefile calls this script; `./tools/run-checks.sh help` lists the
# commands and the variables that tune them. CI
# (.github/workflows/event-sourcing-tests.yml and conformance-tests.yml) is
# the reference for how each suite runs; this script runs the same commands.
#
# Portable to macOS (bash 3.2, BSD userland) and Linux: no associative
# arrays, no mapfile, no `wait -n`, no GNU-only tool flags.
#
# Output is one line per suite or shard; each suite's full output goes to
# .check-logs/<run id>/ and the run ends with a summary of every failing
# test. The exit status is non-zero when anything failed.

# SC2016: the single-quoted `sh -c` scripts expand their own positional
# arguments. SC2001: sed stands in for bash-4 pattern substitution idioms.
# SC2317: the trap handlers are reached through `trap`.
# shellcheck disable=SC2016,SC2001,SC2317

set -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SELF="$ROOT/tools/run-checks.sh"
DURATIONS_FILE="$ROOT/.check-durations"
WORKFLOW="$ROOT/.github/workflows/conformance-tests.yml"

FLUTTER_PACKAGES="event_sourcing reaction event_sourcing/example_action_permissions event_sourcing/example reaction/example event_sourcing/example_clinical_scopes reaction_widgets reaction_widgets_testing"
DART_PACKAGES="provenance canonical_json_jcs"
EXAMPLE_PACKAGES="event_sourcing/example_action_permissions event_sourcing/example reaction/example event_sourcing/example_clinical_scopes"
# event_sourcing's unit suite runs one test file at a time (--concurrency=1,
# as CI); its files are split into UNIT_PIECES pieces run side by side.
UNIT_SPLIT_PACKAGE="event_sourcing"
THROUGHPUT_FILE="test/storage/postgres/postgres_throughput_guard_test.dart"

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
# elspais runs with the pre-push hook's flags (.pre-commit-config.yaml), so
# warnings such as test results not ingested locally do not fail it;
# ELSPAIS_STRICT=1 drops --lenient.
ELSPAIS_ARGS="--lenient"
if [ "${ELSPAIS_STRICT:-}" = 1 ]; then ELSPAIS_ARGS=""; fi
case "$SHARDS" in '' | *[!0-9]* | 0) echo "SHARDS must be a positive integer (got '$SHARDS')" >&2; exit 2 ;; esac
case "$UNIT_PIECES" in '' | *[!0-9]* | 0) echo "UNIT_PIECES must be a positive integer (got '$UNIT_PIECES')" >&2; exit 2 ;; esac
case "$JOBS" in '' | *[!0-9]* | 0) echo "JOBS must be a positive integer (got '$JOBS')" >&2; exit 2 ;; esac
# Every task process re-reads these, so they carry the resolved values.
export SHARDS JOBS UNIT_PIECES PG_IMAGE ELSPAIS_ARGS

usage() {
  cat <<EOF
Usage: make <target> [VAR=value ...]    (or: tools/run-checks.sh <target>)

Targets:
  help               This list (the default target).
  analyze            flutter analyze --no-pub in every Flutter package and
                     dart analyze in provenance and canonical_json_jcs
                     (infos fatal, as CI).
  test-unit          Every package's tests without Postgres (PG_TEST_URL is
                     removed from their environment), packages in parallel.
  test-web           event_sourcing's browser suite (test/web/) in Chrome.
  test-desktop       event_sourcing/example's dual-pane integration test on a
                     Linux desktop build under xvfb-run (skipped without it).
  test-postgres      Every Postgres-gated file listed in
                     .github/workflows/conformance-tests.yml, sharded across
                     SHARDS throwaway Postgres containers (throughput guard
                     excluded).
  test-demos         The example packages' tests: unit, the
                     example_action_permissions Postgres files and the
                     desktop integration test.
  test-throughput    The opt-in throughput guard (EVS_THROUGHPUT_TEST=1)
                     against its own Postgres container.
  elspais            elspais checks --lenient (as the pre-push hook) from the
                     repository root.
  test-all           Everything above (throughput guard included), one suite
                     at a time, exactly as CI runs each, on one Postgres
                     container (plus the second server one file compares
                     against): the slow, simple reference run.
  test-all-parallel  Everything above (throughput guard included), JOBS
                     suites at a time with the Postgres files sharded: the
                     fast full verification.
  pg-up              Start SHARDS throwaway Postgres containers and print
                     their URLs (left running until pg-down).
  pg-down            Remove the containers pg-up started.

Variables (current value in brackets):
  SHARDS    Postgres containers the Postgres files are split across
            [$SHARDS; default cores/3, 1..8].
  JOBS      Suites run at once (unit, analyze, web, desktop) [$JOBS;
            default cores/3, 2..8].
  UNIT_PIECES
            Pieces event_sourcing's one-test-at-a-time unit suite's files
            are split into, run side by side [$UNIT_PIECES; default cores/6, 1..4].
  PG_IMAGE  Postgres image for the containers [$PG_IMAGE].
  ELSPAIS_STRICT
            1 runs elspais checks without --lenient (the pre-push hook's
            flag), so its warnings fail too [${ELSPAIS_STRICT:-unset}].
  PG_TEST_URL
            Reuse this existing server instead of starting containers
            (the Postgres files then run one at a time, no sharding). Its
            role must be able to create roles. PG_TEST_URL_OTHER_SERVER, if
            set, is passed through for the second-server check.

Logs: .check-logs/<run id>/ (one file per suite and per Postgres file).
Per-file Postgres durations: .check-durations (balances the shards).
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

# The one-line label of a task.
task_label() {
  local spec
  case "$1" in
    analyze:*) echo "analyze ${1#analyze:}" ;;
    unit:*@*)
      spec="${1#unit:}"
      echo "unit ${spec%@*} (piece ${spec#*@})"
      ;;
    unit:*) echo "unit ${1#unit:}" ;;
    web) echo "web event_sourcing (chrome)" ;;
    desktop) echo "desktop event_sourcing/example" ;;
    elspais) echo "elspais checks" ;;
    shard:*) echo "postgres shard ${1#shard:}" ;;
    *) echo "$1" ;;
  esac
}

safe_name() { echo "$1" | sed 's#[^A-Za-z0-9_-]#_#g'; }

rel() { echo "${1#"$ROOT"/}"; }

# Runs a command in the background of this task and waits for it, so a TERM
# (or an INT, where it is not ignored) reaches the whole command tree.
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

# Writes a task's status record and prints its one-line result.
#   report <result> <secs> <label> <log> [note]
report() {
  local result="$1" secs="$2" label="$3" log="$4" note="${5:-}"
  local tag
  # An interrupted run's suites end early and may exit 0 (flutter does on
  # SIGINT); they record nothing.
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

# The test targets CI passes for a package: every top-level directory under
# test/ except web/, plus the top-level test files.
unit_targets() {
  (cd "$ROOT/$1" && find test -mindepth 1 -maxdepth 1 \
    \( -type d ! -name web \) -o \( -type f -name '*_test.dart' \) | sort)
}

# CI's per-package flags. Where other `flutter test` runs use the same
# package at the same time (Postgres files, EVS_CHECKS_SHARED_PACKAGES, or
# the pieces of a split suite), --no-test-assets keeps them from rebuilding
# one build/unit_test_assets under each other (the packages declare no
# assets).
# The test files of piece $2 of $3 of a package's unit suite: the same files
# as unit_targets (every *_test.dart under test/ outside test/web/), the
# largest first onto the piece with the fewest bytes so far.
unit_piece_files() {
  (cd "$ROOT/$1" && find test -name '*_test.dart' ! -path 'test/web/*' -exec wc -c {} + |
    awk '$2 != "total" { print $1 "\t" $2 }' | sort -t "$(printf '\t')" -k1,1nr -k2,2 |
    awk -F'\t' -v want="$2" -v n="$3" '
      BEGIN { for (i = 1; i <= n; i++) load[i] = 0 }
      {
        best = 1
        for (i = 2; i <= n; i++) if (load[i] < load[best]) best = i
        load[best] += $1
        if (best == want) print $2
      }')
}

unit_args() {
  local args=""
  case "$1" in
    event_sourcing | event_sourcing/example_action_permissions) args="--concurrency=1" ;;
  esac
  local shared="${EVS_CHECKS_SHARED_PACKAGES:-}"
  if [ "$UNIT_PIECES" -gt 1 ]; then shared="$shared $UNIT_SPLIT_PACKAGE"; fi
  case " $shared " in
    *" $1 "*) args="$args --no-test-assets" ;;
  esac
  echo "$args"
}

is_dart_package() {
  case " $DART_PACKAGES " in *" $1 "*) return 0 ;; esac
  return 1
}

# The Postgres-gated files the workflow runs, as "<package dir>\t<path>"
# lines (the throughput guard included). A file counts when a step of a
# postgres* job runs it with `flutter test` and it gates on PG_TEST_URL.
postgres_files() {
  awk '
    /^  [A-Za-z0-9_-]+:[[:space:]]*$/ { job = $1; sub(/:$/, "", job); path = "" }
    /run:[[:space:]]+(flutter|dart)[[:space:]]+test[[:space:]]/ { path = $NF }
    /working-directory:/ {
      if (path != "" && job ~ /^postgres/) print $2 "\t" path
      path = ""
    }
  ' "$WORKFLOW" | while IFS="$(printf '\t')" read -r dir path; do
    if grep -q -e 'PG_TEST_URL' -e 'testPostgresUrl(' -e 'test_postgres_url.dart' \
      "$ROOT/$dir/$path"; then
      printf '%s\t%s\n' "$dir" "$path"
    fi
  done
}

# Estimated seconds for a Postgres file: the last recorded duration, else a
# heuristic (the two known long files first, then source size).
file_weight() {
  local key="$1" w=""
  if [ -f "$DURATIONS_FILE" ]; then
    w="$(awk -F'\t' -v k="$key" '$2 == k { v = $1 } END { if (v != "") print v }' "$DURATIONS_FILE")"
  fi
  if [ -n "$w" ]; then
    echo "$w"
    return
  fi
  case "$key" in
    *postgres_throughput_guard_test.dart) echo 900 ;;
    *postgres_view_convergence_measured_test.dart) echo 600 ;;
    *)
      local bytes
      bytes="$(wc -c <"$ROOT/$key" | tr -d ' ')"
      echo $((15 + bytes / 2000))
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Tasks (each runs in its own process: `run-checks.sh _task <name>`)
# ---------------------------------------------------------------------------

task_analyze() {
  local pkg="$1" label="analyze $1" log start rc=0
  log="$LOGDIR/$(safe_name "analyze $pkg").log"
  start=$SECONDS
  if is_dart_package "$pkg"; then
    run_logged "$log" sh -c "cd '$ROOT/$pkg' && dart analyze" || rc=$?
  else
    run_logged "$log" sh -c "cd '$ROOT/$pkg' && flutter analyze --no-pub" || rc=$?
  fi
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

# A package's unit tests; "<pkg>@<i>/<n>" runs piece i of n of its test
# files (unit_piece_files), each piece its own process.
task_unit() {
  local spec="$1" pkg label log start rc=0 t piece_args=""
  local targets=()
  pkg="${spec%@*}"
  label="$(task_label "unit:$spec")"
  log="$LOGDIR/$(safe_name "$label").log"
  start=$SECONDS
  if [ "$pkg" != "$spec" ]; then
    local piece="${spec#*@}"
    piece_args="piece"
    while IFS= read -r t; do targets+=("$t"); done < <(unit_piece_files "$pkg" "${piece%/*}" "${piece#*/}")
  else
    while IFS= read -r t; do targets+=("$t"); done < <(unit_targets "$pkg")
  fi
  if [ "${#targets[@]}" -eq 0 ]; then
    echo "no test targets under $pkg/test" >"$log"
    report fail 0 "$label" "$log"
    return
  fi
  if is_dart_package "$pkg"; then
    run_logged "$log" env -u PG_TEST_URL -u PG_TEST_URL_OTHER_SERVER -u EVS_THROUGHPUT_TEST \
      sh -c 'cd "$0" && dart test -r failures-only' "$ROOT/$pkg" || rc=$?
  else
    # shellcheck disable=SC2046 # unit_args is a flag list
    run_logged "$log" env -u PG_TEST_URL -u PG_TEST_URL_OTHER_SERVER -u EVS_THROUGHPUT_TEST \
      sh -c 'cd "$0" && exec flutter test --no-pub -r failures-only "$@"' \
      "$ROOT/$pkg" $(unit_args "$pkg") "${targets[@]}" || rc=$?
  fi
  # 79: a piece that no test ran in (every test of its files gated off).
  if [ "$rc" -eq 79 ] && [ -n "$piece_args" ]; then rc=0; fi
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

web_args() {
  case " ${EVS_CHECKS_SHARED_PACKAGES:-} " in
    *" event_sourcing "*) echo "--no-test-assets" ;;
    *) echo "--test-assets" ;;
  esac
}

task_web() {
  local label="web event_sourcing (chrome)" log start rc=0
  log="$LOGDIR/web_event_sourcing.log"
  start=$SECONDS
  run_logged "$log" sh -c "cd '$ROOT/event_sourcing' && flutter test --no-pub \$0 -r failures-only --platform chrome test/web/" "$(web_args)" || rc=$?
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

task_desktop() {
  local label="desktop event_sourcing/example" log start rc=0
  log="$LOGDIR/desktop_event_sourcing_example.log"
  start=$SECONDS
  if [ "$(uname -s)" != "Linux" ]; then
    echo "skipped: the desktop integration test runs on Linux only" >"$log"
    report skip 0 "$label" "$log" "skipped: Linux only"
    return
  fi
  if ! command -v xvfb-run >/dev/null 2>&1; then
    echo "skipped: xvfb-run not found (install xvfb)" >"$log"
    report skip 0 "$label" "$log" "skipped: xvfb-run not found (install xvfb)"
    return
  fi
  # GDK_BACKEND=x11: without it the app opens on a Wayland session instead
  # of the virtual display.
  run_logged "$log" sh -c "cd '$ROOT/event_sourcing/example' && GDK_BACKEND=x11 xvfb-run -a flutter test --no-pub -r failures-only integration_test/dual_pane_test.dart -d linux" || rc=$?
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

task_elspais() {
  local label="elspais checks" log start rc=0 bin=""
  log="$LOGDIR/elspais.log"
  start=$SECONDS
  if command -v elspais >/dev/null 2>&1; then
    bin="$(command -v elspais)"
  elif [ -x "$ROOT/.venv/bin/elspais" ]; then
    bin="$ROOT/.venv/bin/elspais"
  fi
  if [ -z "$bin" ]; then
    echo "elspais not found on PATH or in .venv/bin (pip install elspais)" >"$log"
    report fail 0 "$label" "$log" "elspais not found"
    return
  fi
  # shellcheck disable=SC2086 # ELSPAIS_ARGS is a flag list
  run_logged "$log" sh -c 'd="$0"; b="$1"; shift; cd "$d" && exec "$b" checks "$@"' \
    "$ROOT" "$bin" $ELSPAIS_ARGS || rc=$?
  if [ "$rc" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$log"
  else
    report fail $((SECONDS - start)) "$label" "$log"
  fi
}

# Runs one shard's Postgres files one after another against its server.
task_shard() {
  local n="$1" list="$LOGDIR/shards/$1.list" url
  local label start rc key dir path log fstart secs passed=0 failed=0 total=0
  url="$(cat "$LOGDIR/shards/$n.url")"
  label="postgres shard $n"
  start=$SECONDS
  : >"$LOGDIR/shards/$n.failed"
  : >"$LOGDIR/shards/$n.durations"
  local k idx shard_args throughput label_key
  while IFS="$(printf '\t')" read -r dir path k idx; do
    [ -n "$path" ] || continue
    total=$((total + 1))
    key="$dir/$path"
    label_key="$key"
    shard_args=""
    if [ "${k:-1}" -gt 1 ]; then
      label_key="$key (tests piece $((idx + 1))/$k)"
      shard_args="--total-shards $k --shard-index $idx"
    fi
    log="$LOGDIR/pg/$(safe_name "$key")$( [ "${k:-1}" -gt 1 ] && echo ".piece$((idx + 1))of$k").log"
    throughput=0
    if [ "$path" = "$THROUGHPUT_FILE" ]; then throughput=1; fi
    fstart=$SECONDS
    rc=0
    # shellcheck disable=SC2086 # shard_args is a flag list
    run_logged "$log" env -u EVS_THROUGHPUT_TEST PG_TEST_URL="$url" \
      PG_TEST_URL_OTHER_SERVER="$EVS_CHECKS_OTHER_URL" \
      sh -c 'd="$0"; t="$1"; shift; cd "$d" || exit 1
             if [ "$t" = 1 ]; then EVS_THROUGHPUT_TEST=1; export EVS_THROUGHPUT_TEST; fi
             exec flutter test --no-pub --no-test-assets -r failures-only "$@"' \
      "$ROOT/$dir" "$throughput" $shard_args "$path" || rc=$?
    # 79: a piece of a split file that no test fell into.
    if [ "$rc" -eq 79 ] && [ "${k:-1}" -gt 1 ]; then rc=0; fi
    secs=$((SECONDS - fstart))
    printf '%s\t%s\n' "$secs" "$key" >>"$LOGDIR/shards/$n.durations"
    if [ "$rc" -eq 0 ]; then
      passed=$((passed + 1))
    else
      failed=$((failed + 1))
      printf '%s|%s\n' "$label_key" "$log" >>"$LOGDIR/shards/$n.failed"
    fi
  done <"$list"
  local note="$total runs"
  if [ "$failed" -gt 0 ]; then note="$total runs, $failed failed"; fi
  if [ "$failed" -eq 0 ]; then
    report pass $((SECONDS - start)) "$label" "$LOGDIR/pg" "$note"
  else
    report fail $((SECONDS - start)) "$label" "$LOGDIR/pg" "$note"
  fi
}

run_task() {
  trap on_task_signal TERM INT
  case "$1" in
    analyze:*) task_analyze "${1#analyze:}" ;;
    unit:*) task_unit "${1#unit:}" ;;
    web) task_web ;;
    desktop) task_desktop ;;
    elspais) task_elspais ;;
    shard:*) task_shard "${1#shard:}" ;;
    *)
      echo "unknown task: $1" >&2
      return 2
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Postgres containers (started and removed by the top-level process only)
# ---------------------------------------------------------------------------

CONTAINER_LABEL=""
BG_PIDS=""
POOL_PID=""

cleanup() {
  local pid ids
  for pid in $BG_PIDS $POOL_PID; do kill_tree "$pid"; done
  if [ -n "$CONTAINER_LABEL" ] && [ "$CONTAINER_LABEL" != "evs-checks.run=pg-up" ]; then
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

on_signal() {
  if [ -n "${LOGDIR:-}" ] && [ -d "$LOGDIR" ]; then : >"$LOGDIR/interrupted"; fi
  echo "" >&2
  echo "interrupted; stopping suites and removing containers" >&2
  cleanup
  exit 130
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found: install Docker, or set PG_TEST_URL to an existing server" >&2
    exit 2
  fi
  if ! docker info >/dev/null 2>&1; then
    echo "docker is not running (docker info failed): start it, or set PG_TEST_URL" >&2
    exit 2
  fi
  if ! docker image inspect "$PG_IMAGE" >/dev/null 2>&1; then
    echo "pulling $PG_IMAGE ..."
    if ! docker pull -q "$PG_IMAGE" >/dev/null; then
      echo "could not pull $PG_IMAGE" >&2
      exit 2
    fi
  fi
}

# Starts a container named <prefix>-<suffix> with a free host port Docker
# chooses; prints nothing. The URL is read later with pg_url.
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
# Orchestration (top-level process)
# ---------------------------------------------------------------------------

new_run() {
  RUNID="$(date +%Y%m%d-%H%M%S)-$$"
  LOGDIR="$ROOT/.check-logs/$RUNID"
  mkdir -p "$LOGDIR/status" "$LOGDIR/shards" "$LOGDIR/pg"
  : >"$LOGDIR/order"
  export LOGDIR RUNID
  RUN_START=$SECONDS
  trap cleanup EXIT
  trap on_signal INT TERM
  echo "run $RUNID  (logs: $(rel "$LOGDIR"))"
}

pub_get() {
  local pkg log="$LOGDIR/pub_get.log"
  for pkg in $DART_PACKAGES $FLUTTER_PACKAGES; do
    echo "== $pkg" >>"$log"
    if is_dart_package "$pkg"; then
      if ! (cd "$ROOT/$pkg" && dart pub get) >>"$log" 2>&1; then
        echo "FAIL  pub get $pkg (see $(rel "$log"))"
        exit 1
      fi
    else
      if ! (cd "$ROOT/$pkg" && flutter pub get) >>"$log" 2>&1; then
        echo "FAIL  pub get $pkg (see $(rel "$log"))"
        exit 1
      fi
    fi
  done
}

# Collects the Postgres files for a selection into $LOGDIR/pgfiles:
#   all | nothroughput | throughput | demos
select_pg_files() {
  local mode="$1" dir path
  : >"$LOGDIR/pgfiles"
  postgres_files | while IFS="$(printf '\t')" read -r dir path; do
    case "$mode" in
      nothroughput) [ "$path" = "$THROUGHPUT_FILE" ] && continue ;;
      throughput) [ "$path" = "$THROUGHPUT_FILE" ] || continue ;;
      demos) [ "$dir" = "event_sourcing/example_action_permissions" ] || continue ;;
    esac
    printf '%s\t%s\n' "$dir" "$path"
  done >"$LOGDIR/pgfiles"
  if [ ! -s "$LOGDIR/pgfiles" ]; then
    echo "no Postgres files selected from $(rel "$WORKFLOW")" >&2
    exit 1
  fi
}

# Splits $LOGDIR/pgfiles across up to $1 shards; writes
# $LOGDIR/shards/<n>.list ("<dir>\t<path>\t<pieces>\t<piece index>" lines)
# and prints the plan. A file estimated at more than half a shard's share is
# cut into pieces of whole tests (the test runner's --total-shards and
# --shard-index), at most one per test and one per shard. The pieces then go,
# longest first, onto the least loaded shard.
plan_shards() {
  local want="$1" nfiles n i best dir path w k tests total=0 target nunits
  local loads=()
  local tab
  tab="$(printf '\t')"
  nfiles="$(wc -l <"$LOGDIR/pgfiles" | tr -d ' ')"
  : >"$LOGDIR/pgweights"
  while IFS="$tab" read -r dir path; do
    w="$(file_weight "$dir/$path")"
    total=$((total + w))
    printf '%s\t%s\t%s\n' "$w" "$dir" "$path" >>"$LOGDIR/pgweights"
  done <"$LOGDIR/pgfiles"
  target=$((total / want))
  if [ "$target" -lt 1 ]; then target=1; fi
  : >"$LOGDIR/pgunits"
  while IFS="$tab" read -r w dir path; do
    k=1
    if [ "$want" -gt 1 ] && [ $((2 * w)) -gt "$target" ]; then
      tests="$(grep -cE '^[[:space:]]*test\(' "$ROOT/$dir/$path")" || tests=1
      k=$(((2 * w + target - 1) / target))
      if [ "$k" -gt "$tests" ]; then k="$tests"; fi
      if [ "$k" -gt "$want" ]; then k="$want"; fi
      if [ "$k" -lt 1 ]; then k=1; fi
    fi
    i=0
    while [ "$i" -lt "$k" ]; do
      printf '%s\t%s\t%s\t%s\t%s\n' $((w / k)) "$dir" "$path" "$k" "$i" >>"$LOGDIR/pgunits"
      i=$((i + 1))
    done
  done <"$LOGDIR/pgweights"
  sort -t "$tab" -k1,1nr "$LOGDIR/pgunits" >"$LOGDIR/pgunits.sorted"
  nunits="$(wc -l <"$LOGDIR/pgunits.sorted" | tr -d ' ')"
  n="$want"
  if [ "$n" -gt "$nunits" ]; then n="$nunits"; fi
  NSHARDS="$n"
  i=1
  while [ "$i" -le "$n" ]; do
    loads[i]=0
    : >"$LOGDIR/shards/$i.list"
    i=$((i + 1))
  done
  while IFS="$tab" read -r w dir path k idx; do
    best=1
    i=2
    while [ "$i" -le "$n" ]; do
      if [ "${loads[i]}" -lt "${loads[best]}" ]; then best=$i; fi
      i=$((i + 1))
    done
    loads[best]=$((loads[best] + w))
    printf '%s\t%s\t%s\t%s\n' "$dir" "$path" "$k" "$idx" >>"$LOGDIR/shards/$best.list"
  done <"$LOGDIR/pgunits.sorted"
  i=1
  local plan=""
  while [ "$i" -le "$n" ]; do
    plan="$plan ${i}:~${loads[i]}s"
    i=$((i + 1))
  done
  echo "postgres: $nfiles files ($nunits pieces) over $n shard(s), estimated load:$plan"
}

# Starts the shard servers (or points every shard at PG_TEST_URL) and the
# second server postgres_generation_guard_test.dart compares against.
start_servers() {
  local i name urls_ok=1 need_other=0
  EVS_CHECKS_OTHER_URL="${PG_TEST_URL_OTHER_SERVER:-}"
  if [ -n "${PG_TEST_URL:-}" ]; then
    i=1
    while [ "$i" -le "$NSHARDS" ]; do
      echo "$PG_TEST_URL" >"$LOGDIR/shards/$i.url"
      i=$((i + 1))
    done
    if [ -z "$EVS_CHECKS_OTHER_URL" ]; then
      echo "note: PG_TEST_URL_OTHER_SERVER unset; the second-server check skips"
    fi
    export EVS_CHECKS_OTHER_URL
    return
  fi
  require_docker
  if grep -q 'postgres_generation_guard_test.dart' "$LOGDIR/pgfiles"; then need_other=1; fi
  CONTAINER_LABEL="evs-checks.run=$RUNID"
  local t0=$SECONDS
  i=1
  while [ "$i" -le "$NSHARDS" ]; do
    name="evs-checks-$RUNID-$i"
    if ! pg_start "$name" "$CONTAINER_LABEL"; then
      echo "could not start container $name" >&2
      exit 1
    fi
    i=$((i + 1))
  done
  if [ "$need_other" -eq 1 ]; then
    if ! pg_start "evs-checks-$RUNID-other" "$CONTAINER_LABEL"; then
      echo "could not start container evs-checks-$RUNID-other" >&2
      exit 1
    fi
  fi
  i=1
  while [ "$i" -le "$NSHARDS" ]; do
    if ! pg_ready_url "evs-checks-$RUNID-$i" >"$LOGDIR/shards/$i.url"; then urls_ok=0; fi
    i=$((i + 1))
  done
  if [ "$need_other" -eq 1 ]; then
    if ! EVS_CHECKS_OTHER_URL="$(pg_ready_url "evs-checks-$RUNID-other")"; then urls_ok=0; fi
  fi
  if [ "$urls_ok" -ne 1 ]; then
    echo "Postgres containers failed to start" >&2
    exit 1
  fi
  export EVS_CHECKS_OTHER_URL
  local extra=""
  if [ "$need_other" -eq 1 ]; then extra=" + 1 second server"; fi
  echo "postgres: $NSHARDS container(s)$extra ready ($PG_IMAGE, $((SECONDS - t0))s)"
}

# Launches every shard in the background.
launch_shards() {
  local i
  i=1
  while [ "$i" -le "$NSHARDS" ]; do
    echo "shard:$i" >>"$LOGDIR/order"
    "$SELF" _task "shard:$i" &
    BG_PIDS="$BG_PIDS $!"
    i=$((i + 1))
  done
}

wait_shards() {
  local pid
  for pid in $BG_PIDS; do
    if ! wait "$pid"; then
      echo "a shard process exited abnormally (pid $pid)" >/dev/null
    fi
  done
  BG_PIDS=""
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

# Merges this run's per-file Postgres durations into .check-durations.
record_durations() {
  local f new="$LOGDIR/durations.new"
  : >"$new"
  for f in "$LOGDIR"/shards/*.durations; do
    [ -f "$f" ] || continue
    cat "$f" >>"$new"
  done
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

# Prints the failing tests recorded in a flutter/dart test log.
failing_tests() {
  local log="$1" found
  found="$(strip_ansi <"$log" | grep -E '\[E\][[:space:]]*$' |
    sed -E 's/^([0-9:]+ )?\+[0-9]+( ~[0-9]+)?( -[0-9]+)?: //; s/[[:space:]]*\[E\][[:space:]]*$//' |
    sed "s#^$ROOT/##" |
    sort -u)" || found=""
  if [ -n "$found" ]; then
    echo "$found" | sed 's/^/      - /'
  else
    echo "      (no test-level failure parsed; see the log)"
  fi
}

summarize() {
  local task label sf result secs log note pass=0 fail=0 skip=0 key flog
  record_durations
  echo ""
  echo "---- summary (wall $((SECONDS - RUN_START))s, logs: $(rel "$LOGDIR"))"
  while IFS= read -r task; do
    label="$(task_label "$task")"
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
        case "$task" in
          shard:*)
            while IFS='|' read -r key flog; do
              echo "FAIL  $key  ($label; log: $(rel "$flog"))"
              failing_tests "$flog"
            done <"$LOGDIR/shards/${task#shard:}.failed"
            ;;
          analyze:* | elspais)
            echo "FAIL  $label  (log: $(rel "$log"))"
            strip_ansi <"$log" | grep -E '^[[:space:]]*(error|warning|info|ERROR|WARN|✗)' | head -20 | sed 's/^/      /'
            ;;
          *)
            echo "FAIL  $label  (log: $(rel "$log"))"
            failing_tests "$log"
            ;;
        esac
        ;;
    esac
  done <"$LOGDIR/order"
  echo "---- $pass passed, $fail failed, $skip skipped"
  [ "$fail" -eq 0 ]
}

analyze_tasks() {
  local p
  for p in $FLUTTER_PACKAGES $DART_PACKAGES; do echo "analyze:$p"; done
}

unit_tasks() {
  local p i
  for p in $FLUTTER_PACKAGES $DART_PACKAGES; do
    if [ "$p" = "$UNIT_SPLIT_PACKAGE" ] && [ "$UNIT_PIECES" -gt 1 ]; then
      i=1
      while [ "$i" -le "$UNIT_PIECES" ]; do
        echo "unit:$p@$i/$UNIT_PIECES"
        i=$((i + 1))
      done
    else
      echo "unit:$p"
    fi
  done
}

# Builds a task list into the TASKS array from the given generator output.
TASKS=()
add_tasks() {
  local t
  while IFS= read -r t; do TASKS+=("$t"); done
}

# Postgres phase: select files, plan, start servers, launch shards.
postgres_phase() {
  local mode="$1" shards="$2"
  select_pg_files "$mode"
  if [ -n "${PG_TEST_URL:-}" ]; then
    if [ "$shards" -gt 1 ]; then
      echo "note: PG_TEST_URL is set, so the Postgres files run one at a time on it"
    fi
    shards=1
  fi
  plan_shards "$shards"
  start_servers
}

main() {
  local cmd="${1:-help}"
  case "$cmd" in
    _task)
      run_task "$2"
      return
      ;;
    help | -h | --help)
      usage
      return 0
      ;;
    pg-up)
      require_docker
      local i name url
      i=1
      mkdir -p "$ROOT/.check-logs"
      : >"$ROOT/.check-logs/pg-up.env"
      while [ "$i" -le "$SHARDS" ]; do
        name="evs-checks-pgup-$i"
        if ! pg_start "$name" "evs-checks.run=pg-up"; then
          echo "could not start $name (already up? run make pg-down)" >&2
          return 1
        fi
        i=$((i + 1))
      done
      i=1
      while [ "$i" -le "$SHARDS" ]; do
        if ! url="$(pg_ready_url "evs-checks-pgup-$i")"; then return 1; fi
        echo "evs-checks-pgup-$i  $url"
        echo "PG_TEST_URL_$i=$url" >>"$ROOT/.check-logs/pg-up.env"
        i=$((i + 1))
      done
      echo "(URLs also in .check-logs/pg-up.env; remove with: make pg-down)"
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
      pub_get
      add_tasks < <(analyze_tasks)
      run_pool "$JOBS" "${TASKS[@]}"
      ;;
    test-unit)
      pub_get
      add_tasks < <(unit_tasks)
      run_pool "$JOBS" "${TASKS[@]}"
      ;;
    test-web)
      pub_get
      run_pool 1 web
      ;;
    test-desktop)
      pub_get
      run_pool 1 desktop
      ;;
    elspais)
      run_pool 1 elspais
      ;;
    test-postgres)
      pub_get
      postgres_phase nothroughput "$SHARDS"
      launch_shards
      wait_shards
      ;;
    test-throughput)
      pub_get
      postgres_phase throughput 1
      launch_shards
      wait_shards
      ;;
    test-demos)
      pub_get
      EVS_CHECKS_SHARED_PACKAGES="event_sourcing/example_action_permissions"
      export EVS_CHECKS_SHARED_PACKAGES
      postgres_phase demos "$SHARDS"
      launch_shards
      local p
      for p in $EXAMPLE_PACKAGES; do TASKS+=("unit:$p"); done
      TASKS+=(desktop)
      run_pool "$JOBS" "${TASKS[@]}"
      wait_shards
      ;;
    test-all)
      pub_get
      # The reference run: every suite exactly as CI runs it, one at a time.
      UNIT_PIECES=1
      export UNIT_PIECES
      add_tasks < <(analyze_tasks)
      add_tasks < <(unit_tasks)
      TASKS+=(web desktop elspais)
      run_pool 1 "${TASKS[@]}"
      postgres_phase all 1
      launch_shards
      wait_shards
      ;;
    test-all-parallel)
      pub_get
      EVS_CHECKS_SHARED_PACKAGES="event_sourcing event_sourcing/example_action_permissions"
      export EVS_CHECKS_SHARED_PACKAGES
      postgres_phase all "$SHARDS"
      launch_shards
      # Longest suites first so the pool's tail is short.
      add_tasks < <(unit_tasks | grep -e '^unit:event_sourcing[@]' -e '^unit:event_sourcing$')
      TASKS+=(unit:event_sourcing/example_action_permissions desktop unit:event_sourcing/example web)
      add_tasks < <(unit_tasks | grep -v -e '^unit:event_sourcing[@]' -e '^unit:event_sourcing$' \
        -e '^unit:event_sourcing/example_action_permissions$' -e '^unit:event_sourcing/example$')
      add_tasks < <(analyze_tasks)
      TASKS+=(elspais)
      run_pool "$JOBS" "${TASKS[@]}"
      wait_shards
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
