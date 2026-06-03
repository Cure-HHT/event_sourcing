#!/usr/bin/env bash
# Build the Flutter web client, boot the demo server, and run the
# Playwright e2e suite against the served bundle.
#
# Usage:  reaction/example/scripts/run-e2e.sh
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EXAMPLE_DIR"

# `flutter` may not be on PATH (this host keeps it under flutter-sdk/).
# Add the known location if the command is missing.
if ! command -v flutter >/dev/null 2>&1; then
  if [[ -x "$HOME/flutter-sdk/flutter/bin/flutter" ]]; then
    export PATH="$HOME/flutter-sdk/flutter/bin:$PATH"
  else
    echo "ERROR: flutter not found on PATH and not at \$HOME/flutter-sdk/flutter/bin" >&2
    exit 1
  fi
fi

SERVER_PORT="${REACTION_SERVER_PORT:-8080}"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# The web platform scaffold (web/index.html etc.) is gitignored and
# regenerated on demand — see .gitignore and README. `flutter build web`
# fails with "not configured for the web" if it is missing, so create it.
if [[ ! -d web ]]; then
  echo "==> Scaffolding web platform (flutter create --platforms web)"
  flutter create . --platforms web --project-name reaction_example >/dev/null
fi

echo "==> Booting demo server on :$SERVER_PORT"
dart run bin/server.dart --port "$SERVER_PORT" &
SERVER_PID=$!

# Wait for the server to bind rather than racing a fixed sleep.
echo "==> Waiting for server readiness on :$SERVER_PORT"
for _ in $(seq 1 30); do
  if curl -sf -o /dev/null "http://127.0.0.1:$SERVER_PORT/" 2>/dev/null; then
    break
  fi
  # A 404 still proves the port is bound and answering HTTP.
  if curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$SERVER_PORT/" \
    2>/dev/null | grep -qE '^[0-9]{3}$'; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "ERROR: demo server exited before binding :$SERVER_PORT" >&2
    exit 1
  fi
  sleep 1
done

echo "==> Building Flutter web bundle"
flutter build web -t lib/client/main.dart \
  --dart-define=REACTION_SERVER_URL="http://127.0.0.1:$SERVER_PORT"

echo "==> Running Playwright suite"
cd e2e
npm install --silent
npx playwright test "$@"
