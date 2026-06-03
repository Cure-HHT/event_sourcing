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

echo "==> Booting demo server on :$SERVER_PORT"
dart run bin/server.dart --port "$SERVER_PORT" &
SERVER_PID=$!

# Give the server a moment to bind.
sleep 2

echo "==> Building Flutter web bundle"
flutter build web -t lib/client/main.dart \
  --dart-define=REACTION_SERVER_URL="http://127.0.0.1:$SERVER_PORT"

echo "==> Running Playwright suite"
cd e2e
npm install --silent
npx playwright test "$@"
