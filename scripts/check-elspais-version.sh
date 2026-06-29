#!/bin/sh
# Pre-push gate: enforce the pinned elspais version.
#
# The pinned version lives ONLY in .github/versions.env (ELSPAIS_VERSION); this
# script carries no version literal. It fails the push when the installed
# elspais is older than the pin (or is missing, or the pin is unset), so the
# `elspais checks` hook always runs against a known-good version locally.
set -eu

root="$(git rev-parse --show-toplevel)"
. "$root/.github/versions.env"
: "${ELSPAIS_VERSION:?ELSPAIS_VERSION not set in .github/versions.env}"

installed="$(elspais --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
if [ -z "$installed" ]; then
  echo "ERROR: elspais is not installed (or its version is unreadable);" >&2
  echo "       this repo requires >= $ELSPAIS_VERSION:" >&2
  echo "       pip install --upgrade 'elspais==$ELSPAIS_VERSION'" >&2
  exit 1
fi

# True when $1 is strictly older than $2 (semantic-version aware).
ver_lt() {
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]
}
if ver_lt "$installed" "$ELSPAIS_VERSION"; then
  echo "ERROR: elspais $installed is older than the pinned $ELSPAIS_VERSION." >&2
  echo "       pip install --upgrade 'elspais==$ELSPAIS_VERSION'" >&2
  exit 1
fi
