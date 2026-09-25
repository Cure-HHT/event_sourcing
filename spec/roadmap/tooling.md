# Roadmap — tooling, tests and examples

Deferred work on the repository's documentation tooling, test coverage
and example deployments. Nothing here changes library behaviour.

## Dart doc references as links

**Baseline.** Dartdoc comments name many library symbols as code spans
(`` `Symbol` ``) rather than doc links (`[Symbol]`), because a link to a
symbol the file does not import needs that import, and adding barrel
imports to library files to satisfy the doc generator would change their
dependencies. The generated API documentation therefore shows those names
as plain code.

**Remaining.** Restore them as links through `/// @docImport` directives,
which make a symbol resolvable for documentation without importing it
into the library, and keep the analyzer's unresolved-reference check
clean.

## A real browser's page visibility in the drain-lock tests

**Baseline.** In a browser the drain lock follows the visible tab. The
browser tests drive visibility through the `pageVisibility` test seam and
through dispatched `pagehide`/`pageshow` and `freeze`/`resume` events,
because headless Chrome always reports the page as visible.

**Remaining.** Exercise real visibility changes (a tab hidden and shown
by the browser, a page frozen and resumed) in a headed or
visibility-capable browser run, so the hand-over is shown against the
browser's own signals rather than the seam.

## Owner and runtime roles in the Postgres example deployment

**Baseline.** The Postgres example's `docker-compose.yml` runs one
superuser, marked development-only. The library's own tests prove the
split it documents: a schema-owner role that provisions, and a runtime
role holding only the grants in `postgresRuntimeRoleGrants`.

**Remaining.** Give the example deployment the same split (an owner that
provisions, a runtime role the server connects as), so the example shows
the least-privilege setup an adopter deploys.
