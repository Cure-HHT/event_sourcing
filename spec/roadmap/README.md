# Roadmap

This directory is the only place in the repository where future work
is recorded. Everything outside `spec/roadmap/` describes the library
as it is; a capability described in these files does not exist yet.

Each file collects the deferred items for one area. Items here are
deliberate deferrals — recorded so they are not re-derived from
scratch — not commitments to a schedule. New primitives that grow out
of these items ship under the Append-Only Primitives discipline (new
names, frozen semantics).

- `multi-source-editing.md` — multi-user/multi-source editing and
  canonicalization (the headline roadmap item): structural conflict
  detection, stale annotations and drafts, proposals, authority rules,
  inbound delivery.
- `reaction.md` — reaction layer: reconnect optimizations,
  observability, pagination, validators, adapters.
- `storage.md` — storage backends: reactive Postgres subscribe and the
  drainer's wake-up, pooling, SQL-native view rows, additional backends,
  keeping the library's storage credentials from application code, a
  browser tab whose
  database handle cannot commit, verifying the inputs the generation
  guard and the drain lock trust, keeping or removing `readEventsReverse`.
- `permissions.md` — permission-model extensions.
- `projections.md` — projection/materializer primitives; views that
  catch up with the log whatever their interest.
- `sync.md` — sync/destination layer: inbound tombstone propagation,
  detecting undeclared delivery-configuration changes, a recovery that
  skips the wedged item, rebuilding a destination in one call, storing
  a large recovery in resumable chunks.
- `versions.md` — versions and the data generation: projection
  specifications in the data generation, reading an older data-format
  major.
- `authentication.md` — substrate-level authentication closure.
- `security-findings.md` — clearing a security finding, reviewing
  findings, timestamp anchors in the chain walk, walking the chain in
  resumable chunks.
- `tooling.md` — tooling, tests and examples: Dart doc references as
  links, a real browser's page visibility in the drain-lock tests, owner
  and runtime roles in the Postgres example deployment.
