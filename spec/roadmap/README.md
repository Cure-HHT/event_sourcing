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
  canonicalization (the headline roadmap item).
- `reaction.md` — reaction layer: reconnect optimizations,
  observability, pagination, validators, adapters.
- `storage.md` — storage backends: reactive Postgres subscribe,
  pooling, SQL-native view rows, additional backends.
- `permissions.md` — permission-model extensions.
- `projections.md` — projection/materializer primitives.
- `authentication.md` — substrate-level authentication closure.
