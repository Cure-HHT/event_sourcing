# reaction

Substrate-agnostic action submission, view subscription, permission
snapshots, and credential lifecycle for apps built on `event_sourcing`.

Pure Dart. The Flutter widget library `reaction_widgets` sits on top.

See `spec/prd-reaction.md` (in the parent repo) for the architectural
spec.

## Status

Pre-shipping. Local in-process impls only (this package's scope per
Plan B-local). Wire codecs + Remote impls land in Plan B-remote.

## Layout

- `lib/src/interfaces/` — the 5 abstract interfaces (transport-agnostic).
- `lib/src/state/` — `ActionState` sealed type + idempotency-key generator.
- `lib/src/local/` — in-process implementations wrapping `event_sourcing`'s
  `ActionDispatcher`, `EventStore.subscribe<T>`, and permission machinery.
