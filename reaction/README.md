# reaction

Substrate-agnostic action submission, view subscription, permission
snapshots, and credential lifecycle for apps built on `event_sourcing`.

Pure Dart at runtime; the Flutter widget library `reaction_widgets`
sits on top. Note: the test harness transitively requires the
Flutter SDK (via `event_sourcing`'s Sembast test binding), even
though the package itself imports no Flutter at runtime.

See `spec/prd-reaction.md` (repo root) for the architectural
spec.

## Status

Shipped. In-process `Local*` implementations, wire codecs, `Remote*`
client implementations, and the reference shelf server handlers
(`ReactionHandlers`) are all present and tested. See
`spec/reaction-remote.md` for the wire-layer spec.

## Layout

- `lib/src/interfaces/` — the abstract interfaces (transport-agnostic).
- `lib/src/state/` — `ActionState` sealed type + idempotency-key generator.
- `lib/src/scope/` — `ReactionScope` abstraction + `LocalScope`, with the
  authoritative `ConnectionStatus` stream; the `RemoteScope` implementation
  lives in `lib/src/remote/`.
- `lib/src/local/` — in-process implementations wrapping `event_sourcing`'s
  `ActionDispatcher`, `EventStore.subscribe<T>`, and permission machinery.
- `lib/src/remote/` — cross-process client implementations (HTTP + WebSocket).
- `lib/src/server/` — reference shelf handlers, auth middleware,
  `AuthorizationWatcher`, `WsConnectionRegistry`, `ViewScopeRegistry`.
- `lib/src/wire/` — wire codecs for the protocol envelopes.
