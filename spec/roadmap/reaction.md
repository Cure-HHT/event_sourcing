# Roadmap — reaction layer

Deferred work for the `reaction` package (cross-process client, wire
codecs, reference server) and the widget layer that consumes it. Each
item states what already exists in code and what remains.

## Resume-from-sequence reconnect

**Baseline.** On reconnect the client re-sends the stored `SubscribeMsg`
verbatim, so each subscription re-replays a full `Snapshot × N →
EndOfReplay → live` — the refetch baseline. Every `Update<T>` already
carries its `sequence`, so the field the optimization needs is present
on the wire.

**Remaining.** A `resume` wire message, client-side tracking of the
last-applied sequence per subscription, and server-side resume logic
that replays only events after that sequence. Purely additive: it does
not change `ConnectionStatus` semantics or the `Update<T>` variant set,
and it eliminates the snapshot-replay flash on reconnect.

## Reconnect attempt count on `Reconnecting`

**Baseline.** The sealed `ConnectionStatus` stream (`Connected`,
`Reconnecting`, `Disconnected`) is shipped end-to-end: `RemoteScope` and
`RemoteConnection` drive the transitions from the WS lifecycle with
exponential backoff, and the widgets consume it (`ViewBuilder`'s `Stale`
state, `ReActionErrorListener`).

**Remaining.** Surface the internal reconnect-loop counter as
`Reconnecting(attempt: N)` so a reconnecting banner can show progress.
The counter exists inside the reconnect loop; only threading it into the
emitted status remains.

## Batched / cursor snapshot wire delivery

**Baseline.** The client half is shipped: `ViewBuilder`'s opt-in
progressive mode (`isProgressive`) renders partial row sets during
snapshot replay, and the `ViewSource.watch<T>` contract is pinned as
additive-ready so batched delivery is a non-breaking evolution.

**Remaining.** A batch wire variant (a `SnapshotBatch` envelope, or a
`lastInBatch` flag on `snapshot`) plus server-side chunking / cursor
emission for very large views. Deferred until a measured large-view
scale demands it.

## Server-side metrics and observability hooks

**Baseline.** None. There are no counters or instrumentation seams on
`ReactionHandlers` or `WsConnectionRegistry`.

**Remaining.** Connection counts, subscription counts, message rates,
and error counts, most likely exposed by `ReactionHandlers` as a metrics
surface feeding a consumer-supplied sink (or per-handler counters a
consumer's existing telemetry middleware can pick up).

## Signature-verifying reference validator

**Baseline.** The pluggable `PrincipalAuthValidator` seam and the
dev/test `TrustingAuthValidator` ship today; the example app adds a
non-crypto `RoleAwareTrustingValidator`. No signature-verifying
validator exists anywhere in the library.

**Remaining.** A crypto-verifying reference validator (JWT-shaped or
similar) plus its tests. It would ship as a sibling package so `reaction`
proper stays free of any one identity provider's shape; concrete
production validators remain consumer-supplied per the trust-boundary
discipline.

## Cross-process named-predicate registry

**Baseline.** `SubscriptionFilter.predicate` is a Dart closure that
cannot be serialized; `FilterCodec` drops it on encode by design, so
decoded remote filters always have `predicate == null`. No consumer uses
`predicate` on a remote subscription today.

**Remaining.** A named-predicate registry: register a predicate under
the same string identifier on both client and server, and ship only the
identifier over the wire (a `predicate` id field). Deferred until a
consumer needs predicate-based remote filtering.

## Structured wire error encoding

**Baseline.** `parse_denied`, `validation_denied`, and `execution_failed`
each carry a substrate `error: Object`; the wire codec emits
`error.toString()` and decodes it back as the raw string, so structured
error types do not round-trip with their full structure.
`authorization_denied` already round-trips its `Permission` structurally.

**Remaining.** Richer client-side error structure — either a substrate
sealed error type the wire can encode/decode generically, or an
`ErrorCodec` extension point consumers register on both sides (analogous
to the named-predicate path). Deferred until a consumer needs structured
errors on the client.

## State-management adapter package

**Baseline.** The headless widget layer exposes `Stream<Update<T>>`,
`ValueListenable`, and `InheritedWidget` — the surface an adapter would
wrap. No adapter package exists.

**Remaining.** An opt-in adapter (for example, a signals-shaped surface
over `Stream<Update<T>>`: a row-set signal that folds
`Snapshot`/`Delta`/`Tombstone`, an action-submission signal, a
connection-status signal). It follows the additive-adapter pattern and
ships only if downstream demand converges on one idiom; the headless
base stays on raw streams so any state-management library remains
first-class.

## `EventLogView` / `TraceView` downstream sugar

**Baseline.** Both substrate prerequisites are complete:
`Stream<Update<StoredEvent>>` subscriptions expose the raw event feed,
and every event carries the correlation fields (`flowToken` plus the
`action_invocation_id` metadata) these views walk.

**Remaining.** The widgets themselves — a filtered event-log surface and
a correlation-trace surface. By design these are downstream-consumer
sugar (they render UI), so they live in consumer apps rather than the
headless base per the widget-contract headless obligation, not in this
library.
