# Roadmap — multi-source editing and canonicalization

The headline deferred capability: letting multiple event-sourcing
deployments contribute events to the same aggregate and resolving which
of those events are canonical. `spec/prd-multi-source-canonicalization.md`
pins the rule grammar (assertions A–F); this file records what remains
to build it and what already exists to build it on.

## Verified baseline (what exists in code)

The retained substrate seams are the authority-identity and ordering
guarantees; the canonicalization layer itself is unbuilt. Concretely,
the code today has only the single-source prerequisite machinery:

- **Per-event authority identity.** `Source` (`event_sourcing/lib/src/storage/source.dart`)
  and the originator hop recorded on each event via `provenance[0]`
  give every event a stable authority identifier.
- **Per-aggregate-per-authority ordering.** `StorageBackend` enforces
  the ordering guarantee for each `(aggregate, authority)` pair.
- **Integrity-verifying ingest with recorded rejection.**
  `EventStore.logRejectedBatch` records a rejected inbound batch, and
  the rejection reasons are integrity-only (decode / hash-chain /
  identity failures).
- **A comment-level gate in `ProjectionRegistry.seal()`** marking where
  future settings-event-driven registration would hook in.

There is no canonicalization code: no `CanonicalView` / `ProposalView`,
no `set_canonicalizer` / `delegate_canonicalization` event types, no
rule interpretation, no canonical-event filtering in the folds, and no
authorization-aware ingest refusal. The single-source-per-aggregate-type
invariant holds today.

## Remaining work

### Canonicalization layer

Build the entire mechanism the rule-grammar PRD describes:

- **Rule events.** Settings event types (`set_canonicalizer`,
  `delegate_canonicalization`) recorded on the same log, so the rules in
  effect for an aggregate at any point are reconstructable from the log
  alone.
- **Rule interpretation.** Evaluate the recorded rules over each event's
  authority identity to classify events as canonical or non-canonical.
- **Canonical-event filtering in the fold.** The materializer folds only
  canonical events into typed state while non-canonical events remain
  visible in the log and in opt-in subscriptions.
- **`CanonicalView` vs `ProposalView`.** Distinguish the canonical
  materialized state from a view that surfaces pending non-canonical
  edits (the approval-pattern surface).
- **Approval pattern.** An event from an authority with rule-granted
  approval power admits an otherwise-non-canonical edit from a different
  authority, making it canonical.
- **Per-entry-type resolution policies** and hash-chain merge under
  parallel authorities.

### Authorization-aware grant visibility

Ingest rejection is integrity-only today. Multi-source authorization
adds authorization-aware ingest refusal plus a recorded refusal event:
an ingesting deployment may refuse to apply another authority's events
to its own views, and that refusal is itself a recorded event. Each
authority remains the authority over its own log — a remote authority's
authorization state at the time it emitted an event is what its log
records; ingesting peers cannot retroactively unmake the remote log.
This item is gated on the canonicalization layer above.

## Motivating scenarios

Multi-user collaborative editing (`docs/scenarios/collaborative-editing.md`)
depends on this capability, as do the multi-authority consolidations in
`docs/scenarios/supply-chain.md`, `docs/scenarios/iot-sensor-network.md`,
and `docs/scenarios/retail-pos.md`: each has a single-authority
per-aggregate story that works today and a cross-authority merge story
that needs the canonicalization layer.
