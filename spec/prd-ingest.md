# EVS-PRD-ingest: Ingest Path

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The ingest path is the inbound counterpart of destinations. It receives events from another event-sourcing deployment — daisy-chained through whatever transport the application chose — and admits them into this deployment's local log while preserving the upstream events' identity, authority, and hash chain. Ingested events join the local log alongside locally-originated events; from the materializer's perspective they participate in canonicalization rules like any other event.

The ingest path is distinct from the action-dispatch path. Dispatch produces *new* events stamped with this deployment's identity; ingest admits *existing* events from upstream while preserving their original identity. Both paths route into the same event log; the distinction is who carries the authority for the resulting events.

## Assertions

A. The library SHALL provide an ingest path through which events from another event-sourcing deployment are admitted into the local event log.

B. Events admitted via the ingest path SHALL retain their upstream identity (hash, originating authority, provenance chain).

C. The library SHALL extend an ingested event's provenance chain to record this deployment's hop.

D. The library SHALL verify the hash-chain integrity of ingested events against the upstream chain before admitting them, rejecting any event whose chain does not verify.

E. Ingested events SHALL participate in the local materializer and the local subscription primitives identically to locally-originated events.

F. The ingest path SHALL be idempotent: re-presenting an event already admitted SHALL not duplicate it in the local log.

G. The ingest path SHALL admit every event that passes this requirement's integrity verifications, without regard to the event's content or the age of its client-authored timestamps.

## Rationale

**Why a distinct path for upstream events vs. local actions?** A locally-dispatched action produces a new event whose authority is this deployment's principal. An ingested event already exists in another deployment, with that deployment's authority on it; this deployment is forwarding it, not authoring it. Conflating the two paths would either lose upstream identity (every event becomes "produced by this deployment") or create authority confusion (this deployment can't tell which events it actually authored). Two paths, distinguishable by who holds authority on the resulting event, keeps the audit story precise.

**Why preserve upstream identity?** The upstream's hash chain is the cryptographic evidence for upstream content. Re-stamping events with this deployment's identity would invalidate that chain at the boundary; downstream verifiers would have to trust this deployment's re-statement instead of the upstream's original. Preserving identity end-to-end keeps each event independently verifiable from the originating authority forward.

**Why extend the provenance chain rather than reset it?** Provenance answers "where has this event been?". Resetting at each hop loses the answer; extending records the transit so downstream observers see the full path. Section EVS-PRD-provenance pins the chain semantics; this PRD pins that ingest is one of the operations that adds a hop.

**Why verify hash-chain integrity at ingest?** Ingest is the boundary between an external deployment's audit trail and this deployment's. Admitting an event whose chain doesn't verify would let upstream tampering propagate downstream. Verifying at the boundary catches it once, at the place that has both the upstream chain and the local trust anchor.

**Why is ingest idempotent?** Cross-tier transports retry. The same upstream event may be presented at the ingest path many times (delivery retries, replay after restart, reconfiguration of upstream destinations). Idempotency on event identity (the upstream hash) makes retries safe and ensures the local log records each upstream event exactly once.

**Why does ingest participate in canonicalization rules rather than being canonical by default?** Multi-source editing is the case where ingested events and locally-originated events both target the same aggregate. The library's resolution of "which events are canonical for this aggregate?" is governed by configurable canonicalization rules (the multi-source-canonicalization PRD specifies the rule grammar), which can be configured per aggregate or per aggregate type. Ingest doesn't presume canonicality; it presents the event for the rule to evaluate.

**Why does the ingest path admit unconditionally?** Rejecting a verifiable event before admission is silent data loss with no audit trail. Selection, exclusion, and canonicalization are post-admission concerns — resolved by projections, canonicalization rules, and analysis — where an exclusion is itself observable and auditable rather than invisible. Offline-first sources legitimately deliver events days or weeks after authoring; the provenance model's distinction between client-authored timestamps and receiving-hop timestamps exists precisely so faithful recording and selective consumption can coexist, rather than forcing ingest to police timestamp age as a proxy for validity.

## Changelog

- 2026-08-10 | 79454334 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-08-06 | a8814731 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-07-02 | 92f2bd91 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Ingest Path* | **Hash**: 79454334
