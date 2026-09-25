# EVS-PRD-ingest: Ingest Path

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The ingest path is the inbound counterpart of destinations. It receives the events another event-sourcing deployment authored, delivered on a delivery channel through whatever transport the application chose, and admits them into this deployment's local log while preserving the upstream events' identity, authority, and hash chain. Ingested events join the local log alongside locally-originated events; from the materializer's perspective they participate in canonicalization rules like any other event.

The ingest path is distinct from the action-dispatch path. Dispatch produces *new* events stamped with this deployment's identity; ingest admits *existing* events from upstream while preserving their original identity. Both paths route into the same event log; the distinction is who carries the authority for the resulting events.

## Assertions

A. The library SHALL provide an ingest path through which events from another event-sourcing deployment are admitted into the local event log.

B. Events admitted via the ingest path SHALL retain their upstream identity (hash, originating authority, provenance chain).

C. The library SHALL extend an ingested event's provenance chain to record this deployment's hop.

D. The library SHALL verify the hash-chain integrity of ingested events against the upstream chain before admitting them, rejecting any event whose chain does not verify.

E. Ingested events SHALL participate in the local materializer and the local subscription primitives identically to locally-originated events.

F. The ingest path SHALL be idempotent: re-presenting an event already admitted SHALL not duplicate it in the local log.

G. The ingest path SHALL admit an event only as part of a delivery on a channel that admits that delivery, SHALL admit every event of such a delivery that passes this requirement's integrity verifications without regard to the event's content or the age of its client-authored timestamps, and the library SHALL expose no public ingest entry point that admits an event outside a delivery.

H. Every ingest entry point SHALL refuse, with a named reason and before any write, an event whose originator provenance entry names the receiving database's identity, whether or not the receiving database holds it.

I. The ingest path SHALL refuse, before any write, an event whose predecessor hash names a held event that the event's originating database did not author, or that its originating database authored at a later position.

J. The ingest path SHALL refuse, with a typed refusal and before any write, an event that follows a held predecessor which another held event of the same originating database already follows, unless a skip event of that database the receiver holds, or the event itself, lists every such event as a successor of that predecessor.

K. The ingest path SHALL refuse, before any write, an event whose recorded kind or eligibility differs from the receiver's declaration for its entry type at the same major, and an event its delivery records as withholding no parent that names an eligible parent the receiver does not hold.

## Rationale

**Why a distinct path for upstream events vs. local actions?** A locally-dispatched action produces a new event whose authority is this deployment's principal. An ingested event already exists in another deployment, with that deployment's authority on it; this deployment is forwarding it, not authoring it. Conflating the two paths would either lose upstream identity (every event becomes "produced by this deployment") or create authority confusion (this deployment can't tell which events it actually authored). Two paths, distinguishable by who holds authority on the resulting event, keeps the audit story precise.

**Why preserve upstream identity?** The upstream's hash chain is the cryptographic evidence for upstream content. Re-stamping events with this deployment's identity would invalidate that chain at the boundary; downstream verifiers would have to trust this deployment's re-statement instead of the upstream's original. Preserving identity end-to-end keeps each event independently verifiable from the originating authority forward.

**Why extend the provenance chain rather than reset it?** Provenance answers "where has this event been?". Resetting at each hop loses the answer; extending records the transit so downstream observers see the full path. Section EVS-PRD-provenance pins the chain semantics; this PRD pins that ingest is one of the operations that adds a hop.

**Why verify hash-chain integrity at ingest?** Ingest is the boundary between an external deployment's audit trail and this deployment's. Admitting an event whose chain doesn't verify would let upstream tampering propagate downstream. Verifying at the boundary catches it once, at the place that has both the upstream chain and the local trust anchor.

**What the verification covers.** Each incoming event's own hash is recomputed over the record exactly as it arrived, whatever the length of its provenance, and each receiver hop's arrival hash over the record as the hop before it stored it. The hash is unkeyed (EVS-PRD-hash-chain-integrity, Rationale), so the check catches a record altered in transit or at rest without its hashes being recomputed, not one whose hashes were recomputed to match. An incoming event's predecessor hash is checked wherever the receiver holds the event it names (assertions I and J). A predecessor hash naming an event the receiver does not hold is accepted, because a destination's filter leaves out events by design and the delivery chain accounts for them (EVS-PRD-delivery-channel).

**Why is ingest idempotent?** Cross-tier transports retry. The same upstream event may be presented at the ingest path many times (delivery retries, replay after restart, reconfiguration of upstream destinations). Idempotency on event identity (the upstream hash) makes retries safe and ensures the local log records each upstream event exactly once.

**Why does ingest participate in canonicalization rules rather than being canonical by default?** Multi-source editing is the case where ingested events and locally-originated events both target the same aggregate. The library's resolution of "which events are canonical for this aggregate?" is governed by configurable canonicalization rules (the multi-source-canonicalization PRD specifies the rule grammar), which can be configured per aggregate or per aggregate type. Ingest doesn't presume canonicality; it presents the event for the rule to evaluate.

**Why does the ingest path admit unconditionally?** Rejecting a verifiable event before admission is silent data loss with no audit trail. Selection, exclusion, and canonicalization are post-admission concerns — resolved by projections, canonicalization rules, and analysis — where an exclusion is itself observable and auditable rather than invisible. Offline-first sources legitimately deliver events days or weeks after authoring; the provenance model's distinction between client-authored timestamps and receiving-hop timestamps exists precisely so faithful recording and selective consumption can coexist, rather than forcing ingest to police timestamp age as a proxy for validity.

**Why refuse a database's own events at ingest (assertion H)?** A database's own events enter its log in one way: it appends them. One that comes back through ingest either duplicates an event the database holds, or is an event the database lost, which then re-enters as if another deployment had sent it: its origin sequence position may already be reused by an event appended since, and nothing records that the database's own history went back in time. Every ingest entry point therefore refuses it, held or not. A sender that lost its own events after a restore gets them back through a separate path, open only to its drainer while it resumes a delivery channel, which checks each event against the deliveries the channel carried and records the recovery in a skip event (EVS-PRD-delivery-channel); the two coexist because the recovery is not an ingest entry point and ingest never takes the recovery's path.

**Why only deliveries the channel admits (assertion G)?** Admission is unconditional about content and age, not about order: a delivery that does not follow the receiver's record of its channel is refused whole, with the record, so the sender delivers again what the receiver lacks or recovers what it lacks itself (EVS-PRD-delivery-channel). The refusal loses nothing: the events stay in the sender's queue, and the refusal is recorded in the receiver's log. An entry point that admitted a single event outside a delivery would let an event reach the log with no channel record, so a receiver's record would no longer describe everything it received from a sender; the library's own tests of the per-event checks reach them through library-internal code, not a public entry point.

**Why refuse a wrong or an unlisted successor (assertions I and J)?** A predecessor the receiver holds, but that another database authored or that its own database authored later, cannot be the event the sender authored before this one. Admitting it would store a link the receiver's own log already contradicts. An event that follows a held predecessor which another held event of its sender already follows forks the sender's origin chain. A restored sender's resume delivers the skip event that lists the fork's successors as the first delivery on the resumed channel, and holds its other channels to the same receiver until that skip is acknowledged, so the explanation always arrives before the fork. A fork that arrives without one is refused before it enters the log, with a typed refusal the sender's drainer wedges on with its own cause. The sender's recovery of its own events is not an ingest entry point: it stores the abandoned branch before the skip event listing it exists, and checks each event against the deliveries its channel carried instead.

**Why refuse a declaration or withheld-parent contradiction (assertion K)?** Kind and eligibility decide which parents an event names; a sender whose build declares an entry type differently from the receiver's at the same major stamps parents the receiver's rules would not, so the difference is stopped at the boundary, by name. A delivery that records no withheld parent states that an earlier accepted delivery of the same channel carried every parent, so a receiver that lacks one has found a contradiction, not a filter.

## Changelog

- 2026-09-25 | e0ce5a1c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add H: every ingest entry point refuses an event the receiving database originated, held or not. Amend G: events are admitted only as part of a delivery its channel admits, and no public entry point admits an event outside a delivery. Add I-K: ingest refuses a predecessor the receiver holds that is not an earlier event of the same originating database, a fork whose successors no skip event it holds lists, an event whose declarations differ from the receiver's within one major, and a withheld-parent record the receiver's log contradicts. Purpose: ingest receives the events another deployment authored, on a delivery channel. Rationale: what ingest verification covers
- 2026-09-24 | 79454334 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale states what ingest verification covers
- 2026-08-10 | 79454334 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-08-06 | a8814731 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-07-02 | 92f2bd91 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Ingest Path* | **Hash**: e0ce5a1c
