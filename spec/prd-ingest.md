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

D. The library SHALL verify the hash-chain integrity of each ingested event against the upstream chain before admitting it, and SHALL admit an event whose chain does not verify as received, recording a security finding for it.

E. Ingested events SHALL participate in the local materializer and the local subscription primitives identically to locally-originated events.

F. The ingest path SHALL be idempotent: re-presenting an event already admitted SHALL not duplicate it in the local log.

G. The ingest path SHALL admit an event only as part of a delivery on a channel that admits that delivery, SHALL admit every event of such a delivery, whatever the outcome of this requirement's integrity verifications, the event's content or the age of its client-authored timestamps, other than a record it cannot store as an event, which it SHALL keep in full in a security finding, and the library SHALL expose no public ingest entry point that admits an event outside a delivery.

H. Every ingest entry point SHALL record a security finding for an event whose originator provenance entry names the receiving database's identity, whether or not the receiving database holds it, and SHALL store such an event as received when it does not hold it.

I. The ingest path SHALL record a security finding for an event whose predecessor hash names a held event that the event's originating database did not author, or that its originating database authored at a later position.

J. The ingest path SHALL record a security finding for an event that follows a held predecessor which another held event of the same originating database already follows, or that sits at an origin position another held event of that database already occupies, unless the fork or the position lies within the range of origin positions, above its branch point (above 0 when it records none) and up to its abandoned head, that a skip event of that database the receiver holds, or the event itself, records.

K. <RETIRED> Ingest compares no declarations: every holder reads the kind and eligibility recorded on each event.

L. The ingest path SHALL record a security finding for an event its delivery records as withholding no parent that names an eligible parent the receiver does not hold.

## Rationale

**Why a distinct path for upstream events vs. local actions?** A locally-dispatched action produces a new event whose authority is this deployment's principal. An ingested event already exists in another deployment, with that deployment's authority on it; this deployment is forwarding it, not authoring it. Conflating the two paths would either lose upstream identity (every event becomes "produced by this deployment") or create authority confusion (this deployment can't tell which events it actually authored). Two paths, distinguishable by who holds authority on the resulting event, keeps the audit story precise.

**Why preserve upstream identity?** The upstream's hash chain is the cryptographic evidence for upstream content. Re-stamping events with this deployment's identity would invalidate that chain at the boundary; downstream verifiers would have to trust this deployment's re-statement instead of the upstream's original. Preserving identity end-to-end keeps each event independently verifiable from the originating authority forward.

**Why extend the provenance chain rather than reset it?** Provenance answers "where has this event been?". Resetting at each hop loses the answer; extending records the transit so downstream observers see the full path. Section EVS-PRD-provenance pins the chain semantics; this PRD pins that ingest is one of the operations that adds a hop.

**Why verify hash-chain integrity at ingest, and record rather than refuse (assertion D)?** Ingest is the boundary between an external deployment's audit trail and this deployment's, the place that has both the upstream chain and the local trust anchor, so it checks there. A refused event is gone from the receiver's record, and a refused delivery holds everything after it on the sender until a person acts. So an event that fails a check is admitted as received with a security finding stating what failed (`EVS-DEV-security-findings`): the receiver's log holds the suspect event and the evidence against it, its default views mark the aggregate, and tampering upstream is visible downstream rather than silently dropped.

**What the verification covers.** Each incoming event's own hash is recomputed over the record exactly as it arrived, whatever the length of its provenance, and each receiver hop's arrival hash over the record as the hop before it stored it. The hash is unkeyed (EVS-PRD-hash-chain-integrity, Rationale), so the check catches a record altered in transit or at rest without its hashes being recomputed, not one whose hashes were recomputed to match. An incoming event's predecessor hash is checked wherever the receiver holds the event it names (assertions I and J), and every failed check is recorded as a security finding. A predecessor hash naming an event the receiver does not hold is accepted, because a destination's filter leaves out events by design and the delivery chain accounts for them (EVS-PRD-delivery-channel).

**Why is ingest idempotent?** Cross-tier transports retry. The same upstream event may be presented at the ingest path many times (delivery retries, replay after restart, reconfiguration of upstream destinations). Idempotency on event identity (the upstream hash) makes retries safe and ensures the local log records each upstream event exactly once.

**Why does ingest participate in canonicalization rules rather than being canonical by default?** Multi-source editing is the case where ingested events and locally-originated events both target the same aggregate. The library's resolution of "which events are canonical for this aggregate?" is governed by configurable canonicalization rules (the multi-source-canonicalization PRD specifies the rule grammar), which can be configured per aggregate or per aggregate type. Ingest doesn't presume canonicality; it presents the event for the rule to evaluate.

**Why does the ingest path admit unconditionally?** Rejecting a verifiable event before admission is silent data loss with no audit trail. Selection, exclusion, and canonicalization are post-admission concerns — resolved by projections, canonicalization rules, and analysis — where an exclusion is itself observable and auditable rather than invisible. Offline-first sources legitimately deliver events days or weeks after authoring; the provenance model's distinction between client-authored timestamps and receiving-hop timestamps exists precisely so faithful recording and selective consumption can coexist, rather than forcing ingest to police timestamp age as a proxy for validity.

**Why a finding for a database's own events at ingest (assertion H)?** A database's own events enter its log by its appends, and a channel carries only what its sender authored, so one of its own events arriving through ingest is a clone's or a tamperer's: it may duplicate an event the database holds, or re-enter at an origin position an event appended since reuses. Ingest records it as a finding, held or not, and stores it as received when it is not held, like any other event whose integrity it cannot vouch for. Storing it gives it no authority over the database's own destinations: the default destination-wedges view folds no event of the database's own identity that it does not hold as authored (`EVS-PRD-destinations/S`). A sender that lost its own events after a restore gets them back through a separate path, open only to its drainer while it resumes a delivery channel, which checks each event against the deliveries the channel carried and records the recovery in a skip event (EVS-PRD-delivery-channel).

**Why only deliveries the channel admits (assertion G)?** Admission is unconditional about content, age and integrity, not about order or form. A record the library cannot store as an event (malformed, or a reserved event it does not admit) is kept in full in a finding instead, so the log holds it and the rest of its delivery is admitted. As for order, a delivery that does not follow the receiver's record of its channel is refused whole, with the record, so the sender delivers again what the receiver lacks, recovers what it lacks itself, or re-anchors the channel (EVS-PRD-delivery-channel). The refusal loses nothing: the events stay in the sender's queue. An entry point that admitted a single event outside a delivery would let an event reach the log with no channel record, so a receiver's record would no longer describe everything it received from a sender; the library's own tests of the per-event checks reach them through library-internal code, not a public entry point.

**Why a finding for a wrong or an unlisted successor (assertions I and J)?** A predecessor the receiver holds, but that another database authored or that its own database authored later, cannot be the event the sender authored before this one. An event that follows a held predecessor which another held event of its sender already follows, or that sits at an origin position another held event of its sender occupies, forks the sender's origin chain; a filter can hide the first of these facts but not the second. A restored sender's resume delivers the skip event that records the range of positions its restore reused as the first delivery on the resumed channel, so on that channel the explanation arrives before the fork. A fork that arrives without one, on another channel of the sender or from a clone, is stored with a finding. The sender's recovery of its own events is not an ingest entry point: it stores the skip event before the abandoned branch and checks each event against the deliveries its channel carried instead.

**Why a finding for a withheld-parent contradiction (assertion L)?** A delivery that records no withheld parent states that an earlier accepted delivery of the same channel carried every parent, so a receiver that lacks one has found a contradiction, not a filter. Beyond that check, the receiver records each event's withheld-parent flag and draws no conclusion from it.

## Changelog

- 2026-09-25 | d60bdba5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | H: an event of the receiver's own identity that the receiver does not hold is stored as received, whatever its entry type. No code or test references H
- 2026-09-25 | f878fb3d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | G: a record ingest cannot store as an event is kept in full in a security finding and the rest of its delivery is admitted. H: an event of the receiver's own identity is stored only when not held. No code or test references G or H
- 2026-09-25 | cf78842f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Security-finding model: D and G admit an event whose integrity verification fails as received, recording a security finding, instead of rejecting it; H, I, J and L record a finding instead of refusing. D is cited by code and tests and changes meaning: an event that fails verification is now admitted with a finding. No code or test references G, H, I, J or L
- 2026-09-25 | 9acd7370 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | J: a skip event that records no branch point covers the positions above 0 up to its abandoned head. No code or test references J
- 2026-09-25 | f10e8753 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | J: a fork or a reused origin position is admitted only within the range of positions a skip event records, above its branch point and up to its abandoned head. Retire K: ingest compares no declarations, since every holder reads the recorded kind and eligibility. Rationale of I and J: the skip is appended first in the recovery's transaction, and a receiver reached by another channel that meets the continuing branch first stops that channel. No code or test references J or K
- 2026-09-25 | b0015099 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Split K into K and L: the declaration check compares the receiver's declaration of the event type; the withheld-parent contradiction is L
- 2026-09-25 | e0ce5a1c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of K: the withheld-parent flag is recorded and not interpreted
- 2026-09-25 | e0ce5a1c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add H: every ingest entry point refuses an event the receiving database originated, held or not. Amend G: events are admitted only as part of a delivery its channel admits, and no public entry point admits an event outside a delivery. Add I-K: ingest refuses a predecessor the receiver holds that is not an earlier event of the same originating database, a fork whose successors no skip event it holds lists, an event whose declarations differ from the receiver's within one major, and a withheld-parent record the receiver's log contradicts. Purpose: ingest receives the events another deployment authored, on a delivery channel. Rationale: what ingest verification covers
- 2026-09-24 | 79454334 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale states what ingest verification covers
- 2026-08-10 | 79454334 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-08-06 | a8814731 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-07-02 | 92f2bd91 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Ingest Path* | **Hash**: d60bdba5
