# Security Findings

## Overview

The library's core obligation is a truthful record. When it meets an integrity anomaly, something the log or a delivery exchange contradicts and no automatic path explains, it does not stop and it does not repair: it records the facts as one reserved security finding, stores the data it received exactly as it received it, and continues. A sender can always record locally, a delivery channel can always resume, and a receiver records every anomaly that could be tampering, including anomalies in its own log. How a finding is reviewed, and how it is cleared, is left open (`spec/roadmap/security-findings.md`).

Two refusals remain, and neither is an integrity finding: a caller the deployment's authentication does not admit for a channel's sender, and a database whose stored identity disagrees with its log at open, which is the one integrity check that does not continue (`EVS-DEV-event-store-open/F`). Permanent refusals of a delivery for the application's validation or an unsupported data-format major wedge the destination's queue head with an operator or upgrade exit (`EVS-PRD-destinations`), and so does a batch the receiver cannot decode as a delivery. A delivery whose delivery hash does not recompute is refused as a transient failure with a finding, so the sender sends it again. An event a delivery or a restore carries that the library cannot store as an event (malformed, or a reserved event of a declared reserved entry type in a shape that type does not have) is kept in full in a finding, and the rest of the delivery is admitted.

### Detection points

Each finding names its detector: the database that detected it and the role in which it did. One anomaly can be met by several roles of one database, and by several databases; each role of each database records it once.

```text
kind                    role      where it is detected
----------------------- --------- -----------------------------------------
hash_mismatch           ingest,   an event's hash or an arrival hash does
                        restore,  not recompute
                        walk
identity_mismatch       ingest,   an event identifier held under another
                        restore   sealed hash
event_malformed         ingest,   a record the library does not store as an
                        restore   event
delivery_hash_mismatch  ingest    a delivery hash that does not recompute;
                                  refused as transient
predecessor_break       ingest,   a held predecessor of another database,
                        restore,  or at a later origin position
                        walk
fork_unrecorded         ingest,   a second event of one database after one
                        restore,  predecessor, at another origin position
                        walk
position_reused         ingest,   a second event of one database at one
                        restore,  origin position (a fork whose successors
                        walk      share one position is recorded only so)
own_event_ingested      ingest    an event its own identity originated
foreign_event           ingest    an event on a channel whose sender did
                                  not author it
channel_unexplained     sender    a receiver record no automatic path
                                  explains; the channel starts a new
                                  generation
sender_regressed        sender    a record of the channel's receiver database
                                  ahead of the sender's
restore_unverified      restore   a served delivery or event fails a check
                                  other than a hash that does not recompute
succession_ahead        ingest    a succession event naming a delivery of a
                                  channel above the receiver's record of it
storage_link_break,     walk      the chain verification operation: the
sequence_missing,                 storage-chain and parent findings its
parent_invalid,                   verdict lists
parents_not_stamped
```

## EVS-DEV-security-findings: Security findings

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

### Purpose

This requirement fixes the reserved event that records an integrity anomaly: its shape, its stable identity, the rule that records each anomaly once per detector, and how findings travel. The detection points state, each where its check is specified, which kind they record; the evidence of every kind is fixed here.

### Assertions

A. The library SHALL declare a reserved entry type `system.security_finding` with one event type, `security_finding_recorded`, and SHALL append it only through the detection points that record a finding.

B. A security finding's data SHALL carry exactly `finding_id`, `kind`, `evidence` (an object), `aggregates` (the identifiers, in ascending order, of the aggregates of the events the evidence names, or for a finding of kind `fork_unrecorded` or `position_reused` of the events of the named database that carry the named predecessor hash or sit at the named origin position, that the detecting database holds when it records the finding, counting the events stored earlier in the recording transaction; or an empty list) and `detector` (an object with exactly `database_id`, the detecting database; `role`, one of `ingest`, `restore`, `sender` or `walk`; and `library_version`, the library version that detected it).

C. A finding's `finding_id` SHALL be the SHA-256, in lowercase hexadecimal, of the canonical JSON of an object with exactly the keys `database_id` (the detecting database), `role` (the detector's role), `kind` and `evidence`.

D. A finding's evidence SHALL hold only event identifiers, hashes, origin and local positions, database identities, channels, delivery numbers and delivery records, named reasons and checks, the values a check compared, and the record an anomaly concerns where the evidence carries one.

E. The library SHALL append a security finding only when, read inside the appending transaction, the detecting database holds as authored no finding with the same `finding_id`.

F. The library SHALL append each finding a detection point other than the chain verification operation records in the transaction that commits that detection point's outcome.

G. Where a detection point finds an event that it cannot store because the holder holds its identifier under another sealed hash, the library SHALL record a finding of kind `identity_mismatch` whose evidence carries the received record in full, and SHALL store the rest of what it received.

H. The library SHALL record a security finding only with a `kind` among `hash_mismatch`, `identity_mismatch`, `event_malformed`, `delivery_hash_mismatch`, `predecessor_break`, `fork_unrecorded`, `position_reused`, `own_event_ingested`, `foreign_event`, `channel_unexplained`, `sender_regressed`, `restore_unverified`, `succession_ahead`, `storage_link_break`, `sequence_missing`, `parent_invalid` and `parents_not_stamped`.

I. Ingest SHALL store a security finding another database originated as it stores any event, whatever its kind and detector, and the library SHALL apply no idempotence rule to it beyond ingest's own.

J. Every finding of kind `fork_unrecorded` SHALL carry as evidence exactly `database_id` (the originating database) and `previous_event_hash` (the predecessor hash the fork's successors carry, null included).

K. Every finding of kind `position_reused` SHALL carry as evidence exactly `database_id` (the originating database) and `origin_sequence_number`.

L. Every finding of kind `hash_mismatch` SHALL carry as evidence exactly `event_id`, `carried_hash` (the hash the record carries, as its `event_hash` or as a receiver entry's arrival hash) and `recomputed_hash` (the hash the record it covers recomputes to).

M. Every finding of kind `predecessor_break` SHALL carry as evidence exactly `database_id` (the originating database), `event_hash` (the sealed hash of the event whose predecessor is broken) and `previous_event_hash`.

N. <RETIRED> Forks and reused origin positions are recorded by the checks of the chain verification in `spec/causal-history.md`.

O. Where ingest or a restore meets a received record that the library does not store as an event because it is malformed, because it is an event of a reserved entry type the release declares whose aggregate type or event type is not one the library declares for that entry type, or because it is an event of a reserved destination audit entry type whose destination identifier or database identity is missing, empty, not a string or contains `|`, or whose database identity is not its originating database, other than a record ingest receives whose originator entry names the receiving database, the library SHALL record a finding of kind `event_malformed` whose evidence carries the received record in full and the reason the library names, SHALL store no event for that record, and SHALL store the rest of what it received.

P. <RETIRED> A received finding is stored as any event, whatever detector it names, as assertion I states.

Q. The library SHALL record each finding under the detector role of the operation that detects it: `ingest` for an ingest entry point, `restore` for the restore operation, `sender` for the drainer and `walk` for the chain verification operation.

R. Every finding of each of the following kinds SHALL carry as evidence exactly the keys listed for its kind: `identity_mismatch`: `event_id`, `held_hash` (the sealed hash the holder holds the identifier under) and `record` (the received record in full); `event_malformed`: `reason` (`record_malformed`, `reserved_type_undeclared` or `audit_identity_invalid`) and `record`; `delivery_hash_mismatch`: `channel`, `delivery_number`, `carried_hash` and `recomputed_hash`; `own_event_ingested`: `event_id`, `sealed_hash` and `record` (the received record in full when the receiver does not hold the event and cannot store it as an event, otherwise null); `foreign_event`: `channel`, `delivery_number`, `event_id` and `sealed_hash`; `channel_unexplained` and `sender_regressed`: `channel`, `sender_record` and `receiver_record` (each an object with exactly `delivery_number` and `delivery_hash`), `recorded_receiver_database_id` (null when the sender channel record holds none) and `responding_receiver_database_id`; `restore_unverified`: `channel`, `delivery_number`, `event_id` (null when the check concerns the delivery) and `check` (`delivery_link`, `delivery_hash`, `originator` or `receiver_entry`); `succession_ahead`: `channel`, `receiver_record` and `succession_record` (the delivery the succession event names, in the same shape); `storage_link_break`: `local_sequence_number`, `event_id`, `field` (`ingest_sequence_number` or `previous_ingest_hash`), `expected` and `actual`; `sequence_missing`: `local_sequence_number`; `parent_invalid`: `local_sequence_number`, `event_id`, `parent` (the parent as the event names it) and `reason` (`other_aggregate`, `annotation`, `ineligible` or `held_under_other_hash`); `parents_not_stamped`: `local_sequence_number`, `event_id`, `expected` (the parents the stamping rule yields) and `actual` (the parents the event names).

### Rationale

**Why record and continue (assertions A and F)?** A stop needs a person before anything further is recorded, and a device that cannot deliver loses nothing only until its storage is lost. Recording the anomaly in the transaction that decides what to do with it keeps the facts and the outcome together: the suspect data is in the log as it arrived, the finding says what is wrong with it, and delivery goes on. Every holder reads the same facts and can apply its own judgement later.

**Why a stable identity, once per detector (assertions B, C and E)?** A later clearing event names the finding it clears, so the identity must not depend on when or how often the anomaly was met: a walk run twice meets the same fork twice, a later successor of a recorded fork arrives, and a crash can repeat a decision. The identity is a digest of the detecting database, its role, the kind and the evidence, and the evidence holds only the facts that fix the anomaly, never a time, an attempt count or a list that grows as more events arrive, so a detector that meets the same anomaly again computes the same identity and appends nothing. The aggregates a finding names are recorded beside the evidence, outside the identity: they state what the detector held when it recorded. The role is part of the identity, so ingest and the walk of one database each record what they found, and the log states which check found it. The lookup matches only findings the detecting database holds as authored: a finding that arrived by ingest is another database's statement, whatever detector it names, so a sender cannot suppress the receiver's own record by forging one. The library version is recorded but not part of the identity, so an upgrade does not record the same anomaly again. Two databases that detect one anomaly each record it: each finding states what its detector saw.

**Why name the aggregates (assertion B)?** The library's default views mark every aggregate a finding names as having an outstanding finding (`EVS-PRD-materializer`). A finding about a channel, rather than about events, names no aggregate.

**Why carry the record when it cannot be stored (assertions G and O)?** A log holds one event per identifier. An event arriving under an identifier the holder holds under another sealed hash is evidence of tampering or of two histories, and storing it in the finding keeps it in the log without replacing what is there. A record the library cannot store as an event (one without a causal record or a library version, one whose data uses a key the views reserve, a reserved event whose aggregate type or event type is not one the library declares for its entry type, or a destination audit whose database identity is not its originating database) would corrupt what reads it; refusing its delivery would hold the channel on an event that every resend repeats. Kept in a finding, it is in the log, stated as what it is, and the channel goes on. A reserved event of an entry type the release does not declare, or with an enumerated value it does not know, is not malformed: a later release of the data-format major added it, and it is stored as received (`EVS-DEV-destination-drain/L`). A record of the receiver's own identity arriving by ingest is recorded once, as an event of the receiver's own identity, and that finding carries the record when it cannot be stored.

**Why a fixed evidence per kind (assertions J to M and R)?** Every kind has an exact key set, and every named reason or check a closed list, so two releases of one data-format major spell the evidence of one anomaly alike, compute one identity, and a detector upgraded between two meetings of the anomaly records it once. The evidence of the kinds in J to M names the anomaly, not where the holder stored it, so a reader compares findings of one kind across detectors and databases directly. The walk's own kinds name the holder's local positions, since only the holder's walk records them. A fork is fixed by its database and the predecessor its successors share, and a reused position by its database and the position; the log locates the events. A successor of a recorded fork that arrives later is the same anomaly, so its detector records nothing again, and the default views mark its aggregate from the finding already held (`EVS-PRD-materializer`). A fork whose successors all sit at one origin position is also a reuse of that position, and each detector records it once, as the reuse.

**Why a received finding is an ordinary event (assertion I)?** A finding a sender detected travels on every channel of the sender whatever the filter (`EVS-DEV-destination-drain`), so every receiver of the sender's events holds the anomalies the sender found. At a receiver it is the sender's statement, stored and folded like any other event, including one of a kind a later release of its data-format major added.

**Layer.** A finding is a Layer 1 fact: the detecting database recorded that a check failed, with the values it compared, under the hash of the event that records it. Treating an aggregate a finding names as suspect is a Layer 2 convention of the default views.

### Changelog

- 2026-09-26 | c2d8a734 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | 29b85b41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | 287480f7 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | D: evidence may name checks and the values a check compared. H: `recovery_unverified` is named `restore_unverified`; add `succession_ahead`. Add R: the exact evidence keys of every kind that J to M do not fix, with closed lists of reasons and checks, so a finding's identity holds across releases of one data-format major. No code or test references D, H or R
- 2026-09-25 | dc8fedf1 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | J and K: the evidence of fork_unrecorded and position_reused is the fixed coordinates of the anomaly (database and predecessor hash, database and origin position), with no list of the events held, so a later successor of a recorded fork is the same anomaly and its detector records nothing; B: those findings name the aggregates of the held events carrying that predecessor or at that position. O: the reserved records kept in an event_malformed finding are exactly the shapes a declared reserved entry type does not have, never an entry type or enumerated value a later release added. Detection points: the restore records predecessor_break, fork_unrecorded and position_reused, and a fork whose successors share one origin position is recorded only as position_reused. No code or test references B, J, K or O
- 2026-09-25 | afb4b5ce | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Add Q: each detection point records its findings under its own detector role. No code or test references Q
- 2026-09-25 | 5af6677b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | B, C and E: the detector names its role (ingest, restore, sender or walk), which the identity covers, so each detector records an anomaly once and ingest and the walk each record theirs; the lookup matches findings the detecting database holds as authored. H: remove the kinds finding_detector_mismatch, unknown_channel, resend_mismatch, predecessor_live, succession_contradicted, index_mismatch, own_resume_recovered, skip_record_mismatch, reconciliation_invalid, channel_break, parent_withheld_contradicted and live_source_fork; add sender_regressed. I: a received finding is stored whatever its kind and detector. J and K: the evidence counts what the detector holds when it records, with no alignment between detectors. O: no recovery. Retire N (a fork is recorded as found) and P (no detector check on a received finding). No code or test references any of these letters
- 2026-09-25 | 4fe21ce3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | E: the once-per-anomaly lookup matches only findings the detecting database holds as its own (originator entry names it, last provenance entry is that entry or its own recovery entry), never a received finding naming it as detector. H and P: add the kind finding_detector_mismatch, recorded at ingest for a received finding whose detector is not its originating database, which is stored as received. I: a received finding is one another database originated. J and K: the evidence counts every event the same delivery, recovery or restore stores, before or after the detecting one. O: ingest records no event_malformed for a record of the receiver's own identity. No code or test references any of these letters
- 2026-09-25 | 00d7dc1d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | E: the once-per-anomaly lookup matches every held finding whose detector names the detecting database, whatever path stored it (appended, recovered or restored). J and K: the evidence counts the incoming event and the events stored earlier in the same delivery, recovery or restore among the held events, so ingest and the walk compute one identity. No code or test references E, J or K
- 2026-09-25 | d2f056fa | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | D: evidence may name a reason. H: add the kinds event_malformed and delivery_hash_mismatch. Add J-M: one evidence per kind for fork_unrecorded, position_reused, hash_mismatch and predecessor_break, whichever detection point records it; N: a fork whose successors share one origin position is recorded only as a reused position; O: a received record the library cannot store as an event is kept in full in an event_malformed finding and the rest of the delivery is stored. No code or test references any of these letters
- 2026-09-25 | 1a5e11f7 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | e738bf4e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 967987e3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-I: the reserved security finding with its stable identity, kind, evidence, aggregates and detector, appended once per anomaly in the transaction of the detection point's outcome, an unstorable event carried in full, and a received finding stored as any event

*End* *Security findings* | **Hash**: c2d8a734
