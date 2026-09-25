# Security Findings

## Overview

The library's core obligation is a truthful record. When it meets an integrity anomaly, something the log or a delivery exchange contradicts and no automatic path explains, it does not stop and it does not repair: it records the facts as one reserved security finding, stores the data it received exactly as it received it, and continues. A sender can always record locally, a delivery channel can always resume, and a receiver records every anomaly that could be tampering, including anomalies in its own log. How a finding is reviewed, and how it is cleared, is left open (`spec/roadmap/security-findings.md`).

Two refusals remain, and neither is an integrity finding: a caller the deployment's authentication does not admit for a channel's sender, and a database whose stored identity disagrees with its log at open, which is the one integrity check that does not continue (`EVS-DEV-event-store-open/F`). Permanent refusals of a delivery for the application's validation or an unsupported data-format major wedge the destination's queue head with an operator or upgrade exit (`EVS-PRD-destinations`), and so does a batch the receiver cannot decode as a delivery. A delivery whose delivery hash does not recompute is refused as a transient failure with a finding, so the sender sends it again. An event a delivery carries that the library cannot store as an event (malformed, or a reserved event it does not admit) is kept in full in a finding, and the rest of the delivery is admitted.

### Detection points

```text
kind                         detector   where it is detected
---------------------------  ---------  ------------------------------------------
hash_mismatch                receiver,  ingest, recovery or restore: an event's
                             sender     hash or an arrival hash does not
                                        recompute
identity_mismatch            receiver,  ingest, recovery or restore: an event
                             sender     identifier held under another sealed
                                        hash
event_malformed              receiver,  ingest, recovery or restore: a record
                             sender     the library does not store as an event
delivery_hash_mismatch       receiver   ingest: a delivery hash that does not
                                        recompute; refused as transient
predecessor_break            receiver   ingest: a held predecessor of another
                                        database, or at a later origin position
fork_unrecorded              receiver   ingest: a second successor of a held
                                        predecessor, outside every skip range
position_reused              receiver   ingest: a second event at a held origin
                                        position, outside every skip range
parent_withheld_contradicted receiver   ingest: a parent recorded as carried
                                        that the receiver does not hold
own_event_ingested           receiver   ingest: an event its own identity
                                        originated
finding_detector_mismatch    receiver   ingest: a finding whose detector is not
                                        its originating database
foreign_event                receiver   ingest: an event on a channel whose
                                        sender did not author it
predecessor_live             receiver   ingest: a delivery from a sender that a
                                        succession names as predecessor
succession_contradicted      receiver   ingest: a succession its log contradicts
channel_break                receiver   ingest: a break delivery accepted
channel_unexplained          sender     a receiver record no automatic path
                                        explains; the channel re-anchors
unknown_channel              sender     check-in: a channel of the sender that
                                        its log does not record
own_resume_recovered         sender     recovery: a resume or succession event
                                        of its own identity that it does not
                                        hold
recovery_unverified          sender     recovery or restore: a served delivery
                                        or event fails a check other than a
                                        hash that does not recompute
resend_mismatch              sender     resend: a retained delivery no longer
                                        hashes as it was sent
storage_link_break,          any holder the chain verification operation: every
sequence_missing,                       finding its verdict lists, under the
skip_record_mismatch,                   verdict's kind (the ingest kinds above
live_source_fork,                       included where the walk finds them)
index_mismatch,
parent_invalid,
parents_not_stamped,
reconciliation_invalid
```

## EVS-DEV-security-findings: Security findings

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

### Purpose

This requirement fixes the reserved event that records an integrity anomaly: its shape, its stable identity, the rule that records each anomaly once, and how findings travel. The detection points state, each where its check is specified, which kind they record and what the evidence names.

### Assertions

A. The library SHALL declare a reserved entry type `system.security_finding` with one event type, `security_finding_recorded`, and SHALL append it only through the detection points that record a finding.

B. A security finding's data SHALL carry exactly `finding_id`, `kind`, `evidence` (an object), `aggregates` (the identifiers, in ascending order, of the aggregates of the events the evidence names that the detecting database holds, or an empty list) and `detector` (an object with exactly `database_id` and `library_version`, the detecting database and the library version that detected it).

C. A finding's `finding_id` SHALL be the SHA-256, in lowercase hexadecimal, of the canonical JSON of an object with exactly the keys `database_id` (the detecting database), `kind` and `evidence`.

D. A finding's evidence SHALL hold only event identifiers, hashes, origin and local positions, database identities, channels, delivery numbers and delivery records, named reasons, and the record an anomaly concerns where the evidence carries one.

E. The library SHALL append a security finding only when, read inside the appending transaction, the detecting database holds no finding with the same `finding_id` whose originator entry names the detecting database and whose last provenance entry is that entry or the detecting database's own recovery entry.

F. The library SHALL append each finding a detection point other than the chain verification operation records in the transaction that commits that detection point's outcome.

G. Where a detection point finds an event that it cannot store because the holder holds its identifier under another sealed hash, the library SHALL record a finding of kind `identity_mismatch` whose evidence carries the received record in full, and SHALL store the rest of what it received.

H. A security finding's `kind` SHALL be one of `hash_mismatch`, `identity_mismatch`, `event_malformed`, `delivery_hash_mismatch`, `predecessor_break`, `fork_unrecorded`, `position_reused`, `parent_withheld_contradicted`, `own_event_ingested`, `finding_detector_mismatch`, `foreign_event`, `predecessor_live`, `succession_contradicted`, `channel_break`, `channel_unexplained`, `unknown_channel`, `own_resume_recovered`, `recovery_unverified`, `resend_mismatch`, `storage_link_break`, `sequence_missing`, `skip_record_mismatch`, `live_source_fork`, `index_mismatch`, `parent_invalid`, `parents_not_stamped` or `reconciliation_invalid`.

I. Ingest SHALL store a security finding another database originated as it stores any event, and the library SHALL apply no idempotence rule to it beyond ingest's own.

J. Every finding of kind `fork_unrecorded` SHALL carry as evidence exactly `database_id` (the originating database), `previous_event_hash` (the predecessor hash the fork's successors carry, null included) and `successors` (the sealed hashes, in ascending order, of the events of that database that carry it and that the detecting database holds or stores in the same delivery, recovery or restore, before or after the event whose storing detects the fork).

K. Every finding of kind `position_reused` SHALL carry as evidence exactly `database_id` (the originating database), `origin_sequence_number` and `events` (the sealed hashes, in ascending order, of the events of that database at that origin position that the detecting database holds or stores in the same delivery, recovery or restore, before or after the event whose storing detects the reuse).

L. Every finding of kind `hash_mismatch` SHALL carry as evidence exactly `event_id`, `carried_hash` (the hash the record carries, as its `event_hash` or as a receiver entry's arrival hash) and `recomputed_hash` (the hash the record it covers recomputes to).

M. Every finding of kind `predecessor_break` SHALL carry as evidence exactly `database_id` (the originating database), `event_hash` (the sealed hash of the event whose predecessor is broken) and `previous_event_hash`.

N. The library SHALL record no finding of kind `fork_unrecorded` for a fork all of whose successors sit at one origin position.

O. Where ingest, a recovery or a restore meets a received record that the library does not store as an event because it is malformed or is a reserved event the library does not admit, other than a record ingest receives whose originator entry names the receiving database, the library SHALL record a finding of kind `event_malformed` whose evidence carries the received record in full and the reason the library names, SHALL store no event for that record, and SHALL store the rest of what it received.

P. Ingest SHALL store as received, recording a security finding of kind `finding_detector_mismatch` carrying as evidence exactly `event_id`, `event_hash` (its sealed hash), `database_id` (its originating database) and `detector_database_id` (the database its detector names), an incoming security finding whose detector names a database other than its originating database, other than one whose originator entry names the receiving database.

### Rationale

**Why record and continue (assertions A and F)?** A stop needs a person before anything further is recorded, and a device that cannot deliver loses nothing only until its storage is lost. Recording the anomaly in the transaction that decides what to do with it keeps the facts and the outcome together: the suspect data is in the log as it arrived, the finding says what is wrong with it, and delivery goes on. Every holder reads the same facts and can apply its own judgement later.

**Why a stable identity, and once per anomaly (assertions C to E)?** A later clearing event names the finding it clears, so the identity must not depend on when or how often the anomaly was met: the walk and ingest meet the same fork, a check-in meets the same unknown channel after every restart, and a crash can repeat a decision. The identity is a digest of the detecting database, the kind and the evidence, and the evidence holds only facts read from the log or the exchange, never a time or an attempt count, so every detection of one anomaly by one database yields one identity and one event. The lookup matches the findings the detecting database holds as its own, whose provenance names it as originator and shows no other holder's hop: the ones it appended, kept through a restore of its storage, or got back as recovered copies, so the next detection of an anomaly after a recovery finds the copy and appends nothing. It never matches a finding that arrived by ingest, whatever detector that finding names: a sender that forged a finding under the receiver's detector would otherwise suppress the receiver's own record of the anomaly. A received finding that names a detector other than its originating database is itself a finding (assertion P), stored beside the finding it concerns. The library version is recorded but not part of the identity, so an upgrade does not record the same anomaly again. Two databases that detect one anomaly each record it: each finding states what its detector saw.

**Why name the aggregates (assertion B)?** The library's default views mark every aggregate a finding names as having an outstanding finding (`EVS-PRD-branch-conflicts`). A finding about a channel, rather than about events, names no aggregate.

**Why carry the record when it cannot be stored (assertions G and O)?** A log holds one event per identifier. An event arriving under an identifier the holder holds under another sealed hash is evidence of tampering or of two histories, and storing it in the finding keeps it in the log without replacing what is there. A record the library cannot store as an event (one without a causal record or a library version, one whose data uses a key the views reserve, a reserved event of a type the library does not declare, or a destination audit whose database identity is not its originating database) would corrupt what reads it; refusing its delivery would hold the channel on an event that every resend repeats. Kept in a finding, it is in the log, stated as what it is, and the channel goes on. The receiver serves it from the finding when a recovery asks for the delivery that carried it. A record of the receiver's own identity arriving by ingest is recorded once, as an event of the receiver's own identity, and that finding carries the record when it cannot be stored.

**Why one evidence per kind (assertions J to N)?** Ingest meets a fork when its second successor arrives, a recovery meets a hash when it checks what it pulled, and the walk meets both again later from the stored log. The evidence of these kinds names the anomaly, not the event the detector happened to be checking or where the holder stored it, so every detection of one anomaly by one database yields the same identity. Ingest records a fork or a reuse as it stores the event that makes it, and the walk meets it afterwards among the stored events, so the evidence counts as held every event the same delivery, recovery or restore stores, before or after the one that makes the fork or reuse. Each stores in one transaction, which computes the evidence over all of them before it records the finding, so the successors one delivery brings yield one finding, and ingest and the walk list the same hashes. A restore's fork and the reuse of the position after it are one anomaly when the successors share the position, so the library records it once, as the reuse, which a filter cannot hide.

**Why a received finding is an ordinary event (assertions I and P)?** A finding a sender detected travels on every channel of the sender whatever the filter (`EVS-DEV-destination-drain`), so every receiver of the sender's events holds the anomalies the sender found. At a receiver it is the sender's statement, stored and folded like any other event. A library records a finding under its own identity as both originator and detector, so a received finding whose detector names another database was not recorded as the library records one, and the receiver says so in a finding of its own.

**Layer.** A finding is a Layer 1 fact: the detecting database recorded that a check failed, with the values it compared, under the hash of the event that records it. Treating an aggregate a finding names as suspect is a Layer 2 convention of the default views.

### Changelog

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

*End* *Security findings* | **Hash**: 4fe21ce3
