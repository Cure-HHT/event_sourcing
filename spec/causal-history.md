# Chains and Causal Parents

## Overview

Every event the library stores takes part in two hash chains and in one causal structure. This file specifies how the library links them, how it verifies them from the stored log alone, what it verifies at ingest, and the security finding it records for each contradiction (`EVS-DEV-security-findings`).

- **Storage chain.** One per database: the events the database stored, in local sequence order, whatever path stored them. The last provenance entry of each stored event records the stored hash of the event before it. A storage chain never forks. A restore of the database truncates it, and later events continue it.
- **Origin chain.** One per originating database: the events the database authored. Each event carries, as its predecessor hash, the sealed hash of the event its database authored before it. A receiver holds the subset its channels delivered. A database that went back in time (restored from a backup) appends events whose predecessor is the last event it authored before the backup, while the events it authored after the backup survive at its receivers. Its origin chain therefore forks, and its new events reuse the origin positions of the lost ones. Every holder that meets a fork or a reused position records it as a security finding.
- **Causal parents.** Within an aggregate, each event names the versions it follows. Kind and eligibility, recorded on each event, decide which events a later event may name.

### Terms

- **Originating database.** The database identity that an event's first provenance entry records.
- **Sealed hash.** The hash the originating database sealed an event under, the same at every holder: the `event_hash` of a copy whose provenance holds exactly one entry, otherwise the `arrival_hash` of the copy's second provenance entry.
- **Origin position.** The local sequence number the originating database stored the event under: the copy's `sequence_number` when its provenance holds exactly one entry, otherwise the `origin_sequence_number` of its second provenance entry.
- **Held as authored.** A copy whose provenance holds exactly one entry, naming the holding database.
- **Fork.** Two or more events of one originating database carrying the same predecessor hash; a null predecessor hash counts as one value.
- **Reused position.** An origin position at which more than one held event of one originating database sits.
- **Version, annotation, eligible.** The kind and eligibility recorded on an event. A version replaces its aggregate's state; an annotation is about the aggregate's current version. Only an eligible version is ever named as a parent.

```text
origin chain of database D, restored from a backup taken after e3

  e1 <- e2 <- e3 <- e4 <- e5              delivered, then lost at D
                ^
                +---- f4 <- f5            appended after the restore

  e4 and f4 both carry sealed(e3) at origin position 4: a fork whose
    successors share one position, recorded once, as a reused position
  f5 sits at the origin position of e5: a reused position, recorded
    as a finding
  a receiver whose channel filtered e4 holds e3 and e5 but sees no
    fork; f5 still reuses position 5, and that is recorded
```

### Reading order

`EVS-DEV-chain-verification` fixes how events are linked, the verification operation, its verdict, and the predecessor, fork and position checks at ingest. `EVS-DEV-causal-parents` fixes the causal record on every event, the declarations it is stamped from, the stamping rule and the verification of parents. How the default views mark an aggregate a finding names is specified with the materializer (`EVS-PRD-materializer`).

## EVS-DEV-chain-verification: Storage and origin chain verification

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

### Purpose

This requirement fixes how the library links each stored event into its database's storage chain and each authored event into its database's origin chain. It fixes how an event's originating database, sealed hash and origin position are read from any copy of it, the verification operation that walks both kinds of chain and the verdict it returns, the checks every ingest entry point applies and the security findings they record, the findings the walk records, and the bounds that keep verification off the append path.

### Assertions

A. The library SHALL read an event copy's originating database from the `database_id` of its first provenance entry; its sealed hash from its `event_hash` when its provenance holds exactly one entry, and otherwise from the `arrival_hash` of its second provenance entry; and its origin position from its `sequence_number` when its provenance holds exactly one entry, and otherwise from the `origin_sequence_number` of its second provenance entry. It SHALL resolve every predecessor hash and every causal parent against sealed hashes, never against a holder's re-stamped `event_hash`.

B. The library SHALL set a locally appended event's `previous_event_hash` to the sealed hash of the event with the highest local sequence number among the events the appending database holds as authored, or to null when it holds none. It SHALL read that event inside the append's transaction.

C. In the last provenance entry of every event it stores (the originator entry of an event it appends, and the entry it adds to an event it ingests or restores), the library SHALL record the event's local sequence number as `ingest_sequence_number`. It SHALL record as `previous_ingest_hash` the stored `event_hash` of the event at the preceding local sequence number, or null for the database's first stored event, read inside the storing transaction.

D. The library SHALL provide a chain verification operation over the holding database's log. The operation SHALL take an optional inclusive range of local sequence numbers and SHALL check every event stored in the range, whatever path stored it: its own hash and every receiver entry's arrival hash, as ingest verifies them; that its last provenance entry's `ingest_sequence_number` equals its local sequence number; and that its last provenance entry's `previous_ingest_hash` equals the stored `event_hash` of the event at the preceding local sequence number. For the first event of the range, that preceding event SHALL be read from outside the range, and for the first event the database stored the expected value is null.

E. The chain verification operation SHALL report each local sequence number in its range, up to the highest the database has stored, at which no event is stored.

F. For every event in its range, the chain verification operation SHALL report a predecessor break in three cases: when the event's `previous_event_hash` is the sealed hash of a held event that its originating database did not author; when it is the sealed hash of an event of that database at an origin position not below the event's own; and when the event is held as authored and its `previous_event_hash` is neither null nor the sealed hash of a held event.

G. The chain verification operation SHALL report, once each, every reused position at which an event in its range sits, and every fork that an event in its range takes part in whose successors do not all sit at one origin position.

H. <RETIRED> Forks and reused positions are reported by assertion G of this requirement.

I. The chain verification operation SHALL return a verdict listing each finding it reports, by kind (`hash_mismatch` or `storage_link_break` for a check of assertion D, `sequence_missing`, `predecessor_break`, `fork_unrecorded`, `position_reused`, `parent_invalid` or `parents_not_stamped`) and evidence, the evidence a security finding of that kind carries. The verdict SHALL also give the number of predecessor hashes naming no held event, and SHALL be valid exactly when it lists no finding.

J. The chain verification operation SHALL refuse, before it reads any event, a range with a negative bound or with a lower bound above its upper bound, and SHALL treat an omitted lower bound, or a lower bound of 0, as the database's first stored event.

K. Every ingest entry point and the restore operation SHALL store as received, recording a security finding of kind `predecessor_break`, an incoming event whose `previous_event_hash` is the sealed hash of a held event that the incoming event's originating database did not author, or of an event of that database at an origin position not below the incoming event's, counting among the held events those stored earlier in the same transaction.

L. Every ingest entry point and the restore operation SHALL store as received, recording a security finding of kind `position_reused`, an incoming event at an origin position at which a held event of the same originating database with another event identifier sits, and, recording one of kind `fork_unrecorded`, an incoming event whose `previous_event_hash` (null included) a held event of that database with another event identifier at another origin position already carries, counting among the held events those stored earlier in the same transaction.

M. <RETIRED> A second authored successor at the holding database is a fork, reported by assertion G of this requirement.

N. <RETIRED> The lookups by sealed hash, predecessor, origin position and latest authored event are served by the storage backend's indexes, as this requirement's Rationale states.

O. <RETIRED> The walk compares no index with the log, since the library keeps no index of its own.

P. Every ingest entry point SHALL store as received, recording a security finding of kind `hash_mismatch` naming the event, the hash it carries and the hash it recomputes to, an incoming event whose `event_hash`, or the arrival hash of one of its receiver entries, does not recompute.

Q. Every ingest entry point SHALL record a security finding of kind `own_event_ingested` naming the event and its sealed hash, and no finding of kind `event_malformed`, for an incoming event whose originator entry names the receiving database's identity, whether or not the receiver holds it; SHALL store such an event as received when the receiver does not hold it and the library can store it as an event; and SHALL carry the received record in full in that finding's evidence when the receiver does not hold it and the library cannot store it as an event.

R. For each finding its verdict lists, the chain verification operation of an open event store SHALL record a security finding of the verdict finding's kind carrying the verdict finding's evidence, in a write transaction of its own committed after the read that found it.

S. The chain verification operation SHALL hold no transaction that an append waits for, and SHALL fix the upper bound of the range it walks when it starts.

T. On the Postgres backend, the library's append throughput and its ingest throughput SHALL each be at least half the throughput that the baseline build its throughput test pins (the library as it stood on its main branch before the data-format major step) reaches on the same workload and host.

### Rationale

**Why read the originating database, the sealed hash and the origin position this way (assertion A)?** Each receiver re-stamps the copy it stores: it appends its entry, gives the event its own local sequence number, and seals the result under a new `event_hash`. So a copy's `event_hash` and `sequence_number` belong to its holder, not to the event. The values the originator sealed survive in the copy: the first receiver entry records the hash the event arrived under and the position the originator stored it at, and ingest's arrival-hash walk verifies that entry against the record. Reading the originating database, the sealed hash and the origin position from these fields gives the same values at every holder: the originator, each receiver and a successor that restored the event. Links and parents therefore resolve alike everywhere. The originator entry names the database identity (`EVS-DEV-event-record`); the application's `Source` identifier is attribution and plays no part in chaining.

**Why does the predecessor name the previous authored event, not the log's last event (assertion B)?** A database delivers only the events it authored (`EVS-PRD-destinations/C`). If an authored event named the last event in its database's log, its predecessor would often be an event the database ingested, and no receiver ever holds that event under the hash the database stored it under. At receivers the origin chain would then be a set of unfollowable links. Naming the previous authored event makes each database's origin chain consist only of events that can travel. A receiver can follow every link whose predecessor its channels delivered, and a destination's filter leaves the only gaps, which the delivery chain accounts for. Reserved events a database appends are authored events and take part in its origin chain.

**Why a separate storage chain (assertions C to E)?** Once the origin link names only authored events, it no longer ties an ingested event into the holder's log. A deletion of an ingested event, or of a locally appended event after which nothing was appended, would break no origin link. The storage chain restores that coverage. Every stored event, whatever path stored it, records the event stored before it in the last entry of its provenance, which that stored copy's hash covers, so deleting, inserting or reordering any stored event breaks the chain at that point. The fields are the ones every receiver entry already carries; a locally appended event carries them in its originator entry. The storage chain is the holder's own and never forks: a restore of the holder's database brings it back to a prefix of itself.

**Why check the first event of a range (assertion D)?** A verification that takes its range's first event as an anchor without checking it cannot detect a tampered link at the start of any range it is given, and a caller that walks a log range by range misses the break at every seam. The operation reads the event before the range, the one read outside it, so a range verification checks exactly the links a whole-log verification checks within that range.

**Why these predecessor checks (assertion F)?** At a receiver, a predecessor hash naming no held event is expected: the destination's filter left the event out, or it came before the channel's start date. The delivery chain, not the origin chain, shows the receiver holds every delivery. So the walk counts such links rather than reporting them. What the holder can refute, it reports. A predecessor that is held but was authored by another database, or that sits at a later origin position, cannot be the event authored before this one. The holder's own authored events are all held unless the holder lost one, so a dangling predecessor of an event held as authored is a break.

**Why record every fork and every reused position (assertions G, K and L)?** A database that went back in time and appended forks its origin chain where its backup ended, and its new events sit at origin positions its lost events held. So does a second live copy of a database, and so does tampering. The holder cannot tell these apart from the chain, and the library does not try: each fork and each reused position it meets is a finding, the events are stored as received, and delivery continues. The regressed sender records its own finding when its receiver's record reveals the regression, and the application rebuilds it as a successor (`EVS-PRD-delivery-channel`). Both kinds are checked because a filter can hide either: a receiver whose channel left out the lost event after the fork point sees no fork, yet still holds a lost event at a position the restored database uses again. At the database that authored a chain, a fork of its own authored events is the same finding. A fork whose successors all sit at one origin position is also a reuse of that position, and each detector records it once, as the reuse, so an event that reuses a position after a restore is one finding per detector; a fork whose successors sit at different positions (the restored database stored other events in between) is recorded as a fork. The restore that rebuilds a regressed sender as a successor brings back both branches, so it applies the same checks and records what it meets under its own role: the successor's default views then mark the forked aggregates as the receiver's do.

**Why a storage chain can be checked without exception.** No library operation removes or rewrites a stored event. Retention and redaction act on the security context stored beside an event, not on the event itself, and a destination's queue items are not events. A gap or a changed link in the storage chain therefore always means that something outside the library's operations wrote to the log.

**Why this verdict (assertion I)?** An auditor needs to tell tampering from what a filter leaves out. Findings are the facts the log contradicts. The count of unresolved predecessors shows how much of each origin chain the holder cannot follow.

**What ingest checks (assertions K, L, P and Q).** Ingest, and the restore for K and L, checks what a delivery can contradict: an event whose hash does not recompute, a predecessor that is held but belongs to another database or sits at a later position, a second event after a held predecessor or at an occupied origin position, and an event of the receiver's own identity. Each is stored as received with a security finding (`EVS-DEV-security-findings`), and delivery continues: the receiver's log holds the suspect event and the statement of what is wrong with it. An event of the receiver's own identity is recorded as that, and not also as a malformed record, so one event arriving where it should not is one finding; one the library cannot store as an event is kept in that finding instead. An unheld predecessor is accepted without a finding: the delivery chain covers it. Ingest does not check that the named predecessor is the event its originator authored immediately before, because a receiver whose channel filters events cannot know which events came in between.

**How the lookups are served.** Every append reads the database's latest authored event, and every ingested event is looked up by sealed hash, by predecessor and by origin position. The library keeps no index of its own for these. On Postgres they are columns of the events table, written by the insert that stores each event, and the database's indexes over them. On Sembast the backend keeps one record of the latest event held as authored, written in the transaction that stores such an event, and scans the log for the other lookups, which only ingest and the restore make. Recording a finding looks up the held authored findings by `finding_id` the same way: an index over the finding identity on Postgres, a scan of the reserved finding events on Sembast, where findings are rare. That record is persisted state under the storage precondition (`EVS-PRD-destinations/L`); a record that drifted from the log shows in the walk as a predecessor break or a fork. The throughput bound (assertion T) keeps the lookups proportionate.

**Why record the walk's findings, and hold nothing appends wait for (assertions R and S)?** A verdict returned to a caller is lost with the caller; recorded as findings, the anomalies the walk meets are facts of the holder's log, each once, whoever runs the walk and however often. The walk only reads, and appends never wait for it: on Postgres it reads in one read-only snapshot, and readers never block writers; on Sembast it reads through the database outside any transaction, and only transactions take the lock appends wait for. Its upper bound is fixed when it starts, so events appended meanwhile are left to the next walk. Its findings are appended in short transactions of their own; a crash repeats nothing, because a detector records a finding once. A verifier that holds only the log computes the same verdict and appends nothing. A full walk of a large log is slow, so the library does not schedule it: a caller runs it, over a range from its last verified position or over the whole log, where a deployment chooses in a maintenance window. Walking in resumable chunks is recorded in `spec/roadmap/security-findings.md`.

**Why a throughput bound (assertion T)?** The chains and the causal stamping add reads and writes to every append and every ingested event. Measured against the last release of the preceding major on the same workload and host, a fixed baseline the test pins, the bound keeps that cost proportionate and makes a regression fail the suite.

**Succession.** A successor is a new database with its own identity, so its authored events form their own origin chain, rooted at its first event, and its predecessor's events form the predecessor's chain as the successor restored them, forks included. A succession therefore creates no fork, and the walk verifies the two chains separately. Continuity of authorship across a succession is a Layer 2 interpretation. The Layer 1 facts are the two chains and the succession event.

**Layer.** Every check here is Layer 1: it compares recorded hashes and positions with each other and records what they contradict. The hashes are unkeyed (`EVS-PRD-hash-chain-integrity`, Rationale), so the checks detect a change made without recomputing the hashes it affects, not one whose author recomputed them.

### Changelog

- 2026-09-26 | 201d5630 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | I: every verdict finding carries the evidence a security finding of its kind carries. T: the baseline is the build the throughput test pins, the library on its main branch before the data-format major step. No code or test references I or T
- 2026-09-25 | 9d31a14d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | G and L: a fork whose successors all sit at one origin position is reported and recorded only as the reused position, so an event reusing a position is one finding per detector. K and L: the restore operation applies the predecessor, fork and reused-position checks, counting the events stored earlier in its transaction. No code or test references G, K or L
- 2026-09-25 | b3a7a9ab | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | G and L: every fork and every reused origin position is reported by the walk and recorded at ingest, with no skip-range coverage. I: no skip_record_mismatch, live_source_fork, index_mismatch or reconciliation_invalid kinds and no accepted forks in the verdict. Retire H (no skip records to check), M (a fork at the author is reported by G), N and O (the lookups are served by the storage backend's indexes, not a library-kept chain index the walk compares). C: no recovery path. R: no index_mismatch. Code and tests reference N (event_sourcing/lib/src/storage/chain_index_entry.dart, storage_backend.dart, sembast_backend.dart, postgres/postgres_schema.dart, postgres/postgres_backend.dart and event_store.dart; test/storage/storage_backend_conformance.dart and test/event_store/chain_index_reads_test.dart), and C (event_store.dart, provenance_stamping_conformance.dart), unchanged in substance; no code or test references G, H, I, L, M, O or R
- 2026-09-25 | 84414096 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | N: the index keys the held findings the holder holds as its own (originator entry names it, last provenance entry is that entry or its own recovery entry), not every finding naming it as detector. Q: an event of the receiver's own identity gets only the own_event_ingested finding, no event_malformed or finding_detector_mismatch; one the library cannot store as an event is carried in full in that finding. R: an index_mismatch is rechecked against the whole log as the recording transaction reads it. No code or test references N, Q or R
- 2026-09-25 | ef03515e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | N: the index keys every held finding whose detector names the holder, whatever path stored it. O: the walk compares the index only up to its fixed upper bound, and an entry naming a latest event only when that event is at or below the bound. R: an index_mismatch is recorded only when it still holds, read again inside the recording transaction. T: the baseline is the last release of the preceding data-format major. No code or test references N, O, R or T
- 2026-09-25 | 44a41d7b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | One finding per anomaly: G reports each uncovered fork and reused position once, not once per event taking part; I and R: a walk finding of kind hash_mismatch, predecessor_break, fork_unrecorded or position_reused carries the evidence every detection point gives that kind; K and L: the evidence is that of the kind. N: the chain index locates each authored security finding by its identity. Q: an event of the receiver's own identity is a finding whether or not the receiver holds it, and is stored only when it does not. No code or test references any of these letters
- 2026-09-25 | 2525bd43 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | b6831f70 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Security-finding model: K and L store an event whose predecessor, fork or reused position ingest contradicts as received and record a security finding instead of refusing it; add P and Q: an event whose hash does not recompute, and an event of the receiver's own identity, are stored with a finding; I: the verdict's finding kinds are enumerated; add R: the walk records a finding for each finding of its verdict; add S: the walk holds no transaction an append waits for and fixes its upper bound when it starts; add T: append and ingest throughput on Postgres stay within half of the preceding major's. No code or test references I, K or L
- 2026-09-25 | 55f8ef2d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | H: the order of branch point and abandoned head is checked only for a skip that records a branch point. No code or test references H
- 2026-09-25 | fb9dd94d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | G, H, I and L: a skip event records its abandoned range of origin positions, above its branch point and up to its abandoned head, in place of a list of forks; the walk reports, and ingest refuses with `fork_unrecorded` or `position_reused`, a fork or a reused origin position of one database that no held skip's range covers, so a restore whose fork a filter hid is still detected; the verdict lists no declaration notes. No code or test references G, H, I or L
- 2026-09-25 | 1ede8b97 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | N: the chain index also locates, per delivery channel, the latest accepted-delivery audit the holder authored. Rationale of G and L: a skip appended with nothing after the restore is a successor at the fork it lists; shortened. No code or test references N
- 2026-09-25 | 162b4322 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Split N: the index is kept and read by the append, ingest and parent-stamping paths (N) and checked by the walk (O). Rationale of G and L: a receiver reached by several channels can meet the continuing branch before the skip, and that channel stops. No code or test references N
- 2026-09-25 | 5a531379 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend G and I: no pending recovery; a recovery appends its skip in the transaction that stores the recovered events, so every unlisted fork of the holder's own chain is a finding. Rationale: an unlisted fork a receiver meets before its skip stops the channel for a person to reconcile; a receiver behind a skip receives it again in the exact resend. No code or test references G or I
- 2026-09-25 | 954cdf2b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-N: sealed hash, origin position and originating database read alike at every holder; the predecessor hash names the previous authored event; every stored event records its storage link in its last provenance entry; the chain verification operation over the storage chain and every origin chain, checking the first event of a range, reporting predecessor breaks, unrecorded forks (successors no skip lists by name) and skip record mismatches, listing pending recoveries, with its verdict; the predecessor check and the fork_unrecorded refusal at ingest; a second authored successor at the holding database reported as a fork of live sources; the chain index checked against the log

*End* *Storage and origin chain verification* | **Hash**: 201d5630

## EVS-DEV-causal-parents: Causal parents within an aggregate

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-hash-chain-integrity

### Purpose

This requirement fixes the causal record each event carries, the declarations of kind and eligibility it is stamped from, the rule the library stamps parents by, and how the chain verification operation checks parents.

### Assertions

A. Every event record SHALL carry a `causal` object with exactly these keys: `kind` (`version` or `annotation`); `eligible` (a boolean); and `parents` (a list, in ascending order of event identifier, of objects with exactly `event_id` and `event_hash`, each naming an event of the same aggregate by its identifier and sealed hash).

B. The library SHALL treat a record that carries no `causal` object of that shape as malformed: every ingest entry point and restore SHALL store no event for it, and every append and read SHALL refuse it, naming the field.

C. The library SHALL let an entry-type definition declare, for each event type, its kind and its eligibility, and SHALL stamp an event type the definition does not declare as an eligible version.

D. The library SHALL refuse an entry-type definition that declares one event type twice.

E. The library SHALL declare every event type of every reserved entry type an ineligible annotation.

F. On every append, the library SHALL stamp `kind` and `eligible` from the appended entry type's declaration for the appended event type, and `parents` by the stamping rule, inside the append transaction.

G. The public append operations SHALL take no argument that sets any part of `causal`.

H. For every appended event, the library SHALL stamp `parents` as the latest eligible version of the aggregate in the appending database's log: the event with the highest local sequence number among the events of that aggregate the database holds whose `causal` records an eligible version, or no parent when there is none.

I. <RETIRED> Every appended event is stamped by the one rule of assertion H of this requirement.

J. <RETIRED> Every appended event is stamped by the one rule of assertion H of this requirement.

K. For every event in its range and every parent it names that the holder holds, the chain verification operation SHALL report an invalid parent in each of these cases: the parent belongs to another aggregate; its `causal` records an annotation; its `causal` records it ineligible; or the holder holds the named event identifier under another sealed hash.

L. For every event in its range that the holding database holds as authored, the chain verification operation SHALL report parents not stamped when the event's `parents` differ from what the stamping rule yields from the events the holder stored before it.

M. <RETIRED> The chain verification checks parents by assertions K and L of this requirement.

N. <RETIRED> Stamping and the walk read the kind and eligibility recorded on each event (assertions F and K of this requirement), so the boot compares no declarations.

O. <RETIRED> The walk reads the kind and eligibility recorded on each event (assertion K of this requirement) and lists no declaration notes.

P. <RETIRED> A delivery envelope carries per-delivery facts as optional attributes, specified with the delivery channel in `spec/delivery-continuity.md`.

Q. <RETIRED> Appends are stamped by the latest eligible version the appending transaction reads, as assertion H of this requirement states.

R. <RETIRED> Ingest compares no declarations: every holder reads the kind and eligibility recorded on each event (assertion K of this requirement).

S. <RETIRED> Ingest checks an event's parents only as the chain verification does, by assertion K of this requirement.

### Rationale

**Why a causal record on every event, and why record kind and eligibility on it (assertions A to G)?** A version names the version it follows, which records under the event's hash what history its author held when it wrote; later structural conflict detection builds on that record (`spec/roadmap/multi-source-editing.md`). The library stamps it inside the append transaction from the log, so the application cannot make it claim a history it did not hold. Kind and eligibility are recorded on the event as well as declared, so a verifier holding another database's events needs no registry (`EVS-PRD-hash-chain-integrity`), and the record stays the truth whichever build wrote it. Reserved events are ineligible annotations, so the library's own records never enter an aggregate's causal structure.

**Why is the stamping rule the latest eligible version in the appender's log (assertion H)?** While an aggregate has one writer of versions, the version it follows is the latest one its writer holds. Reading the appender's log order, not origin positions, covers the events the appender holds without having authored them: a successor's first edit of its predecessor's entry names the predecessor's last version it restored. Where the holder holds two branches of a fork, the rule names the latest in log order, which a restore fixes by storing the branches in the order its receiver accepted them, and decides nothing between them: the aggregate carries the outstanding-finding mark, and detecting the conflict from the parents is roadmap work (`spec/roadmap/multi-source-editing.md`). An ineligible event, such as a draft, is local working state and never a parent. An annotation with no eligible version held is a root annotation with no parents. Annotations are skipped when parents are chosen, so a score recorded between two versions does not become the later version's parent. The rule reads the log and the library keeps no record of its own for it: on Postgres the backend's index over the eligible versions of each aggregate serves it, and on Sembast the backend scans the aggregate's events.

**Why check parents this way (assertions K and L)?** Some checks need no history. A parent that is an annotation, an ineligible event or an event of another aggregate breaks the stamping rule at any holder. So does an identifier held under another hash. Exactness does need history, and only the database that appended an event knows exactly what it held: its log order and the versions a filter kept from any other holder. So the appender checks its own events against its log order, and every other holder checks validity only.

**Why are declarations compared nowhere?** Every rule that reads kind or eligibility reads the values recorded on the event under its hash: the stamping rule and the walk. A build that declares an event type otherwise, within one entry-type major or across majors, records its own events truthfully under its own declaration, and every holder reads what was recorded, so no boot, ingest or walk compares a build's declarations with another's, and changing a declaration is no entry-type major step. The registry audit records each build's declarations (`EVS-DEV-version-compatibility/K`), so the log shows which declarations were in force when each event was appended.

**Layer.** The causal record on each event is a Layer 1 fact: what the library stamped, covered by the hash. Two things are Layer 2 conventions: the vocabulary of versions, annotations and eligibility, and the rule that a version follows the latest eligible version. The substrate could equally track causality per field, or not at all. The verification checks the Layer 1 record against the Layer 2 rule, and reports every place where the two disagree.

### Changelog

- 2026-09-26 | e2eee9c4 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | B: a refusal names the field. N, O and R: the retired notes name where their subject is stated. Rationale of H: the library keeps no latest-eligible-version record; Postgres serves the rule from its index and Sembast scans the aggregate's events. B is cited by code and tests, and its obligation is unchanged
- 2026-09-25 | 64bc4d16 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | A: code, tests and docs still carry the retired `reconciles` key: event_sourcing/lib/src/causal_record.dart, storage/stored_event.dart and event_store.dart (the stamping comments); test/causal_record_test.dart, test_support/record_fixtures.dart, test_support/ingest_hash_conformance.dart, event_store/event_hash_versions_test.dart, event_store/provenance_stamping_conformance.dart and event_store/causal_stamping_conformance.dart; event_sourcing/CHANGELOG.md. Rationale of H: a restore stores a regressed sender's branches in a fixed order, and the aggregate carries the outstanding-finding mark
- 2026-09-25 | 64bc4d16 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | A: the causal record has no `reconciles` key; F, H and L: parents only, stamped by the one latest-eligible-version rule, with no conflict records. B: no recovery. Retire I, J and M (no conflict records or reconciliations), P and S (no withheld-parent record; per-delivery facts ride optional envelope attributes), Q (no skip serialization). Code and tests reference A (event_sourcing/lib/src/causal_record.dart and test/causal_record_test.dart carry `reconciles`), F and H (event_store.dart, storage_backend.dart, sembast_backend.dart, postgres/postgres_backend.dart, test/event_store/causal_stamping_conformance.dart, which asserts `reconciles` null) and B (unchanged in substance); no code or test references I, J, L, M, P, Q or S
- 2026-09-25 | aeb5731f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: ingest, recovery and restore store no event for a record without a well-formed causal object, keeping it in a security finding, instead of refusing the delivery; append and read still refuse it. No code or test references B
- 2026-09-25 | dbfe78ad | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | S: an event whose withheld-parent record the receiver's log contradicts is stored with a security finding instead of refused. No code or test references S
- 2026-09-25 | d06f1b45 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Retire N, O and R: kind and eligibility are recorded on every event under its hash, and stamping, the conflict records and the walk read the recorded values, so no boot, ingest or walk compares declarations and a changed declaration is no major step. Rationale of H: an eligible event other than a reconciliation is refused on a conflicted aggregate. No code or test references N, O or R
- 2026-09-25 | 345fd18f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale shortened to the reason for each rule
- 2026-09-25 | 345fd18f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Remove the retired I and re-letter: split A into A-B, B into C-E and C into F-G; D-H, E-I, F-J, G-K, H-L, J-M, K-N, L-O, M-P, O-R, P-S. N (was K) reads the latest registry audit the database holds as authored and takes an eligible version for an event type it does not record, so a non-default declaration of any event type is a major step; O and R (were L and O) compare event types the build declares. P (was M): a parent the sender recovered from the registration is not withheld. Q replaces the working copy (was N) with the serialization of a skip and the appends it touches; the working copy moves to the Rationale. J: one open record, one skip. No code or test references any of these letters; an event type the registry does not declare counts as an eligible version as well
- 2026-09-25 | a3d85a1b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend F and N: no possibly-incomplete marks; retire I: exactness is verified at the appender only; amend D: no uncovered recovered events; K: the boot of EventStore.open refuses a build whose declarations differ from the latest registry audit, before it writes; M: a parent carried by an earlier item that is neither tombstoned nor deleted is not withheld
- 2026-09-25 | 64844bd2 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-P: the causal record on every event, with kind, eligibility, parents and the skip events a reconciliation closes; per-event-type declarations of kind and eligibility, reserved event types ineligible annotations; the stamping rule for versions, annotations, ineligible events on a conflicted aggregate and reconciliations, ignoring uncovered recovered events; parent validity and exactness checks in the chain verification, and reconciliation checks; declarations refused at open when they differ within one major and at ingest when they differ from the receiver's, and listed as notes by the walk; the withheld-parent record by queue membership, and the ingest refusal of a contradicting one; the per-aggregate causal working copy, serialized with the skip and checked against the log

*End* *Causal parents within an aggregate* | **Hash**: e2eee9c4
