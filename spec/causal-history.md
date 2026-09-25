# Chains and Causal Parents

## Overview

Every event the library stores takes part in two hash chains and in one causal structure. This file specifies how the library links them, how it verifies them from the stored log alone, what it verifies at ingest, and the security finding it records for each contradiction (`EVS-DEV-security-findings`).

- **Storage chain.** One per database: the events the database stored, in local sequence order, whatever path stored them. The last provenance entry of each stored event records the stored hash of the event before it. A storage chain never forks. A restore of the database truncates it, and later events continue it.
- **Origin chain.** One per originating database: the events the database authored. Each event carries, as its predecessor hash, the sealed hash of the event its database authored before it. A receiver holds the subset its channels delivered. A database restored from a backup appends events whose predecessor is the last event it authored before the backup, while the events it authored after the backup survive at the receivers. Its origin chain therefore forks, its new events reuse the origin positions of the lost ones, and the skip event that records the resume records the range of origin positions within which the fork and the reuse lie.
- **Causal parents.** Within an aggregate, each event names the versions it follows. Kind and eligibility, recorded on each event, decide which events a later event may name.

### Terms

- **Originating database.** The database identity that an event's first provenance entry records.
- **Sealed hash.** The hash the originating database sealed an event under, the same at every holder: the `event_hash` of a copy whose provenance holds exactly one entry, otherwise the `arrival_hash` of the copy's second provenance entry.
- **Origin position.** The local sequence number the originating database stored the event under: the copy's `sequence_number` when its provenance holds exactly one entry, otherwise the `origin_sequence_number` of its second provenance entry.
- **Held as authored.** A copy whose provenance holds exactly one entry, naming the holding database.
- **Fork.** Two or more events of one originating database carrying the same predecessor hash; a null predecessor hash counts as one value.
- **Reused position.** An origin position at which more than one held event of one originating database sits.
- **Abandoned range.** The origin positions above a skip event's branch point (above 0 when it records no branch point) and up to its abandoned head; empty when it records no abandoned head.
- **Covered.** A reused position is covered when it lies in the abandoned range of a skip event of the same originating database that the holder holds. A fork is covered when the lowest origin position among the held events carrying its predecessor hash lies in such a range.
- **Succession lineage.** A database together with the databases it succeeded and the database that succeeded it, as the library's lineage read derives them from succession events.
- **Version, annotation, eligible.** The kind and eligibility recorded on an event. A version replaces its aggregate's state; an annotation is about the aggregate's current version. Only an eligible version is ever named as a parent.

```text
origin chain of database D, restored from a backup taken after e3

  e1 <- e2 <- e3 <- e4 <- e5              abandoned: delivered, then lost at D
                ^
                +---- f4 <- f5 <- skip    continuing: appended after the restore

  e4 and f4 both carry sealed(e3): a fork, lowest position 4
  f4 and f5 sit at the origin positions of e4 and e5: reused positions
  the skip records branch point e3 and abandoned head e5: range (3, 5]
  the fork and positions 4 and 5 lie in the range: covered
  a receiver whose channel filtered e4 holds e3 and e5 but sees no fork;
    f5 still reuses position 5, and without the skip it is reported
```

### Reading order

`EVS-DEV-chain-verification` fixes how events are linked, the verification operation, its verdict, the predecessor checks at ingest, and the index that serves them. `EVS-DEV-causal-parents` fixes the causal record on every event, the declarations it is stamped from, the stamping rule, the verification of parents, and the per-delivery record of withheld parents. What the library does with a recorded fork's conflicting aggregates is specified with branch conflicts (`spec/branch-conflicts.md`).

## EVS-DEV-chain-verification: Storage and origin chain verification

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

### Purpose

This requirement fixes how the library links each stored event into its database's storage chain and each authored event into its database's origin chain. It fixes how an event's originating database, sealed hash and origin position are read from any copy of it, and the verification operation that walks both kinds of chain, including which forks it accepts and the verdict it returns. It fixes the checks every ingest entry point applies and the security findings they record, the findings the walk records, the index those checks read, and the bounds that keep verification off the append path.

### Assertions

A. The library SHALL read an event copy's originating database from the `database_id` of its first provenance entry; its sealed hash from its `event_hash` when its provenance holds exactly one entry, and otherwise from the `arrival_hash` of its second provenance entry; and its origin position from its `sequence_number` when its provenance holds exactly one entry, and otherwise from the `origin_sequence_number` of its second provenance entry. It SHALL resolve every predecessor hash and every causal parent against sealed hashes, never against a holder's re-stamped `event_hash`.

B. The library SHALL set a locally appended event's `previous_event_hash` to the sealed hash of the event with the highest local sequence number among the events the appending database holds as authored, or to null when it holds none. It SHALL read that event inside the append's transaction.

C. In the last provenance entry of every event it stores (the originator entry of an event it appends, and the entry it adds to an event it ingests, recovers or restores), the library SHALL record the event's local sequence number as `ingest_sequence_number`. It SHALL record as `previous_ingest_hash` the stored `event_hash` of the event at the preceding local sequence number, or null for the database's first stored event, read inside the storing transaction.

D. The library SHALL provide a chain verification operation over the holding database's log. The operation SHALL take an optional inclusive range of local sequence numbers and SHALL check every event stored in the range, whatever path stored it: its own hash and every receiver entry's arrival hash, as ingest verifies them; that its last provenance entry's `ingest_sequence_number` equals its local sequence number; and that its last provenance entry's `previous_ingest_hash` equals the stored `event_hash` of the event at the preceding local sequence number. For the first event of the range, that preceding event SHALL be read from outside the range, and for the first event the database stored the expected value is null.

E. The chain verification operation SHALL report each local sequence number in its range, up to the highest the database has stored, at which no event is stored.

F. For every event in its range, the chain verification operation SHALL report a predecessor break in three cases: when the event's `previous_event_hash` is the sealed hash of a held event that its originating database did not author; when it is the sealed hash of an event of that database at an origin position not below the event's own; and when the event is held as authored and its `previous_event_hash` is neither null nor the sealed hash of a held event.

G. The chain verification operation SHALL report, once each, every fork that is not covered and that an event in its range takes part in, as an unrecorded fork, and every reused position that is not covered and at which an event in its range sits, as an unrecorded position reuse.

H. The chain verification operation SHALL report a skip record mismatch for each held skip event in its range whose branch point or abandoned head names an event hash that a held event of the skip's originating database carries at an origin position other than the one named, or that records a branch point whose origin position is not below its abandoned head's.

I. The chain verification operation SHALL return a verdict listing each finding it reports. A finding names its kind (`hash_mismatch` or `storage_link_break` for a check of assertion D, `sequence_missing`, `predecessor_break`, `fork_unrecorded`, `position_reused`, `skip_record_mismatch`, `live_source_fork`, `index_mismatch`, `parent_invalid`, `parents_not_stamped` or `reconciliation_invalid`) and its evidence: for `hash_mismatch`, `predecessor_break`, `fork_unrecorded` and `position_reused` the evidence a security finding of that kind carries at every detection point, and for every other kind the local sequence number and identifier of the event it concerns, the originating database for a finding about an origin chain, and the expected and actual values. The verdict SHALL also list each fork and each reused position the operation accepted, with the skip events whose abandoned ranges cover it, and the number of predecessor hashes naming no held event. The operation SHALL report the verdict valid exactly when it lists no finding.

J. The chain verification operation SHALL refuse, before it reads any event, a range with a negative bound or with a lower bound above its upper bound, and SHALL treat an omitted lower bound, or a lower bound of 0, as the database's first stored event.

K. Every ingest entry point SHALL store as received, recording a security finding of kind `predecessor_break`, an incoming event whose `previous_event_hash` is the sealed hash of a held event that the incoming event's originating database did not author, or of an event of that database at an origin position not below the incoming event's, counting among the held events those stored earlier in the same delivery.

L. Every ingest entry point SHALL store as received, recording a security finding of kind `fork_unrecorded`, an incoming event whose `previous_event_hash` (null included) a held event of the same originating database with another event identifier already carries, and, recording one of kind `position_reused`, an incoming event at an origin position at which a held event of that database with another event identifier sits, unless, counting the incoming event and the events stored earlier in the same delivery among the held events, the fork or the reused position is covered by a skip event the receiver holds or by the incoming event itself.

M. For the holding database's own origin chain, the chain verification operation SHALL report as a fork of live sources each predecessor hash, null included, that more than one event the holding database holds as authored carries.

N. The library SHALL keep a chain index, written only in the transaction that stores each event, holding for every held event its originating database, sealed hash, origin position and `previous_event_hash`, for the holding database its event with the highest local sequence number held as authored, for each delivery channel its `ingest.delivery_accepted` audit with the highest delivery number among those the holding database authored, and each held security finding whose detector names the holding database, whatever path stored it, by its `finding_id`, and SHALL read it, rather than scan the log, on every append, ingest and parent stamping.

O. The chain verification operation SHALL report each entry of the chain index that differs from what the log yields up to the upper bound of its range, comparing only entries for events at or below that bound, and an entry that names the latest event of a kind only when the event it names is at or below that bound.

P. Every ingest entry point SHALL store as received, recording a security finding of kind `hash_mismatch` naming the event, the hash it carries and the hash it recomputes to, an incoming event whose `event_hash`, or the arrival hash of one of its receiver entries, does not recompute.

Q. Every ingest entry point SHALL record a security finding of kind `own_event_ingested` naming the event and its sealed hash for an incoming event whose originator entry names the receiving database's identity, whether or not the receiver holds it, and SHALL store such an event as received when the receiver does not hold it.

R. For each finding its verdict lists, the chain verification operation of an open event store SHALL record a security finding of the verdict finding's kind carrying the verdict finding's evidence, in a write transaction of its own committed after the read that found it, and SHALL record one of kind `index_mismatch` only when, read again inside that write transaction, the index entry still differs from what the log yields.

S. The chain verification operation SHALL hold no transaction that an append waits for, and SHALL fix the upper bound of the range it walks when it starts.

T. On the Postgres backend, the library's append throughput and its ingest throughput SHALL each be at least half the throughput the last release of the preceding data-format major reaches on the same workload and host.

### Rationale

**Why read the originating database, the sealed hash and the origin position this way (assertion A)?** Each receiver re-stamps the copy it stores: it appends its entry, gives the event its own local sequence number, and seals the result under a new `event_hash`. So a copy's `event_hash` and `sequence_number` belong to its holder, not to the event. The values the originator sealed survive in the copy: the first receiver entry records the hash the event arrived under and the position the originator stored it at, and ingest's arrival-hash walk verifies that entry against the record. Reading the originating database, the sealed hash and the origin position from these fields gives the same values at every holder: the originator, each receiver, a sender that recovered the event and a successor that restored it. Links and parents therefore resolve alike everywhere. The originator entry names the database identity (`EVS-DEV-event-record`); the application's `Source` identifier is attribution and plays no part in chaining.

**Why does the predecessor name the previous authored event, not the log's last event (assertion B)?** A database delivers only the events it authored (`EVS-PRD-destinations/C`). If an authored event named the last event in its database's log, its predecessor would often be an event the database ingested, and no receiver ever holds that event under the hash the database stored it under. At receivers the origin chain would then be a set of unfollowable links. Naming the previous authored event makes each database's origin chain consist only of events that can travel. A receiver can follow every link whose predecessor its channels delivered, and a destination's filter leaves the only gaps, which the delivery chain accounts for. Reserved events a database appends are authored events and take part in its origin chain. A recovered event is held through a recovery, not as authored, so a database's next event does not name an event of the history it abandoned.

**Why a separate storage chain (assertions C to E)?** Once the origin link names only authored events, it no longer ties an ingested event into the holder's log. A deletion of an ingested event, or of a locally appended event after which nothing was appended, would break no origin link. The storage chain restores that coverage. Every stored event, whatever path stored it, records the event stored before it in the last entry of its provenance, which that stored copy's hash covers, so deleting, inserting or reordering any stored event breaks the chain at that point. The fields are the ones every receiver entry already carries; a locally appended event carries them in its originator entry. The storage chain is the holder's own and never forks: a restore of the holder's database brings it back to a prefix of itself.

**Why check the first event of a range (assertion D)?** A verification that takes its range's first event as an anchor without checking it cannot detect a tampered link at the start of any range it is given, and a caller that walks a log range by range misses the break at every seam. The operation reads the event before the range, the one read outside it, so a range verification checks exactly the links a whole-log verification checks within that range.

**Why these predecessor checks (assertion F)?** At a receiver, a predecessor hash naming no held event is expected: the destination's filter left the event out, or it came before the channel's start date. The delivery chain, not the origin chain, shows the receiver holds every delivery. So the walk counts such links rather than reporting them. What the holder can refute, it reports. A predecessor that is held but was authored by another database, or that sits at a later origin position, cannot be the event authored before this one. The holder's own authored events are all held unless the holder lost one, so a dangling predecessor of an event held as authored is a break.

**Why accept a fork or a reused position only inside a skip's abandoned range (assertions G and L)?** A restored database appends after the last event its backup kept, so its new events fork its origin chain there and sit at origin positions its lost events held. The skip event that records the resume names its branch point and abandoned head (`EVS-DEV-resume-event/B`), and a legitimate restore's forks and reuses lie between them: each reused position is above the branch point and at or below the abandoned head, and a fork's successors lie above its predecessor, which is at or above the branch point, one of them being abandoned and so at or below the abandoned head. The walk and ingest check both facts, because a filter can hide either: a receiver whose channel left out the lost event after the fork point sees no fork, yet still holds a lost event at a position the restored database uses again. A fork or reuse that no range covers is the pattern tampering leaves, or a database run twice: the walk reports it, and ingest stores the event as received and records a finding, so the fork is in the log beside the finding and delivery continues. The range records that positions were reused, not which events reused them, so a further event inside a covered range is accepted; a second live copy shows where the copies' deliveries meet (`EVS-PRD-delivery-channel`). A recovery stores its skip before any recovered event (`EVS-DEV-delivery-resume/R`), so no holder, the recovering database included, holds a recovered event without its skip. A skip appended with nothing else appended after the restore is itself a successor at the fork, which lies inside its own range; a receiver reached by another channel can meet a reused position before the skip, and records a finding for it.

**Why check the skip's records (assertion H)?** A skip whose branch point or abandoned head names a held event at another position, or whose branch point is not below its abandoned head, misdescribes the range it claims to record.

**Why a fork of live sources at the author (assertion M)?** At the database that authored a chain, a restore leaves exactly one branch held as authored, since the abandoned branch comes back only through a recovery. A second authored successor there is never a restore, whatever the skips say.

**Why a storage chain can be checked without exception.** No library operation removes or rewrites a stored event. Retention and redaction act on the security context stored beside an event, not on the event itself, and a destination's queue items are not events. A gap or a changed link in the storage chain therefore always means that something outside the library's operations wrote to the log.

**Why this verdict (assertion I)?** An auditor needs to tell tampering from what a filter leaves out. Findings are the facts the log contradicts. Accepted forks and reused positions, with the skips whose ranges cover them, show where the log recorded a restore. The count of unresolved predecessors shows how much of each origin chain the holder cannot follow.

**What ingest checks (assertions K, L, P and Q).** Ingest checks what a delivery can contradict: an event whose hash does not recompute, a predecessor that is held but belongs to another database or sits at a later position, a second event after a held predecessor or at an occupied origin position outside every skip's range, and an event of the receiver's own identity. Each is stored as received with a security finding (`EVS-DEV-security-findings`), and delivery continues: the receiver's log holds the suspect event and the statement of what is wrong with it. An unheld predecessor is accepted without a finding: the delivery chain covers it. Ingest does not check that the named predecessor is the event its originator authored immediately before, because a receiver whose channel filters events cannot know which events came in between. A sender's recovery of its own events is not an ingest entry point, and it stores the abandoned branch after the skip recording it, so assertion L does not apply to it. The recovery instead checks each event against the deliveries its channel carried (`EVS-DEV-delivery-resume/O`).

**Why an index (assertions N and O)?** Every append reads the database's latest authored event, and every ingested event is looked up by sealed hash and by predecessor. A sealed hash lives inside the provenance of a copy, and the latest authored event can sit behind any number of ingested ones, so without an index each append and each ingest would scan. Every finding is appended only after a lookup of its identity, so the index serves that lookup too, keyed on the finding's detector rather than on how the holder came to hold it: a finding the holder detected, lost to a restore and got back through a recovery is still its own. The index is written in the storing transaction, so it agrees with the log at every commit and a restore brings the two back together. It is part of the library's persisted state under the storage precondition (`EVS-PRD-destinations/L`), and the walk checks it against the log. The stamping rule of causal parents (`EVS-DEV-causal-parents`) reads it too, and a receiver reads each channel's record from the audit it locates (`EVS-DEV-delivery-receiver`).

**Why record the walk's findings, and hold nothing appends wait for (assertions R and S)?** A verdict returned to a caller is lost with the caller; recorded as findings, the anomalies the walk meets are facts of the holder's log, each once, whoever runs the walk and however often. A fork, a reused position, a predecessor break and a hash that does not recompute carry the same evidence whether ingest, a recovery or the walk meets them, and the walk reports each fork and reuse once rather than once per event taking part, so one anomaly is one finding at its detector. The walk only reads, and appends never wait for it: on Postgres it reads in one read-only snapshot, and readers never block writers; on Sembast it reads through the database outside any transaction, and only transactions take the lock appends wait for. Its upper bound is fixed when it starts, so events appended meanwhile are left to the next walk. The index entries that name a latest event (the highest authored event, the latest accepted delivery of each channel) move with every append, and on Sembast the walk reads the index and the log outside any transaction, so its two reads can straddle a commit; it therefore compares only what its bound fixes, and an index mismatch is read again, entry and log together, inside the transaction that would record it, so a race never becomes a finding. Its findings are appended in short transactions of their own; a crash repeats nothing, because a finding is recorded once. A verifier that holds only the log computes the same verdict and appends nothing. A full walk of a large log is slow, so the library does not schedule it: a caller runs it, over a range from its last verified position or over the whole log, where a deployment chooses in a maintenance window. Walking in resumable chunks is recorded in `spec/roadmap/security-findings.md`.

**Why a throughput bound (assertion T)?** The chains, the index and the causal stamping add reads and writes to every append and every ingested event. Measured against the last release of the preceding major on the same workload and host, a fixed baseline the test pins, the bound keeps that cost proportionate and makes a regression fail the suite.

**Succession.** A successor is a new database with its own identity, so its authored events form their own origin chain, rooted at its first event, and its predecessor's events form the predecessor's chain as the successor restored them. A succession therefore creates no fork, and the walk verifies the two chains separately. Continuity of authorship across a succession is a Layer 2 interpretation, which the causal parents and the default views apply through the succession lineage. The Layer 1 facts are the two chains and the succession event.

**Layer.** Every check here is Layer 1: it compares recorded hashes and positions with each other and records what they contradict. The hashes are unkeyed (`EVS-PRD-hash-chain-integrity`, Rationale), so the checks detect a change made without recomputing the hashes it affects, not one whose author recomputed them.

### Changelog

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

*End* *Storage and origin chain verification* | **Hash**: ef03515e

## EVS-DEV-causal-parents: Causal parents within an aggregate

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-hash-chain-integrity

### Purpose

This requirement fixes the causal record each event carries, the declarations of kind and eligibility it is stamped from, the rule the library stamps parents by, and how the chain verification operation checks parents. It fixes the per-event record of a parent that a channel withholds, which the sender computes at fill time and which the library records without interpreting, and the security finding ingest records for a withheld-parent record its log contradicts.

### Assertions

A. Every event record SHALL carry a `causal` object with exactly these keys: `kind` (`version` or `annotation`); `eligible` (a boolean); `parents` (a list, in ascending order of event identifier, of objects with exactly `event_id` and `event_hash`, each naming an event of the same aggregate by its identifier and sealed hash); and `reconciles` (null, or a non-empty list, in ascending order of event identifier, of objects with exactly `event_id` and `event_hash` naming skip events).

B. The library SHALL treat a record that carries no `causal` object of that shape as malformed, naming the field: every ingest entry point, recovery and restore SHALL store no event for it, and every append and read SHALL refuse it.

C. The library SHALL let an entry-type definition declare, for each event type, its kind and its eligibility, and SHALL stamp an event type the definition does not declare as an eligible version.

D. The library SHALL refuse an entry-type definition that declares one event type twice.

E. The library SHALL declare every event type of every reserved entry type an ineligible annotation.

F. On every append, the library SHALL stamp `kind` and `eligible` from the appended entry type's declaration for the appended event type, and `parents` and `reconciles` by the stamping rule, inside the append transaction.

G. The public append operations SHALL take no argument that sets any part of `causal`.

H. For an appended event that is not a reconciliation and whose aggregate has no open conflict record at the appending database, the library SHALL stamp `reconciles` null and `parents` as the latest eligible version of the aggregate in the appending database's log: the event with the highest local sequence number among the events of that aggregate the database holds whose `causal` records an eligible version, or no parent when there is none.

I. For an ineligible event appended on an aggregate that has an open conflict record at the appending database, the library SHALL stamp `parents` as the heads that the aggregate's open conflict record names, and `reconciles` null.

J. For a reconciliation, the library SHALL stamp `kind` version, `eligible` true, `parents` as the heads that the aggregate's open conflict record at the appending database names, and `reconciles` as the skip event that records that open record.

K. For every event in its range and every parent it names that the holder holds, the chain verification operation SHALL report an invalid parent in each of these cases: the parent belongs to another aggregate; its `causal` records an annotation; its `causal` records it ineligible; or the holder holds the named event identifier under another sealed hash.

L. For every event in its range that the holding database holds as authored, the chain verification operation SHALL report parents not stamped when the event's `parents` or `reconciles` differ from what the stamping rule yields from the events the holder stored before it.

M. For every reconciliation in its range, the chain verification operation SHALL report an invalid reconciliation when a skip event that its `reconciles` names is held and records, for the reconciliation's aggregate, a head that its `parents` do not include, or when its originating database is outside the succession lineage of that skip's originating database.

N. <RETIRED> Stamping and the walk read the kind and eligibility recorded on each event, so the boot compares no declarations.

O. <RETIRED> The walk reads the kind and eligibility recorded on each event and lists no declaration notes.

P. When the fill enqueues an event on a destination that serializes natively, it SHALL record on the queue item, one boolean per event in the order the item carries its events, whether any parent the event names is withheld from the channel: a parent is not withheld when an item of the same registration, enqueued before this event's item and neither tombstoned nor deleted, carries it, or when the sender holds it through a recovery from that registration.

Q. The transaction that appends a skip event, and every append on an aggregate that an abandoned-branch event of that skip touches, SHALL serialize.

R. <RETIRED> Ingest compares no declarations: every holder reads the kind and eligibility recorded on each event.

S. Every ingest entry point SHALL store as received, recording a security finding of kind `parent_withheld_contradicted` naming the event and the parent, an incoming event that its delivery records as withholding no parent and that names an eligible parent the receiver does not hold, counting events stored earlier in the same delivery.

### Rationale

**Why a causal record on every event, and why record kind and eligibility on it (assertions A to G)?** A version names the version it follows, which records under the event's hash what history its author held when it wrote; a reconciliation needs that record, and so does later structural conflict detection (`spec/roadmap/multi-source-editing.md`). The library stamps it inside the append transaction from the log, so the application cannot make it claim a history it did not hold. Kind and eligibility are recorded on the event as well as declared, so a verifier holding another database's events needs no registry (`EVS-PRD-hash-chain-integrity`), and the record stays the truth whichever build wrote it. Reserved events are ineligible annotations, so the library's own records never enter an aggregate's causal structure.

**Why is the stamping rule the latest eligible version in the appender's log (assertions H to J)?** While an aggregate has one writer of versions, the version it follows is the latest one its writer holds. Reading the appender's log order, not origin positions, covers the events the appender holds without having authored them. A successor's first edit of its predecessor's entry names the predecessor's last version it restored. After a recovery, a sender's next edit of an aggregate that only the abandoned branch touched names the abandoned branch's latest version, which is the state the user last saw on that entry. On a conflicted aggregate, the latest version is ambiguous: the two branches each have one. So the library refuses an eligible event there, other than a reconciliation (`EVS-DEV-branch-conflicts`), and a reconciliation names both heads. An aggregate has at most one open conflict record at a time, so a reconciliation names one skip. An ineligible event, such as a draft, is local working state and never a parent. On a conflicted aggregate it names both heads, so the draft records which histories it was written against, without resolving anything. An annotation with no eligible version held is a root annotation with no parents. Annotations are skipped when parents are chosen, so a score recorded between two versions does not become the later version's parent.

**Why check parents this way (assertions K to M)?** Some checks need no history. A parent that is an annotation, an ineligible event or an event of another aggregate breaks the stamping rule at any holder. So does an identifier held under another hash. Exactness does need history, and only the database that appended an event knows exactly what it held: its log order and the versions a filter kept from any other holder. So the appender checks its own events against its log order, and every other holder checks validity only. A reconciliation must name every head that the skip it closes records.

**Why are declarations compared nowhere?** Every rule that reads kind or eligibility reads the values recorded on the event under its hash: the stamping rule, the conflict records and the walk. A build that declares an event type otherwise, within one entry-type major or across majors, records its own events truthfully under its own declaration, and every holder reads what was recorded, so no boot, ingest or walk compares a build's declarations with another's, and changing a declaration is no entry-type major step. The registry audit records each build's declarations (`EVS-DEV-version-compatibility/K`), so the log shows which declarations were in force when each event was appended.

**Why record a withheld parent per delivered event, by what the channel carried (assertions P and S)?** A receiver that does not hold a parent cannot tell whether the filter left it out or something was lost; the sender can, from its own queue. The record describes what the channel carried, not what the filter would decide now, so a changed predicate cannot hide a gap. An event the database ingested, or restored from a predecessor, is enqueued on no channel, so such a parent is always withheld. A parent recorded as not withheld was carried earlier on the same channel, so a receiver that does not hold it has found a contradiction, and ingest records it as a finding. The library draws no conclusion from the flag: whether a withheld parent leaves a holder's state incomplete depends on what the application's views need (`spec/roadmap/multi-source-editing.md`).

**Why serialize with the skip (assertion Q)?** A skip computes its conflict set from the events its transaction reads; an append committed beside it on an aggregate it touches would miss the conflict. Serialized, the append either precedes the skip and is counted among its continuing events, or follows it and meets the recorded conflict. An implementation may keep, per aggregate, a working copy of its latest eligible version and open conflict record, written only in the transactions that change them; it is persisted state under the storage precondition (`EVS-PRD-destinations/L`), and a copy that drifted from the log shows as parents not stamped.

**Layer.** The causal record on each event is a Layer 1 fact: what the library stamped, covered by the hash. So is the withheld-parent record in each delivery. Two things are Layer 2 conventions: the vocabulary of versions, annotations and eligibility, and the rule that a version follows the latest eligible version. The substrate could equally track causality per field, or not at all. The verification checks the Layer 1 record against the Layer 2 rule, and reports every place where the two disagree.

### Changelog

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

*End* *Causal parents within an aggregate* | **Hash**: aeb5731f
