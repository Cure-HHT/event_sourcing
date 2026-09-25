# EVS-PRD-hash-chain-integrity: Hash-Chain Integrity

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library makes the event log tamper-evident: any after-the-fact modification of a stored event is detectable by an independent observer who has access only to the stored log, the canonical-JSON serializer, and the chain-anchor convention. The audit story does not depend on trust in the storage backend; integrity is established cryptographically.

This PRD pins the integrity contract. Structural append-only and ordering properties are specified separately in EVS-PRD-event-log.

## Assertions

A. Each event SHALL carry a cryptographic hash deterministically derived from the event's canonical-form content.

B. Each event SHALL carry the hash of the event its originating database authored immediately before it in that database's history, or no hash when it is the first event that database authored, so that the events each database authors form one chain anchored at its first.

C. The library SHALL provide an operation by which any holder of the stored log can recompute the chain from end to end and verify integrity, without privileged access.

D. Hash and chain values SHALL be reproducible: any two observers running the same canonical-form serializer over the same stored events SHALL compute identical hashes.

E. Each database SHALL record, on every event it stores, the hash of the event it stored immediately before it, or no hash for its first, so that its log forms one unforked chain in storage order from its first stored event.

F. The library SHALL provide an operation that verifies, from the stored log alone and without privileged access, every link of the holding database's storage chain and every link it can resolve of the origin chain of every database whose events the log holds, the first event of any range it is asked to verify included, and reports each break.

G. The operation SHALL report as tampering every event that follows a predecessor another event of the same database also follows, unless a skip event of that database which the log holds lists that event as a successor of that predecessor.

H. Every event SHALL carry the causal parents the library stamped for it within its aggregate, covered by its hash.

I. The operation SHALL report every event whose causal parents name an annotation, an ineligible event or an event of another aggregate, or differ from those the library's stamping rule yields where the log holds the history that decides them.

## Rationale

**Why a chain, not just per-event hashes?** A flat list of hashes detects modification of an individual event, but not insertion or deletion: a forged event with a recomputed hash slots in undetectably. A chain ties each event to its predecessor, so an insertion or a modification breaks the chain at that point, and the break propagates to every later event.

**Why anchor the chain?** Without an anchor, two independently started logs could be spliced together. The anchor binds each chain to the database that produced it. An origin chain starts at the first event its database authored, and a storage chain at the first event its database stored. An event sequence therefore cannot be moved between databases without breaking integrity.

**Two chains (assertions B and E).** Every event takes part in two chains, which answer different questions.

- **The origin chain** of a database links the events it authored, each naming the one its database authored before it. It is the chain that travels. A receiver holds the events its channels delivered, and can check each link whose predecessor it holds. The predecessor is always an authored event: a database delivers only what it authored, so an event naming an ingested predecessor would name one no receiver ever holds under the hash its holder stored it under.
- **The storage chain** of a database links everything that database stored, in the order it stored it, however each event arrived. It is the holder's own tamper evidence. Deleting, inserting or reordering a stored event, an ingested one included, breaks it. It covers what the origin chain cannot: the ingested events, which no authored event names.

A storage chain never forks. An origin chain forks when its database is restored from a backup and appends before it learns of the restore: the events it appends after the restore, and the events it authored after the backup that receivers still hold, both follow the last event the backup kept.

**What does the hash cover?** An event's hash is derived from the following fields:

- id, aggregate id, entry type, entry-type version, data-format version and event type;
- sequence number, data, initiator, flow token and client timestamp;
- the hash of the event its database authored before it;
- its causal record;
- its metadata, provenance included, and with it the database identity, the library version and the storage-chain link that every provenance entry records.

The two versions are covered because they decide whether a receiver accepts an event and how it promotes it (EVS-DEV-version-compatibility/J). The causal record is covered because it states which versions of its aggregate the event follows, which a reconciliation and the parent verification rely on. The provenance is covered because each entry records who stamped the event, from which database and with which library build, and the storage-chain link of the copy that entry sealed; the library version is covered so that the build an event is attributed to is sealed with the event (EVS-DEV-event-record/I).

**Which spelling of a field is hashed?** Each field is hashed as the record holds it: the client timestamp as its string, and the initiator, the two version maps and the causal record as maps with every key they carry. Reading a record keeps those spellings, and every backend stores and returns them unchanged. So a stored copy, and the copy a receiver serves back in a recovery or a restore, hashes to the value its holder sealed. The library writes the times it stamps in UTC, and a client timestamp names one instant on every host (EVS-DEV-event-record). A top-level key outside the fields listed above is kept with the record but not hashed.

**What is verified at ingest, and what by the walk.** Ingest checks each incoming event before any write. It recomputes the event's own hash over the record as it arrived, and each receiver entry's arrival hash over the record as the hop before it stored it. It refuses a predecessor hash that names a held event authored by another database, or one at a later origin position. It refuses a second event after a held predecessor unless a skip event of the same database lists it as a successor there. It refuses an event whose recorded kind or eligibility differs from the receiver's declaration for the same entry-type major, and one whose delivery says no parent was withheld while the receiver lacks a parent. It does not check a predecessor hash that names no held event: a destination's filter leaves out events by design, and the delivery chain (EVS-PRD-delivery-channel) is what shows the receiver holds every delivery in sequence. Nor can it check that a held predecessor is the event its originator authored immediately before, because a receiver cannot know which filtered events came in between.

The verification operation (assertion F) walks both kinds of chain from the stored log alone, with no application code. It checks every link of the holder's storage chain, including the first event of any range it is given. For every database whose events the log holds, it checks every origin-chain link it can resolve, reports every successor of a fork that no skip event lists (assertion G), and reports the holder's own authored events whose predecessor the holder no longer holds. It checks every event's causal parents (assertion I). Its verdict lists each finding, the forks it accepted with the skip events that list their successors, the forks of a recovery still in progress, and how many predecessor links name an event the holder does not hold. Those are links the holder cannot follow, not findings. One comparison depends on the holder's build rather than the log alone: an event's recorded kind and eligibility against the build's declarations. The verdict lists its differences as notes, which do not make the log invalid.

**Why accept a fork only for successors a skip event lists (assertion G)?** A restored database that appended before learning of its restore forks its own origin chain, and nothing it does afterwards can remove the fork from the receivers' logs. The library makes the fork explicit instead. The skip event that resumes the channel lists each fork its database holds by its shared predecessor and every successor, and the walk accepts exactly the listed successors. Any other successor, at the same predecessor or another, is the pattern tampering leaves, or the pattern of a database run twice, so the walk reports it. The skip event is itself an event of the same database, chained and hashed like any other, so the explanation of a fork is as tamper-evident as the fork.

**Causal parents (assertions H and I).** The hash chains order a database's events. The causal parents record, within one aggregate, which versions an event follows: while one writer edits an aggregate, the version it last held. The record is stamped by the library from the log inside the append transaction, never supplied by the application, and covered by the hash. Which events may be named follows declarations that are part of the entry type's registered version. The walk reports every parent the declarations forbid, and every set of parents the stamping rule contradicts where the log holds the history that decides it. The recorded parents are Layer 1 facts. The vocabulary of versions, annotations and eligibility, and the rule that a version follows the latest eligible version, are the library's Layer 2 conventions of causal structure.

**What the hash does not establish.** The hash is an unkeyed SHA-256. It detects a change to a hashed field made without recomputing every hash the change affects, and a change made after a later hash was sealed over the record, whether by a later hop or a later event of the same chain. A writer that changes a field and recomputes every hash it forwards is detected only where a receiver already holds a hash sealed over the record before the change. The hash does not detect a forger who recomputes the hashes, nor a receiver that fabricates a history under a sender's identity (EVS-PRD-delivery-channel, Trust). The aggregate type is not part of the hash input, so a change to it alone is not detected. A receiver cannot tell from the origin chain alone which of a sender's events its channel left out. The delivery chain answers that.

**Why hash the canonical form, not the wire form?** Wire forms vary across platforms, library versions and locales: JSON property order, Unicode normalization, numeric representation. A hash over the wire form would let a benign re-serialization look like tampering. Hashing the canonical form (per EVS-PRD-canonical-json) gives observers on different platforms a single, reproducible value to compare.

**Why third-party verifiability?** Regulatory audit cannot rest on trusting the system being audited. Making integrity verifiable from the log alone, with no application code and no privileged credentials, separates the system that produced the log from the system that verifies it. That separation is what lets a regulator independently confirm the audit trail. The causal record carries its own kind and eligibility for the same reason: a verifier holding another database's events checks them without that database's registry.

## Changelog

- 2026-09-25 | 6b5b70fa | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend B: an event names the event its database authored before it. Add E-I: the storage chain of every database; the verification operation over the storage chain and every origin chain, the first event of a range included; fork successors reported unless a skip event lists them; causal parents carried under the hash and verified. Rationale: two chains, what the hash covers (the causal record, and the database identity and library version of every provenance entry, included), what is verified at ingest and by the walk, which comparison depends on the holder's build, why forks need listed successors, causal parents, what an unkeyed digest detects
- 2026-09-24 | efeb5afb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: the version maps are hashed as spelled; a top-level key outside the hashed fields is kept but not hashed
- 2026-09-24 | efeb5afb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale states which spelling is hashed and what the hash does not establish
- 2026-09-23 | efeb5afb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale names the hashed fields, the entry-type and data-format versions included
- 2026-08-10 | efeb5afb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | b49cdace | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Hash-Chain Integrity* | **Hash**: 6b5b70fa
