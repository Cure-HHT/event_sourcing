# EVS-PRD-provenance: Provenance Chain Tracking

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The `provenance` package provides the value types and pure-functional operations for the chain of hops an event traverses on its way through a multi-tier deployment. Every event carries its own provenance chain alongside the event payload; the chain grows at each deployment that stamps it: the originator that appends it, each receiver that ingests it, and a successor that restores it from a receiver, so any later observer can answer "where has this event been, and when?" from the event itself.

The package is intentionally narrow — types and pure functions, no I/O, no transport. It is a pure-Dart utility usable independently of the rest of the event-sourcing stack.

## Assertions

A. The package SHALL define an immutable `ProvenanceEntry` value type recording, for each hop in an event's transit: the hop's identifier, the time the hop received the entry, the version of software that handled it, any transformation applied at the hop, and, when a library stamped the entry, that library's version.

B. The package SHALL provide pure-functional append: adding an entry to a chain SHALL produce a new immutable chain.

C. The package SHALL serialize and deserialize entries and chains as JSON without loss of information, suitable for cross-tier and cross-system transmission.

D. The package SHALL be pure Dart and run identically on every Dart-supported platform.

E. When the package decodes an entry from JSON and encodes it again, the encoded entry SHALL carry exactly the keys of the decoded JSON, with the value of every key the entry type does not model unchanged.

## Rationale

**Why an explicit chain on every event?** An event moves between deployments: from its originator to each receiver of its channels, and back to a successor from a receiver. Downstream auditors need to answer "this event arrived here -- where did it come from, and through what software versions?" Embedding the answer in the event itself keeps audit decisions self-contained: a single event in hand carries its full transit history. A deployment delivers only the events it authored, so an event reaches a receiver in one hop from its originator; the chain grows past two entries only when a restore brings an event back.

**Why immutability and pure-functional append?** Provenance entries are themselves audit data. A chain that can be silently mutated downstream offers an attacker the same surface as a mutable event log. Pure-functional append gives strong static guarantees that no hop can rewrite earlier entries.

**Why a separate package?** Provenance chains are useful beyond event sourcing — any component that processes structured data through multiple stages (signing, transformation, distribution) can use the same chain types. Keeping the package narrow and dependency-free preserves that reusability.

**Why record a library's version on the entry (assertion A)?** The software version an entry records is the application's. A hop's behaviour also depends on the library build that stamped the entry, and while builds of one library share a store, that build is not otherwise recorded per event. The field is optional in the package, which is usable without any library; a library that stamps entries fills it.

**Why keep the decoded keys (assertion E)?** Entries are audit data carried inside hashed records. A later release may add a field to an entry. A consumer that decoded the entry and encoded only the fields it models would drop that field, and one that encoded an absent optional field as null would add a key. Either way, a record re-encoded like that would no longer hash to the value its sender sealed. Keeping exactly the decoded keys makes a decode and re-encode preserve them. The modelled `received_at` is re-encoded in UTC. A consumer that forwards a record keeps each entry as the record spelled it rather than re-encoding it, which is what the event-sourcing library does.

## Changelog

- 2026-09-26 | d9fe1da3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: an event returns to a successor from a receiver; no restored sender recovers its own. No assertion changes
- 2026-09-25 | d9fe1da3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Purpose and Rationale: only a successor's restore brings an event back from a receiver; no sender recovers its own events
- 2026-09-25 | d9fe1da3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A: an entry records the version of the library that stamped it; add E: a decode and re-encode keeps exactly the decoded keys. Purpose and Rationale: the chain grows at the originator, each receiver, and a recovering sender or restoring successor; deployments deliver only the events they authored
- 2026-08-10 | 3a037c9e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 4755ef8b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Provenance Chain Tracking* | **Hash**: d9fe1da3
