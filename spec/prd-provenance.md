# EVS-PRD-provenance: Provenance Chain Tracking

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The `provenance` package provides the value types and pure-functional operations for the chain of hops an event traverses on its way through a multi-tier deployment. Every event carries its own provenance chain alongside the event payload; the chain grows as the event passes through originators, relays, and controllers, so any later observer can answer "where has this event been, and when?" from the event itself.

The package is intentionally narrow — types and pure functions, no I/O, no transport. It is a pure-Dart utility usable independently of the rest of the event-sourcing stack.

## Assertions

A. The package SHALL define an immutable `ProvenanceEntry` value type recording, for each hop in an event's transit: the hop's identifier, the time the hop received the entry, the version of software that handled it, and any transformation applied at the hop.

B. The package SHALL provide pure-functional append: adding an entry to a chain SHALL produce a new immutable chain.

C. The package SHALL serialize and deserialize entries and chains as JSON without loss of information, suitable for cross-tier and cross-system transmission.

D. The package SHALL be pure Dart and run identically on every Dart-supported platform.

## Rationale

**Why an explicit chain on every event?** Multi-tier deployments (originator → relay → controller) need a way for downstream auditors to answer "this event arrived here — where did it come from, and through what software versions?" Embedding the answer in the event itself keeps audit decisions self-contained: a single event in hand carries its full transit history.

**Why immutability and pure-functional append?** Provenance entries are themselves audit data. A chain that can be silently mutated downstream offers an attacker the same surface as a mutable event log. Pure-functional append gives strong static guarantees that no hop can rewrite earlier entries.

**Why a separate package?** Provenance chains are useful beyond event sourcing — any component that processes structured data through multiple stages (signing, transformation, distribution) can use the same chain types. Keeping the package narrow and dependency-free preserves that reusability.

## Changelog

- 2026-07-02 | 4755ef8b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Provenance Chain Tracking* | **Hash**: 4755ef8b
