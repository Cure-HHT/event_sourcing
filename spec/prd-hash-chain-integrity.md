# EVS-PRD-hash-chain-integrity: Hash-Chain Integrity

**Level**: prd | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library makes the event log tamper-evident: any after-the-fact modification of a stored event is detectable by an independent observer who has access only to the stored log, the canonical-JSON serializer, and the chain-anchor convention. The audit story does not depend on trust in the storage backend; integrity is established cryptographically.

This PRD pins the integrity contract. Structural append-only and ordering properties are specified separately in EVS-PRD-event-log.

## Assertions

A. Each event SHALL carry a cryptographic hash deterministically derived from the event's canonical-form content.

B. Each event's hash SHALL chain to the predecessor in its chain, with each chain anchored to the deployment that originated it.

C. The library SHALL provide an operation by which any holder of the stored log can recompute the chain from end to end and verify integrity, without privileged access.

D. Hash and chain values SHALL be reproducible: any two observers running the same canonical-form serializer over the same stored events SHALL compute identical hashes.

## Rationale

**Why a chain, not just per-event hashes?** A flat list of hashes detects modification of an individual event, but does not detect insertion or deletion — a forged event with a recomputed hash slots in undetectably. A chain ties each event to its predecessor, so any insertion or modification breaks the chain at that point and propagates to every later event.

**Why anchor the chain?** Without an anchor, two independently-started logs could be spliced. The anchor binds the chain to the deployment that produced it, so an event sequence cannot be moved between deployments without breaking integrity.

**Why hash the canonical form, not the wire form?** Wire forms vary across platforms, library versions, and locales — JSON property order, Unicode normalization, numeric representation. A hash over the wire form would let a benign re-serialization look like tampering. Hashing the canonical form (per EVS-PRD-canonical-json) gives observers on different platforms a single, reproducible value to compare.

**Why third-party verifiability?** Regulatory audit cannot rest on trusting the system being audited. By making integrity verifiable from the log alone — no application code, no privileged credentials — the library separates "the system that produced the log" from "the system that verifies it", which is the property that lets a regulator independently confirm the audit trail.

**Multiple chains per log.** A deployment that holds only locally-originated events has one chain, anchored to that deployment. A deployment that ingests events from upstream (per EVS-PRD-ingest) also holds the chains of its upstreams — each chain remains anchored to its originating deployment. Verification is the union of per-chain verifications: each chain is verified against its own anchor, and ingested events are verified at the boundary against the upstream chain they came from.

*End* *Hash-Chain Integrity* | **Hash**: b49cdace
