# EVS-PRD-portability: Portability

**Level**: prd | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library is intended to run on every Dart-supported runtime — server, mobile (iOS, Android), desktop (Linux, macOS, Windows), and web — from a single codebase. A diary deployment running on a participant's phone, a relay running on a Dart-VM server, a portal running in a browser tab, and an EDC running in a server-side container all use the same library. Behavior is identical across runtimes; where platforms genuinely diverge (file system, networking, notifications), the library abstracts the divergence behind Dart-side interfaces that the consuming application implements per platform.

## Assertions

A. The library's core SHALL be pure Dart, depending only on the Dart SDK and pure-Dart packages.

B. The library SHALL load and execute on every Dart-supported runtime: Dart VM (server, CLI tooling), Flutter on mobile (iOS, Android), Flutter on desktop (Linux, macOS, Windows), and Flutter on web.

C. The library SHALL produce identical observable behavior across supported runtimes for any given input.

D. The library SHALL abstract platform-divergent capabilities (persistent storage, networking, notification delivery) behind Dart-side interfaces that the consuming application implements per platform.

## Rationale

**Why pure Dart core?** A core that pulled in Flutter or platform-specific packages would force every consumer onto that toolchain. A server-side relay does not run Flutter; a CLI auditing tool does not run any platform stack at all. By keeping the core pure-Dart, the library is reusable across all the runtimes a Cure-HHT deployment topology actually spans.

**Why all Dart-supported runtimes?** Different parts of a deployment topology run on different platforms. A diary on a participant's phone (Flutter on iOS/Android), a portal accessed by clinicians in a browser (Flutter on web), a relay or EDC running in a server-side container (Dart VM) — all use the same library. Excluding any runtime forces a parallel codebase for that tier; the audit divergence costs of parallel codebases are exactly what the library exists to prevent.

**Why identical observable behavior across platforms?** Cross-tier audit and verification rest on hashes computed from canonical-form serialization. If the same input produces different hashes on different platforms, the audit chain breaks at every cross-tier boundary, and integrity verification (per EVS-PRD-hash-chain-integrity) becomes platform-dependent rather than universal. Identical behavior across runtimes is the property that makes hash-based verification meaningful end-to-end.

**Why abstract platform-divergent capabilities?** Some capabilities genuinely differ across platforms — file-system storage on the Dart VM vs. IndexedDB on web vs. application-private directories on mobile; HTTP via dart:io on the VM vs. dart:html on web; notification delivery via push services on mobile vs. OS notifications on desktop vs. browser notifications on web. The library cannot pick any single platform's API without breaking the others. By defining Dart-side interfaces (storage, transport, notification) and accepting application-supplied implementations, the library stays platform-agnostic while letting consumers adapt to whatever their target environment provides.

## Future work

- **Horizontal scaling beyond a single backend instance.** The reference `PostgresBackend` (and the abstract `StorageBackend` contract) target a single backing store per substrate instance. The IoT scenario (`docs/scenarios/iot-sensor-network.md`) reaches volumes — millions of events per day per fleet — where a single-Postgres deployment runs out of headroom. Two paths exist today, both load-bearing the same `event_sourcing` library:

  1. **Substrate-per-shard** — deploy one substrate per logical shard (per-farm, per-tenant, per-region), with an app-layer aggregator subscribing across shards via separate `RemoteScope` connections. Works today; the audit story is "per-shard log" rather than "one global log," which is the right answer for tenant-isolated deployments.
  2. **Backend-side partitioning** — a hypothetical `ShardedPostgresBackend` (or similar) that partitions the event table across multiple Postgres instances by `(originatorId, aggregateType)` or by sequence range, presenting the substrate with a single logical `StorageBackend`. This is out of scope for v1: it requires a non-trivial sequence-number coordination strategy across shards, and the hash chain's per-installation linearity is what makes integrity verifiable in the first place.

  The substrate's commitment is that v1 ships one reference `StorageBackend` per supported deployment shape (`SembastBackend` on mobile, `PostgresBackend` on server) and any horizontal-partitioning impl is a downstream extension under the same trust-boundary discipline (CLAUDE.md, "Trust boundaries"). Cited here so future work doesn't quietly assume "obviously you'd shard Postgres" without engaging with the hash-chain-integrity costs.

*End* *Portability* | **Hash**: edf3c977
