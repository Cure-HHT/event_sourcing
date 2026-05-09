# EVS-PRD-portability: Portability

**Level**: PRD | **Status**: Draft | **Refines**: EVS-PRD-library-charter

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

*End* *Portability* | **Hash**: 00000000
