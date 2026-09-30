# EVS-PRD-portability: Portability

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library is intended to run on every Dart-supported runtime — server, mobile (iOS, Android), desktop (Linux, macOS, Windows), and web — from a single codebase. A mobile app running on a participant's phone, a relay running on a Dart-VM server, a web client running in a browser tab, and a server-side service running in a container all use the same library. Behavior is identical across runtimes; where platforms genuinely diverge (file system, networking, notifications), the library abstracts the divergence behind Dart-side interfaces that the consuming application implements per platform.

## Assertions

A. The library's core SHALL be pure Dart, depending only on the Dart SDK and pure-Dart packages.

B. The library SHALL load and execute on every Dart-supported runtime: Dart VM (server, CLI tooling), Flutter on mobile (iOS, Android), Flutter on desktop (Linux, macOS, Windows), and Flutter on web.

C. The library SHALL produce identical observable behavior across supported runtimes for any given input.

D. The library SHALL abstract platform-divergent capabilities (persistent storage, networking, notification delivery) behind Dart-side interfaces, which the consuming application implements per platform for networking and notification delivery, and for persistent storage other than the storage the library opens itself for the backends it ships.

## Rationale

**Why pure Dart core?** A core that pulled in Flutter or platform-specific packages would force every consumer onto that toolchain. A server-side relay does not run Flutter; a CLI auditing tool does not run any platform stack at all. By keeping the core pure-Dart, the library is reusable across all the runtimes a deployment topology actually spans. Two capabilities under the library's `lib/` are platform-specific: the wrapper of the browser's lock manager, which the incompatible-generation guard and the drain lock use on the web, and the selection of the Sembast database factory, which is the native file factory where `dart:io` exists and the browser factory where the browser's interop exists. Each is reached only through a conditional import, with a counterpart on every other runtime, so the core still loads on every runtime.

**Why all Dart-supported runtimes?** Different parts of a deployment topology run on different platforms. A mobile app on a participant's phone (Flutter on iOS/Android), a web client accessed by users in a browser (Flutter on web), a relay or server-side service running in a container (Dart VM) — all use the same library. Excluding any runtime forces a parallel codebase for that tier; the audit divergence costs of parallel codebases are exactly what the library exists to prevent.

**Why identical observable behavior across platforms?** Cross-tier audit and verification rest on hashes computed from canonical-form serialization. If the same input produces different hashes on different platforms, the audit chain breaks at every cross-tier boundary, and integrity verification (per EVS-PRD-hash-chain-integrity) becomes platform-dependent rather than universal. Identical behavior across runtimes is the property that makes hash-based verification meaningful end-to-end. One documented per-runtime difference is the scope of the incompatible-generation guard (EVS-DEV-version-compatibility/H): it covers every session on a Postgres database and every tab of an origin on a browser database, and relies on a Sembast database file outside the browser being opened by one isolate of one process. The difference decides which builds may run beside each other on a database, not what any event, hash or view holds. A second documented difference is the scope of the drain lock (EVS-DEV-destination-drain-lock/A): it covers every session on a Postgres database, every tab of an origin on a browser database, where it follows the visible tab, and one isolate per open database handle on Sembast outside the browser. It decides which delivery cycle drains a database, not what any event, hash or view holds.

**Why abstract platform-divergent capabilities?** Some capabilities genuinely differ across platforms — file-system storage on the Dart VM vs. IndexedDB on web vs. application-private directories on mobile; HTTP via dart:io on the VM vs. dart:html on web; notification delivery via push services on mobile vs. OS notifications on desktop vs. browser notifications on web. The library cannot pick any single platform's API without breaking the others. By defining Dart-side interfaces (storage, transport, notification) and accepting application-supplied implementations, the library stays platform-agnostic while letting consumers adapt to whatever their target environment provides. The library itself owns one platform-divergent capability: the cross-tab lock it takes on the web, through the browser's lock manager, to keep a tab of an incompatible build from opening a shared database. A consumer-supplied lock would be a new trusted input deciding which builds may write the database (EVS-PRD-library-charter/H), so the library does not accept one. The library also owns the choice of the Sembast database factory for the backends it ships. An application-supplied factory would receive the database the library opens and could keep it (EVS-PRD-storage-barrier), so the library selects the native file factory, the browser factory or the in-memory one itself, and the application implements storage only when it supplies a backend of its own.

## Future work

Deferred work for this area (horizontal scaling beyond a single backend instance) is recorded in `spec/roadmap/storage.md`.

## Changelog

- 2026-09-25 | 4fd789d6 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend D: the application implements storage only beside the storage the library opens for its shipped backends; the library selects the Sembast factory per runtime
- 2026-09-23 | 9a3f1e98 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of C: the drain lock covers every tab of an origin on a browser database and follows the visible tab
- 2026-09-23 | 9a3f1e98 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of C names the per-runtime scope of the drain lock, and that a browser Sembast database grants none
- 2026-08-10 | 9a3f1e98 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | edf3c977 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Portability* | **Hash**: 4fd789d6
