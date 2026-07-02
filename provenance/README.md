# Provenance

Chain-of-custody provenance types for cross-system event flow. Pure Dart; no Flutter dependencies.

Each hop that receives an event (e.g. originating device, relay server, controlling server) appends exactly one `ProvenanceEntry` to `event.metadata.provenance`. Prior entries are never modified. The resulting chain is a complete, immutable record of the event's journey across systems, directly supporting the ALCOA+ *Attributable* and *Contemporaneous* principles.

## References

- Spec: `spec/prd-provenance.md` (repo root).

## Exports

This package exports:

- `ProvenanceEntry` — immutable value type with fields `hop`, `receivedAt`, `identifier`, `softwareVersion`, and optional `transformVersion`.
- `appendHop(chain, entry)` — pure function returning a new unmodifiable list with `entry` appended; does not mutate input.

## Installation

Add to a downstream package's `pubspec.yaml`:

```yaml
dependencies:
  provenance:
    path: ../provenance
```

## Usage

```dart
import 'package:provenance/provenance.dart';

final firstHop = ProvenanceEntry(
  hop: 'mobile-device',
  receivedAt: DateTime.now().toUtc(),
  identifier: deviceUuid,
  softwareVersion: 'my_app@1.2.3+45',
);

final chain = appendHop(const [], firstHop);
```
