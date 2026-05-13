// Verifies: EVS-DEV-find-all-events-extended-filters/A — findAllEvents accepts
//   entryType, clientTimestampStart, clientTimestampEnd optional parameters.
// Verifies: EVS-DEV-find-all-events-extended-filters/B — findAllEventsInTxn
//   accepts the same three parameters with the same semantics.
// Verifies: EVS-DEV-find-all-events-extended-filters/C — all filters AND-
//   compose; existing afterSequence + limit parameters are unaffected.
// Verifies: EVS-PRD-event-log/D — reads in sequence_number order from any
//   position after applying the filters.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

// Construct a StoredEvent with a single-entry provenance chain. Per-test
// callers override `entryType` and `clientTimestamp` to vary along the
// dimensions under test.
StoredEvent _event({
  required int seq,
  required String entryType,
  required DateTime clientTimestamp,
  String aggregateId = 'agg-1',
}) => StoredEvent(
  key: 0,
  eventId: 'e$seq',
  aggregateId: aggregateId,
  aggregateType: 'note',
  entryType: entryType,
  entryTypeVersion: 1,
  libFormatVersion: 1,
  eventType: 'finalized',
  sequenceNumber: seq,
  data: const <String, Object?>{},
  metadata: <String, Object?>{
    'change_reason': 'initial',
    'provenance': <Map<String, Object?>>[
      <String, Object?>{
        'hop': 'mobile-device',
        'received_at': '2026-04-26T00:00:00.000Z',
        'identifier': 'install-A',
        'software_version': 'app@1.0.0',
      },
    ],
  },
  initiator: const UserInitiator('u1'),
  clientTimestamp: clientTimestamp,
  eventHash: 'h$seq',
);

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'find-all-events-filters-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

Future<void> _append(
  SembastBackend backend,
  StoredEvent Function(int seq) build,
) async {
  await backend.transaction((txn) async {
    final s = await backend.nextSequenceNumber(txn);
    await backend.appendEvent(txn, build(s));
  });
}

void main() {
  group('findAllEvents extended filters', () {
    test('entryType filter returns only matching events', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'lights',
            clientTimestamp: DateTime.utc(2026, 1, 2),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 3),
          ),
        );

        final notes = await backend.findAllEvents(entryType: 'note');
        expect(notes.map((e) => e.sequenceNumber).toList(), <int>[1, 3]);

        final lights = await backend.findAllEvents(entryType: 'lights');
        expect(lights.map((e) => e.sequenceNumber).toList(), <int>[2]);
      } finally {
        await backend.close();
      }
    });

    test('clientTimestampStart filter is inclusive-lower-bound', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 5),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 10),
          ),
        );

        final later = await backend.findAllEvents(
          clientTimestampStart: DateTime.utc(2026, 1, 5),
        );
        expect(later.map((e) => e.sequenceNumber).toList(), <int>[2, 3]);
      } finally {
        await backend.close();
      }
    });

    test('clientTimestampEnd filter is inclusive-upper-bound', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 5),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 10),
          ),
        );

        final earlier = await backend.findAllEvents(
          clientTimestampEnd: DateTime.utc(2026, 1, 5),
        );
        expect(earlier.map((e) => e.sequenceNumber).toList(), <int>[1, 2]);
      } finally {
        await backend.close();
      }
    });

    test('AND-composes entryType with timestamp range', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'lights',
            clientTimestamp: DateTime.utc(2026, 1, 3),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 5),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 10),
          ),
        );

        final filtered = await backend.findAllEvents(
          entryType: 'note',
          clientTimestampStart: DateTime.utc(2026, 1, 3),
          clientTimestampEnd: DateTime.utc(2026, 1, 7),
        );
        expect(filtered.map((e) => e.sequenceNumber).toList(), <int>[3]);
      } finally {
        await backend.close();
      }
    });

    test('existing afterSequence + limit filters still work alongside the new '
        'ones', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 2),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'lights',
            clientTimestamp: DateTime.utc(2026, 1, 3),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 4),
          ),
        );

        final all = await backend.findAllEvents();
        expect(all.map((e) => e.sequenceNumber).toList(), <int>[1, 2, 3, 4]);

        // afterSequence + entryType compose.
        final afterAndType = await backend.findAllEvents(
          afterSequence: 1,
          entryType: 'note',
        );
        expect(afterAndType.map((e) => e.sequenceNumber).toList(), <int>[2, 4]);

        // limit + entryType compose.
        final limited = await backend.findAllEvents(
          entryType: 'note',
          limit: 2,
        );
        expect(limited.map((e) => e.sequenceNumber).toList(), <int>[1, 2]);
      } finally {
        await backend.close();
      }
    });

    test('empty result when no events match', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );

        final none = await backend.findAllEvents(entryType: 'no_such_type');
        expect(none, isEmpty);
      } finally {
        await backend.close();
      }
    });

    test('findAllEventsInTxn honors the same filters', () async {
      final backend = await _openBackend();
      try {
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 1),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'lights',
            clientTimestamp: DateTime.utc(2026, 1, 5),
          ),
        );
        await _append(
          backend,
          (s) => _event(
            seq: s,
            entryType: 'note',
            clientTimestamp: DateTime.utc(2026, 1, 10),
          ),
        );

        final inTxn = await backend.transaction(
          (txn) => backend.findAllEventsInTxn(
            txn,
            entryType: 'note',
            clientTimestampStart: DateTime.utc(2026, 1, 5),
          ),
        );
        expect(inTxn.map((e) => e.sequenceNumber).toList(), <int>[3]);
      } finally {
        await backend.close();
      }
    });
  });
}
