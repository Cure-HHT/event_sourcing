// Ingest refuses, before any log write, an event whose data-format major
// differs from the receiver's or whose entry-type major is above the
// registered major; the data-format check runs first. A batch mixing a
// compatible event with a refused one is covered, on both backends, by
// test_support/version_compatibility_conformance.dart.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

const _uuid = Uuid();

/// Build an `esd/batch@2` envelope manually with a one-event payload, with
/// caller-controlled `entry_type_version` / `lib_format_version` on the
/// embedded event. Mirrors the shape produced by
/// `event_sourcing/example/lib/synthetic_ingest.dart`'s
/// `SyntheticBatchBuilder.buildSingleEventBatch`, but lives here so the
/// ingest-validation tests stay self-contained.
Uint8List _envelope({
  required String entryType,
  required EntryTypeVersion entryTypeVersion,
  required DataFormatVersion libFormatVersion,
}) {
  final now = DateTime.now().toUtc();
  const senderHop = 'remote-mobile-1';
  const senderIdentifier = 'remote-device-uuid-demo';
  const senderSoftwareVersion = 'remote-my_app@1.0.0';
  final originEntry = <String, Object?>{
    'hop': senderHop,
    'received_at': now.toIso8601String(),
    'identifier': senderIdentifier,
    'software_version': senderSoftwareVersion,
  };
  final eventId = _uuid.v4();
  final eventMap = <String, Object?>{
    'event_id': eventId,
    'aggregate_id': 'remote-aggregate-1',
    'aggregate_type': 'note',
    'entry_type': entryType,
    'entry_type_version': entryTypeVersion.toJson(),
    'lib_format_version': libFormatVersion.toJson(),
    'event_type': 'finalized',
    'sequence_number': 1001,
    'data': <String, Object?>{
      'answers': <String, Object?>{
        'title': 'remote note',
        'body': 'ingested from $senderHop at ${now.toIso8601String()}',
        'date': now.toIso8601String(),
      },
    },
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[originEntry],
    },
    'initiator': const UserInitiator('remote-user-1').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'event_hash': 'synthetic-origin-hash-$eventId',
    'previous_event_hash': null,
  };
  final envelope = BatchEnvelope(
    batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
    batchId: 'test-ingest-${now.microsecondsSinceEpoch}',
    senderHop: senderHop,
    senderIdentifier: senderIdentifier,
    senderSoftwareVersion: senderSoftwareVersion,
    sentAt: now,
    events: <Map<String, Object?>>[eventMap],
  );
  return envelope.encode();
}

Future<EventStoreBundle> _bootstrapWithRegistry({
  required EntryTypeVersion registeredVersion,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-validation-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  return bootstrapEventStore(
    backend: backend,
    source: const Source(
      hopId: 'control-server',
      identifier: 'demo-control',
      softwareVersion: 'test',
    ),
    entryTypes: <EntryTypeDefinition>[
      EntryTypeDefinition(
        id: 'demo_note',
        registeredVersion: registeredVersion,
        name: 'demo_note',
      ),
    ],
    destinations: const <Destination>[],
  );
}

void main() {
  group('data-format major differs', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('throws IngestDataFormatIncompatible and writes nothing', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(1, 0),
      );
      final backend = ds.eventStore.backend;
      final eventsBefore = (await backend.findAllEvents()).length;
      final counterBefore = await backend.readSequenceCounter();
      final bytes = _envelope(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(1, 0),
        libFormatVersion: const DataFormatVersion(3, 0),
      );
      await expectLater(
        ds.eventStore.ingestBatch(bytes, wireFormat: BatchEnvelope.wireFormat),
        throwsA(isA<IngestDataFormatIncompatible>()),
      );
      expect((await backend.findAllEvents()).length, eventsBefore);
      expect(await backend.readSequenceCounter(), counterBefore);
    });
  });

  group('entry-type major ahead', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('throws IngestEntryTypeVersionAhead and writes nothing', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(2, 0),
      );
      final backend = ds.eventStore.backend;
      final eventsBefore = (await backend.findAllEvents()).length;
      final counterBefore = await backend.readSequenceCounter();
      final bytes = _envelope(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(5, 0),
        libFormatVersion: const DataFormatVersion(2, 0),
      );
      await expectLater(
        ds.eventStore.ingestBatch(bytes, wireFormat: BatchEnvelope.wireFormat),
        throwsA(isA<IngestEntryTypeVersionAhead>()),
      );
      expect((await backend.findAllEvents()).length, eventsBefore);
      expect(await backend.readSequenceCounter(), counterBefore);
    });
  });

  group('validation order', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('the data format is checked before the entry-type version', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(2, 0),
      );
      final bytes = _envelope(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(5, 0), // also too high
        libFormatVersion: const DataFormatVersion(3, 0), // also refused
      );
      await expectLater(
        ds.eventStore.ingestBatch(bytes, wireFormat: BatchEnvelope.wireFormat),
        throwsA(isA<IngestDataFormatIncompatible>()),
      );
    });
  });

  group('happy path', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('a lower major with no view to promote for ingests cleanly', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(5, 0),
      );
      final bytes = _envelope(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(3, 0),
        libFormatVersion: const DataFormatVersion(2, 0),
      );
      final result = await ds.eventStore.ingestBatch(
        bytes,
        wireFormat: BatchEnvelope.wireFormat,
      );
      expect(result.events.length, 1);
    });
  });
}
