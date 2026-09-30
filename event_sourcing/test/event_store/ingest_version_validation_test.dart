// The receiver refuses, `rejected` and before any log write, a delivery
// carrying an event whose data-format major differs from the receiver's or
// whose entry-type major is above the registered major, naming the refusal
// and the event; the data-format check runs first. A batch mixing a
// compatible event with a refused one is covered, on both backends, by
// test_support/version_compatibility_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

import '../test_support/deliveries.dart';
import '../test_support/record_fixtures.dart';

const _uuid = Uuid();

/// A peer's event record with caller-controlled `entry_type_version` /
/// `lib_format_version`.
Map<String, Object?> _record({
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
    'database_id': kPeerDatabaseId,
    'library_version': kPeerLibraryVersion,
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
    'previous_event_hash': null,
    'causal': kRootVersionCausalJson,
  };
  eventMap['event_hash'] = canonicalEventHash(eventMap);
  return eventMap;
}

Future<EventStoreBundle> _bootstrapWithRegistry({
  required EntryTypeVersion registeredVersion,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-validation-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  return bootstrapEventStore(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
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
    test(
      'is refused rejected naming the data format, and writes nothing',
      () async {
        final ds = await _bootstrapWithRegistry(
          registeredVersion: const EntryTypeVersion(1, 0),
        );
        final reader = ds.eventStore.reader;
        final eventsBefore = (await reader.findAllEvents()).length;
        final counterBefore = await reader.readSequenceCounter();
        final record = _record(
          entryType: 'demo_note',
          entryTypeVersion: const EntryTypeVersion(1, 0),
          libFormatVersion: DataFormatVersion(
            LibVersion.dataFormat.major + 1,
            0,
          ),
        );
        final answer = (await deliverTo(ds.eventStore, [record])).response;
        expect(answer, isA<ReceiverRefusal>());
        final refusal = answer as ReceiverRefusal;
        expect(refusal.refusal, RefusalKind.rejected);
        expect(refusal.reason, IngestDataFormatIncompatible.refusalReason);
        expect(refusal.refusedEventId, record['event_id']);
        expect((await reader.findAllEvents()).length, eventsBefore);
        expect(await reader.readSequenceCounter(), counterBefore);
      },
    );
  });

  group('entry-type major ahead', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test(
      'is refused rejected naming the entry-type version, and writes nothing',
      () async {
        final ds = await _bootstrapWithRegistry(
          registeredVersion: const EntryTypeVersion(2, 0),
        );
        final reader = ds.eventStore.reader;
        final eventsBefore = (await reader.findAllEvents()).length;
        final counterBefore = await reader.readSequenceCounter();
        final record = _record(
          entryType: 'demo_note',
          entryTypeVersion: const EntryTypeVersion(5, 0),
          libFormatVersion: LibVersion.dataFormat,
        );
        final answer = (await deliverTo(ds.eventStore, [record])).response;
        expect(answer, isA<ReceiverRefusal>());
        final refusal = answer as ReceiverRefusal;
        expect(refusal.refusal, RefusalKind.rejected);
        expect(refusal.reason, IngestEntryTypeVersionAhead.refusalReason);
        expect(refusal.refusedEventId, record['event_id']);
        expect((await reader.findAllEvents()).length, eventsBefore);
        expect(await reader.readSequenceCounter(), counterBefore);
      },
    );
  });

  group('validation order', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('the data format is checked before the entry-type version', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(2, 0),
      );
      final record = _record(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(5, 0), // also too high
        // also refused
        libFormatVersion: DataFormatVersion(LibVersion.dataFormat.major + 1, 0),
      );
      final answer = (await deliverTo(ds.eventStore, [record])).response;
      expect(answer, isA<ReceiverRefusal>());
      final refusal = answer as ReceiverRefusal;
      expect(refusal.refusal, RefusalKind.rejected);
      expect(refusal.reason, IngestDataFormatIncompatible.refusalReason);
      expect(refusal.refusedEventId, record['event_id']);
    });
  });

  group('happy path', () {
    // Verifies: EVS-DEV-version-compatibility/D
    test('a lower major with no view to promote for ingests cleanly', () async {
      final ds = await _bootstrapWithRegistry(
        registeredVersion: const EntryTypeVersion(5, 0),
      );
      final record = _record(
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(3, 0),
        libFormatVersion: LibVersion.dataFormat,
      );
      final delivery = await deliverTo(ds.eventStore, [record]);
      expect(delivery.accepted, isTrue);
      expect(await recordOutcomes(ds.eventStore, delivery), <IngestOutcome>[
        IngestOutcome.ingested,
      ]);
    });
  });
}
