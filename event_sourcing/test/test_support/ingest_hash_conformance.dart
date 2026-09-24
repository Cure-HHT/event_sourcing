// Backend-agnostic scenarios for the event-hash check at ingest: every
// ingested event's `event_hash` must be the canonical hash of the record it
// arrives as, whatever the length of its provenance, and a refused event
// leaves the log and the sequence counter as they were. Sembast runs them
// from test/ingest/ingest_hash_test.dart and Postgres from
// test/storage/postgres/postgres_ingest_hash_test.dart.
//
// This file exposes [runIngestHashScenarios] and registers no `main()` of
// its own. Traceability lives on the individual tests.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'queue_registry_conformance.dart' show QueueTestDatabase;

const Initiator _init = AutomationInitiator(service: 'ingest-hash-scenarios');
const String _noteType = 'hash_note';
const Source _receiverSource = Source(
  hopId: 'server',
  identifier: 'receiver-install',
  softwareVersion: 'test@1.0.0',
);
const Source _peerSource = Source(
  hopId: 'mobile-device',
  identifier: 'peer-install',
  softwareVersion: 'test@1.0.0',
);
const Source _relaySource = Source(
  hopId: 'relay',
  identifier: 'relay-install',
  softwareVersion: 'test@1.0.0',
);

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: _noteType,
  registeredVersion: EntryTypeVersion(1, 0),
  name: _noteType,
);

var _built = 0;

/// An event as a peer sends it, with one origin provenance entry and an
/// `event_hash` that is the canonical hash of the record.
StoredEvent _originEvent({Map<String, Object?>? data}) {
  _built += 1;
  final now = DateTime.utc(2026, 9, 1, 12, 0, _built % 60);
  final record = <String, Object?>{
    'event_id': 'hash-origin-$_built-${DateTime.now().microsecondsSinceEpoch}',
    'aggregate_id': 'hash-aggregate-$_built',
    'aggregate_type': 'note',
    'entry_type': _noteType,
    'entry_type_version': _noteDef.registeredVersion.toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': 'finalized',
    'sequence_number': 7000 + _built,
    'data': data ?? <String, Object?>{'title': 'note $_built'},
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[
        ProvenanceEntry(
          hop: _peerSource.hopId,
          receivedAt: now,
          identifier: _peerSource.identifier,
          softwareVersion: _peerSource.softwareVersion,
        ).toJson(),
      ],
    },
    'initiator': const UserInitiator('peer-user').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'previous_event_hash': null,
  };
  record['event_hash'] = canonicalEventHash(record);
  return StoredEvent.fromMap(record, 0);
}

/// [event] with [changes] laid over its record and its `event_hash` kept.
StoredEvent _altered(StoredEvent event, Map<String, Object?> changes) =>
    StoredEvent.fromMap(<String, Object?>{
      ...Map<String, Object?>.from(event.toMap()),
      ...changes,
    }, 0);

Uint8List _batchOf(List<StoredEvent> events) => BatchEnvelope(
  batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
  batchId: 'ingest-hash-batch-${events.first.eventId}',
  senderHop: _peerSource.hopId,
  senderIdentifier: _peerSource.identifier,
  senderSoftwareVersion: _peerSource.softwareVersion,
  sentAt: DateTime.utc(2026, 9, 1, 12),
  events: <Map<String, Object?>>[
    for (final e in events) Map<String, Object?>.from(e.toMap()),
  ],
).encode();

/// The two ingest entry points, each taking the whole list of events: the
/// batch in one envelope, or each event in its own `ingestEvent` call.
final Map<String, Future<void> Function(EventStore, List<StoredEvent>)>
_ingestPaths = <String, Future<void> Function(EventStore, List<StoredEvent>)>{
  'ingestBatch': (store, events) async {
    await store.ingestBatch(
      _batchOf(events),
      wireFormat: BatchEnvelope.wireFormat,
    );
  },
  'ingestEvent': (store, events) async {
    for (final e in events) {
      await store.ingestEvent(e);
    }
  },
};

Matcher _eventHashRefused(StoredEvent event) => isA<IngestChainBroken>()
    .having((e) => e.eventId, 'eventId', event.eventId)
    .having((e) => e.kind, 'kind', ChainFailureKind.eventHashMismatch)
    .having(
      (e) => e.hopIndex,
      'hopIndex',
      (event.metadata['provenance']! as List).length - 1,
    )
    .having((e) => e.expectedHash, 'expectedHash', event.eventHash)
    .having(
      (e) => e.actualHash,
      'actualHash',
      canonicalEventHash(event.toMap()),
    );

/// An event store on its own in-memory Sembast database.
Future<EventStore> _openSembastStore(String name, Source source) async {
  final db = await newDatabaseFactoryMemory().openDatabase(name);
  final backend = SembastBackend(database: db);
  return EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry()..register(_noteDef),
    source: source,
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

/// Run every ingest-hash scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
void runIngestHashScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('ingest event-hash scenarios ($label)', () {
    late QueueTestDatabase db;
    late StorageBackend backend;
    late EventStore store;
    var available = false;
    var opened = 0;

    setUp(() async {
      final database = await databaseFactory();
      if (database == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      db = database;
      backend = await db.openBackend();
      store = await EventStore.openForTest(
        storage: backend,
        entryTypes: EntryTypeRegistry()..register(_noteDef),
        source: _receiverSource,
        securityContexts: db.securityFor(backend),
      );
    });

    tearDown(() async {
      if (available) await db.close();
    });

    /// The log and the sequence counter, which a refused ingest leaves as
    /// they were.
    Future<Map<String, Object?>> snapshot() async => <String, Object?>{
      'events': <String>[
        for (final e in await backend.findAllEvents()) e.eventId,
      ],
      'counter': await backend.readSequenceCounter(),
    };

    /// A note a peer appended, relayed through another deployment: the
    /// relay's stored copy, with the peer's origin entry and the relay's
    /// receiver entry.
    Future<StoredEvent> relayedEvent() async {
      opened += 1;
      final peer = await _openSembastStore(
        'ingest-hash-peer-$label-$opened.db',
        _peerSource,
      );
      final relay = await _openSembastStore(
        'ingest-hash-relay-$label-$opened.db',
        _relaySource,
      );
      final origin = (await peer.append(
        entryType: _noteType,
        aggregateId: 'relayed-$opened',
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': 'relayed $opened'},
        initiator: _init,
      ))!;
      await relay.ingestEvent(origin);
      final relayed = await relay.backend.findEventById(origin.eventId);
      expect(
        (relayed!.metadata['provenance']! as List).length,
        2,
        reason: 'the relay stamped its own hop',
      );
      return relayed;
    }

    for (final path in _ingestPaths.entries) {
      group(path.key, () {
        // Verifies: EVS-PRD-ingest/D
        // Verifies: EVS-PRD-hash-chain-integrity/A
        test(
          'admits a first-hop event whose hash is its canonical hash',
          () async {
            if (!available) return;
            final event = _originEvent();
            await path.value(store, <StoredEvent>[event]);
            final stored = (await backend.findEventById(event.eventId))!;
            expect(canonicalEventHash(stored.toMap()), stored.eventHash);
          },
        );

        // Verifies: EVS-PRD-ingest/D
        test('admits a relayed event whose every hop verifies', () async {
          if (!available) return;
          final event = await relayedEvent();
          await path.value(store, <StoredEvent>[event]);
          final stored = (await backend.findEventById(event.eventId))!;
          expect((stored.metadata['provenance']! as List).length, 3);
          expect(canonicalEventHash(stored.toMap()), stored.eventHash);
        });

        // Verifies: EVS-PRD-ingest/D
        // Verifies: EVS-PRD-hash-chain-integrity/A
        test('refuses a first-hop event whose hash is not the hash of its '
            'content, writing nothing', () async {
          if (!available) return;
          final event = _altered(_originEvent(), <String, Object?>{
            'event_hash': 'not-the-hash-of-this-event',
          });
          final before = await snapshot();
          await expectLater(
            path.value(store, <StoredEvent>[event]),
            throwsA(_eventHashRefused(event)),
          );
          expect(await snapshot(), before);
        });

        // Verifies: EVS-PRD-ingest/D
        // Verifies: EVS-PRD-hash-chain-integrity/A
        test('refuses a first-hop event with a field changed after hashing, '
            'writing nothing', () async {
          if (!available) return;
          final event = _altered(_originEvent(), <String, Object?>{
            'data': <String, Object?>{'title': 'tampered'},
          });
          final before = await snapshot();
          await expectLater(
            path.value(store, <StoredEvent>[event]),
            throwsA(_eventHashRefused(event)),
          );
          expect(await snapshot(), before);
        });

        // Verifies: EVS-PRD-ingest/D
        test('refuses a relayed event with a field changed after its last hop, '
            'writing nothing', () async {
          if (!available) return;
          final event = _altered(await relayedEvent(), <String, Object?>{
            'data': <String, Object?>{'title': 'tampered'},
          });
          final before = await snapshot();
          await expectLater(
            path.value(store, <StoredEvent>[event]),
            throwsA(_eventHashRefused(event)),
          );
          expect(await snapshot(), before);
        });
      });
    }

    // Verifies: EVS-PRD-ingest/D
    test('ingestBatch refuses a whole batch when one first-hop event does not '
        'verify, writing nothing', () async {
      if (!available) return;
      final good = _originEvent();
      final bad = _altered(_originEvent(), <String, Object?>{
        'event_hash': 'not-the-hash-of-this-event',
      });
      final alsoGood = _originEvent();
      final before = await snapshot();
      await expectLater(
        store.ingestBatch(
          _batchOf(<StoredEvent>[good, bad, alsoGood]),
          wireFormat: BatchEnvelope.wireFormat,
        ),
        throwsA(_eventHashRefused(bad)),
      );
      expect(await snapshot(), before);
      expect(await backend.findEventById(good.eventId), isNull);
    });

    // Verifies: EVS-PRD-hash-chain-integrity/A
    test('verifyEventChain reports an event whose hash is not the hash of '
        'its content', () async {
      if (!available) return;
      final event = _altered(_originEvent(), <String, Object?>{
        'data': <String, Object?>{'title': 'tampered'},
      });
      final verdict = await store.verifyEventChain(event);
      expect(verdict.isValid, isFalse);
      expect(verdict.failures.single.kind, ChainFailureKind.eventHashMismatch);
      expect((await store.verifyEventChain(_originEvent())).isValid, isTrue);
    });
  });
}
