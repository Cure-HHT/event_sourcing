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

/// The changes that set [key] of the last provenance entry of [event] to
/// [value].
Map<String, Object?> _withLastHop(StoredEvent event, String key, Object value) {
  final provenance = <Map<String, Object?>>[
    for (final entry in event.metadata['provenance']! as List)
      Map<String, Object?>.from(entry as Map),
  ];
  provenance.last[key] = value;
  return <String, Object?>{
    'metadata': <String, Object?>{...event.metadata, 'provenance': provenance},
  };
}

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

/// A record as a hand-built sender seals it, with an origin provenance
/// entry received at [receivedAt] (by default [clientTimestamp]),
/// [clientTimestamp], [initiator] and the version maps spelled as given,
/// the top-level [extraFields] beside its own, a `null` member in its data
/// and its metadata, and an `event_hash` that is the canonical hash of the
/// record as it stands.
Map<String, Object?> _spelledRecord({
  required String clientTimestamp,
  required Map<String, Object?> initiator,
  String? receivedAt,
  Map<String, Object?>? entryTypeVersion,
  Map<String, Object?>? libFormatVersion,
  Map<String, Object?> extraFields = const <String, Object?>{},
}) {
  _built += 1;
  final record = <String, Object?>{
    ...extraFields,
    'event_id': 'hash-spelled-$_built-${DateTime.now().microsecondsSinceEpoch}',
    'aggregate_id': 'hash-spelled-aggregate-$_built',
    'aggregate_type': 'note',
    'entry_type': _noteType,
    'entry_type_version':
        entryTypeVersion ?? _noteDef.registeredVersion.toJson(),
    'lib_format_version': libFormatVersion ?? LibVersion.dataFormat.toJson(),
    'event_type': 'finalized',
    'sequence_number': 8000 + _built,
    'data': <String, Object?>{'title': 'spelled $_built', 'note': null},
    'metadata': <String, Object?>{
      'change_reason': null,
      'provenance': <Map<String, Object?>>[
        <String, Object?>{
          'hop': _peerSource.hopId,
          'received_at': receivedAt ?? clientTimestamp,
          'identifier': _peerSource.identifier,
          'software_version': _peerSource.softwareVersion,
        },
      ],
    },
    'initiator': initiator,
    'flow_token': null,
    'client_timestamp': clientTimestamp,
    'previous_event_hash': null,
  };
  record['event_hash'] = canonicalEventHash(record);
  return record;
}

/// Sender spellings of a record that a parse-and-reserialize would
/// rewrite: the hashed `client_timestamp`, `initiator` and version maps,
/// and top-level keys this build does not read.
typedef _Spelling = ({
  String timestamp,
  Map<String, Object?> initiator,
  Map<String, Object?>? entryTypeVersion,
  Map<String, Object?>? libFormatVersion,
  Map<String, Object?> extraFields,
});

const Map<String, Object?> _peerUser = <String, Object?>{
  'type': 'user',
  'user_id': 'peer-user',
};

_Spelling _spelling({
  String timestamp = '2026-09-01T12:00:00.000Z',
  Map<String, Object?> initiator = _peerUser,
  Map<String, Object?>? entryTypeVersion,
  Map<String, Object?>? libFormatVersion,
  Map<String, Object?> extraFields = const <String, Object?>{},
}) => (
  timestamp: timestamp,
  initiator: initiator,
  entryTypeVersion: entryTypeVersion,
  libFormatVersion: libFormatVersion,
  extraFields: extraFields,
);

final Map<String, _Spelling> _spellings = <String, _Spelling>{
  'a client timestamp without a fraction': _spelling(
    timestamp: '2026-09-01T12:00:00Z',
  ),
  'a client timestamp with a +00:00 offset': _spelling(
    timestamp: '2026-09-01T12:00:00+00:00',
  ),
  'a client timestamp with a -05:30 offset': _spelling(
    timestamp: '2026-09-01T06:30:00.25-05:30',
  ),
  'an initiator with a key this build does not read': _spelling(
    initiator: <String, Object?>{
      'type': 'user',
      'user_id': 'peer-user',
      'display_name': 'Peer User',
    },
  ),
  'an automation initiator without its optional key': _spelling(
    initiator: <String, Object?>{
      'type': 'automation',
      'service': 'peer-service',
    },
  ),
  'an entry-type version with a key this build does not read': _spelling(
    entryTypeVersion: <String, Object?>{
      ..._noteDef.registeredVersion.toJson(),
      'patch': 3,
    },
  ),
  'a data-format version with a key this build does not read': _spelling(
    libFormatVersion: <String, Object?>{
      ...LibVersion.dataFormat.toJson(),
      'label': 'later',
    },
  ),
  'a top-level key this build does not read': _spelling(
    extraFields: <String, Object?>{
      'later_field': <String, Object?>{'note': 'kept'},
    },
  ),
};

/// Client timestamps a record may not carry, each refused as a malformed
/// record naming `client_timestamp`.
const Map<String, String> _malformedTimestamps = <String, String>{
  'no offset': '2026-09-01T12:00:00',
  'no offset, with a fraction': '2026-09-01T12:00:00.000',
  'a five-digit year': '10000-01-01T00:00:00Z',
  'a negative year': '-0001-01-01T00:00:00Z',
  '30 February': '2026-02-30T00:00:00Z',
  'hour 24': '2026-09-01T24:00:00Z',
};

/// Changes to one hashed field of a [_spelledRecord] each, the `event_hash`
/// kept.
final Map<String, Map<String, Object?> Function(Map<String, Object?>)>
_tampers = <String, Map<String, Object?> Function(Map<String, Object?>)>{
  'event_id': (r) => <String, Object?>{'event_id': '${r['event_id']}-x'},
  'aggregate_id': (r) => <String, Object?>{
    'aggregate_id': '${r['aggregate_id']}-x',
  },
  'entry_type': (r) => const <String, Object?>{'entry_type': 'other_note'},
  'entry_type_version': (r) => <String, Object?>{
    'entry_type_version': const EntryTypeVersion(1, 1).toJson(),
  },
  'lib_format_version': (r) => <String, Object?>{
    'lib_format_version': LibVersion.dataFormat.nextMinor.toJson(),
  },
  'event_type': (r) => const <String, Object?>{'event_type': 'checkpoint'},
  'sequence_number': (r) => <String, Object?>{
    'sequence_number': (r['sequence_number']! as int) + 1,
  },
  'data': (r) => const <String, Object?>{
    'data': <String, Object?>{'title': 'tampered', 'note': null},
  },
  'a null data member removed': (r) => <String, Object?>{
    'data': <String, Object?>{'title': (r['data']! as Map)['title']},
  },
  'initiator': (r) => <String, Object?>{
    'initiator': const UserInitiator('someone-else').toJson(),
  },
  'an initiator key this build does not read': (r) => <String, Object?>{
    'initiator': <String, Object?>{
      ...(r['initiator']! as Map<String, Object?>),
      'display_name': 'Someone Else',
    },
  },
  'flow_token': (r) => const <String, Object?>{'flow_token': 'flow-x'},
  'client_timestamp': (r) => <String, Object?>{
    'client_timestamp': DateTime.parse(
      r['client_timestamp']! as String,
    ).add(const Duration(seconds: 1)).toUtc().toIso8601String(),
  },
  'an entry-type version key this build does not read': (r) =>
      <String, Object?>{
        'entry_type_version': <String, Object?>{
          ...(r['entry_type_version']! as Map<String, Object?>),
          'patch': 1,
        },
      },
  'a data-format version key this build does not read': (r) =>
      <String, Object?>{
        'lib_format_version': <String, Object?>{
          ...(r['lib_format_version']! as Map<String, Object?>),
          'patch': 1,
        },
      },
  'client_timestamp respelled at the same instant': (r) => <String, Object?>{
    'client_timestamp': DateTime.parse(
      r['client_timestamp']! as String,
    ).toUtc().toIso8601String(),
  },
  'previous_event_hash': (r) => const <String, Object?>{
    'previous_event_hash': 'some-earlier-hash',
  },
  'metadata': (r) => <String, Object?>{
    'metadata': <String, Object?>{
      ...(r['metadata']! as Map<String, Object?>),
      'change_reason': 'tampered',
    },
  },
};

Uint8List _batchOfRecords(List<Map<String, Object?>> records) => BatchEnvelope(
  batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
  batchId: 'ingest-hash-batch-${records.first['event_id']}',
  senderHop: _peerSource.hopId,
  senderIdentifier: _peerSource.identifier,
  senderSoftwareVersion: _peerSource.softwareVersion,
  sentAt: DateTime.utc(2026, 9, 1, 12),
  events: records,
).encode();

/// The two ingest entry points over records as a sender spelled them: the
/// batch carries each record as it is, and `ingestEvent` takes each record
/// as `StoredEvent.fromMap` parses it.
final Map<String, Future<void> Function(EventStore, List<Map<String, Object?>>)>
_recordIngestPaths =
    <String, Future<void> Function(EventStore, List<Map<String, Object?>>)>{
      'ingestBatch': (store, records) async {
        await store.ingestBatch(
          _batchOfRecords(records),
          wireFormat: BatchEnvelope.wireFormat,
        );
      },
      'ingestEvent': (store, records) async {
        for (final r in records) {
          await store.ingestEvent(StoredEvent.fromMap(r, 0));
        }
      },
    };

const Source _downstreamSource = Source(
  hopId: 'archive',
  identifier: 'downstream-install',
  softwareVersion: 'test@1.0.0',
);

/// A wall clock that is not UTC: `toIso8601String` of the times it returns
/// carries no zone designator.
DateTime Function() _localClock() {
  var tick = 0;
  return () {
    tick += 1;
    return DateTime(2026, 9, 1, 12, 0, 0, 0, tick);
  };
}

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
      // The store under test runs on a wall clock that is not UTC, so the
      // events it appends and the hops it stamps are timed by one.
      store = await EventStore.openForTest(
        storage: backend,
        entryTypes: EntryTypeRegistry()..register(_noteDef),
        source: _receiverSource,
        securityContexts: db.securityFor(backend),
        clock: _localClock(),
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

    /// A store of its own on an in-memory Sembast database, downstream of
    /// the store under test.
    Future<EventStore> downstream() async {
      opened += 1;
      return _openSembastStore(
        'ingest-hash-downstream-$label-$opened.db',
        _downstreamSource,
      );
    }

    for (final path in _recordIngestPaths.entries) {
      group('${path.key} of records as a sender spelled them', () {
        for (final spelling in _spellings.entries) {
          // Verifies: EVS-PRD-ingest/B+D
          // Verifies: EVS-PRD-hash-chain-integrity/A+D
          // Verifies: EVS-DEV-event-record/A+B
          test('admits an event with ${spelling.key}, stores the record as '
              'it arrived, and a downstream store admits the copy it '
              'forwards', () async {
            if (!available) return;
            final spelled = spelling.value;
            final record = _spelledRecord(
              clientTimestamp: spelled.timestamp,
              initiator: spelled.initiator,
              entryTypeVersion: spelled.entryTypeVersion,
              libFormatVersion: spelled.libFormatVersion,
              extraFields: spelled.extraFields,
            );
            await path.value(store, <Map<String, Object?>>[record]);
            final stored = (await backend.findEventById(
              record['event_id']! as String,
            ))!;
            final storedMap = stored.toMap();
            for (final key in <String>[
              'client_timestamp',
              'initiator',
              'entry_type_version',
              'lib_format_version',
              'data',
              ...spelled.extraFields.keys,
            ]) {
              expect(storedMap[key], record[key], reason: key);
            }
            expect(canonicalEventHash(storedMap), stored.eventHash);
            final provenance = stored.metadata['provenance']! as List;
            expect(
              (provenance.last as Map)['arrival_hash'],
              record['event_hash'],
            );

            final next = await downstream();
            await next.ingestEvent(stored);
            final forwarded = (await next.backend.findEventById(
              stored.eventId,
            ))!;
            expect((forwarded.metadata['provenance']! as List).length, 3);
            expect((await next.verifyEventChain(forwarded)).isValid, isTrue);
            final forwardedMap = forwarded.toMap();
            for (final key in <String>[
              'entry_type_version',
              'lib_format_version',
              ...spelled.extraFields.keys,
            ]) {
              expect(forwardedMap[key], record[key], reason: key);
            }
          });
        }

        for (final malformed in _malformedTimestamps.entries) {
          // Verifies: EVS-DEV-event-record/A
          test('refuses an event whose client timestamp has '
              '${malformed.key} as a malformed record naming the field, '
              'writing nothing', () async {
            if (!available) return;
            final record = _spelledRecord(
              clientTimestamp: malformed.value,
              receivedAt: '2026-09-01T12:00:00Z',
              initiator: _peerUser,
            );
            final before = await snapshot();
            await expectLater(
              path.value(store, <Map<String, Object?>>[record]),
              throwsA(
                anyOf(
                  isA<IngestDecodeFailure>().having(
                    (e) => e.message,
                    'message',
                    contains('"client_timestamp"'),
                  ),
                  isA<FormatException>().having(
                    (e) => e.message,
                    'message',
                    contains('"client_timestamp"'),
                  ),
                ),
              ),
            );
            expect(await snapshot(), before);
          });
        }

        for (final tamper in _tampers.entries) {
          // Verifies: EVS-PRD-ingest/D
          // Verifies: EVS-PRD-hash-chain-integrity/A
          test('refuses an event whose ${tamper.key} changed after it was '
              'sealed, writing nothing', () async {
            if (!available) return;
            final record = _spelledRecord(
              clientTimestamp: '2026-09-01T12:00:00Z',
              initiator: <String, Object?>{
                'type': 'user',
                'user_id': 'peer-user',
                'display_name': 'Peer User',
              },
            );
            final tampered = <String, Object?>{
              ...record,
              ...tamper.value(record),
            };
            expect(
              canonicalEventHash(tampered),
              isNot(record['event_hash']),
              reason: 'the change is to a hashed field',
            );
            final before = await snapshot();
            await expectLater(
              path.value(store, <Map<String, Object?>>[tampered]),
              throwsA(
                isA<IngestChainBroken>()
                    .having(
                      (e) => e.kind,
                      'kind',
                      ChainFailureKind.eventHashMismatch,
                    )
                    .having(
                      (e) => e.expectedHash,
                      'expectedHash',
                      record['event_hash'],
                    )
                    .having(
                      (e) => e.actualHash,
                      'actualHash',
                      canonicalEventHash(tampered),
                    ),
              ),
            );
            expect(await snapshot(), before);
          });
        }

        // Verifies: EVS-PRD-ingest/D
        test('refuses an event whose metadata is null as missing its '
            'provenance, writing nothing', () async {
          if (!available) return;
          final record = <String, Object?>{
            ..._spelledRecord(
              clientTimestamp: '2026-09-01T12:00:00Z',
              initiator: const UserInitiator('peer-user').toJson(),
            ),
            'metadata': null,
          };
          record['event_hash'] = canonicalEventHash(record);
          final before = await snapshot();
          await expectLater(
            path.value(store, <Map<String, Object?>>[record]),
            throwsA(
              isA<IngestChainBroken>().having(
                (e) => e.kind,
                'kind',
                ChainFailureKind.provenanceMissing,
              ),
            ),
          );
          expect(await snapshot(), before);
        });
      });
    }

    // Verifies: EVS-DEV-event-record/A
    test('ingestEvent refuses an event built with a client timestamp outside '
        'the four-digit years as a decode failure naming the field, writing '
        'nothing', () async {
      if (!available) return;
      final event = StoredEvent.synthetic(
        eventId: 'hash-far-future',
        aggregateId: 'hash-far-future',
        aggregateType: 'note',
        entryType: _noteType,
        initiator: const UserInitiator('peer-user'),
        clientTimestamp: DateTime.utc(10000),
        eventHash: 'unsealed',
        metadata: <String, dynamic>{
          'provenance': <Map<String, Object?>>[
            ProvenanceEntry(
              hop: _peerSource.hopId,
              receivedAt: DateTime.utc(2026, 9, 1, 12),
              identifier: _peerSource.identifier,
              softwareVersion: _peerSource.softwareVersion,
            ).toJson(),
          ],
        },
      );
      final before = await snapshot();
      await expectLater(
        store.ingestEvent(event),
        throwsA(
          isA<IngestDecodeFailure>().having(
            (e) => e.message,
            'message',
            contains('"client_timestamp"'),
          ),
        ),
      );
      expect(await snapshot(), before);
    });

    // Verifies: EVS-PRD-ingest/D
    // Verifies: EVS-PRD-hash-chain-integrity/D
    test('an event appended on a wall clock that is not UTC is stored with '
        'UTC timestamps and admitted by a receiver', () async {
      if (!available) return;
      final appended = (await store.append(
        entryType: _noteType,
        aggregateId: 'local-clock',
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': 'local clock'},
        initiator: _init,
      ))!;
      final stored = (await backend.findEventById(appended.eventId))!;
      final storedMap = stored.toMap();
      expect(storedMap['client_timestamp'], endsWith('Z'));
      expect(
        ((stored.metadata['provenance']! as List).single as Map)['received_at'],
        endsWith('Z'),
      );
      expect(
        storedMap['client_timestamp'],
        appended.toMap()['client_timestamp'],
      );
      expect(canonicalEventHash(storedMap), stored.eventHash);

      final receiver = await downstream();
      await receiver.ingestEvent(stored);
      expect(await receiver.backend.findEventById(stored.eventId), isNotNull);
    });

    // Verifies: EVS-PRD-ingest/D
    test('a receiver hop stamped on a wall clock that is not UTC records its '
        'arrival in UTC, and a downstream store admits the copy', () async {
      if (!available) return;
      final event = _originEvent();
      await store.ingestEvent(event);
      final stored = (await backend.findEventById(event.eventId))!;
      expect(
        ((stored.metadata['provenance']! as List).last as Map)['received_at'],
        endsWith('Z'),
      );
      final next = await downstream();
      await next.ingestEvent(stored);
      expect(await next.backend.findEventById(stored.eventId), isNotNull);
    });

    // A change confined to the last hop's own provenance entry, or to the
    // sequence number the last hop assigned, leaves every arrival hash
    // below it intact: the arrival-hash walk alone admits it, and only the
    // event-hash check refuses it.
    final lastHopTampers = <String, Map<String, Object?> Function(StoredEvent)>{
      "the last hop's received_at": (e) =>
          _withLastHop(e, 'received_at', '2020-01-01T00:00:00.000Z'),
      "the last hop's identifier": (e) =>
          _withLastHop(e, 'identifier', 'someone-else'),
      "the last hop's sequence number": (e) => <String, Object?>{
        'sequence_number': e.sequenceNumber + 100,
      },
    };
    for (final tamper in lastHopTampers.entries) {
      // Verifies: EVS-PRD-ingest/D
      // Verifies: EVS-PRD-hash-chain-integrity/A
      test('refuses a relayed event with ${tamper.key} changed, which only '
          'the event-hash check catches, writing nothing', () async {
        if (!available) return;
        final relayed = await relayedEvent();
        final event = _altered(relayed, tamper.value(relayed));
        final verdict = await store.verifyEventChain(event);
        expect(
          verdict.failures.map((f) => f.kind),
          <ChainFailureKind>[ChainFailureKind.eventHashMismatch],
          reason: 'every arrival hash still verifies',
        );
        final before = await snapshot();
        for (final ingest in _ingestPaths.values) {
          await expectLater(
            ingest(store, <StoredEvent>[event]),
            throwsA(_eventHashRefused(event)),
          );
        }
        expect(await snapshot(), before);
      });
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
