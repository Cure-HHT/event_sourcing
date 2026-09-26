// Backend-agnostic scenarios for the event-hash check at ingest: every
// ingested event's `event_hash` is checked against the canonical hash of the
// record it arrives as, whatever the length of its provenance; an event
// whose hash does not recompute is stored as received with a hash_mismatch
// finding, and a record the library does not store as an event is kept in
// an event_malformed finding with nothing else written. Sembast runs them
// from test/ingest/ingest_hash_test.dart and Postgres from
// test/storage/postgres/postgres_ingest_hash_test.dart.
//
// This file exposes [runIngestHashScenarios] and registers no `main()` of
// its own. Traceability lives on the individual tests.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/verification/chain_walk.dart'
    show hashMismatchEvidence;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'record_fixtures.dart';

/// Marks a field the record leaves out.
const Object _absentValue = Object();

/// The refusal of the parse that precedes `ingestEvent` of a malformed
/// record, naming [field].
Matcher _parseRefusalNaming(String field) =>
    isA<FormatException>().having((e) => e.message, 'message', contains(field));

/// [record] as the receiver decodes it from a delivery: JSON-encoded and
/// decoded again.
Map<String, Object?> _asReceived(Map<String, Object?> record) =>
    (jsonDecode(jsonEncode(record)) as Map).cast<String, Object?>();

/// A `hash_mismatch` finding's kind and evidence: the event [eventId], the
/// hash it [carried] and the hash its record [recomputed] to.
Map<String, Object?> _hashMismatch({
  required String eventId,
  required String carried,
  required String recomputed,
}) => <String, Object?>{
  'kind': 'hash_mismatch',
  'evidence': <String, Object?>{
    'event_id': eventId,
    'carried_hash': carried,
    'recomputed_hash': recomputed,
  },
};

/// The peer's originator provenance entry.
Map<String, Object?> _peerEntry() => ProvenanceEntry(
  hop: _peerSource.hopId,
  receivedAt: DateTime.utc(2026, 9, 1, 12),
  identifier: _peerSource.identifier,
  softwareVersion: _peerSource.softwareVersion,
  databaseId: kPeerDatabaseId,
  libraryVersion: kPeerLibraryVersion,
).toJson();

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

/// The causal object of the first version of an aggregate.
const Map<String, Object?> _rootVersion = <String, Object?>{
  'kind': 'version',
  'eligible': true,
  'parents': <Object?>[],
};

/// An event as a peer sends it, with one origin provenance entry and an
/// `event_hash` that is the canonical hash of the record. Its origin
/// position and its predecessor hash are its own, and the predecessor names
/// no event, so events built here form no fork among themselves.
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
          databaseId: kPeerDatabaseId,
          libraryVersion: kPeerLibraryVersion,
        ).toJson(),
      ],
    },
    'initiator': const UserInitiator('peer-user').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'previous_event_hash': 'unheld-predecessor-$_built',
    'causal': _rootVersion,
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
          'database_id': kPeerDatabaseId,
          'library_version': kPeerLibraryVersion,
        },
      ],
    },
    'initiator': initiator,
    'flow_token': null,
    'client_timestamp': clientTimestamp,
    'previous_event_hash': null,
    'causal': <String, Object?>{
      'kind': 'version',
      'eligible': true,
      'parents': <Object?>[
        <String, Object?>{'event_id': 'hash-parent', 'event_hash': 'h-parent'},
      ],
    },
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

/// Timestamps a record may not carry, each refused as a malformed record
/// naming the field that carries it: `client_timestamp`, or a provenance
/// entry's `received_at`.
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
  'causal parents': (r) => <String, Object?>{
    'causal': <String, Object?>{
      ...(r['causal']! as Map<String, Object?>),
      'parents': <Object?>[
        <String, Object?>{'event_id': 'hash-parent', 'event_hash': 'h-other'},
      ],
    },
  },
  'causal kind': (r) => <String, Object?>{
    'causal': <String, Object?>{
      ...(r['causal']! as Map<String, Object?>),
      'kind': 'annotation',
    },
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

    /// The event identifiers of the log, the security findings left out.
    Future<List<String>> logBesideFindings() async => <String>[
      for (final e in await backend.findAllEvents())
        if (e.entryType != kSecurityFindingEntryType) e.eventId,
    ];

    /// The kind and evidence of every security finding the store under
    /// test recorded, in log order.
    Future<List<Map<String, Object?>>> findings() async =>
        <Map<String, Object?>>[
          for (final e in await backend.findAllEvents(
            entryType: kSecurityFindingEntryType,
          ))
            <String, Object?>{
              'kind': e.data['kind'],
              'evidence': e.data['evidence'],
            },
        ];

    /// Delivers the malformed [record] on the path [pathKey] names:
    /// `ingestBatch` stores no event for it and keeps it in full in one
    /// `event_malformed` finding; a record that does not parse never
    /// reaches `ingestEvent`, whose caller's parse refuses it naming
    /// [field].
    Future<void> expectKeptInFinding(
      String pathKey,
      Map<String, Object?> record,
      String field,
    ) async {
      if (pathKey != 'ingestBatch') {
        expect(
          () => StoredEvent.fromMap(record, 0),
          throwsA(_parseRefusalNaming(field)),
        );
        return;
      }
      final before = await logBesideFindings();
      final countBefore = (await findings()).length;
      await _recordIngestPaths[pathKey]!(store, <Map<String, Object?>>[record]);
      expect(await logBesideFindings(), before);
      final recorded = await findings();
      expect(recorded.skip(countBefore), <Map<String, Object?>>[
        <String, Object?>{
          'kind': 'event_malformed',
          'evidence': <String, Object?>{
            'reason': 'record_malformed',
            'record': _asReceived(record),
          },
        },
      ]);
    }

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
      final relayed = await relay.reader.findEventById(origin.eventId);
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
          // Verifies: EVS-DEV-event-record/J
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
              'causal',
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
            final forwarded = (await next.reader.findEventById(
              stored.eventId,
            ))!;
            expect((forwarded.metadata['provenance']! as List).length, 3);
            expect((await next.reader.verifyChains()).isValid, isTrue);
            final forwardedMap = forwarded.toMap();
            for (final key in <String>[
              'entry_type_version',
              'lib_format_version',
              'causal',
              ...spelled.extraFields.keys,
            ]) {
              expect(forwardedMap[key], record[key], reason: key);
            }
          });
        }

        for (final malformed in _malformedTimestamps.entries) {
          // Verifies: EVS-DEV-event-record/A
          // Verifies: EVS-DEV-security-findings/O
          test('keeps an event whose client timestamp has '
              '${malformed.key} in an event_malformed finding, storing no '
              'event', () async {
            if (!available) return;
            final record = _spelledRecord(
              clientTimestamp: malformed.value,
              receivedAt: '2026-09-01T12:00:00Z',
              initiator: _peerUser,
            );
            await expectKeptInFinding(path.key, record, '"client_timestamp"');
          });
        }

        for (final malformed in _malformedTimestamps.entries) {
          // Verifies: EVS-DEV-event-record/C
          // Verifies: EVS-DEV-security-findings/O
          test('keeps an event whose provenance received_at has '
              '${malformed.key} in an event_malformed finding, storing no '
              'event', () async {
            if (!available) return;
            final record = _spelledRecord(
              clientTimestamp: '2026-09-01T12:00:00Z',
              receivedAt: malformed.value,
              initiator: _peerUser,
            );
            await expectKeptInFinding(path.key, record, '"received_at"');
          });
        }

        for (final field in <String>['database_id', 'library_version']) {
          for (final value in <String, Object?>{
            'missing': _absentValue,
            'empty': '',
          }.entries) {
            // Verifies: EVS-DEV-event-record/H
            // Verifies: EVS-DEV-security-findings/O
            test(
              'keeps an event whose provenance entry has ${value.key} '
              '$field in an event_malformed finding, storing no event',
              () async {
                if (!available) return;
                final record = _spelledRecord(
                  clientTimestamp: '2026-09-01T12:00:00Z',
                  initiator: _peerUser,
                );
                final metadata = record['metadata']! as Map<String, Object?>;
                final entry =
                    (metadata['provenance']! as List).single
                        as Map<String, Object?>;
                if (identical(value.value, _absentValue)) {
                  entry.remove(field);
                } else {
                  entry[field] = value.value;
                }
                record['event_hash'] = canonicalEventHash(record);
                await expectKeptInFinding(path.key, record, '"$field"');
              },
            );
          }
        }

        // Verifies: EVS-DEV-causal-parents/B
        // Verifies: EVS-DEV-security-findings/O
        test('keeps an event with no causal object, or one with a key '
            'outside its shape, in an event_malformed finding, storing no '
            'event', () async {
          if (!available) return;
          for (final causal in <String, Object?>{
            'no causal': _absentValue,
            'an extra key': <String, Object?>{
              ...kRootVersionCausalJson,
              'extra': true,
            },
          }.entries) {
            final record = _spelledRecord(
              clientTimestamp: '2026-09-01T12:00:00Z',
              initiator: _peerUser,
            );
            if (identical(causal.value, _absentValue)) {
              record.remove('causal');
            } else {
              record['causal'] = causal.value;
            }
            record['event_hash'] = canonicalEventHash(record);
            await expectKeptInFinding(path.key, record, 'causal');
          }
        });

        for (final tamper in _tampers.entries) {
          // Verifies: EVS-PRD-ingest/D
          // Verifies: EVS-PRD-hash-chain-integrity/A
          // Verifies: EVS-DEV-event-record/K
          // Verifies: EVS-DEV-chain-verification/P
          test('stores an event whose ${tamper.key} changed after it was '
              'sealed as received, with one hash_mismatch finding', () async {
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
            await path.value(store, <Map<String, Object?>>[tampered]);
            final stored = await backend.findEventById(
              tampered['event_id']! as String,
            );
            expect(stored, isNotNull, reason: 'stored as received');
            expect(await findings(), <Map<String, Object?>>[
              _hashMismatch(
                eventId: tampered['event_id']! as String,
                carried: record['event_hash']! as String,
                recomputed: canonicalEventHash(tampered),
              ),
            ]);
          });
        }

        // Verifies: EVS-DEV-security-findings/O
        test('keeps an event whose metadata is null, and so carries no '
            'provenance, in an event_malformed finding', () async {
          if (!available) return;
          final record = <String, Object?>{
            ..._spelledRecord(
              clientTimestamp: '2026-09-01T12:00:00Z',
              initiator: const UserInitiator('peer-user').toJson(),
            ),
            'metadata': null,
          };
          record['event_hash'] = canonicalEventHash(record);
          final before = await logBesideFindings();
          await path.value(store, <Map<String, Object?>>[record]);
          expect(await logBesideFindings(), before);
          final recorded = await findings();
          expect(recorded, hasLength(1));
          expect(recorded.single['kind'], 'event_malformed');
          expect(
            (recorded.single['evidence']! as Map)['reason'],
            'record_malformed',
          );
        });
      });
    }

    final constructed = <String, StoredEvent Function()>{
      'a client timestamp outside the four-digit years': () =>
          StoredEvent.synthetic(
            eventId: 'hash-far-future',
            aggregateId: 'hash-far-future',
            aggregateType: 'note',
            entryType: _noteType,
            initiator: const UserInitiator('peer-user'),
            clientTimestamp: DateTime.utc(10000),
            eventHash: 'unsealed',
            metadata: <String, dynamic>{
              'provenance': <Map<String, Object?>>[_peerEntry()],
            },
          ),
      'a provenance received_at without an offset': () => StoredEvent.synthetic(
        eventId: 'hash-offsetless-received-at',
        aggregateId: 'hash-offsetless-received-at',
        aggregateType: 'note',
        entryType: _noteType,
        initiator: const UserInitiator('peer-user'),
        clientTimestamp: DateTime.utc(2026, 9, 1, 12),
        eventHash: 'unsealed',
        metadata: <String, dynamic>{
          'provenance': <Map<String, Object?>>[
            <String, Object?>{
              ..._peerEntry(),
              'received_at': '2026-09-01T12:00:00',
            },
          ],
        },
      ),
      'no causal object': () => StoredEvent(
        key: 0,
        eventId: 'hash-no-causal',
        aggregateId: 'hash-no-causal',
        aggregateType: 'note',
        entryType: _noteType,
        entryTypeVersion: _noteDef.registeredVersion,
        libFormatVersion: LibVersion.dataFormat,
        eventType: 'finalized',
        sequenceNumber: 1,
        data: const <String, dynamic>{},
        metadata: <String, dynamic>{
          'provenance': <Map<String, Object?>>[_peerEntry()],
        },
        initiator: const UserInitiator('peer-user'),
        clientTimestamp: DateTime.utc(2026, 9, 1, 12),
        eventHash: 'unsealed',
      ),
      'a provenance entry lacking library_version': () => StoredEvent.synthetic(
        eventId: 'hash-no-library-version',
        aggregateId: 'hash-no-library-version',
        aggregateType: 'note',
        entryType: _noteType,
        initiator: const UserInitiator('peer-user'),
        clientTimestamp: DateTime.utc(2026, 9, 1, 12),
        eventHash: 'unsealed',
        metadata: <String, dynamic>{
          'provenance': <Map<String, Object?>>[
            <String, Object?>{..._peerEntry()}..remove('library_version'),
          ],
        },
      ),
    };
    for (final c in constructed.entries) {
      // Verifies: EVS-DEV-event-record/A
      // Verifies: EVS-DEV-event-record/C
      // Verifies: EVS-DEV-event-record/H
      // Verifies: EVS-DEV-causal-parents/B
      // Verifies: EVS-DEV-security-findings/O
      test('ingestEvent keeps an event built with ${c.key} in an '
          'event_malformed finding carrying the record it writes, storing '
          'no event', () async {
        if (!available) return;
        final event = c.value();
        final before = await logBesideFindings();
        final outcome = await store.ingestEvent(event);
        expect(outcome.outcome, IngestOutcome.keptInFinding);
        expect(await logBesideFindings(), before);
        final recorded = await findings();
        expect(recorded, hasLength(1));
        expect(recorded.single['kind'], 'event_malformed');
        expect(recorded.single['evidence'], <String, Object?>{
          'reason': 'record_malformed',
          'record': _asReceived(Map<String, Object?>.from(event.toMap())),
        });
      });
    }

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
      final outcome = await receiver.ingestEvent(stored);
      expect(outcome.outcome, IngestOutcome.ingested);
      expect(await receiver.reader.findEventById(stored.eventId), isNotNull);
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
      final outcome = await next.ingestEvent(stored);
      expect(outcome.outcome, IngestOutcome.ingested);
      expect(await next.reader.findEventById(stored.eventId), isNotNull);
    });

    // A change confined to the last hop's own provenance entry, or to the
    // sequence number the last hop assigned, leaves every arrival hash
    // below it intact: the arrival-hash walk alone admits it, and only the
    // event-hash check records it.
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
      // Verifies: EVS-DEV-chain-verification/P
      test(
        'stores a relayed event with ${tamper.key} changed, which only '
        'the event-hash check catches, with one hash_mismatch finding',
        () async {
          if (!available) return;
          final relayed = await relayedEvent();
          final event = _altered(relayed, tamper.value(relayed));
          expect(
            hashMismatchEvidence(event).map((e) => e['carried_hash']),
            <String>[event.eventHash],
            reason: 'every arrival hash still verifies',
          );
          for (final ingest in _ingestPaths.values) {
            await ingest(store, <StoredEvent>[event]);
          }
          expect(
            await findings(),
            <Map<String, Object?>>[
              _hashMismatch(
                eventId: event.eventId,
                carried: event.eventHash,
                recomputed: canonicalEventHash(event.toMap()),
              ),
            ],
            reason: 'the second path finds the event held and the finding too',
          );
          expect(
            (await logBesideFindings()).where((id) => id == event.eventId),
            hasLength(1),
          );
        },
      );
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
            expect(await findings(), isEmpty);
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
          expect(await findings(), isEmpty);
        });

        final altered = <String, Future<StoredEvent> Function()>{
          'a first-hop event whose hash is not the hash of its content':
              () async => _altered(_originEvent(), <String, Object?>{
                'event_hash': 'not-the-hash-of-this-event',
              }),
          'a first-hop event with a field changed after hashing': () async =>
              _altered(_originEvent(), <String, Object?>{
                'data': <String, Object?>{'title': 'tampered'},
              }),
          'a relayed event with a field changed after its last hop': () async =>
              _altered(await relayedEvent(), <String, Object?>{
                'data': <String, Object?>{'title': 'tampered'},
              }),
        };
        for (final c in altered.entries) {
          // Verifies: EVS-PRD-ingest/D
          // Verifies: EVS-PRD-hash-chain-integrity/A
          // Verifies: EVS-DEV-chain-verification/P
          test('stores ${c.key} as received, with one hash_mismatch '
              'finding', () async {
            if (!available) return;
            final event = await c.value();
            await path.value(store, <StoredEvent>[event]);
            final stored = await backend.findEventById(event.eventId);
            expect(stored!.data, event.data, reason: 'stored as received');
            // One finding per hash that does not recompute: a change to the
            // content of a relayed event also breaks the arrival hashes
            // below its last hop.
            final mismatches = hashMismatchEvidence(event);
            expect(mismatches, isNotEmpty);
            expect(await findings(), <Map<String, Object?>>[
              for (final m in mismatches)
                _hashMismatch(
                  eventId: event.eventId,
                  carried: m['carried_hash']! as String,
                  recomputed: m['recomputed_hash']! as String,
                ),
            ]);
            expect(
              (await findings()).first,
              _hashMismatch(
                eventId: event.eventId,
                carried: event.eventHash,
                recomputed: canonicalEventHash(event.toMap()),
              ),
            );
          });
        }
      });
    }

    // Verifies: EVS-PRD-ingest/D
    // Verifies: EVS-PRD-ingest/G
    test('ingestBatch admits every event of a batch in which one first-hop '
        'event does not verify, recording one finding', () async {
      if (!available) return;
      final good = _originEvent();
      final bad = _altered(_originEvent(), <String, Object?>{
        'event_hash': 'not-the-hash-of-this-event',
      });
      final alsoGood = _originEvent();
      final result = await store.ingestBatch(
        _batchOf(<StoredEvent>[good, bad, alsoGood]),
        wireFormat: BatchEnvelope.wireFormat,
      );
      expect(result.events.map((e) => e.outcome), <IngestOutcome>[
        IngestOutcome.ingested,
        IngestOutcome.ingestedWithFinding,
        IngestOutcome.ingested,
      ]);
      for (final e in <StoredEvent>[good, bad, alsoGood]) {
        expect(await backend.findEventById(e.eventId), isNotNull);
      }
      expect(await findings(), hasLength(1));
    });

    // Verifies: EVS-PRD-hash-chain-integrity/A
    test('the hash check reports an event whose hash is not the hash of '
        'its content', () async {
      if (!available) return;
      final event = _altered(_originEvent(), <String, Object?>{
        'data': <String, Object?>{'title': 'tampered'},
      });
      expect(
        hashMismatchEvidence(event).map((e) => e['carried_hash']),
        <String>[event.eventHash],
      );
      expect(hashMismatchEvidence(_originEvent()), isEmpty);
    });
  });
}
