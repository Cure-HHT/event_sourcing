// Backend-agnostic scenarios for entry-type and data-format versions: two
// builds registering different minors of one major over one database,
// downgrade refusal by major, and the ingest version table. Run on Sembast
// by test/version_compatibility_test.dart and on Postgres by
// test/storage/postgres/postgres_versions_test.dart.
//
// Traceability lives on the individual tests below.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';

import 'deliveries.dart';
import 'record_fixtures.dart';
import 'test_backends.dart';

/// One database the scenarios open several backends over, as several
/// builds of the library would.
abstract class VersionTestDatabase {
  /// Opens a new backend over this database.
  Future<StorageBackend> openBackend();

  /// The security-context store an event store over [backend] uses.
  MutableSecurityContextStore securityFor(StorageBackend backend);

  /// Stops the instance [store] belongs to, as a stop-then-start deployment
  /// stops the old revision before the new one opens: on Postgres, where
  /// each instance holds its own connections and generation locks, it
  /// closes the store; on Sembast, where the scenarios share one database
  /// handle and a registration holds nothing, it does nothing.
  Future<void> stop(EventStore store);

  /// Closes every backend this database opened.
  Future<void> close();
}

const _kType = 'versioned_note';
const _kView = 'versioned_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{'deleted'},
);

const _kSource = Source(
  hopId: 'versions-hop',
  identifier: 'versions-install',
  softwareVersion: 'versions-test',
);

/// Opens an event store over a new backend of [db] with [_kType] registered
/// at [registered], the [projections] registered, and [promoters].
Future<EventStore> _openStore(
  VersionTestDatabase db, {
  required EntryTypeVersion registered,
  List<PromoterSpec> promoters = const <PromoterSpec>[],
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) async {
  final backend = await db.openBackend();
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes.register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: registered,
      name: _kType,
    ),
  );
  final promoterRegistry = PromoterRegistry();
  for (final spec in promoters) {
    promoterRegistry.register(spec);
  }
  final projectionRegistry = ProjectionRegistry();
  for (final spec in projections) {
    projectionRegistry.register(spec);
  }
  final store = await EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: entryTypes,
    source: _kSource,
    projections: projectionRegistry,
    promoters: promoterRegistry,
  );
  trackTestBackend(store, backend);
  return store;
}

Future<void> _appendNote(
  EventStore store,
  String aggregateId,
  Map<String, Object?> data, {
  String eventType = 'finalized',
}) async {
  await store.append(
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: eventType,
    data: data,
    initiator: const UserInitiator('versions-user'),
  );
}

Future<Map<String, Object?>?> _row(EventStore store, String aggregateId) =>
    store.reader.transaction(
      (txn) async => (await store.reader.readViewRowInTxn(
        txn,
        _kView,
        aggregateId,
      )).row.dataOrNull,
    );

/// Registers the version-compatibility scenarios against databases produced
/// by [openDatabase]. A null database skips the test.
void runVersionCompatibilityScenarios(
  Future<VersionTestDatabase?> Function() openDatabase, {
  required String backendLabel,
}) {
  group('version compatibility ($backendLabel)', () {
    VersionTestDatabase? db;

    setUp(() async {
      db = await openDatabase();
      if (db == null) markTestSkipped('no database for $backendLabel');
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    group('downgrade refusal compares majors', () {
      // Verifies: EVS-DEV-entry-type-downgrade-refusal/A+C
      test('a generation record of major 2 refuses a build registering 1.5, '
          'naming both majors, and changes nothing', () async {
        if (db == null) return;
        final current = await _openStore(
          db!,
          registered: const EntryTypeVersion(2, 0),
        );
        await _appendNote(current, 'agg-1', <String, Object?>{'a': 1});
        final eventsBefore = await current.reader.findAllEvents();
        final counterBefore = await current.reader.readSequenceCounter();
        await db!.stop(current);
        final reader = await db!.openBackend();

        await expectLater(
          _openStore(db!, registered: const EntryTypeVersion(1, 5)),
          throwsA(
            isA<EntryTypeVersionDowngradeError>()
                .having((e) => e.entryType, 'entryType', _kType)
                .having(
                  (e) => e.fromVersion,
                  'fromVersion',
                  const EntryTypeVersion(2, 0),
                )
                .having(
                  (e) => e.toVersion,
                  'toVersion',
                  const EntryTypeVersion(1, 5),
                )
                .having(
                  (e) => e.toString(),
                  'message',
                  allOf(contains('2.0'), contains('1.5'), contains('major')),
                ),
          ),
        );
        expect((await reader.findAllEvents()).length, eventsBefore.length);
        expect(await reader.readSequenceCounter(), counterBefore);
      });

      // Verifies: EVS-DEV-entry-type-downgrade-refusal/A
      test('a generation record of major 1 admits a build registering a lower '
          'minor of the same major', () async {
        if (db == null) return;
        await _openStore(db!, registered: const EntryTypeVersion(1, 3));
        // Majors only rise: a lower minor of the same major is not a
        // downgrade and opens without throwing.
        await _openStore(db!, registered: const EntryTypeVersion(1, 1));
      });
    });

    group('ingest compares majors', () {
      Future<EventStore> openReceiver() => _openStore(
        db!,
        registered: const EntryTypeVersion(1, 4),
        promoters: const <PromoterSpec>[
          PromoterSpec(
            viewName: _kView,
            entryType: _kType,
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: <TransformPrimitive>[
              DefaultField(fieldName: 'added', defaultValue: 'default'),
            ],
          ),
        ],
      );

      final paths = <String, Future<void> Function(EventStore, StoredEvent)>{
        'delivery': (store, event) async {
          await deliverEventsOrThrow(store, <StoredEvent>[event]);
        },
        'ingestEvent': (store, event) async {
          await ingestEventForTest(store, event);
        },
      };

      final refusals =
          <String, (EntryTypeVersion, DataFormatVersion, Matcher, String)>{
            'data format 1.0': (
              const EntryTypeVersion(1, 4),
              const DataFormatVersion(1, 0),
              isA<IngestDataFormatIncompatible>()
                  .having(
                    (e) => e.wireFormat,
                    'wireFormat',
                    const DataFormatVersion(1, 0),
                  )
                  .having(
                    (e) => e.receiverFormat,
                    'receiverFormat',
                    LibVersion.dataFormat,
                  ),
              IngestDataFormatIncompatible.refusalReason,
            ),
            'the next data-format major': (
              const EntryTypeVersion(1, 4),
              DataFormatVersion(LibVersion.dataFormat.major + 1, 0),
              isA<IngestDataFormatIncompatible>(),
              IngestDataFormatIncompatible.refusalReason,
            ),
            'entry type 2.0 under 1.4': (
              const EntryTypeVersion(2, 0),
              LibVersion.dataFormat,
              isA<IngestEntryTypeVersionAhead>()
                  .having(
                    (e) => e.wireVersion,
                    'wireVersion',
                    const EntryTypeVersion(2, 0),
                  )
                  .having(
                    (e) => e.receiverVersion,
                    'receiverVersion',
                    const EntryTypeVersion(1, 4),
                  ),
              IngestEntryTypeVersionAhead.refusalReason,
            ),
          };

      for (final path in paths.entries) {
        for (final refusal in refusals.entries) {
          // Verifies: EVS-DEV-version-compatibility/D
          test('${path.key} refuses ${refusal.key} before any write', () async {
            if (db == null) return;
            final receiver = await openReceiver();
            final (entryVersion, dataFormat, matcher, reason) = refusal.value;
            final eventsBefore = await receiver.reader.findAllEvents();
            final counterBefore = await receiver.reader.readSequenceCounter();
            final event = _peerEvent(
              entryTypeVersion: entryVersion,
              dataFormat: dataFormat,
              data: const <String, Object?>{'title': 'peer'},
            );
            await expectLater(
              path.value(receiver, event),
              throwsA(
                path.key == 'delivery'
                    ? _refusedRejected(reason, event.eventId)
                    : matcher,
              ),
            );
            expect(
              (await receiver.reader.findAllEvents()).length,
              eventsBefore.length,
            );
            expect(await receiver.reader.readSequenceCounter(), counterBefore);
            expect((await receiver.reader.findViewRows(_kView)).rows, isEmpty);
          });
        }

        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} accepts a later minor of the data format', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final laterMinor = DataFormatVersion(LibVersion.dataFormat.major, 7);
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 4),
            dataFormat: laterMinor,
            data: const <String, Object?>{'title': 'peer'},
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.libFormatVersion, laterMinor);
        });

        // Verifies: EVS-DEV-version-compatibility/D
        // Verifies: EVS-DEV-ingest-promotes-before-fold/D
        test('${path.key} accepts entry type 1.9 under 1.4, folds it unchanged '
            'and reads it back as 1.9', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 9),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'newer'},
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(1, 9));
          final row = await _row(receiver, event.aggregateId);
          expect(row!['title'], 'newer');
          expect(row, isNot(contains('added')));
        });

        // Verifies: EVS-DEV-version-compatibility/D
        // Verifies: EVS-DEV-ingest-promotes-before-fold/A
        test('${path.key} promotes entry type 1.0 under 1.4', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 0),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'older'},
          );
          await path.value(receiver, event);
          final row = await _row(receiver, event.aggregateId);
          expect(row!['title'], 'older');
          expect(row['added'], 'default');
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(1, 0));
        });
      }

      // A batch whose later event is refused commits nothing, not even the
      // compatible event staged before it.
      final laterRefusals = <String, (EntryTypeVersion, DataFormatVersion)>{
        'the next data-format major': (
          const EntryTypeVersion(1, 4),
          DataFormatVersion(LibVersion.dataFormat.major + 1, 0),
        ),
        'entry type 2.0 under 1.4': (
          const EntryTypeVersion(2, 0),
          LibVersion.dataFormat,
        ),
      };
      for (final refusal in laterRefusals.entries) {
        // Verifies: EVS-DEV-version-compatibility/D
        test('a delivery of [a compatible event, ${refusal.key}] writes '
            'nothing', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          await _appendNote(receiver, 'agg-held', <String, Object?>{'a': 1});
          final eventsBefore = (await receiver.reader.findAllEvents()).length;
          final counterBefore = await receiver.reader.readSequenceCounter();
          final rowsBefore = (await receiver.reader.findViewRows(_kView)).rows;
          final (entryVersion, dataFormat) = refusal.value;
          final refused = _peerEvent(
            entryTypeVersion: entryVersion,
            dataFormat: dataFormat,
            data: const <String, Object?>{'title': 'refused'},
          );
          await expectLater(
            deliverEventsOrThrow(receiver, <StoredEvent>[
              _peerEvent(
                entryTypeVersion: const EntryTypeVersion(1, 0),
                dataFormat: LibVersion.dataFormat,
                data: const <String, Object?>{'title': 'staged'},
              ),
              refused,
            ]),
            throwsA(
              anyOf(
                _refusedRejected(
                  IngestDataFormatIncompatible.refusalReason,
                  refused.eventId,
                ),
                _refusedRejected(
                  IngestEntryTypeVersionAhead.refusalReason,
                  refused.eventId,
                ),
              ),
            ),
          );
          expect((await receiver.reader.findAllEvents()).length, eventsBefore);
          expect(await receiver.reader.readSequenceCounter(), counterBefore);
          expect((await receiver.reader.findViewRows(_kView)).rows, rowsBefore);
        });
      }

      for (final path in paths.entries) {
        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} refuses a lower major no registered step promotes, '
            'by name and before any write', () async {
          if (db == null) return;
          final receiver = await _openStore(
            db!,
            registered: const EntryTypeVersion(2, 0),
          );
          final eventsBefore = (await receiver.reader.findAllEvents()).length;
          final counterBefore = await receiver.reader.readSequenceCounter();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 3),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'unpromotable'},
          );
          await expectLater(
            path.value(receiver, event),
            throwsA(
              path.key == 'delivery'
                  ? _refusedRejected(
                      IngestEntryTypeVersionUnpromotable.refusalReason,
                      event.eventId,
                    )
                  : isA<IngestEntryTypeVersionUnpromotable>()
                        .having((e) => e.eventId, 'eventId', event.eventId)
                        .having((e) => e.entryType, 'entryType', _kType)
                        .having((e) => e.viewName, 'viewName', _kView)
                        .having(
                          (e) => e.wireVersion,
                          'wireVersion',
                          const EntryTypeVersion(1, 3),
                        )
                        .having(
                          (e) => e.receiverVersion,
                          'receiverVersion',
                          const EntryTypeVersion(2, 0),
                        ),
            ),
          );
          expect((await receiver.reader.findAllEvents()).length, eventsBefore);
          expect(await receiver.reader.readSequenceCounter(), counterBefore);
          expect((await receiver.reader.findViewRows(_kView)).rows, isEmpty);
        });

        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} accepts an entry type the receiver does not '
            'register, at any version, and stores it unchanged', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(9, 3),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'relayed'},
            entryType: 'unregistered_type',
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(9, 3));
          expect(stored.data, event.data);
          expect((await receiver.reader.findViewRows(_kView)).rows, isEmpty);
        });
      }

      // Verifies: EVS-DEV-version-compatibility/Q
      test('an event of data format 2.0 that also lacks library_version and '
          'causal is refused on every ingest entry point naming data-format '
          'major 2, not a missing field, before any write', () async {
        if (db == null) return;
        final receiver = await openReceiver();
        final eventsBefore = (await receiver.reader.findAllEvents()).length;
        final counterBefore = await receiver.reader.readSequenceCounter();
        final record = _dataFormat2Record();
        final refusal = isA<IngestDataFormatIncompatible>()
            .having((e) => e.wireFormat.major, 'wireFormat.major', 2)
            .having((e) => e.toString(), 'toString', contains('2.0'));
        final answer = (await deliverTo(receiver, <Map<String, Object?>>[
          record,
        ], channel: testChannel(kPeerDatabaseId))).response;
        expect(
          answer,
          isA<ReceiverRefusal>()
              .having((r) => r.refusal, 'refusal', RefusalKind.rejected)
              .having(
                (r) => r.reason,
                'reason',
                IngestDataFormatIncompatible.refusalReason,
              )
              .having(
                (r) => r.refusedEventId,
                'refusedEventId',
                record['event_id'],
              ),
        );
        await expectLater(
          ingestEventForTest(receiver, _dataFormat2Event(record)),
          throwsA(refusal),
        );
        expect((await receiver.reader.findAllEvents()).length, eventsBefore);
        expect(await receiver.reader.readSequenceCounter(), counterBefore);
      });

      // Verifies: EVS-DEV-version-compatibility/D
      // Verifies: EVS-DEV-security-findings/O
      test('a delivery keeps an event with a malformed version in an '
          'event_malformed finding, storing no event for it', () async {
        if (db == null) return;
        final receiver = await openReceiver();
        Future<List<String>> besideFindings() async => <String>[
          for (final e in await receiver.reader.findAllEvents())
            if (e.entryType != kSecurityFindingEntryType &&
                e.eventType != 'ingest.delivery_accepted')
              e.eventId,
        ];
        final eventsBefore = await besideFindings();
        final malformed = <String, Map<String, Object?>>{
          'entry_type_version': <String, Object?>{'major': 0, 'minor': 0},
          'lib_format_version': <String, Object?>{
            'major': LibVersion.dataFormat.major,
          },
        };
        for (final field in malformed.entries) {
          final map = Map<String, Object?>.from(
            _peerEvent(
              entryTypeVersion: const EntryTypeVersion(1, 4),
              dataFormat: LibVersion.dataFormat,
              data: const <String, Object?>{'title': 'malformed'},
            ).toMap(),
          )..[field.key] = field.value;
          final delivery = await deliverTo(receiver, <Map<String, Object?>>[
            map,
          ]);
          expect(await recordOutcomes(receiver, delivery), <IngestOutcome>[
            IngestOutcome.keptInFinding,
          ], reason: field.key);
          final findings = await receiver.reader.findAllEvents(
            entryType: kSecurityFindingEntryType,
          );
          expect(
            findings.last.data['kind'],
            'event_malformed',
            reason: field.key,
          );
          expect(
            ((findings.last.data['evidence']! as Map)['record']!
                as Map)[field.key],
            field.value,
            reason: field.key,
          );
        }
        expect(await besideFindings(), eventsBefore);
      });

      // Verifies: EVS-DEV-version-compatibility/D
      test('a batch in the earlier envelope format is refused by name before '
          'any write', () async {
        if (db == null) return;
        final receiver = await openReceiver();
        final eventsBefore = await receiver.reader.findAllEvents();
        // A batch in the envelope format of data format 1, which names no
        // delivery channel.
        final bytes = Uint8List.fromList(
          utf8.encode(
            jsonEncode(<String, Object?>{
              'batch_format_version': '1',
              'batch_id': 'versions-batch-1',
              'sender_hop': 'peer-hop',
              'sender_identifier': 'peer-install',
              'sender_software_version': 'peer@1',
              'sent_at': DateTime.utc(2026, 9, 1, 12).toIso8601String(),
              'events': <Object?>[
                _peerEvent(
                  entryTypeVersion: const EntryTypeVersion(1, 4),
                  dataFormat: LibVersion.dataFormat,
                  data: const <String, Object?>{'title': 'peer'},
                ).toMap(),
              ],
            }),
          ),
        );
        await expectLater(
          receiver.receiverEndpoint.accept(
            bytes,
            senderDatabaseIds: const <String>{kPeerDatabaseId},
          ),
          throwsA(
            isA<IngestDecodeFailure>().having(
              (e) => e.message,
              'message',
              contains('batch_format_version'),
            ),
          ),
        );
        expect(
          (await receiver.reader.findAllEvents()).length,
          eventsBefore.length,
        );
      });
    });
  });
}

var _peerCounter = 0;

/// An event as a peer at [entryTypeVersion] and [dataFormat] sends it: one
/// origin provenance entry and the canonical hash of its record.
StoredEvent _peerEvent({
  required EntryTypeVersion entryTypeVersion,
  required DataFormatVersion dataFormat,
  required Map<String, Object?> data,
  String? aggregateId,
  String entryType = _kType,
}) {
  _peerCounter += 1;
  final now = DateTime.utc(2026, 9, 1, 12, 0, _peerCounter);
  final record = <String, Object?>{
    'event_id': 'peer-event-$_peerCounter-${now.microsecondsSinceEpoch}',
    'aggregate_id': aggregateId ?? 'peer-aggregate-$_peerCounter',
    'aggregate_type': 'note',
    'entry_type': entryType,
    'entry_type_version': entryTypeVersion.toJson(),
    'lib_format_version': dataFormat.toJson(),
    'event_type': 'finalized',
    'sequence_number': 1000 + _peerCounter,
    'data': data,
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[
        ProvenanceEntry(
          hop: 'peer-hop',
          receivedAt: now,
          identifier: 'peer-install',
          softwareVersion: 'peer@1',
          databaseId: kPeerDatabaseId,
          libraryVersion: kPeerLibraryVersion,
        ).toJson(),
      ],
    },
    'initiator': const UserInitiator('peer-user').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'previous_event_hash': null,
    'causal': kRootVersionCausalJson,
  };
  record['event_hash'] = canonicalEventHash(record);
  return StoredEvent.fromMap(record, 0);
}

/// A record as a build of data format 2.0 sent it: its provenance entry
/// carries no `library_version` and no `database_id`, and it carries no
/// `causal` object.
Map<String, Object?> _dataFormat2Record() {
  final record = Map<String, Object?>.from(
    _peerEvent(
      entryTypeVersion: const EntryTypeVersion(1, 4),
      dataFormat: const DataFormatVersion(2, 0),
      data: const <String, Object?>{'title': 'data format 2'},
    ).toMap(),
  )..remove('causal');
  final metadata = Map<String, Object?>.from(record['metadata']! as Map);
  metadata['provenance'] = <Map<String, Object?>>[
    for (final entry in metadata['provenance']! as List)
      Map<String, Object?>.from(entry as Map)
        ..remove('library_version')
        ..remove('database_id'),
  ];
  record['metadata'] = metadata;
  record['event_hash'] = canonicalEventHash(record);
  return record;
}

/// [record], a record of data format 2.0, as an event built in process,
/// without the parse that refuses its shape.
StoredEvent _dataFormat2Event(Map<String, Object?> record) => StoredEvent(
  key: 0,
  eventId: record['event_id']! as String,
  aggregateId: record['aggregate_id']! as String,
  aggregateType: record['aggregate_type']! as String,
  entryType: record['entry_type']! as String,
  entryTypeVersion: EntryTypeVersion.fromJson(record['entry_type_version']),
  libFormatVersion: DataFormatVersion.fromJson(record['lib_format_version']),
  eventType: record['event_type']! as String,
  sequenceNumber: record['sequence_number']! as int,
  data: Map<String, dynamic>.from(record['data']! as Map),
  metadata: Map<String, dynamic>.from(record['metadata']! as Map),
  initiator: const UserInitiator('peer-user'),
  clientTimestamp: DateTime.parse(record['client_timestamp']! as String),
  eventHash: record['event_hash']! as String,
);

/// Matches the [TestDeliveryRefused] of a delivery the receiver refused
/// `rejected`, naming [reason] and the event [eventId].
Matcher _refusedRejected(String reason, String eventId) =>
    isA<TestDeliveryRefused>()
        .having((e) => e.refusal.refusal, 'refusal', RefusalKind.rejected)
        .having((e) => e.refusal.reason, 'reason', reason)
        .having((e) => e.refusal.refusedEventId, 'refusedEventId', eventId);
