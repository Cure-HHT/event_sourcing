// Backend-agnostic scenarios for the provenance entries the library stamps
// and the two chain links every stored event records: each record-assembly
// site (an append, a reserved append, the boot's library-version event, a
// snapshot-promotion audit, a duplicate-received audit, and the receiver
// entry of an ingested event) stamps the stamping database's identity and
// the build's library version; an appended event's predecessor is the
// latest event its database holds as authored, and its originator entry
// records its storage link; ingest keeps every incoming entry as it
// arrived. Run on Sembast by test/event_store/provenance_stamping_test.dart
// and on Postgres by
// test/storage/postgres/postgres_provenance_stamping_test.dart.
//
// Traceability lives on the individual tests below.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kIngestDuplicateReceivedEventType,
        kSecurityContextRedactedEntryType,
        kViewSnapshotPromotedEntryType;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

const _kType = 'stamped_note';
const _kView = 'stamped_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// An application version distinct from the library's, so a stamp that
/// took the application's version for the library's is visible.
const _kAppVersion = 'stamping-app@7.7.7';

/// A build declaration of the compiled data format under another package
/// version.
const _kDeclared = (
  version: '0.0.0-declared-build',
  dataFormat: LibVersion.dataFormat,
);

const _kInitiator = UserInitiator('stamping-user');

Source _source(String identifier) => Source(
  hopId: 'stamping-hop',
  identifier: identifier,
  softwareVersion: _kAppVersion,
);

EntryTypeRegistry _registry(EntryTypeVersion noteVersion) => EntryTypeRegistry()
  ..register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: noteVersion,
      name: _kType,
    ),
  );

/// Opens an event store over [backend] with `EventStore.open`, registering
/// [_kType] at [noteVersion] (at `1.1` with the `DefaultField` step adding
/// `b`).
Future<EventStore> _open(
  VersionTestDatabase db,
  StorageBackend backend, {
  String identifier = 'stamping-install',
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
}) {
  final promoters = PromoterRegistry();
  if (noteVersion == const EntryTypeVersion(1, 1)) {
    promoters.register(
      const PromoterSpec(
        viewName: _kView,
        entryType: _kType,
        fromVersion: EntryTypeVersion(1, 0),
        toVersion: EntryTypeVersion(1, 1),
        transforms: <TransformPrimitive>[
          DefaultField(fieldName: 'b', defaultValue: 0),
        ],
      ),
    );
  }
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: _registry(noteVersion),
    source: _source(identifier),
    projections: ProjectionRegistry()..register(_kSpec),
    promoters: promoters,
  );
}

Future<StoredEvent> _appendNote(
  EventStore store,
  String aggregateId, {
  SecurityDetails? security,
}) async => (await store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: _kType,
  eventType: 'noted',
  data: <String, Object?>{'title': aggregateId},
  initiator: _kInitiator,
  security: security,
))!;

List<Map<String, Object?>> _provenanceOf(StoredEvent event) =>
    <Map<String, Object?>>[
      for (final entry in event.metadata['provenance']! as List)
        Map<String, Object?>.from(entry as Map),
    ];

/// The events of [store]'s log, oldest first.
Future<List<StoredEvent>> _log(EventStore store) =>
    store.reader.findAllEvents();

/// The only event of [store]'s log matching [test].
Future<StoredEvent> _single(
  EventStore store,
  bool Function(StoredEvent) test,
) async {
  final matches = (await _log(store)).where(test).toList();
  expect(matches, hasLength(1));
  return matches.single;
}

/// Expects [entry] to name [databaseId] and [libraryVersion].
void _expectStamped(
  Map<String, Object?> entry, {
  required String databaseId,
  String libraryVersion = LibVersion.version,
}) {
  expect(entry['database_id'], databaseId);
  expect(entry['library_version'], libraryVersion);
}

/// Runs the provenance-stamping scenarios. [openDatabase] and
/// [openOtherDatabase] return two distinct databases; [skip] skips the
/// group when set.
void runProvenanceStampingScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required Future<VersionTestDatabase?> Function() openOtherDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('provenance stamping ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<VersionTestDatabase> database({bool other = false}) async {
      final db = (await (other ? openOtherDatabase() : openDatabase()))!;
      databases.add(db);
      return db;
    }

    Future<EventStore> open(
      VersionTestDatabase db, {
      String identifier = 'stamping-install',
      EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
    }) async {
      final store = await _open(
        db,
        await db.openBackend(),
        identifier: identifier,
        noteVersion: noteVersion,
      );
      opened.add(store);
      return store;
    }

    /// A store over a database of its own and an event it appended, as a
    /// peer database sends it.
    Future<(EventStore, StoredEvent)> peerEvent() async {
      final peer = await open(
        await database(other: true),
        identifier: 'peer-install',
      );
      return (peer, await _appendNote(peer, 'peer-note'));
    }

    tearDown(() async {
      for (final s in opened.reversed) {
        await s.close();
      }
      opened.clear();
      for (final d in databases.reversed) {
        await d.close();
      }
      databases.clear();
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    // Verifies: EVS-PRD-provenance/A
    test('the originator entry of an append names the database and the '
        'library version', () async {
      final store = await open(await database());
      final event = await _appendNote(store, 'note-1');

      final provenance = _provenanceOf(event);
      expect(provenance, hasLength(1));
      _expectStamped(provenance.single, databaseId: store.databaseId);
      expect(provenance.single['software_version'], _kAppVersion);
      final stored = await _single(store, (e) => e.eventId == event.eventId);
      _expectStamped(
        _provenanceOf(stored).single,
        databaseId: store.databaseId,
      );
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('the originator entry of a reserved append names the database and '
        'the library version', () async {
      final store = await open(await database());
      final subject = await _appendNote(
        store,
        'note-1',
        security: const SecurityDetails(ipAddress: '10.0.0.1'),
      );
      await store.clearSecurityContext(
        subject.eventId,
        reason: 'test redaction',
        redactedBy: _kInitiator,
      );

      final redaction = await _single(
        store,
        (e) => e.entryType == kSecurityContextRedactedEntryType,
      );
      _expectStamped(
        _provenanceOf(redaction).single,
        databaseId: store.databaseId,
      );
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('the boot minted the identity before its library-version event, '
        'whose originator entry names it and the library version', () async {
      final store = await open(await database());

      final initialized = await _single(
        store,
        (e) => e.eventType == LibVersionEvents.initialized,
      );
      expect(initialized.data['database_id'], store.databaseId);
      _expectStamped(
        _provenanceOf(initialized).single,
        databaseId: store.databaseId,
      );
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('the originator entry of a snapshot-promotion audit names the '
        'database and the library version', () async {
      final db = await database();
      final first = await open(db);
      await _appendNote(first, 'note-1');
      final databaseId = first.databaseId;
      await db.stop(first);
      opened.remove(first);

      final second = await open(db, noteVersion: const EntryTypeVersion(1, 1));

      expect(second.databaseId, databaseId);
      final audit = await _single(
        second,
        (e) => e.entryType == kViewSnapshotPromotedEntryType,
      );
      _expectStamped(_provenanceOf(audit).single, databaseId: databaseId);
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('the originator entry of a duplicate-received audit names the '
        'database and the library version', () async {
      final store = await open(await database());
      final (_, sent) = await peerEvent();
      await store.ingestEvent(sent);
      final outcome = await store.ingestEvent(sent);
      expect(outcome.outcome, IngestOutcome.duplicate);

      final audit = await _single(
        store,
        (e) => e.eventType == kIngestDuplicateReceivedEventType,
      );
      _expectStamped(_provenanceOf(audit).single, databaseId: store.databaseId);
    });

    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('the receiver entry of an ingested event names the receiving '
        'database and the library version', () async {
      final store = await open(await database());
      final (peer, sent) = await peerEvent();
      await store.ingestEvent(sent);

      final stored = await _single(store, (e) => e.eventId == sent.eventId);
      final provenance = _provenanceOf(stored);
      expect(provenance, hasLength(2));
      _expectStamped(provenance.first, databaseId: peer.databaseId);
      _expectStamped(provenance.last, databaseId: store.databaseId);
      expect(store.databaseId, isNot(peer.databaseId));
    });

    // Verifies: EVS-DEV-event-record/E
    // Verifies: EVS-DEV-event-record/F
    test('a build declaration installed by a test is the library version '
        'every site stamps', () async {
      final db = await database();
      final (_, sent) = await peerEvent();
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(buildDeclaration: _kDeclared),
        () async {
          final store = await open(db);
          final appended = await _appendNote(store, 'note-1');
          await store.ingestEvent(sent);
          await store.ingestEvent(sent);

          final log = await _log(store);
          final initialized = log.singleWhere(
            (e) => e.eventType == LibVersionEvents.initialized,
          );
          final duplicate = log.singleWhere(
            (e) => e.eventType == kIngestDuplicateReceivedEventType,
          );
          final ingested = log.singleWhere((e) => e.eventId == sent.eventId);
          for (final entry in <Map<String, Object?>>[
            _provenanceOf(initialized).single,
            _provenanceOf(appended).single,
            _provenanceOf(duplicate).single,
            _provenanceOf(ingested).last,
          ]) {
            _expectStamped(
              entry,
              databaseId: store.databaseId,
              libraryVersion: _kDeclared.version,
            );
          }
        },
      );
    });

    // Verifies: EVS-DEV-chain-verification/B
    // Verifies: EVS-DEV-chain-verification/C
    // Verifies: EVS-PRD-hash-chain-integrity/B
    test('an append after an ingest links to the latest authored event, and '
        'its originator entry records its storage link', () async {
      final store = await open(await database());
      final initialized = await _single(
        store,
        (e) => e.eventType == LibVersionEvents.initialized,
      );
      expect(initialized.previousEventHash, isNull);
      final e1 = await _appendNote(store, 'note-1');
      expect(e1.previousEventHash, initialized.eventHash);

      final (_, sent) = await peerEvent();
      final ingested = await store.ingestEvent(sent);
      final e2 = await _appendNote(store, 'note-2');

      expect(e2.previousEventHash, e1.eventHash);
      final originator = _provenanceOf(e2).single;
      expect(originator['ingest_sequence_number'], e2.sequenceNumber);
      expect(originator['previous_ingest_hash'], ingested.resultHash);
      final storedIngested = await _single(
        store,
        (e) => e.eventId == sent.eventId,
      );
      expect(storedIngested.sequenceNumber, e2.sequenceNumber - 1);
      expect(storedIngested.eventHash, ingested.resultHash);
    });

    // Verifies: EVS-DEV-chain-verification/B
    // Verifies: EVS-DEV-chain-verification/C
    test('an audit the library appends after an ingest links to the latest '
        'authored event, and records its storage link', () async {
      final store = await open(await database());
      final e1 = await _appendNote(store, 'note-1');
      final (_, sent) = await peerEvent();
      final ingested = await store.ingestEvent(sent);
      await store.ingestEvent(sent);

      final audit = await _single(
        store,
        (e) => e.eventType == kIngestDuplicateReceivedEventType,
      );
      expect(audit.previousEventHash, e1.eventHash);
      final originator = _provenanceOf(audit).single;
      expect(originator['ingest_sequence_number'], audit.sequenceNumber);
      expect(originator['previous_ingest_hash'], ingested.resultHash);
    });

    // Verifies: EVS-DEV-chain-verification/C
    test('the first event a database stores records a null storage link, '
        'and the library-version event of a later open records the one '
        'before it', () async {
      final db = await database();
      final first = await open(db);
      final initialized = await _single(
        first,
        (e) => e.eventType == LibVersionEvents.initialized,
      );
      final initializedEntry = _provenanceOf(initialized).single;
      expect(initializedEntry['ingest_sequence_number'], 1);
      expect(initializedEntry['previous_ingest_hash'], isNull);
      final e1 = await _appendNote(first, 'note-1');
      await db.stop(first);
      opened.remove(first);

      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(buildDeclaration: _kDeclared),
        () async {
          final second = await open(db);
          final changed = await _single(
            second,
            (e) => e.eventType == LibVersionEvents.changed,
          );
          expect(changed.previousEventHash, e1.eventHash);
          final entry = _provenanceOf(changed).single;
          expect(entry['ingest_sequence_number'], changed.sequenceNumber);
          expect(entry['previous_ingest_hash'], e1.eventHash);
        },
      );
    });

    // Verifies: EVS-DEV-event-record/G
    test('ingest keeps every incoming provenance entry and each of its keys '
        'as the record carries them, and adds only its own', () async {
      final store = await open(await database());
      final originator = <String, Object?>{
        'hop': 'peer-hop',
        'received_at': '2026-09-01T12:00:00.000+00:00',
        'identifier': 'peer-install',
        'software_version': 'peer-app@1.0.0',
        'database_id': 'peer-database',
        'library_version': '0.0.1',
        'ingest_sequence_number': 4,
        'previous_ingest_hash': null,
        'field_of_a_later_release': <String, Object?>{'n': 1},
      };
      final record = <String, Object?>{
        'event_id': 'verbatim-entry-event',
        'aggregate_id': 'verbatim-note',
        'aggregate_type': _kType,
        'entry_type': _kType,
        'entry_type_version': const EntryTypeVersion(1, 0).toJson(),
        'lib_format_version': LibVersion.dataFormat.toJson(),
        'event_type': 'noted',
        'sequence_number': 4,
        'data': <String, Object?>{'title': 'verbatim'},
        'metadata': <String, Object?>{
          'change_reason': 'initial',
          'provenance': <Map<String, Object?>>[originator],
        },
        'initiator': _kInitiator.toJson(),
        'flow_token': null,
        'client_timestamp': '2026-09-01T12:00:00.000+00:00',
        'previous_event_hash': null,
        'causal': <String, Object?>{
          'kind': 'version',
          'eligible': true,
          'parents': <Object?>[],
          'reconciles': null,
        },
      };
      record['event_hash'] = canonicalEventHash(record);

      await store.ingestEvent(StoredEvent.fromMap(record, 0));

      final stored = await _single(
        store,
        (e) => e.eventId == 'verbatim-entry-event',
      );
      final provenance = _provenanceOf(stored);
      expect(provenance, hasLength(2));
      expect(provenance.first, equals(originator));
      expect(provenance.last['arrival_hash'], record['event_hash']);
      _expectStamped(provenance.last, databaseId: store.databaseId);
    });
  });
}
