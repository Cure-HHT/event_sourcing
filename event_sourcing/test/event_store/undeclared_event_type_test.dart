// An entry type declares every event type it carries: the library appends no
// event of an event type its entry type does not declare, and ingest refuses
// one, as it refuses a higher entry-type major, recording the refusal as an
// undeclared_event_type finding in a transaction of its own unless the store
// was opened not to record them. The registry audit records each entry
// type's patch.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kIngestAuditEntryType;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

import '../test_support/deliveries.dart';
import '../test_support/fake_destination.dart';
import '../test_support/record_fixtures.dart';

const _uuid = Uuid();

const _source = Source(
  hopId: 'control-server',
  identifier: 'undeclared-control',
  softwareVersion: 'test',
);

const _noteType = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'demo_note',
  patch: 3,
  declarations: <EventTypeDeclaration>[
    EventTypeDeclaration(
      eventType: 'finalized',
      kind: CausalKind.version,
      eligible: true,
    ),
  ],
);

/// A peer's record of [_noteType] with event type [eventType].
Map<String, Object?> _peerRecord({required String eventType}) {
  final now = DateTime.utc(2026, 10, 30, 12);
  final eventMap = <String, Object?>{
    'event_id': _uuid.v4(),
    'aggregate_id': 'remote-aggregate-1',
    'aggregate_type': 'note',
    'entry_type': _noteType.id,
    'entry_type_version': _noteType.registeredVersion.toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': eventType,
    'sequence_number': 1001,
    'data': const <String, Object?>{'title': 'remote note'},
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[
        <String, Object?>{
          'hop': 'remote-mobile-1',
          'received_at': now.toIso8601String(),
          'identifier': 'remote-device',
          'software_version': 'remote-app@1.0.0',
          'database_id': kPeerDatabaseId,
          'library_version': kPeerLibraryVersion,
        },
      ],
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

Future<EventStoreBundle> _bootstrap({
  bool? recordUndeclaredIngestFindings,
  List<Destination> destinations = const <Destination>[],
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'undeclared-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final storage = ApplicationSuppliedStorage(
    backend,
    SembastSecurityContextStore(backend: backend),
  );
  const entryTypes = <EntryTypeDefinition>[_noteType];
  return recordUndeclaredIngestFindings == null
      ? bootstrapEventStore(
          storage: storage,
          source: _source,
          entryTypes: entryTypes,
          destinations: destinations,
        )
      : bootstrapEventStore(
          storage: storage,
          source: _source,
          entryTypes: entryTypes,
          destinations: destinations,
          recordUndeclaredIngestFindings: recordUndeclaredIngestFindings,
        );
}

Future<List<StoredEvent>> _findingsOfKind(
  EventStore store,
  String kind,
) async => (await store.reader.findAllEvents())
    .where(
      (e) => e.entryType == kSecurityFindingEntryType && e.data['kind'] == kind,
    )
    .toList();

void main() {
  // Verifies: EVS-DEV-causal-parents/F
  test(
    'append of an undeclared event type throws UndeclaredEventTypeError and writes nothing',
    () async {
      final ds = await _bootstrap();
      final store = ds.eventStore;
      final counterBefore = await store.reader.readSequenceCounter();
      final eventsBefore = (await store.reader.findAllEvents()).length;

      await expectLater(
        store.append(
          entryType: _noteType.id,
          aggregateId: 'note-1',
          aggregateType: 'note',
          eventType: 'archived',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u1'),
        ),
        throwsA(
          isA<UndeclaredEventTypeError>()
              .having((e) => e.entryType, 'entryType', _noteType.id)
              .having((e) => e.eventType, 'eventType', 'archived'),
        ),
      );
      expect(await store.reader.readSequenceCounter(), counterBefore);
      expect((await store.reader.findAllEvents()).length, eventsBefore);

      // The declared event type of the same entry type appends.
      final appended = await store.append(
        entryType: _noteType.id,
        aggregateId: 'note-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const UserInitiator('u1'),
      );
      expect(appended, isNotNull);
      await store.close();
    },
  );

  // Verifies: EVS-DEV-version-compatibility/D
  test(
    'ingest of an undeclared event type throws IngestUndeclaredEventType and records one undeclared_event_type finding',
    () async {
      final ds = await _bootstrap();
      final store = ds.eventStore;
      final record = _peerRecord(eventType: 'archived');
      final counterBefore = await store.reader.readSequenceCounter();

      final matcher = throwsA(
        isA<IngestUndeclaredEventType>()
            .having((e) => e.eventId, 'eventId', record['event_id'])
            .having((e) => e.entryType, 'entryType', _noteType.id)
            .having((e) => e.eventType, 'eventType', 'archived'),
      );
      await expectLater(
        ingestEventForTest(store, StoredEvent.fromMap(record, 0)),
        matcher,
      );
      expect(
        await store.reader.findEventById(record['event_id']! as String),
        isNull,
      );
      final findings = await _findingsOfKind(store, 'undeclared_event_type');
      expect(findings, hasLength(1));
      final data = findings.single.data;
      expect(data['evidence'], <String, Object?>{
        'event_id': record['event_id'],
        'entry_type': _noteType.id,
        'event_type': 'archived',
        'origin_database_id': kPeerDatabaseId,
      });
      expect(data['aggregates'], isEmpty);
      expect((data['detector']! as Map)['role'], 'ingest');
      // The finding is the only write.
      expect(await store.reader.readSequenceCounter(), counterBefore + 1);

      // A second delivery of the same event records no second finding.
      await expectLater(
        ingestEventForTest(store, StoredEvent.fromMap(record, 0)),
        matcher,
      );
      expect(
        await _findingsOfKind(store, 'undeclared_event_type'),
        hasLength(1),
      );
      await store.close();
    },
  );

  // Verifies: EVS-DEV-version-compatibility/D
  test(
    'a delivery carrying an undeclared event type is refused rejected, naming the event, with one finding',
    () async {
      final ds = await _bootstrap();
      final store = ds.eventStore;
      final declared = _peerRecord(eventType: 'finalized');
      final undeclared = _peerRecord(eventType: 'archived');

      for (var attempt = 0; attempt < 2; attempt++) {
        final answer = (await deliverTo(store, [
          declared,
          undeclared,
        ])).response;
        expect(answer, isA<ReceiverRefusal>());
        final refusal = answer as ReceiverRefusal;
        expect(refusal.refusal, RefusalKind.rejected);
        expect(refusal.reason, IngestUndeclaredEventType.refusalReason);
        expect(refusal.refusedEventId, undeclared['event_id']);
      }
      // Neither event of the refused delivery is stored.
      expect(
        await store.reader.findEventById(declared['event_id']! as String),
        isNull,
      );
      final findings = await _findingsOfKind(store, 'undeclared_event_type');
      expect(findings, hasLength(1));
      expect(
        (findings.single.data['evidence']! as Map)['event_id'],
        undeclared['event_id'],
      );
      await store.close();
    },
  );

  // Verifies: EVS-DEV-version-compatibility/D
  test(
    'a store opened with recordUndeclaredIngestFindings: false refuses the same ingest and records no finding',
    () async {
      final ds = await _bootstrap(recordUndeclaredIngestFindings: false);
      final store = ds.eventStore;
      final record = _peerRecord(eventType: 'archived');
      final counterBefore = await store.reader.readSequenceCounter();

      await expectLater(
        ingestEventForTest(store, StoredEvent.fromMap(record, 0)),
        throwsA(isA<IngestUndeclaredEventType>()),
      );
      final answer = (await deliverTo(store, [record])).response;
      expect(
        (answer as ReceiverRefusal).reason,
        IngestUndeclaredEventType.refusalReason,
      );
      expect(await _findingsOfKind(store, 'undeclared_event_type'), isEmpty);
      expect(await store.reader.readSequenceCounter(), counterBefore);
      await store.close();
    },
  );

  // Verifies: EVS-DEV-causal-parents/E
  test(
    'every reserved system entry type declares every event type the library appends',
    () async {
      // Every reserved shape's event types are declared by its entry type
      // (test/security/reserved_event_shapes_test.dart); this drives the
      // library's own appends on a fresh store: the registry audit at
      // bootstrap, a destination's registration, start and end dates and
      // deletion, a delivery's accepted audit, a security finding, a
      // redaction and a retention sweep. Any of them appending an event type
      // its reserved entry type does not declare throws
      // UndeclaredEventTypeError.
      final ds = await _bootstrap();
      final store = ds.eventStore;
      await ds.destinations.addDestination(
        FakeDestination(id: 'notes-out', allowHardDelete: true),
        initiator: const AutomationInitiator(service: 'test'),
      );
      await ds.destinations.setStartDate(
        'notes-out',
        DateTime.utc(2026, 10, 1),
        initiator: const AutomationInitiator(service: 'test'),
      );
      await ds.destinations.setEndDate(
        'notes-out',
        DateTime.utc(2026, 12, 1),
        initiator: const AutomationInitiator(service: 'test'),
      );
      await ds.destinations.deleteDestination(
        'notes-out',
        initiator: const AutomationInitiator(service: 'test'),
      );
      expect(
        (await deliverTo(store, [
          _peerRecord(eventType: 'finalized'),
        ])).accepted,
        isTrue,
      );
      await expectLater(
        ingestEventForTest(
          store,
          StoredEvent.fromMap(_peerRecord(eventType: 'archived'), 0),
        ),
        throwsA(isA<IngestUndeclaredEventType>()),
      );
      final local = await store.append(
        entryType: _noteType.id,
        aggregateId: 'note-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const UserInitiator('u1'),
        security: const SecurityDetails(ipAddress: '10.0.0.1'),
      );
      await store.clearSecurityContext(
        local!.eventId,
        reason: 'test',
        redactedBy: const UserInitiator('admin'),
      );
      await store.applyRetentionPolicy();

      final appendedTypes = <String>{
        for (final e in await store.reader.findAllEvents()) e.entryType,
      };
      expect(
        appendedTypes,
        containsAll(<String>[
          kEntryTypeRegistryInitializedEntryType,
          kDestinationRegisteredEntryType,
          kDestinationStartDateSetEntryType,
          kDestinationEndDateSetEntryType,
          kDestinationDeletedEntryType,
          kIngestAuditEntryType,
          kSecurityFindingEntryType,
          kSecurityContextRedactedEntryType,
        ]),
      );
      await store.close();
    },
  );

  // Verifies: EVS-DEV-version-compatibility/K
  test("the registry audit records each entry type's patch", () async {
    final ds = await _bootstrap();
    final audits = (await ds.eventStore.reader.findAllEvents())
        .where((e) => e.entryType == kEntryTypeRegistryInitializedEntryType)
        .toList();
    expect(audits, hasLength(1));
    final patches = audits.single.data['patches']! as Map<String, Object?>;
    expect(patches.length, ds.entryTypes.all().length);
    expect(patches[_noteType.id], 3);
    for (final definition in kSystemEntryTypes) {
      expect(patches[definition.id], 0);
    }
    // The stamped version stays a major and a minor.
    expect(audits.single.entryTypeVersion, const EntryTypeVersion(1, 0));
    expect(audits.single.toMap()['entry_type_version'], <String, Object?>{
      'major': 1,
      'minor': 0,
    });
    await ds.eventStore.close();
  });
}
