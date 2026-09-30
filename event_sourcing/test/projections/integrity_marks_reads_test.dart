// The outstanding-finding marks read the log only as far as a held finding
// can apply: an append with no finding held reads no finding and no
// aggregate's events; a finding held as authored marks its aggregates
// without reading who authored their events; a received chain finding's
// succession-lineage resolution never scans the whole event store, on an
// ordinary append or on a catch-up transaction of a behind copy.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionChannel, SenderSuccessionData;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/deliveries.dart';
import '../test_support/ingest_chain_findings_conformance.dart'
    show chained, originChain;
import '../test_support/ingest_record_findings_conformance.dart'
    show sealedRecord;

const String _kType = 'finding_note';

const AggregateProjectionSpec _kNotesSpec = AggregateProjectionSpec(
  viewName: 'read_counted_notes',
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// A Sembast backend counting the reads the marks make.
class _CountingBackend extends SembastBackend {
  _CountingBackend({required super.database});

  int aggregateReads = 0;
  int findingReads = 0;

  /// Calls to [findAllEventsInTxn] filtered to the sender-succession entry
  /// type: the succession-lineage scan R14 forbids on an append or a
  /// catch-up step.
  int senderSuccessionScans = 0;

  /// Calls to [findAllEventsInTxn] with `afterSequence: null`: the shape
  /// only a full-log replay scan makes (it reads from the log's start,
  /// unlike a catch-up step's or a live currency check's own chunked read,
  /// both of which always pass the copy's numeric watermark). Zero of
  /// these, whatever else the store's background catch-up driver and live
  /// currency checks do concurrently, is the invariant this guards.
  int nullAfterSequenceScans = 0;

  @override
  // ignore: invalid_use_of_internal_member
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) {
    aggregateReads += 1;
    // ignore: invalid_use_of_internal_member
    return super.findEventsForAggregateInTxn(txn, aggregateId);
  }

  @override
  // ignore: invalid_use_of_internal_member
  Future<List<StoredEvent>> findSecurityFindingsInTxn(Transaction txn) {
    findingReads += 1;
    // ignore: invalid_use_of_internal_member
    return super.findSecurityFindingsInTxn(txn);
  }

  @override
  // ignore: invalid_use_of_internal_member
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) {
    if (entryType == kDestinationSenderSucceededEntryType) {
      senderSuccessionScans += 1;
    } else if (entryType == null && afterSequence == null) {
      nullAfterSequenceScans += 1;
    }
    // ignore: invalid_use_of_internal_member
    return super.findAllEventsInTxn(
      txn,
      afterSequence: afterSequence,
      limit: limit,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
  }
}

var _counter = 0;

Future<(EventStore, _CountingBackend)> _open() async {
  _counter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'integrity-marks-reads-$_counter.db',
  );
  final backend = _CountingBackend(database: db);
  final store = await EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    entryTypes: EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: _kType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: _kType,
        ),
      ),
    projections: ProjectionRegistry()..register(_kNotesSpec),
    source: const Source(
      hopId: 'receiver-hop',
      identifier: 'receiver-install',
      softwareVersion: 'receiver-app@1.0.0',
    ),
  );
  addTearDown(() async {
    await store.close();
    await db.close();
  });
  return (store, backend);
}

Future<void> _appendNote(EventStore store, String aggregateId) async {
  await store.append(
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'title': aggregateId},
    initiator: const UserInitiator('u1'),
  );
}

void main() {
  // Verifies: EVS-PRD-materializer/D
  test('an append with no finding held reads no finding and no events of '
      'an aggregate for the marks', () async {
    final (store, backend) = await _open();
    await _appendNote(store, 'note-1');
    backend
      ..aggregateReads = 0
      ..findingReads = 0;
    await _appendNote(store, 'note-1');
    await _appendNote(store, 'note-2');
    expect(backend.findingReads, 0);
    expect(backend.aggregateReads, 0);
    final row = (await store.reader.readViewRowsByKeys(_kNotesSpec.viewName, {
      'note-1',
    })).rows['note-1']!.dataOrNull!;
    expect(row[r'$integrity'], <String, Object?>{
      'security_findings': <String>[],
    });
  });

  // Verifies: EVS-PRD-materializer/D
  test('a finding held as authored marks its aggregate without a read of '
      'who authored its events', () async {
    final (store, backend) = await _open();
    final sealed = sealedRecord(aggregateId: 'tampered');
    final tampered = <String, Object?>{
      ...sealed,
      'data': <String, Object?>{'title': 'changed after sealing'},
    };
    await deliverTo(store, <Map<String, Object?>>[tampered]);
    final findingId =
        (await store.reader.findAllEvents(
              entryType: kSecurityFindingEntryType,
            )).single.data['finding_id']!
            as String;
    backend.aggregateReads = 0;
    await _appendNote(store, 'tampered');
    await _appendNote(store, 'unrelated');
    expect(backend.aggregateReads, 0);
    final rows = (await store.reader.readViewRowsByKeys(_kNotesSpec.viewName, {
      'tampered',
      'unrelated',
    })).rows;
    expect(rows['tampered']!.dataOrNull![r'$integrity'], <String, Object?>{
      'security_findings': <String>[findingId],
    });
    expect(rows['unrelated']!.dataOrNull![r'$integrity'], <String, Object?>{
      'security_findings': <String>[],
    });
  });

  // Verifies: EVS-PRD-materializer/E
  test(
    'an append with a received chain finding held, whose originating '
    'database has a succession lineage, makes no sender-succession scan',
    () async {
      final (store, backend) = await _open();
      const predecessorId = 'reads-reused-pred-db';
      const successorId = 'reads-reused-succ-db';
      final chain = originChain(predecessorId, 3);
      await deliverByOriginator(store, chain);

      const receivedId = 'reads-received-position-reused';
      final received = sealedRecord(
        databaseId: successorId,
        entryType: kSecurityFindingEntryType,
        aggregateType: 'security_finding',
        eventType: kSecurityFindingRecordedEventType,
        aggregateId: receivedId,
        data: <String, Object?>{
          'finding_id': receivedId,
          'kind': 'position_reused',
          'evidence': <String, Object?>{
            'database_id': predecessorId,
            'origin_sequence_number': 2,
          },
          'aggregates': const <String>[],
          'detector': <String, Object?>{
            'database_id': successorId,
            'role': 'ingest',
            'library_version': '0.0.0',
          },
        },
      );
      await deliverTo(store, <Map<String, Object?>>[received]);

      const successionData = SenderSuccessionData(
        id: 'reads-destination',
        registrationId: 'reads-registration',
        databaseId: successorId,
        predecessorDatabaseId: predecessorId,
        predecessorChannels: <SenderSuccessionChannel>[],
      );
      final successionRecord = sealedRecord(
        databaseId: successorId,
        entryType: kDestinationSenderSucceededEntryType,
        aggregateType: kDestinationAuditAggregateType,
        eventType: kDestinationSenderSucceededEventType,
        data: successionData.toJson(),
      );
      await deliverTo(store, <Map<String, Object?>>[successionRecord]);

      backend.senderSuccessionScans = 0;
      await _appendNote(store, 'reads-unrelated-append');
      expect(backend.senderSuccessionScans, 0);
    },
  );

  // Verifies: EVS-DEV-view-convergence/N
  test('a catch-up transaction of a behind copy with a counted finding held '
      'makes no full-log replay scan', () async {
    final (store, backend) = await _open();
    const db = 'reads-catchup-reused-db';
    final chain = originChain(db, 3);
    await deliverByOriginator(store, chain);
    final second = chained(db, 2, previous: chain[0]);
    await deliverByOriginator(store, <Map<String, Object?>>[second]);

    await rebuildView(
      store: store,
      viewName: _kNotesSpec.viewName,
      deadline: DateTime.now().toUtc().add(const Duration(seconds: 20)),
    );
    expect(
      backend.nullAfterSequenceScans,
      0,
      reason:
          'a catch-up step and a live currency check both read from the '
          "copy's own numeric watermark; only a full-log replay scan "
          "reads from the log's start",
    );
  });
}
