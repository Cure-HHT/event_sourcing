// The outstanding-finding marks read the log only as far as a held finding
// can apply: an append with no finding held reads no finding and no
// aggregate's events, and a finding held as authored marks its aggregates
// without reading who authored their events.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/ingest_record_findings_conformance.dart'
    show envelopeOf, sealedRecord;

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
    }))['note-1']!;
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
    final result = await store.ingestBatch(
      envelopeOf(<Map<String, Object?>>[tampered]).encode(),
      wireFormat: BatchEnvelope.wireFormat,
    );
    final findingId = result.events.single.findingIds.single;
    backend.aggregateReads = 0;
    await _appendNote(store, 'tampered');
    await _appendNote(store, 'unrelated');
    expect(backend.aggregateReads, 0);
    final rows = await store.reader.readViewRowsByKeys(_kNotesSpec.viewName, {
      'tampered',
      'unrelated',
    });
    expect(rows['tampered']![r'$integrity'], <String, Object?>{
      'security_findings': <String>[findingId],
    });
    expect(rows['unrelated']![r'$integrity'], <String, Object?>{
      'security_findings': <String>[],
    });
  });
}
