// Runs the scenarios of delivery_receiver_conformance.dart on Sembast (in
// memory), and checks with a spying backend that an unauthenticated
// delivery reads no channel record.
//
// The scenarios' assertions are cited on their own tests in
// test_support/delivery_receiver_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:meta/meta.dart';
import 'package:sembast/sembast_memory.dart'
    show Database, newDatabaseFactoryMemory;

import '../test_support/delivery_receiver_conformance.dart';
import '../test_support/ingest_record_findings_conformance.dart'
    show sealedRecord;
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

/// A [SembastBackend] counting the reads of a channel's accepted-delivery
/// audits and the transactions it runs.
class _SpyingBackend extends SembastBackend {
  _SpyingBackend({required super.database});

  int recordReads = 0;
  int transactions = 0;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    transactions += 1;
    return super.transaction(body);
  }

  @internal
  @override
  Future<StoredEvent?> readLatestAuthoredOfAggregateInTxn(
    Transaction txn, {
    required String databaseId,
    required String aggregateId,
  }) {
    recordReads += 1;
    return super.readLatestAuthoredOfAggregateInTxn(
      txn,
      databaseId: databaseId,
      aggregateId: aggregateId,
    );
  }
}

class _SembastDatabase implements VersionTestDatabase {
  _SembastDatabase(this._db, {this.spying = false});

  final Database _db;
  final bool spying;
  final List<_SpyingBackend> spies = <_SpyingBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    if (!spying) return SembastBackend(database: _db);
    final spy = _SpyingBackend(database: _db);
    spies.add(spy);
    return spy;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

/// A view whose row write always throws, so its copy's fold under
/// always-stored mode meets a storage failure rather than a fold failure:
/// the write happens after the row-key/row-data computation the fold
/// wraps, so nothing about it is a fold failure.
const String _kAlwaysFailingView = 'always_failing_notes';

const TableProjectionSpec _kAlwaysFailingSpec = TableProjectionSpec(
  viewName: _kAlwaysFailingView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// A [SembastBackend] whose row write for [_kAlwaysFailingView] always
/// throws, injecting a storage failure into the ingest transaction's
/// always-stored fold.
class _FailingRowWriteBackend extends SembastBackend {
  _FailingRowWriteBackend({required super.database});

  @internal
  @override
  Future<void> upsertTableViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  }) {
    throw StateError('injected storage failure');
  }
}

class _FailingRowWriteDatabase implements VersionTestDatabase {
  _FailingRowWriteDatabase(this._db);

  final Database _db;

  @override
  Future<StorageBackend> openBackend() async =>
      _FailingRowWriteBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

/// A view whose fold, under always-stored mode, must run inside
/// `runInSavepointInTxn`: this spec's row write always succeeds, so the
/// only observation is whether the call happened.
const String _kSavepointCountView = 'savepoint_count_notes';

const TableProjectionSpec _kSavepointCountSpec = TableProjectionSpec(
  viewName: _kSavepointCountView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// A [SembastBackend] counting calls to `runInSavepointInTxn`.
class _SavepointCountingBackend extends SembastBackend {
  _SavepointCountingBackend({required super.database});

  int savepointCalls = 0;

  @internal
  @override
  Future<T> runInSavepointInTxn<T>(Transaction txn, Future<T> Function() body) {
    savepointCalls += 1;
    return super.runInSavepointInTxn(txn, body);
  }
}

class _SavepointCountingDatabase implements VersionTestDatabase {
  _SavepointCountingDatabase(this._db);

  final Database _db;
  final List<_SavepointCountingBackend> backends =
      <_SavepointCountingBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = _SavepointCountingBackend(database: _db);
    backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _counter = 0;

Future<Database> _memoryDatabase() {
  _counter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'delivery-receiver-$_counter.db',
  );
}

void main() {
  runDeliveryReceiverScenarios(
    openDatabase: () async => _SembastDatabase(await _memoryDatabase()),
    backendLabel: 'sembast',
  );

  // Verifies: EVS-DEV-delivery-receiver/N
  test('an unauthenticated delivery is refused before any read of its '
      'channel record and any transaction', () async {
    final db = _SembastDatabase(await _memoryDatabase(), spying: true);
    // Every transaction the spy counts from here on is the test's own.
    final store = await openReceiverStoreWithCatchUpParked(db);
    addTearDown(() async {
      await store.close();
      await db.close();
    });
    final spy = db.spies.single;
    // The spy observes the rejection path and the accept path alike.
    await store.receiverEndpoint.accept(
      sealedDelivery().encode(),
      senderDatabaseIds: <String>{deliveryChannel().senderDatabaseId},
    );
    expect(spy.recordReads, greaterThan(0), reason: 'the spy sees reads');
    final reads = spy.recordReads;
    final transactions = spy.transactions;

    await expectLater(
      store.receiverEndpoint.accept(
        sealedDelivery().encode(),
        senderDatabaseIds: <String>{'someone-else'},
      ),
      throwsA(isA<DeliveryAuthenticationRefused>()),
    );

    expect(spy.recordReads, reads);
    expect(spy.transactions, transactions);
  });

  // Verifies: EVS-DEV-view-convergence/E
  test('a storage failure during an always-stored fold refuses the whole '
      'delivery, storing nothing', () async {
    final db = _FailingRowWriteDatabase(await _memoryDatabase());
    final store = await openReceiverStore(
      db,
      projections: ProjectionRegistry()..register(_kAlwaysFailingSpec),
    );
    addTearDown(() async {
      await store.close();
      await db.close();
    });
    final record = sealedRecord(data: <String, Object?>{'k': 'x'});
    final delivery = sealedDelivery(records: <Map<String, Object?>>[record]);

    await expectLater(
      store.receiverEndpoint.accept(
        delivery.encode(),
        senderDatabaseIds: <String>{deliveryChannel().senderDatabaseId},
      ),
      throwsA(isA<StateError>()),
      reason:
          'a throw other than a FoldFailure is a storage failure: it is '
          'not caught by the always-stored fold and refuses the whole '
          'delivery, not a pass-over',
    );
    expect(
      await store.reader.findEventById(record['event_id']! as String),
      isNull,
      reason: 'nothing of the refused delivery is stored',
    );
    expect(
      await store.reader.findAllEvents(entryType: kSecurityFindingEntryType),
      isEmpty,
      reason:
          'a storage failure is not recorded as a fold_failed finding: '
          'nothing committed for the refused delivery',
    );
  });

  // Verifies: EVS-DEV-view-convergence/E
  test("an always-stored event's fold into its current copy runs through "
      'runInSavepointInTxn, not a direct call', () async {
    final db = _SavepointCountingDatabase(await _memoryDatabase());
    final store = await openReceiverStore(
      db,
      projections: ProjectionRegistry()..register(_kSavepointCountSpec),
    );
    addTearDown(() async {
      await store.close();
      await db.close();
    });
    final backend = db.backends.single;
    final before = backend.savepointCalls;

    final record = sealedRecord(data: <String, Object?>{'k': 'x'});
    await store.receiverEndpoint.accept(
      sealedDelivery(records: <Map<String, Object?>>[record]).encode(),
      senderDatabaseIds: <String>{deliveryChannel().senderDatabaseId},
    );

    expect(
      backend.savepointCalls,
      greaterThan(before),
      reason:
          "an always-stored event's fold into each current copy runs "
          'inside runInSavepointInTxn, so a fold failure of that copy '
          "never aborts the delivery's storing transaction",
    );
  });
}
