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
    final store = await openReceiverStore(db);
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
}
