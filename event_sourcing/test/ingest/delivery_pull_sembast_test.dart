// Runs the scenarios of delivery_pull_conformance.dart on Sembast (in
// memory); checks with a spying backend that an unauthenticated pull reads
// nothing, and, by removing a stored event beneath the library, that a
// delivery within the record whose events the log no longer holds cannot
// be served.
//
// The scenarios' assertions are cited on their own tests in
// test_support/delivery_pull_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart' hide Finder;
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart'
    show Database, newDatabaseFactoryMemory;

import '../test_support/delivery_pull_conformance.dart';
import '../test_support/delivery_receiver_conformance.dart';
import '../test_support/ingest_record_findings_conformance.dart'
    show sealedRecord;
import '../test_support/record_fixtures.dart' show kPeerDatabaseId;
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

/// A [SembastBackend] counting the transactions it runs.
class _SpyingBackend extends SembastBackend {
  _SpyingBackend({required super.database});

  int transactions = 0;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    transactions += 1;
    return super.transaction(body);
  }
}

class _SembastDatabase implements VersionTestDatabase {
  _SembastDatabase(this.db, {this.spying = false});

  final Database db;
  final bool spying;
  final List<_SpyingBackend> spies = <_SpyingBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    if (!spying) return SembastBackend(database: db);
    final spy = _SpyingBackend(database: db);
    spies.add(spy);
    return spy;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => db.close();
}

var _counter = 0;

Future<Database> _memoryDatabase() {
  _counter += 1;
  return newDatabaseFactoryMemory().openDatabase('delivery-pull-$_counter.db');
}

void main() {
  runDeliveryPullScenarios(
    openDatabase: () async => _SembastDatabase(await _memoryDatabase()),
    backendLabel: 'sembast',
  );

  // Verifies: EVS-DEV-delivery-receiver/N
  test('an unauthenticated pull is refused before any transaction', () async {
    final db = _SembastDatabase(await _memoryDatabase(), spying: true);
    // Every transaction the spy counts from here on is the test's own.
    final store = await openReceiverStoreWithCatchUpParked(db);
    addTearDown(() async {
      await store.close();
      await db.close();
    });
    final spy = db.spies.single;
    final range = DeliveryRangePull(
      channel: deliveryChannel(),
      fromDeliveryNumber: 1,
      toDeliveryNumber: 1,
    );
    // The spy observes the pull's reads.
    final before = spy.transactions;
    await store.receiverEndpoint.pull(
      range,
      senderDatabaseIds: <String>{kPeerDatabaseId},
    );
    expect(spy.transactions, greaterThan(before), reason: 'the spy sees reads');
    final transactions = spy.transactions;

    for (final request in <PullRequest>[
      range,
      const ChannelListingPull(senderDatabaseId: kPeerDatabaseId),
    ]) {
      await expectLater(
        store.receiverEndpoint.pull(
          request,
          senderDatabaseIds: <String>{'someone-else'},
        ),
        throwsA(isA<DeliveryAuthenticationRefused>()),
      );
    }

    expect(spy.transactions, transactions);
  });

  // Verifies: EVS-DEV-delivery-receiver/P
  test('a delivery within the record whose event the log no longer holds '
      'cannot be served, and is named', () async {
    final db = _SembastDatabase(await _memoryDatabase());
    final store = await openReceiverStore(db);
    addTearDown(() async {
      await store.close();
      await db.close();
    });
    final first = sealedDelivery();
    final lost = sealedRecord();
    final second = sealedDelivery(
      number: 2,
      link: first.deliveryHash,
      records: <Map<String, Object?>>[lost],
    );
    for (final d in <DeliveryEnvelope>[first, second]) {
      await store.receiverEndpoint.accept(
        d.encode(),
        senderDatabaseIds: <String>{d.channel.senderDatabaseId},
      );
    }
    // Storage changed beneath the library, outside the storage
    // precondition, so the log no longer holds the event.
    await sembast.intMapStoreFactory
        .store('events')
        .delete(
          db.db,
          finder: sembast.Finder(
            filter: sembast.Filter.equals('event_id', lost['event_id']),
          ),
        );

    final range =
        await pullThroughDecoder(
              store,
              DeliveryRangePull(
                channel: first.channel,
                fromDeliveryNumber: 1,
                toDeliveryNumber: 2,
              ),
            )
            as DeliveryRange;

    expect(range.record, recordAfter(second));
    expect(<int>[for (final d in range.deliveries) d.deliveryNumber], <int>[1]);
    expect(range.unservableDeliveryNumber, 2);
  });
}
