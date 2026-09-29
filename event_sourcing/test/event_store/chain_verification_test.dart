// Runs the scenarios of chain_verification_conformance.dart on Sembast (in
// memory), and checks on Sembast that the verification's reads run no
// transaction and that a refused range reads nothing.
//
// The shared scenarios' assertions are cited on their own tests in
// test_support/chain_verification_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show verifyChainsForTest;
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/chain_verification_conformance.dart';
import '../test_support/manual_timers.dart' show neverFiringTimer;

class _SembastChainDatabase implements ChainTestDatabase {
  _SembastChainDatabase(this._db);

  final sembast.Database _db;

  static final _events = sembast.intMapStoreFactory.store('events');

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> rewriteEvent(
    int sequenceNumber,
    Map<String, Object?> record,
  ) async {
    await _events.record(sequenceNumber).put(_db, record);
  }

  @override
  Future<void> deleteEvent(int sequenceNumber) async {
    await _events.record(sequenceNumber).delete(_db);
  }

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

/// A Sembast backend that counts its transactions and its non-blocking
/// reads.
class _CountingBackend extends SembastBackend {
  _CountingBackend({required super.database});

  int transactions = 0;
  int nonBlockingReads = 0;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    transactions += 1;
    return super.transaction(body);
  }

  @override
  // ignore: invalid_use_of_internal_member
  Future<T> nonBlockingRead<T>(Future<T> Function(Transaction reads) body) {
    nonBlockingReads += 1;
    return super.nonBlockingRead(body);
  }
}

var _counter = 0;

Future<sembast.Database> _memoryDatabase() {
  _counter += 1;
  return newDatabaseFactoryMemory().openDatabase('chain-walk-$_counter.db');
}

void main() {
  runChainVerificationScenarios(
    openDatabase: () async => _SembastChainDatabase(await _memoryDatabase()),
    backendLabel: 'sembast',
  );

  group('chain verification reads on Sembast', () {
    late _CountingBackend backend;
    late EventStore store;

    setUp(() async {
      backend = _CountingBackend(database: await _memoryDatabase());
      // The counting assertions below need the catch-up driver's own
      // background transactions to never land inside their window: open
      // (and so `start()`'s persistent loop) inside a zone whose
      // timerFactory never fires, so after the driver's one, harmless
      // initial discovery pass (this store registers no views) its idle
      // wait never completes and it never runs a transaction again.
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(timerFactory: neverFiringTimer),
        () async {
          store = await EventStore.open(
            storage: ApplicationSuppliedStorage(
              backend,
              SembastSecurityContextStore(backend: backend),
            ),
            entryTypes: EntryTypeRegistry()
              ..register(
                const EntryTypeDefinition(
                  id: 'walked_note',
                  registeredVersion: EntryTypeVersion(1, 0),
                  name: 'walked_note',
                ),
              ),
            source: const Source(
              hopId: 'walk-hop',
              identifier: 'walk-install',
              softwareVersion: 'walk-app@1.0.0',
            ),
          );
        },
      );
      // Let the driver's one, harmless initial discovery pass finish
      // before the test's own transactions run: with the never-firing
      // timer factory above, it then never runs another transaction, so
      // the transaction count below never races it.
      await pumpEventQueue(times: 50);
      for (var i = 0; i < 3; i++) {
        await store.append(
          entryType: 'walked_note',
          aggregateId: 'agg-$i',
          aggregateType: 'note',
          eventType: 'finalized',
          data: <String, Object?>{'n': i},
          initiator: const AutomationInitiator(service: 'walk'),
        );
      }
    });

    tearDown(() async {
      await store.close();
      await backend.close();
    });

    // Verifies: EVS-DEV-chain-verification/S
    // Verifies: EVS-PRD-hash-chain-integrity/K
    test('the reads run no transaction', () async {
      final atStart = backend.transactions;
      final duringRead = <int>[];

      final verdict = await verifyChainsForTest(
        store,
        pageSize: 1,
        afterPage: () async => duringRead.add(backend.transactions - atStart),
      );

      expect(verdict.isValid, isTrue);
      expect(duringRead, isNotEmpty);
      expect(duringRead, everyElement(0));
      expect(backend.transactions, atStart, reason: 'nothing to record');
      expect(backend.nonBlockingReads, 1);
    });

    // Verifies: EVS-DEV-chain-verification/S
    test('the non-blocking read handle refuses a write', () async {
      await expectLater(
        backend.nonBlockingRead(
          (reads) => backend.writeSchemaVersion(reads, 99),
        ),
        throwsStateError,
      );
    });

    // Verifies: EVS-DEV-chain-verification/J
    test('a refused range reads nothing', () async {
      for (final range in <(int?, int?)>[(-1, null), (null, -1), (3, 2)]) {
        await expectLater(
          store.verifyChains(from: range.$1, to: range.$2),
          throwsArgumentError,
        );
        await expectLater(
          store.reader.verifyChains(from: range.$1, to: range.$2),
          throwsArgumentError,
        );
      }
      expect(backend.nonBlockingReads, 0);
    });
  });
}
