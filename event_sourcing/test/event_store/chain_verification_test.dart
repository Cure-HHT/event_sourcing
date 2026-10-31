// Runs the scenarios of chain_verification_conformance.dart on Sembast (in
// memory), and checks on Sembast that the verification's reads run no
// transaction and that a refused range reads nothing.
//
// The shared scenarios' assertions are cited on their own tests in
// test_support/chain_verification_conformance.dart.
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show verifyChainsForTest;
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/chain_verification_conformance.dart';
import '../test_support/declarations.dart';
import '../test_support/manual_timers.dart' show openWithCatchUpParked;

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

/// A Sembast backend whose page reads fail from the [failOnPageRead]th on
/// while [failing] is set.
class _FailingReadBackend extends SembastBackend {
  _FailingReadBackend({required super.database});

  bool failing = false;
  int failOnPageRead = 1;
  int _pageReads = 0;

  void failFromPageRead(int n) {
    failing = true;
    failOnPageRead = n;
    _pageReads = 0;
  }

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) {
    if (failing) {
      _pageReads += 1;
      if (_pageReads >= failOnPageRead) {
        throw StateError('connection dropped');
      }
    }
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
      // transactions to stay out of their window. Open registers the
      // library's own views, so the driver's first pass, which starts
      // after open returns, runs transactions over them. Open the store
      // with that pass waited out and the driver parked: from then on it
      // runs no transaction.
      store = await openWithCatchUpParked(
        () => EventStore.open(
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
                declarations: <EventTypeDeclaration>[
                  EligibleVersion('finalized'),
                ],
              ),
            ),
          source: const Source(
            hopId: 'walk-hop',
            identifier: 'walk-install',
            softwareVersion: 'walk-app@1.0.0',
          ),
        ),
      );
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

  group('an interrupted chain walk on Sembast', () {
    late sembast.Database db;
    late _FailingReadBackend backend;
    late EventStore store;

    setUp(() async {
      db = await _memoryDatabase();
      backend = _FailingReadBackend(database: db);
      store = await openWithCatchUpParked(
        () => EventStore.open(
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
                declarations: <EventTypeDeclaration>[
                  EligibleVersion('finalized'),
                ],
              ),
            ),
          source: const Source(
            hopId: 'walk-hop',
            identifier: 'walk-install',
            softwareVersion: 'walk-app@1.0.0',
          ),
        ),
      );
      for (var i = 0; i < 5; i++) {
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

    /// Rewrites the last provenance entry's previous_ingest_hash of the
    /// event at [sequenceNumber].
    Future<void> breakStorageLink(int sequenceNumber) async {
      final events = sembast.intMapStoreFactory.store('events');
      final raw = await events.record(sequenceNumber).get(db);
      final record = jsonDecode(jsonEncode(raw)) as Map<String, Object?>;
      final provenance =
          (record['metadata']! as Map<String, Object?>)['provenance']! as List;
      (provenance.last as Map<String, Object?>)['previous_ingest_hash'] =
          'rewritten-link';
      await events.record(sequenceNumber).put(db, record);
    }

    Future<ChainWalkInterrupted> interrupted(Future<Object?> walk) async {
      try {
        await walk;
      } on ChainWalkInterrupted catch (e) {
        return e;
      }
      fail('the walk was not interrupted');
    }

    // Verifies: EVS-DEV-chain-verification/U
    test('a read failure part-way throws ChainWalkInterrupted with the last '
        'verified position', () async {
      backend.failFromPageRead(3);

      final e = await interrupted(verifyChainsForTest(store, pageSize: 1));

      expect(e.verifiedThrough, 2);
      expect(e.cause, isA<StateError>());
      expect(e.findings, isEmpty);
    });

    // Verifies: EVS-DEV-chain-verification/U
    test(
      'a read failure before any event is verified reports no position',
      () async {
        backend.failFromPageRead(1);

        final e = await interrupted(verifyChainsForTest(store, pageSize: 1));

        expect(e.verifiedThrough, isNull);
      },
    );

    // Verifies: EVS-DEV-chain-verification/U
    test('the storage reader reports the position too', () async {
      backend.failFromPageRead(1);
      final e = await interrupted(store.reader.verifyChains());

      expect(e.verifiedThrough, isNull);
    });

    // Verifies: EVS-DEV-chain-verification/V
    test('findings reported before the interruption stay recorded', () async {
      await breakStorageLink(1);
      backend.failFromPageRead(3);

      final e = await interrupted(verifyChainsForTest(store, pageSize: 1));
      backend.failing = false;

      expect(
        e.findings.map((f) => f.kind),
        contains(FindingKind.storageLinkBreak),
      );
      final held = await backend.findAllEvents(
        entryType: 'system.security_finding',
      );
      expect(held, hasLength(e.findings.length));
      final again = await store.verifyChains(to: 5);
      expect(again.findings, isNotEmpty);
      expect(
        await backend.findAllEvents(entryType: 'system.security_finding'),
        hasLength(held.length),
        reason: 'the resumed walk records no finding twice',
      );
    });

    // Verifies: EVS-DEV-chain-verification/D
    // Verifies: EVS-DEV-chain-verification/U
    test('resuming from verifiedThrough + 1 verifies the remaining range and '
        'detects a broken link at the seam', () async {
      await breakStorageLink(3);
      backend.failFromPageRead(3);
      final e = await interrupted(verifyChainsForTest(store, pageSize: 1));
      backend.failing = false;
      expect(e.verifiedThrough, 2);
      expect(e.findings, isEmpty);

      final verdict = await store.verifyChains(from: e.verifiedThrough! + 1);

      expect(verdict.from, 3);
      expect(verdict.to, greaterThanOrEqualTo(5));
      final links = verdict.findings.where(
        (f) => f.kind == FindingKind.storageLinkBreak,
      );
      expect(links, hasLength(1));
      expect(links.single.evidence['local_sequence_number'], 3);
    });
  });
}
