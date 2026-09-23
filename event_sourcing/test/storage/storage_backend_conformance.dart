// Backend-agnostic conformance harness for the abstract StorageBackend
// contract. Concrete implementations call this from their own test
// entrypoints; the harness is the canonical place where the contract is
// exercised, so a second concrete backend is proven interchangeable by
// passing exactly this suite.
//
// Traceability lives on the individual tests below, not on this header:
// elspais binds a `Verifies:` comment to the `test(...)` immediately
// beneath it, so a file- or group-level citation credits nothing.
//
// The extended-filter assertion covering the single shared
// _composeFindAllEventsFilter helper is a SembastBackend structural
// property this backend-agnostic harness cannot observe; it is covered by
// find_all_events_shared_filter_test.dart.
//
// This file MUST NOT register any `main()` of its own — it exposes one
// public function, [runStorageBackendConformanceTests], which concrete
// backends call from their own test entrypoints.
//
// Decomposition strategy: a single outer `group` per call, with several
// `_register…Tests(backendOf)` helpers building the inner subgroups. The
// closure (`StorageBackend Function() backendOf`) safely captures the
// late `backend` variable across the nested group/setUp boundary.
//
// `setUp` calls the supplied factory; if it returns null (e.g., the
// Postgres harness with no `PG_TEST_URL`), the test marks itself skipped.
// `tearDown` calls `backend.close()` inside try/catch so a skipped-test
// teardown does not raise.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/event_hash.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/fifo_entry_helpers.dart';

/// Run the backend-agnostic `StorageBackend` conformance suite against
/// the implementation produced by [factory].
///
/// [factory] is called in each test's `setUp` to produce a fresh, empty
/// backend. Returning `null` from [factory] marks every test in the suite
/// as skipped (this is how the Postgres harness gates on `PG_TEST_URL`
/// absence). The factory may also be invoked from inside an individual
/// test body — the foreign-Transaction test in [_registerTransactionTests]
/// constructs a second backend instance to obtain a Transaction from a
/// different backend.
///
/// [backendLabel] is the human-readable name folded into the outer group
/// title (e.g. `'sembast (memory)'`, `'postgres'`).
///
/// [securityStoreOf] returns the security-context store that stores beside
/// the events of the backend it is given; the suite uses it to write the
/// context `queryAudit` joins.
void runStorageBackendConformanceTests(
  Future<StorageBackend?> Function() factory, {
  required String backendLabel,
  required MutableSecurityContextStore Function(StorageBackend backend)
  securityStoreOf,
}) {
  group('StorageBackend conformance ($backendLabel)', () {
    late StorageBackend backend;
    var initialized = false;

    setUp(() async {
      final candidate = await factory();
      if (candidate == null) {
        initialized = false;
        markTestSkipped('no backend available for $backendLabel');
        return;
      }
      backend = candidate;
      initialized = true;
    });

    tearDown(() async {
      if (!initialized) return;
      try {
        await backend.close();
      } catch (_) {
        // close() on a backend whose underlying connection is already
        // broken (e.g., test-induced) must not bubble out of teardown.
      }
    });

    _registerTransactionTests(() => backend, () => initialized, factory);
    _registerEventLogTests(() => backend, () => initialized);
    _registerFindAllEventsFilterTests(() => backend, () => initialized);
    _registerOriginatorFilterTests(() => backend, () => initialized);
    _registerViewRowTests(() => backend, () => initialized);
    _registerViewTargetVersionTests(() => backend, () => initialized);
    _registerFifoTests(() => backend, () => initialized);
    _registerListFifoEntriesTests(() => backend, () => initialized);
    _registerFillCursorTests(() => backend, () => initialized);
    _registerQueueRecordTests(() => backend, () => initialized);
    _registerBackendStateTests(() => backend, () => initialized);
    _registerEventByIdTests(() => backend, () => initialized);
    _registerEventVersionColumnTests(
      () => backend,
      () => initialized,
      securityStoreOf,
    );
    _registerCloseTests(() => backend, () => initialized);
  });
}

// -------- Fixtures shared across subgroups --------

StoredEvent _event(
  String eventId,
  int sequenceNumber, {
  String aggregateId = 'agg-1',
}) {
  return StoredEvent(
    key: 0,
    eventId: eventId,
    aggregateId: aggregateId,
    aggregateType: 'note',
    entryType: 'epistaxis_event',
    entryTypeVersion: const EntryTypeVersion(1, 0),
    libFormatVersion: const DataFormatVersion(2, 0),
    eventType: 'Event',
    sequenceNumber: sequenceNumber,
    data: const <String, dynamic>{},
    metadata: const <String, dynamic>{},
    initiator: const UserInitiator('u'),
    clientTimestamp: DateTime.utc(2026, 4, 22),
    eventHash: 'hash-$eventId',
  );
}

StoredEvent _eventWithProvenance({
  required int seq,
  required String entryType,
  required DateTime clientTimestamp,
  String aggregateId = 'agg-1',
  String hopId = 'mobile-device',
  String identifier = 'install-A',
  String eventId = '',
  String eventType = 'finalized',
}) => StoredEvent(
  key: 0,
  eventId: eventId.isEmpty ? 'e$seq' : eventId,
  aggregateId: aggregateId,
  aggregateType: 'note',
  entryType: entryType,
  entryTypeVersion: const EntryTypeVersion(1, 0),
  libFormatVersion: const DataFormatVersion(2, 0),
  eventType: eventType,
  sequenceNumber: seq,
  data: const <String, Object?>{},
  metadata: <String, Object?>{
    'change_reason': 'initial',
    'provenance': <Map<String, Object?>>[
      <String, Object?>{
        'hop': hopId,
        'received_at': '2026-04-26T00:00:00.000Z',
        'identifier': identifier,
        'software_version': 'app@1.0.0',
      },
    ],
  },
  initiator: const UserInitiator('u1'),
  clientTimestamp: clientTimestamp,
  eventHash: 'h$seq',
);

// Append [build] to the log, reserving its sequence number via
// nextSequenceNumber so [build]'s int -> StoredEvent body can stamp it.
// Returns the StoredEvent that was built and appended.
Future<StoredEvent> _appendBuilt(
  StorageBackend backend,
  StoredEvent Function(int seq) build,
) async {
  late final StoredEvent built;
  await backend.transaction((txn) async {
    final s = await backend.nextSequenceNumber(txn);
    built = build(s);
    await backend.appendEvent(txn, built);
  });
  return built;
}

// -------- Transaction subgroup --------
//
// successful body commits all writes
//   atomically; thrown exception rolls back all writes; Transaction handle is
//   invalidated when body returns or throws; a Transaction from one backend
//   instance is rejected by another (defense-in-depth on the type-and-
//   identity check).
void _registerTransactionTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
  Future<StorageBackend?> Function() factory,
) {
  group('transaction', () {
    // Verifies: EVS-PRD-event-log/A
    // Verifies: EVS-PRD-portability/D
    test('successful body commits all writes', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final s = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', s));
      });
      final stored = await backend.findAllEvents();
      expect(stored.map((e) => e.eventId), ['ev-1']);
    });

    // Verifies: EVS-PRD-event-log/A
    test('thrown exception rolls back all writes', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.transaction((txn) async {
          final s = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-rollback', s));
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );
      final stored = await backend.findAllEvents();
      expect(stored, isEmpty);
    });

    test('mid-body throw rolls back earlier writes too', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.transaction((txn) async {
          final s1 = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-a', s1));
          final s2 = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-b', s2));
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );
      expect(await backend.findAllEvents(), isEmpty);
    });

    test('Transaction cannot be used after body returns', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Transaction escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(
        backend.appendEvent(escaped, _event('ev-late', 1)),
        throwsStateError,
      );
    });

    test('Transaction cannot be used after body throws', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Transaction escaped;
      await expectLater(
        backend.transaction((txn) async {
          escaped = txn;
          throw StateError('boom');
        }),
        throwsStateError,
      );
      await expectLater(
        backend.appendEvent(escaped, _event('ev-late', 1)),
        throwsStateError,
      );
    });

    test(
      'sequential transactions: second transaction sees first commit',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        await backend.transaction((txn) async {
          final s = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-1', s));
        });
        await backend.transaction((txn) async {
          final s = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-2', s));
        });
        final stored = await backend.findAllEvents();
        expect(stored.map((e) => e.eventId), ['ev-1', 'ev-2']);
      },
    );

    // Defense-in-depth: a Transaction handed out by a *different* backend
    // instance must be rejected when re-used against this one. The
    // type-and-identity check guards against accidentally feeding one
    // backend's transaction into another's state.
    test(
      'foreign Transaction (from a different backend) is rejected',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        final other = await factory();
        if (other == null) {
          markTestSkipped('factory returned null on second invocation');
          return;
        }
        late Transaction foreignTxn;
        await other.transaction((txn) async {
          foreignTxn = txn;
        });
        await other.close();

        // The foreign Transaction is already invalidated by its own backend's
        // end-of-body invalidation, so the validity check fires first.
        // Even if it were still valid, the type-and-identity check would
        // catch it.
        await expectLater(
          backend.appendEvent(foreignTxn, _event('ev-foreign', 1)),
          throwsStateError,
        );
      },
    );
  });
}

// -------- Event log subgroup --------
//
// append-only writes; sequence
//   counter monotonicity (reserve-and-increment); per-aggregate order;
//   in-order reads from any starting position; findAllEventsInTxn coherent
//   with same-txn writes; readLatestEventHash transactional read.
void _registerEventLogTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('event log', () {
    // Verifies: EVS-PRD-event-log/A
    test('two appendEvents in one transaction both land', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final seq1 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', seq1));
        final seq2 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-2', seq2));
      });

      final stored = await backend.findAllEvents();
      expect(stored.map((e) => e.eventId), ['ev-1', 'ev-2']);
    });

    // Verifies: EVS-PRD-event-log/A
    test('thrown body rolls back both writes', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.transaction((txn) async {
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-rollback', seq));
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );

      // Event did not land.
      expect(await backend.findAllEvents(), isEmpty);
      // Sequence counter was not advanced.
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 1);
      });
    });

    // Verifies: EVS-PRD-event-log/B
    test('appendEvent advances sequence counter', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', seq));
      });

      // A second transaction sees the advanced counter.
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 2);
      });
    });

    // Verifies: EVS-PRD-event-log/B
    test('nextSequenceNumber is monotonic across transactions', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final seen = <int>[];
      for (var i = 0; i < 5; i++) {
        await backend.transaction((txn) async {
          final seq = await backend.nextSequenceNumber(txn);
          seen.add(seq);
          await backend.appendEvent(txn, _event('ev-$i', seq));
        });
      }
      expect(seen, [1, 2, 3, 4, 5]);
    });

    // Verifies: EVS-PRD-event-log/C
    test(
      'findEventsForAggregate returns events sorted by sequence_number',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        await backend.transaction((txn) async {
          final s1 = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('a1', s1, aggregateId: 'A'));
        });
        await backend.transaction((txn) async {
          final s2 = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('b1', s2, aggregateId: 'B'));
        });
        await backend.transaction((txn) async {
          final s3 = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('a2', s3, aggregateId: 'A'));
        });

        final aEvents = await backend.findEventsForAggregate('A');
        expect(aEvents.map((e) => e.eventId), ['a1', 'a2']);
        final bEvents = await backend.findEventsForAggregate('B');
        expect(bEvents.map((e) => e.eventId), ['b1']);
      },
    );

    // Verifies: EVS-PRD-event-log/D
    test(
      'findAllEvents(afterSequence, limit) slices correctly and keeps order',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        for (var i = 0; i < 5; i++) {
          await backend.transaction((txn) async {
            final s = await backend.nextSequenceNumber(txn);
            await backend.appendEvent(txn, _event('ev-$i', s));
          });
        }

        final all = await backend.findAllEvents();
        expect(all.map((e) => e.sequenceNumber), [1, 2, 3, 4, 5]);

        final afterTwo = await backend.findAllEvents(afterSequence: 2);
        expect(afterTwo.map((e) => e.sequenceNumber), [3, 4, 5]);

        final limited = await backend.findAllEvents(limit: 2);
        expect(limited.map((e) => e.sequenceNumber), [1, 2]);

        final both = await backend.findAllEvents(afterSequence: 2, limit: 2);
        expect(both.map((e) => e.sequenceNumber), [3, 4]);
      },
    );

    // counter equals total appends after multi-append txn (PRD-event-log/B —
    // reserve-and-increment).
    test('counter equals total appends after multi-append txn', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final s1 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', s1));
        final s2 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-2', s2));
      });
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 3);
      });
    });

    // Counter is advanced by nextSequenceNumber, so appendEvent rejects
    // a mismatched sequence number rather than silently advancing the
    // counter implicitly.
    test('appendEvent throws when sequenceNumber does not match the reserved '
        'counter value (Prereq B, Option 1)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.transaction((txn) async {
          // Skip nextSequenceNumber; pass a wrong value.
          await backend.appendEvent(txn, _event('ev-bad', 42));
        }),
        throwsStateError,
      );
      // Nothing landed.
      expect(await backend.findAllEvents(), isEmpty);
    });

    // Verifies: EVS-PRD-event-log/B
    test('two nextSequenceNumber calls in one txn return '
        'current+1 and current+2 (reserve-and-increment)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 1);
        expect(await backend.nextSequenceNumber(txn), 2);
      });
      // The transaction committed, so the counter is at 2.
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 3);
      });
    });

    test('appendEvent consumes the reservation without '
        're-advancing the counter (Prereq B, Option 1)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        expect(seq, 1);
        await backend.appendEvent(txn, _event('ev-1', seq));
      });
      // Counter is at 1 after the append. If appendEvent had re-advanced,
      // the next nextSequenceNumber would return 3.
      await backend.transaction((txn) async {
        expect(await backend.nextSequenceNumber(txn), 2);
      });
    });

    // readLatestEventHash is transactional — value reflects writes staged
    // in the same transaction body so a caller can build the next event's
    // previous_event_hash atomically with the append that uses it.
    test('readLatestEventHash returns null on an empty log', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        expect(await backend.readLatestEventHash(txn), isNull);
      });
    });

    test('readLatestEventHash returns hash of highest-seq event', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final s1 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', s1));
      });
      await backend.transaction((txn) async {
        final s2 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-2', s2));
      });
      await backend.transaction((txn) async {
        expect(await backend.readLatestEventHash(txn), 'hash-ev-2');
      });
    });

    test('readLatestEventHash sees writes staged in the same txn', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        expect(await backend.readLatestEventHash(txn), isNull);
        final s = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-in-tx', s));
        // Sees the just-appended event's hash without leaving the txn.
        expect(await backend.readLatestEventHash(txn), 'hash-ev-in-tx');
      });
    });

    test('readLatestEventHash rejects use outside its transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Transaction escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(backend.readLatestEventHash(escaped), throwsStateError);
    });

    // Verifies: EVS-PRD-event-log/D
    test('findAllEventsInTxn returns events ordered by sequence_number '
        'including txn-staged ones', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final s1 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', s1));
      });

      await backend.transaction((txn) async {
        final s2 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-2', s2));
        final all = await backend.findAllEventsInTxn(txn);
        expect(all.map((e) => e.eventId), ['ev-1', 'ev-2']);
        expect(all.map((e) => e.sequenceNumber), [1, 2]);
      });
    });

    test('findAllEventsInTxn returns empty list when log is empty', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        expect(await backend.findAllEventsInTxn(txn), isEmpty);
      });
    });

    test('findAllEventsInTxn rejects use outside its transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Transaction escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(backend.findAllEventsInTxn(escaped), throwsStateError);
    });

    // Verifies: EVS-PRD-event-log/D
    test('findAllEventsInTxn paginates via afterSequence and limit — the full '
        'log can be walked without ever holding more than `limit` events at '
        'once', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      for (var i = 1; i <= 7; i++) {
        await backend.transaction((txn) async {
          final s = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(txn, _event('ev-$i', s));
        });
      }

      await backend.transaction((txn) async {
        final chunk1 = await backend.findAllEventsInTxn(txn, limit: 3);
        expect(chunk1.map((e) => e.sequenceNumber), [1, 2, 3]);

        final chunk2 = await backend.findAllEventsInTxn(
          txn,
          afterSequence: chunk1.last.sequenceNumber,
          limit: 3,
        );
        expect(chunk2.map((e) => e.sequenceNumber), [4, 5, 6]);

        final chunk3 = await backend.findAllEventsInTxn(
          txn,
          afterSequence: chunk2.last.sequenceNumber,
          limit: 3,
        );
        // Partial trailing chunk — fewer than `limit`, signals exhaustion.
        expect(chunk3.map((e) => e.sequenceNumber), [7]);

        final chunk4 = await backend.findAllEventsInTxn(
          txn,
          afterSequence: chunk3.last.sequenceNumber,
          limit: 3,
        );
        expect(chunk4, isEmpty);
      });
    });
  });
}

// -------- findAllEvents extended filters --------
//
// entryType,
//   clientTimestampStart, clientTimestampEnd on findAllEvents and
//   findAllEventsInTxn; filters AND-compose; existing afterSequence +
//   limit unaffected.
void _registerFindAllEventsFilterTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('findAllEvents extended filters', () {
    // Verifies: EVS-DEV-find-all-events-extended-filters/A
    test('entryType filter returns only matching events', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'lights',
          clientTimestamp: DateTime.utc(2026, 1, 2),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 3),
        ),
      );

      final notes = await backend.findAllEvents(entryType: 'note');
      expect(notes.map((e) => e.sequenceNumber).toList(), <int>[1, 3]);

      final lights = await backend.findAllEvents(entryType: 'lights');
      expect(lights.map((e) => e.sequenceNumber).toList(), <int>[2]);
    });

    // Verifies: EVS-DEV-find-all-events-extended-filters/A
    test('clientTimestampStart filter is inclusive-lower-bound', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 5),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 10),
        ),
      );

      final later = await backend.findAllEvents(
        clientTimestampStart: DateTime.utc(2026, 1, 5),
      );
      expect(later.map((e) => e.sequenceNumber).toList(), <int>[2, 3]);
    });

    // Verifies: EVS-DEV-find-all-events-extended-filters/A
    test('clientTimestampEnd filter is inclusive-upper-bound', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 5),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 10),
        ),
      );

      final earlier = await backend.findAllEvents(
        clientTimestampEnd: DateTime.utc(2026, 1, 5),
      );
      expect(earlier.map((e) => e.sequenceNumber).toList(), <int>[1, 2]);
    });

    // Verifies: EVS-DEV-find-all-events-extended-filters/C
    test('AND-composes entryType with timestamp range', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'lights',
          clientTimestamp: DateTime.utc(2026, 1, 3),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 5),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 10),
        ),
      );

      final filtered = await backend.findAllEvents(
        entryType: 'note',
        clientTimestampStart: DateTime.utc(2026, 1, 3),
        clientTimestampEnd: DateTime.utc(2026, 1, 7),
      );
      expect(filtered.map((e) => e.sequenceNumber).toList(), <int>[3]);
    });

    // Verifies: EVS-DEV-find-all-events-extended-filters/C
    test('existing afterSequence + limit filters still work alongside the new '
        'ones', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 2),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'lights',
          clientTimestamp: DateTime.utc(2026, 1, 3),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 4),
        ),
      );

      final all = await backend.findAllEvents();
      expect(all.map((e) => e.sequenceNumber).toList(), <int>[1, 2, 3, 4]);

      // afterSequence + entryType compose.
      final afterAndType = await backend.findAllEvents(
        afterSequence: 1,
        entryType: 'note',
      );
      expect(afterAndType.map((e) => e.sequenceNumber).toList(), <int>[2, 4]);

      // limit + entryType compose.
      final limited = await backend.findAllEvents(entryType: 'note', limit: 2);
      expect(limited.map((e) => e.sequenceNumber).toList(), <int>[1, 2]);
    });

    test('empty result when no events match', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );

      final none = await backend.findAllEvents(entryType: 'no_such_type');
      expect(none, isEmpty);
    });

    // Verifies: EVS-DEV-find-all-events-extended-filters/B
    test('findAllEventsInTxn honors the same filters', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 1),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'lights',
          clientTimestamp: DateTime.utc(2026, 1, 5),
        ),
      );
      await _appendBuilt(
        backend,
        (s) => _eventWithProvenance(
          seq: s,
          entryType: 'note',
          clientTimestamp: DateTime.utc(2026, 1, 10),
        ),
      );

      final inTxn = await backend.transaction(
        (txn) => backend.findAllEventsInTxn(
          txn,
          entryType: 'note',
          clientTimestampStart: DateTime.utc(2026, 1, 5),
        ),
      );
      expect(inTxn.map((e) => e.sequenceNumber).toList(), <int>[3]);
    });
  });
}

// -------- Originator filters --------
//
// originatorHopId and
//   originatorIdentifier filters; each filters on provenance[0]; AND'd when
//   both supplied.
void _registerOriginatorFilterTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('findAllEvents originator filters', () {
    Future<void> seedThreeOrigins(StorageBackend backend) async {
      await backend.transaction((txn) async {
        final s1 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          _eventWithProvenance(
            seq: s1,
            entryType: 'epistaxis_event',
            clientTimestamp: DateTime.utc(2026, 4, 26),
            aggregateId: 'agg-ev-mobileA',
            hopId: 'mobile-device',
            identifier: 'install-A',
            eventId: 'ev-mobileA',
          ),
        );
        final s2 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          _eventWithProvenance(
            seq: s2,
            entryType: 'epistaxis_event',
            clientTimestamp: DateTime.utc(2026, 4, 26),
            aggregateId: 'agg-ev-mobileB',
            hopId: 'mobile-device',
            identifier: 'install-B',
            eventId: 'ev-mobileB',
          ),
        );
        final s3 = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          _eventWithProvenance(
            seq: s3,
            entryType: 'epistaxis_event',
            clientTimestamp: DateTime.utc(2026, 4, 26),
            aggregateId: 'agg-ev-controlP',
            hopId: 'control-server',
            identifier: 'install-P',
            eventId: 'ev-controlP',
          ),
        );
      });
    }

    // Verifies: EVS-DEV-find-all-events-extended-filters/C
    test('originatorIdentifier alone — install-A returns 1 event', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await seedThreeOrigins(backend);
      final result = await backend.findAllEvents(
        originatorIdentifier: 'install-A',
      );
      expect(result.map((e) => e.eventId), <String>['ev-mobileA']);
    });

    test('originatorHopId alone — mobile-device returns 2 events', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await seedThreeOrigins(backend);
      final result = await backend.findAllEvents(
        originatorHopId: 'mobile-device',
      );
      expect(result.map((e) => e.eventId), <String>[
        'ev-mobileA',
        'ev-mobileB',
      ]);
    });

    test('both filters AND — mobile-device + install-A returns 1', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await seedThreeOrigins(backend);
      final result = await backend.findAllEvents(
        originatorHopId: 'mobile-device',
        originatorIdentifier: 'install-A',
      );
      expect(result.map((e) => e.eventId), <String>['ev-mobileA']);
    });
  });
}

// -------- Generic view storage --------
//
// generic view-storage methods are part
//   of the StorageBackend abstraction; round-trip, missing-key-null,
//   delete, find with limit/offset, clearView, viewName isolation.
void _registerViewRowTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('generic view storage', () {
    // Verifies: EVS-PRD-portability/D
    test('readViewRowInTxn on missing key returns null', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'test_view', 'missing'),
      );
      expect(row, isNull);
    });

    test('upsert then read round-trips', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'test_view', 'k1', {
          'a': 1,
          'b': 's',
        });
      });
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'test_view', 'k1'),
      );
      expect(row, {'a': 1, 'b': 's'});
    });

    test('delete removes the row; read-after-delete returns null', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'test_view', 'k', {'x': 1});
        await backend.deleteViewRowInTxn(txn, 'test_view', 'k');
      });
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'test_view', 'k'),
      );
      expect(row, isNull);
    });

    test('findViewRows iterates with limit and offset', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        for (var i = 0; i < 5; i++) {
          await backend.upsertViewRowInTxn(txn, 'v', 'k$i', {'i': i});
        }
      });
      final all = await backend.findViewRows('v');
      expect(all, hasLength(5));
      final two = await backend.findViewRows('v', limit: 2);
      expect(two, hasLength(2));
      final skip2 = await backend.findViewRows('v', limit: 100, offset: 2);
      expect(skip2, hasLength(3));
    });

    // Verifies: EVS-PRD-subscription/A — the batched key-set read returns
    //   exactly the present keys (as a key->row map), omitting absent keys, in
    //   a single query. Backs the scoped (filtered materialized-state)
    //   AggregateMode snapshot's one-round-trip materialization (replacing the
    //   per-id transaction loop).
    test('readViewRowsByKeys returns present keys as a key->row map, '
        'omitting absent keys', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        for (var i = 0; i < 5; i++) {
          await backend.upsertViewRowInTxn(txn, 'v_bykeys', 'k$i', {'i': i});
        }
      });
      final got = await backend.readViewRowsByKeys('v_bykeys', {
        'k1',
        'k3',
        'absent',
      });
      expect(got.keys.toSet(), {'k1', 'k3'});
      expect(got['k1'], {'i': 1});
      expect(got['k3'], {'i': 3});
      expect(got.containsKey('absent'), isFalse);
    });

    test('readViewRowsByKeys with an empty key set returns an empty map '
        '(no query needed)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'v_bykeys_empty', 'k0', {'i': 0});
      });
      final got = await backend.readViewRowsByKeys(
        'v_bykeys_empty',
        <String>{},
      );
      expect(got, isEmpty);
    });

    // Verifies: EVS-PRD-permissions-as-events — multi-row in-txn read is
    //   the substrate primitive that lets the scoped-permissions authorize
    //   stage enumerate user_role_scopes inside the dispatch transaction
    //   (so the authorize-stage read and the execute-stage append share
    //   one read-consistent snapshot).
    // Verifies: EVS-PRD-action-dispatch — same dispatch-transaction
    //   coherence requirement on the authorize side.
    test('findViewRowsInTxn returns rows matching column equality filter '
        'inside a transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'demo_findviewrows', 'k1', {
          'user_id': 'U1',
          'role': 'SC',
        });
        await backend.upsertViewRowInTxn(txn, 'demo_findviewrows', 'k2', {
          'user_id': 'U1',
          'role': 'SUP',
        });
        await backend.upsertViewRowInTxn(txn, 'demo_findviewrows', 'k3', {
          'user_id': 'U2',
          'role': 'SC',
        });
      });
      final rows = await backend.transaction(
        (txn) => backend.findViewRowsInTxn(
          txn,
          'demo_findviewrows',
          where: {'user_id': 'U1', 'role': 'SC'},
        ),
      );
      expect(rows, hasLength(1));
      expect(rows.single['user_id'], 'U1');
      expect(rows.single['role'], 'SC');
    });

    test('findViewRowsInTxn with null where returns all rows '
        '(limit/offset apply)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        for (var i = 0; i < 5; i++) {
          await backend.upsertViewRowInTxn(txn, 'v_nullwhere', 'k$i', {'i': i});
        }
      });
      final all = await backend.transaction(
        (txn) => backend.findViewRowsInTxn(txn, 'v_nullwhere'),
      );
      expect(all, hasLength(5));
      final two = await backend.transaction(
        (txn) => backend.findViewRowsInTxn(txn, 'v_nullwhere', limit: 2),
      );
      expect(two, hasLength(2));
      final skip2 = await backend.transaction(
        (txn) => backend.findViewRowsInTxn(
          txn,
          'v_nullwhere',
          limit: 100,
          offset: 2,
        ),
      );
      expect(skip2, hasLength(3));
    });

    test('findViewRowsInTxn with empty where returns all rows '
        '(no filtering)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'v_emptywhere', 'k1', {'a': 1});
        await backend.upsertViewRowInTxn(txn, 'v_emptywhere', 'k2', {'a': 2});
      });
      final rows = await backend.transaction(
        (txn) => backend.findViewRowsInTxn(
          txn,
          'v_emptywhere',
          where: const <String, Object?>{},
        ),
      );
      expect(rows, hasLength(2));
    });

    test(
      'findViewRowsInTxn sees uncommitted writes from same transaction',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        final rows = await backend.transaction((txn) async {
          await backend.upsertViewRowInTxn(txn, 'v_coherent', 'k1', {
            'user_id': 'U1',
          });
          await backend.upsertViewRowInTxn(txn, 'v_coherent', 'k2', {
            'user_id': 'U2',
          });
          return backend.findViewRowsInTxn(
            txn,
            'v_coherent',
            where: {'user_id': 'U1'},
          );
        });
        expect(rows, hasLength(1));
        expect(rows.single['user_id'], 'U1');
      },
    );

    test('clearViewInTxn empties one view without touching others', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'a', 'k', {'x': 1});
        await backend.upsertViewRowInTxn(txn, 'b', 'k', {'y': 2});
        await backend.clearViewInTxn(txn, 'a');
      });
      expect(await backend.findViewRows('a'), isEmpty);
      expect(await backend.findViewRows('b'), hasLength(1));
    });

    test('viewName isolation: writing to "a" never affects "b"', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'a', 'k', {'src': 'a'});
        await backend.upsertViewRowInTxn(txn, 'b', 'k', {'src': 'b'});
      });
      final a = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'a', 'k'),
      );
      final bb = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'b', 'k'),
      );
      expect(a, {'src': 'a'});
      expect(bb, {'src': 'b'});
    });
  });
}

// -------- View target versions --------
//
// view-target-version persistence is
//   part of the StorageBackend abstraction (round-trip, null-on-unknown,
//   readAll, clear, cross-view isolation).
void _registerViewTargetVersionTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('view_target_versions storage', () {
    // Verifies: EVS-PRD-portability/D
    // Verifies: EVS-DEV-version-compatibility/A
    test('round-trip read/write keeps major and minor', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'demo_note',
          const EntryTypeVersion(1, 3),
        );
      });
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(
            txn,
            'diary_entries',
            'demo_note',
          ),
          const EntryTypeVersion(1, 3),
        );
      });
    });

    test('returns null for unknown (view, entry_type)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(
            txn,
            'diary_entries',
            'unknown',
          ),
          isNull,
        );
      });
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('readAll returns full map for one view', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'demo_note',
          const EntryTypeVersion(2, 1),
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'epistaxis',
          const EntryTypeVersion(5, 0),
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'other_view',
          'demo_note',
          const EntryTypeVersion(1, 0),
        );
      });
      await backend.transaction((txn) async {
        final map = await backend.readAllViewTargetVersionsInTxn(
          txn,
          'diary_entries',
        );
        expect(map, const <String, EntryTypeVersion>{
          'demo_note': EntryTypeVersion(2, 1),
          'epistaxis': EntryTypeVersion(5, 0),
        });
      });
    });

    test('clear removes only the named view', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'view_a',
          'x',
          const EntryTypeVersion(1, 0),
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'view_b',
          'x',
          const EntryTypeVersion(2, 0),
        );
      });
      await backend.transaction((txn) async {
        await backend.clearViewTargetVersionsInTxn(txn, 'view_a');
      });
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'view_a', 'x'),
          isNull,
        );
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'view_b', 'x'),
          const EntryTypeVersion(2, 0),
        );
      });
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('overwrite, including a lower minor within the major', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'v',
          'e',
          const EntryTypeVersion(1, 1),
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'v',
          'e',
          const EntryTypeVersion(1, 1),
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'v',
          'e',
          const EntryTypeVersion(1, 4),
        );
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'v', 'e'),
          const EntryTypeVersion(1, 4),
        );
      });
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'v',
          'e',
          const EntryTypeVersion(1, 2),
        );
      });
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'v', 'e'),
          const EntryTypeVersion(1, 2),
        );
      });
    });

    // Verifies: EVS-DEV-version-compatibility/E
    test('a write in a transaction that throws is rolled back', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'v',
          'e',
          const EntryTypeVersion(1, 3),
        );
      });
      await expectLater(
        backend.transaction((txn) async {
          await backend.writeViewTargetVersionInTxn(
            txn,
            'v',
            'e',
            const EntryTypeVersion(1, 0),
          );
          throw StateError('injected failure after the write');
        }),
        throwsStateError,
      );
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'v', 'e'),
          const EntryTypeVersion(1, 3),
        );
      });
    });
  });
}

// -------- FIFO subgroup --------
//
// FIFO persistence methods (enqueueFifoTxn,
//   readFifoHead, listFifoEntries, appendAttemptTxn, setFinalStatusTxn,
//   hasFifoWedged/wedgedFifos) are part of the StorageBackend abstraction.
//   setFinalStatusTxn allows exactly pending -> sent, pending -> wedged
//   and wedged -> tombstoned; a repeated status or a missing item throws
//   StateError. appendAttemptTxn throws StateError on a missing or
//   terminal item.
void _registerFifoTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('FIFO', () {
    // -------- enqueueFifoTxn + validation --------

    // Verifies: EVS-PRD-portability/D
    test('enqueueFifoTxn + readFifoHead round-trip', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final enqueued = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final head = await backend.readFifoHead('primary');
      expect(head, isNotNull);
      expect(head!.entryId, enqueued.entryId);
      // entry_id is a v4 UUID, not the event id.
      expect(head.entryId, isNot('e1'));
      expect(head.entryId, matches(RegExp(r'^[0-9a-f-]{36}$')));
      expect(head.eventIds, ['e1']);
      expect(head.sequenceRange, (firstSeq: 1, lastSeq: 1));
      expect(head.finalStatus, isNull);
      expect(head.attempts, isEmpty);
      expect(head.sentAt, isNull);
      expect(head.sequenceInQueue, enqueued.sequenceInQueue);
      // Whole-value parity: the entry read back equals the one enqueueFifoTxn
      // returned, field for field. Sembast persists FifoEntry.toJson and
      // Postgres maps explicit columns, so a field added to FifoEntry is
      // carried automatically by one backend and silently dropped by the
      // other until its column is added. Comparing values rather than
      // hand-listed fields makes that divergence a test failure instead of
      // something a reviewer has to notice.
      expect(head, equals(enqueued));
    });

    test('enqueueFifoTxn rejects an empty batch with ArgumentError', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.transaction(
          (txn) => backend.enqueueFifoTxn(
            txn,
            'primary',
            const [],
            wirePayload: wirePayloadJson(const {'k': 'v'}),
          ),
        ),
        throwsArgumentError,
      );
    });

    test('enqueueFifoTxn assigns distinct UUID entry_ids even when the same '
        'event id is enqueued twice', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final first = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final second = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 2,
      );
      expect(first.entryId, isNot(second.entryId));
      expect(first.eventIds, ['e1']);
      expect(second.eventIds, ['e1']);
    });

    test('two FIFOs produce independent UUID entry_ids even with the same '
        'event id', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a = await enqueueSingle(
        backend,
        'A',
        eventId: 'shared',
        sequenceNumber: 1,
      );
      final b = await enqueueSingle(
        backend,
        'B',
        eventId: 'shared',
        sequenceNumber: 1,
      );
      expect(a.entryId, isNot(b.entryId));
      expect((await backend.readFifoHead('A'))?.entryId, a.entryId);
      expect((await backend.readFifoHead('B'))?.entryId, b.entryId);
    });

    // -------- enqueueFifoTxn — native vs 3rd-party wire-format branch --------

    test('enqueueFifoTxn with nativeEnvelope persists '
        'envelope_metadata and nulls wire_payload', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      final envelope = BatchEnvelopeMetadata(
        batchFormatVersion: '2',
        batchId: 'batch-x',
        senderHop: 'mobile-1',
        senderIdentifier: 'device-uuid',
        senderSoftwareVersion: 'diary@1.2.3',
        sentAt: DateTime.utc(2026, 4, 25, 12),
      );
      await backend.transaction(
        (txn) => backend.enqueueFifoTxn(txn, 'dest', [
          event,
        ], nativeEnvelope: envelope),
      );
      final head = await backend.readFifoHead('dest');
      expect(head, isNotNull);
      expect(
        head!.wirePayload,
        isNull,
        reason: 'native enqueue MUST null wire_payload',
      );
      expect(head.envelopeMetadata, isNotNull);
      expect(head.envelopeMetadata!.batchId, 'batch-x');
      expect(head.envelopeMetadata!.senderHop, 'mobile-1');
      expect(head.envelopeMetadata!.senderIdentifier, 'device-uuid');
      expect(head.envelopeMetadata!.senderSoftwareVersion, 'diary@1.2.3');
      expect(head.envelopeMetadata!.batchFormatVersion, '2');
      expect(head.wireFormat, BatchEnvelope.wireFormat);
      expect(
        head.transformVersion,
        isNull,
        reason: 'native rows carry no transform_version',
      );
    });

    test('enqueueFifoTxn with wirePayload stores '
        'wire_payload, envelope_metadata is null', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      final payload = wirePayloadJson(
        const <String, Object?>{'kind': 'csv-row', 'value': 42},
        contentType: 'application/json',
        transformVersion: 'json-v1',
      );
      await backend.transaction(
        (txn) =>
            backend.enqueueFifoTxn(txn, 'dest', [event], wirePayload: payload),
      );
      final head = await backend.readFifoHead('dest');
      expect(head, isNotNull);
      expect(head!.wirePayload, isNotNull);
      expect(head.wirePayload, <String, Object?>{
        'kind': 'csv-row',
        'value': 42,
      });
      expect(
        head.envelopeMetadata,
        isNull,
        reason: '3rd-party rows MUST NOT carry envelope_metadata',
      );
      expect(head.wireFormat, 'application/json');
    });

    test('enqueueFifoTxn rejects supplying both wirePayload '
        'and nativeEnvelope', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await expectLater(
        backend.transaction(
          (txn) => backend.enqueueFifoTxn(
            txn,
            'dest',
            [event],
            wirePayload: wirePayloadJson(const {'k': 'v'}),
            nativeEnvelope: BatchEnvelopeMetadata(
              batchFormatVersion: '2',
              batchId: 'batch-x',
              senderHop: 'mobile-1',
              senderIdentifier: 'device-uuid',
              senderSoftwareVersion: 'diary@1.2.3',
              sentAt: DateTime.utc(2026, 4, 25, 12),
            ),
          ),
        ),
        throwsArgumentError,
      );
    });

    test('enqueueFifoTxn rejects supplying neither wirePayload '
        'nor nativeEnvelope', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await expectLater(
        backend.transaction(
          (txn) => backend.enqueueFifoTxn(txn, 'dest', [event]),
        ),
        throwsArgumentError,
      );
    });

    // -------- FIFO ordering --------

    test('multiple enqueues preserve insertion order', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final first = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'primary', eventId: 'e2', sequenceNumber: 2);
      await enqueueSingle(backend, 'primary', eventId: 'e3', sequenceNumber: 3);

      final head = await backend.readFifoHead('primary');
      expect(head?.entryId, first.entryId);
      expect(head?.eventIds, ['e1']);
    });

    test('per-destination isolation', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a = await enqueueSingle(
        backend,
        'A',
        eventId: 'a-only',
        sequenceNumber: 1,
      );
      final b = await enqueueSingle(
        backend,
        'B',
        eventId: 'b-only',
        sequenceNumber: 1,
      );

      expect((await backend.readFifoHead('A'))?.entryId, a.entryId);
      expect((await backend.readFifoHead('A'))?.eventIds, ['a-only']);
      expect((await backend.readFifoHead('B'))?.entryId, b.entryId);
      expect((await backend.readFifoHead('B'))?.eventIds, ['b-only']);
    });

    // -------- appendAttemptTxn --------

    // Verifies: EVS-DEV-destination-drain/D
    // an attempt is appended to a pending
    //   item without changing its status.
    test('appendAttemptTxn appends without changing final_status', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );

      final attempt = AttemptResult(
        attemptedAt: DateTime.utc(2026, 4, 22, 11),
        outcome: 'transient',
        errorMessage: 'timeout',
        httpStatus: 503,
      );
      await appendAttemptForTest(backend, 'primary', e1.entryId, attempt);

      final head = await backend.readFifoHead('primary');
      expect(head?.attempts, [attempt]);
      expect(head?.finalStatus, isNull);

      // Second attempt also appends, preserving order.
      final attempt2 = AttemptResult(
        attemptedAt: DateTime.utc(2026, 4, 22, 12),
        outcome: 'transient',
        errorMessage: 'timeout',
        httpStatus: 503,
      );
      await appendAttemptForTest(backend, 'primary', e1.entryId, attempt2);
      final head2 = await backend.readFifoHead('primary');
      expect(head2?.attempts, [attempt, attempt2]);
    });

    // Verifies: EVS-DEV-destination-drain/D
    // recording an attempt on a missing
    //   item is an error, and nothing changes.
    test('appendAttemptTxn throws StateError on a missing item', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await expectLater(
        appendAttemptForTest(
          backend,
          'primary',
          'nonexistent',
          AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
        ),
        throwsStateError,
      );
      final head = await backend.readFifoHead('primary');
      expect(head?.entryId, e1.entryId);
      expect(head?.attempts, isEmpty);
    });

    // Verifies: EVS-DEV-destination-drain/D
    // recording an attempt on a destination
    //   with no queue is an error, and nothing is created.
    test('appendAttemptTxn throws StateError on a missing queue', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        appendAttemptForTest(
          backend,
          'ghost-dest',
          'any-entry',
          AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
        ),
        throwsStateError,
      );
      expect(await backend.readFifoHead('ghost-dest'), isNull);
      expect(await backend.listFifoEntries('ghost-dest'), isEmpty);
    });

    // Verifies: EVS-DEV-destination-drain/D
    // recording an attempt on a terminal
    //   item is an error, and the item is unchanged.
    test('appendAttemptTxn throws StateError on a terminal item', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final sent = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final wedged = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      await seedSentRowForTest(backend, 'primary', sent.entryId);
      await setStatusForTest(
        backend,
        'primary',
        wedged.entryId,
        FinalStatus.wedged,
      );
      for (final entry in <FifoEntry>[sent, wedged]) {
        final before = await backend.readFifoRow('primary', entry.entryId);
        await expectLater(
          appendAttemptForTest(
            backend,
            'primary',
            entry.entryId,
            AttemptResult(
              attemptedAt: DateTime.utc(2026, 4, 22),
              outcome: 'ok',
            ),
          ),
          throwsStateError,
        );
        final after = await backend.readFifoRow('primary', entry.entryId);
        expect(after!.toJson(), before!.toJson());
      }
    });

    // Verifies: EVS-DEV-destination-drain/C
    // an attempt written in a transaction
    //   that rolls back is not recorded.
    test('appendAttemptTxn rolls back with its transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await expectLater(
        backend.transaction((txn) async {
          await backend.appendAttemptTxn(
            txn,
            'primary',
            e1.entryId,
            AttemptResult(
              attemptedAt: DateTime.utc(2026, 4, 22),
              outcome: 'ok',
            ),
          );
          await backend.setFinalStatusTxn(
            txn,
            'primary',
            e1.entryId,
            FinalStatus.sent,
          );
          throw StateError('simulated failure after the outcome writes');
        }),
        throwsStateError,
      );
      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row!.attempts, isEmpty);
      expect(row.finalStatus, isNull);
      expect(row.sentAt, isNull);
    });

    // -------- setFinalStatusTxn --------

    test('status sent retains the entry', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await seedSentRowForTest(backend, 'primary', e1.entryId);

      // After marking sent, readFifoHead moves past it to the next pending.
      expect(await backend.readFifoHead('primary'), isNull);
      expect(
        (await backend.readFifoRow('primary', e1.entryId))!.finalStatus,
        FinalStatus.sent,
      );

      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final nextHead = await backend.readFifoHead('primary');
      expect(nextHead?.entryId, e2.entryId);
    });

    test('status sent sets sent_at', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final before = DateTime.now().toUtc();
      await seedSentRowForTest(backend, 'primary', e1.entryId);
      final after = DateTime.now().toUtc();

      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row, isNotNull);
      expect(row!.sentAt, isNotNull);
      expect(row.sentAt!.isAfter(before) || row.sentAt == before, isTrue);
      expect(row.sentAt!.isBefore(after) || row.sentAt == after, isTrue);
    });

    test('status wedged does NOT set sent_at', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await setStatusForTest(
        backend,
        'primary',
        e1.entryId,
        FinalStatus.wedged,
      );

      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row, isNotNull);
      expect(row!.sentAt, isNull);
    });

    // Verifies: EVS-DEV-destination-drain/B
    // wedged -> tombstoned keeps the
    //   wedge's attempts and leaves sent_at unset.
    test('wedged -> tombstoned keeps attempts', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final attempt = AttemptResult(
        attemptedAt: DateTime.utc(2026, 4, 22, 9),
        outcome: 'permanent',
        errorMessage: 'refused',
      );
      await appendAttemptForTest(backend, 'primary', e1.entryId, attempt);
      await setStatusForTest(
        backend,
        'primary',
        e1.entryId,
        FinalStatus.wedged,
      );
      await setStatusForTest(
        backend,
        'primary',
        e1.entryId,
        FinalStatus.tombstoned,
      );
      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row!.finalStatus, FinalStatus.tombstoned);
      expect(row.attempts, [attempt]);
      expect(row.sentAt, isNull);
    });

    // Verifies: EVS-DEV-destination-drain/B
    // every transition other than
    //   null -> sent, null -> wedged and wedged -> tombstoned throws, and the
    //   item is unchanged.
    test('every illegal transition throws and leaves the item '
        'unchanged', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      var seq = 0;
      Future<FifoEntry> itemAt(FinalStatus? status) async {
        seq += 1;
        final e = await enqueueSingle(
          backend,
          'primary',
          eventId: 'ill-$seq',
          sequenceNumber: seq,
        );
        switch (status) {
          case null:
            break;
          case FinalStatus.sent:
            await seedSentRowForTest(backend, 'primary', e.entryId);
          case FinalStatus.wedged:
            await setStatusForTest(
              backend,
              'primary',
              e.entryId,
              FinalStatus.wedged,
            );
          case FinalStatus.tombstoned:
            await setStatusForTest(
              backend,
              'primary',
              e.entryId,
              FinalStatus.wedged,
            );
            await setStatusForTest(
              backend,
              'primary',
              e.entryId,
              FinalStatus.tombstoned,
            );
        }
        return e;
      }

      const illegal = <(FinalStatus?, FinalStatus)>[
        (null, FinalStatus.tombstoned),
        (FinalStatus.sent, FinalStatus.sent),
        (FinalStatus.sent, FinalStatus.wedged),
        (FinalStatus.sent, FinalStatus.tombstoned),
        (FinalStatus.wedged, FinalStatus.wedged),
        (FinalStatus.wedged, FinalStatus.sent),
        (FinalStatus.tombstoned, FinalStatus.tombstoned),
        (FinalStatus.tombstoned, FinalStatus.sent),
        (FinalStatus.tombstoned, FinalStatus.wedged),
      ];
      for (final (from, to) in illegal) {
        final e = await itemAt(from);
        final before = await backend.readFifoRow('primary', e.entryId);
        await expectLater(
          setStatusForTest(backend, 'primary', e.entryId, to),
          throwsStateError,
          reason: '$from -> $to',
        );
        final after = await backend.readFifoRow('primary', e.entryId);
        expect(after!.toJson(), before!.toJson(), reason: '$from -> $to');
      }
    });

    // Verifies: EVS-DEV-destination-drain/B
    // a status change on a missing item
    //   throws.
    test('setFinalStatusTxn throws StateError on a missing item', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        setStatusForTest(backend, 'primary', 'ghost', FinalStatus.sent),
        throwsStateError,
      );
      expect(await backend.listFifoEntries('primary'), isEmpty);
    });

    test('after status sent, readFifoHead returns next pending', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );

      await seedSentRowForTest(backend, 'primary', e1.entryId);

      final head = await backend.readFifoHead('primary');
      expect(head?.entryId, e2.entryId);
    });

    test('readFifoHead returns first row with finalStatus in '
        '{null, wedged} — wedged row is returned, not skipped', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      await enqueueSingle(backend, 'primary', eventId: 'e3', sequenceNumber: 3);

      await seedSentRowForTest(backend, 'primary', e1.entryId);
      await setStatusForTest(
        backend,
        'primary',
        e2.entryId,
        FinalStatus.wedged,
      );
      // e3 is left pending.

      final head = await backend.readFifoHead('primary');
      expect(head, isNotNull);
      // e1 (sent) is skipped; e2 (wedged) is the first row in
      // sequence_in_queue order whose final_status is in {null, wedged}.
      expect(head!.entryId, e2.entryId);
      expect(head.finalStatus, FinalStatus.wedged);
    });

    test('readFifoHead skips tombstoned rows', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      await setStatusForTest(
        backend,
        'primary',
        e1.entryId,
        FinalStatus.wedged,
      );
      await setStatusForTest(
        backend,
        'primary',
        e1.entryId,
        FinalStatus.tombstoned,
      );

      final head = await backend.readFifoHead('primary');
      expect(head, isNotNull);
      expect(head!.entryId, e2.entryId);
      expect(head.finalStatus, isNull);
    });

    test('readFifoHead returns null when only terminal-passable '
        'rows exist (sent and tombstoned)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      await seedSentRowForTest(backend, 'primary', e1.entryId);
      await setStatusForTest(
        backend,
        'primary',
        e2.entryId,
        FinalStatus.wedged,
      );
      await setStatusForTest(
        backend,
        'primary',
        e2.entryId,
        FinalStatus.tombstoned,
      );

      expect(await backend.readFifoHead('primary'), isNull);
    });

    // Verifies: EVS-DEV-destination-drain/D
    // the head read inside a transaction
    //   reflects a status change staged in that transaction.
    test('readFifoHeadTxn sees a status change staged in its '
        'transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final seen = await backend.transaction((txn) async {
        final before = await backend.readFifoHeadTxn(txn, 'primary');
        await backend.setFinalStatusTxn(
          txn,
          'primary',
          e1.entryId,
          FinalStatus.sent,
        );
        final after = await backend.readFifoHeadTxn(txn, 'primary');
        return (before?.entryId, after?.entryId);
      });
      expect(seen, (e1.entryId, e2.entryId));
      expect(
        await backend.transaction(
          (txn) => backend.readFifoHeadTxn(txn, 'unknown'),
        ),
        isNull,
      );
    });

    // -------- trail sweep --------

    // Verifies: EVS-DEV-destination-drain/F
    // the trail sweep deletes only the
    //   pending items behind the given position and reports the lowest event
    //   any of them carried.
    test('deleteNullRowsAfterSequenceInQueueTxn reports count and lowest '
        'first_seq', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final head = await enqueueSingle(
        backend,
        'primary',
        eventId: 'h',
        sequenceNumber: 10,
      );
      await setStatusForTest(
        backend,
        'primary',
        head.entryId,
        FinalStatus.wedged,
      );
      await enqueueSingle(
        backend,
        'primary',
        eventId: 't1',
        sequenceNumber: 12,
      );
      // A later item carrying a lower event (a gap replay's item).
      await enqueueSingle(backend, 'primary', eventId: 't2', sequenceNumber: 4);
      final sweep = await backend.transaction(
        (txn) => backend.deleteNullRowsAfterSequenceInQueueTxn(
          txn,
          'primary',
          head.sequenceInQueue,
        ),
      );
      expect(sweep, const TrailSweepResult(deletedCount: 2, minFirstSeq: 4));
      final rows = await backend.listFifoEntries('primary');
      expect(rows.map((r) => r.entryId), [head.entryId]);

      final empty = await backend.transaction(
        (txn) => backend.deleteNullRowsAfterSequenceInQueueTxn(
          txn,
          'primary',
          head.sequenceInQueue,
        ),
      );
      expect(empty, const TrailSweepResult(deletedCount: 0));
    });

    // -------- queue retirement --------

    // Verifies: EVS-DEV-destination-drain/A
    // retirement refuses a pending head and
    //   changes nothing.
    test('retireQueueTxn refuses a pending head; nothing changes', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'primary', eventId: 'e2', sequenceNumber: 2);
      await writeFillCursorForTest(backend, 'primary', 2);
      final before = [
        for (final r in await backend.listFifoEntries('primary')) r.toJson(),
      ];
      await expectLater(
        backend.transaction((txn) => backend.retireQueueTxn(txn, 'primary')),
        throwsStateError,
      );
      expect([
        for (final r in await backend.listFifoEntries('primary')) r.toJson(),
      ], before);
      expect(await backend.readFillCursor('primary'), 2);
      expect((await backend.readFifoHead('primary'))!.entryId, e1.entryId);
    });

    // Verifies: EVS-DEV-destination-drain/A
    // retirement tombstones a wedged head,
    //   deletes the pending items and the fill cursor, keeps terminal items
    //   and the sequence_in_queue counter.
    test('retireQueueTxn tombstones a wedged head, deletes pending items and '
        'the cursor, keeps terminal items and the counter', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final sent = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final head = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      await enqueueSingle(backend, 'primary', eventId: 'e3', sequenceNumber: 3);
      await enqueueSingle(backend, 'primary', eventId: 'e4', sequenceNumber: 4);
      await seedSentRowForTest(backend, 'primary', sent.entryId);
      await setStatusForTest(
        backend,
        'primary',
        head.entryId,
        FinalStatus.wedged,
      );
      await writeFillCursorForTest(backend, 'primary', 4);

      final retirement = await backend.transaction(
        (txn) => backend.retireQueueTxn(txn, 'primary'),
      );
      expect(
        retirement,
        QueueRetirement(tombstonedRowId: head.entryId, deletedPendingCount: 2),
      );
      final rows = await backend.listFifoEntries('primary');
      expect(rows.map((r) => (r.entryId, r.finalStatus)), [
        (sent.entryId, FinalStatus.sent),
        (head.entryId, FinalStatus.tombstoned),
      ]);
      expect(await backend.readFillCursor('primary'), -1);
      expect(await backend.readFifoHead('primary'), isNull);
      expect(await backend.hasFifoWedged(), isFalse);

      // The counter is kept: a later item continues above the retained ones.
      final next = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e5',
        sequenceNumber: 5,
      );
      expect(next.sequenceInQueue, 5);
    });

    // Verifies: EVS-DEV-destination-drain/A
    // retiring an empty queue, or one with
    //   only terminal items, tombstones nothing.
    test('retireQueueTxn on an empty or all-terminal queue', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      expect(
        await backend.transaction(
          (txn) => backend.retireQueueTxn(txn, 'never-used'),
        ),
        const QueueRetirement(tombstonedRowId: null, deletedPendingCount: 0),
      );
      final sent = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await seedSentRowForTest(backend, 'primary', sent.entryId);
      expect(
        await backend.transaction(
          (txn) => backend.retireQueueTxn(txn, 'primary'),
        ),
        const QueueRetirement(tombstonedRowId: null, deletedPendingCount: 0),
      );
      expect(
        (await backend.readFifoRow('primary', sent.entryId))!.finalStatus,
        FinalStatus.sent,
      );
    });

    // Verifies: EVS-DEV-destination-drain/A
    // a retirement in a transaction that
    //   rolls back changes nothing.
    test('retireQueueTxn rolls back with its transaction', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final head = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'primary', eventId: 'e2', sequenceNumber: 2);
      await setStatusForTest(
        backend,
        'primary',
        head.entryId,
        FinalStatus.wedged,
      );
      await writeFillCursorForTest(backend, 'primary', 2);
      final before = [
        for (final r in await backend.listFifoEntries('primary')) r.toJson(),
      ];
      await expectLater(
        backend.transaction((txn) async {
          await backend.retireQueueTxn(txn, 'primary');
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );
      expect([
        for (final r in await backend.listFifoEntries('primary')) r.toJson(),
      ], before);
      expect(await backend.readFillCursor('primary'), 2);
    });

    // -------- hasFifoWedged + wedgedFifos --------

    test('hasFifoWedged true iff any FIFO is wedged', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a1 = await enqueueSingle(
        backend,
        'A',
        eventId: 'a1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'B', eventId: 'b1', sequenceNumber: 1);

      expect(await backend.hasFifoWedged(), isFalse);

      await setStatusForTest(backend, 'A', a1.entryId, FinalStatus.wedged);
      expect(await backend.hasFifoWedged(), isTrue);
    });

    test('wedgedFifos returns one summary per wedged FIFO', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a1 = await enqueueSingle(
        backend,
        'A',
        eventId: 'a1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'B', eventId: 'b1', sequenceNumber: 1);
      final c1 = await enqueueSingle(
        backend,
        'C',
        eventId: 'c1',
        sequenceNumber: 1,
      );

      // Record an attempt on A's head so the summary has a lastError.
      await appendAttemptForTest(
        backend,
        'A',
        a1.entryId,
        AttemptResult(
          attemptedAt: DateTime.utc(2026, 4, 22, 12, 30),
          outcome: 'permanent',
          errorMessage: 'HTTP 400: bad request',
          httpStatus: 400,
        ),
      );
      await setStatusForTest(backend, 'A', a1.entryId, FinalStatus.wedged);
      await setStatusForTest(backend, 'C', c1.entryId, FinalStatus.wedged);

      final summaries = await backend.wedgedFifos();
      final byDest = {for (final s in summaries) s.destinationId: s};
      expect(byDest.keys.toSet(), {'A', 'C'});
      expect(byDest['A']!.headEntryId, a1.entryId);
      expect(byDest['A']!.headEventId, 'a1');
      expect(byDest['A']!.lastError, 'HTTP 400: bad request');
      expect(byDest['A']!.wedgedAt, DateTime.utc(2026, 4, 22, 12, 30));
    });

    test('wedgedFifos returns empty when nothing is wedged', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await enqueueSingle(backend, 'primary', eventId: 'e1', sequenceNumber: 1);
      expect(await backend.wedgedFifos(), isEmpty);
    });

    test('wedgedFifos reports sensible fallbacks when wedged with no '
        'attempts', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final bare = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e-bare',
        sequenceNumber: 1,
      );
      await setStatusForTest(
        backend,
        'primary',
        bare.entryId,
        FinalStatus.wedged,
      );

      final summary = (await backend.wedgedFifos()).single;
      expect(summary.destinationId, 'primary');
      expect(summary.headEntryId, bare.entryId);
      expect(summary.headEventId, 'e-bare');
      expect(summary.lastError, contains('no attempts'));
    });

    test('a FIFO with only sent entries is NOT wedged', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await setStatusForTest(backend, 'primary', e1.entryId, FinalStatus.sent);
      expect(await backend.hasFifoWedged(), isFalse);
      expect(await backend.wedgedFifos(), isEmpty);
    });

    // -------- backend-owned sequence_in_queue ---

    // The backend assigns sequence_in_queue monotonically starting at 1,
    // independent of any caller-side sequencing.
    test('enqueueFifoTxn assigns its own monotonic sequence_in_queue '
        '(Prereq A, Option 1)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final r1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final r2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final r3 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e3',
        sequenceNumber: 3,
      );

      // Inspect through the public listFifoEntries API.
      final rows = await backend.listFifoEntries('primary');
      expect(rows.map((r) => r.sequenceInQueue).toList(), [1, 2, 3]);
      expect(rows.map((r) => r.entryId).toList(), [
        r1.entryId,
        r2.entryId,
        r3.entryId,
      ]);
      expect(rows.map((r) => r.eventIds).toList(), [
        ['e1'],
        ['e2'],
        ['e3'],
      ]);
    });

    test('sequence_in_queue advances across sent/wedged entries '
        '(Prereq A, Option 1)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await setStatusForTest(backend, 'primary', e1.entryId, FinalStatus.sent);
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      // e2 should get sequence 2, not 1 (the slot vacated by e1 going sent
      // is NOT reused — terminal-state rows are retained for the database's lifetime).
      final row = await backend.readFifoRow('primary', e2.entryId);
      expect(row, isNotNull);
      expect(row!.sequenceInQueue, 2);
    });
  });
}

// -------- listFifoEntries subgroup --------
//
// listFifoEntries enumerates entries
//   ordered by sequence_in_queue with optional afterSequenceInQueue +
//   limit slicing; empty list on unknown destination.
void _registerListFifoEntriesTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('listFifoEntries', () {
    // Verifies: EVS-PRD-portability/D
    test('listFifoEntries on unknown destination returns empty list', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final result = await backend.listFifoEntries('never-registered');
      expect(result, isEmpty);
    });

    test(
      'listFifoEntries returns entries ordered by sequence_in_queue',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        final r1 = await enqueueSingle(
          backend,
          'dest',
          eventId: 'e1',
          sequenceNumber: 1,
        );
        final r2 = await enqueueSingle(
          backend,
          'dest',
          eventId: 'e2',
          sequenceNumber: 2,
        );
        final r3 = await enqueueSingle(
          backend,
          'dest',
          eventId: 'e3',
          sequenceNumber: 3,
        );
        final result = await backend.listFifoEntries('dest');
        expect(result, hasLength(3));
        expect(result[0].sequenceInQueue < result[1].sequenceInQueue, isTrue);
        expect(result[1].sequenceInQueue < result[2].sequenceInQueue, isTrue);
        expect(result[0].eventIds, ['e1']);
        expect(result[1].eventIds, ['e2']);
        expect(result[2].eventIds, ['e3']);
        expect(result[0].entryId, r1.entryId);
        expect(result[1].entryId, r2.entryId);
        expect(result[2].entryId, r3.entryId);
      },
    );

    test('listFifoEntries afterSequenceInQueue is exclusive', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      for (var i = 1; i <= 4; i++) {
        await enqueueSingle(backend, 'dest', eventId: 'e$i', sequenceNumber: i);
      }
      final all = await backend.listFifoEntries('dest');
      expect(all, hasLength(4));
      final secondRow = all[1];
      final after = await backend.listFifoEntries(
        'dest',
        afterSequenceInQueue: secondRow.sequenceInQueue,
      );
      expect(after, hasLength(2));
      expect(after.first.sequenceInQueue > secondRow.sequenceInQueue, isTrue);
    });

    test('listFifoEntries limit caps result size', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      for (var i = 1; i <= 5; i++) {
        await enqueueSingle(backend, 'dest', eventId: 'e$i', sequenceNumber: i);
      }
      final two = await backend.listFifoEntries('dest', limit: 2);
      expect(two, hasLength(2));
      expect(two[0].eventIds, ['e1']);
      expect(two[1].eventIds, ['e2']);
    });
  });
}

// -------- Fill-cursor subgroup --------
//
// fill_cursor read/write (standalone and
//   transactional variants) is part of the StorageBackend abstraction;
//   default sentinel, round-trip, rollback semantics, per-destination
//   isolation, validation of legal range.
void _registerFillCursorTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('fill_cursor', () {
    // Verifies: EVS-PRD-portability/D
    test('readFillCursor returns -1 when unset', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      expect(await backend.readFillCursor('primary'), -1);
    });

    test('writeFillCursorTxn then readFillCursor round-trips', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await writeFillCursorForTest(backend, 'primary', 42);
      expect(await backend.readFillCursor('primary'), 42);

      // A second write replaces the prior value (monotonic advance is
      // caller policy; the backend contract just stores what it's given).
      await writeFillCursorForTest(backend, 'primary', 100);
      expect(await backend.readFillCursor('primary'), 100);
    });

    test('writeFillCursorTxn inside a transaction participates in '
        'atomicity (rollback confirms cursor was NOT advanced)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      // Pre-transaction baseline.
      await writeFillCursorForTest(backend, 'primary', 7);
      expect(await backend.readFillCursor('primary'), 7);

      await expectLater(
        backend.transaction((txn) async {
          await backend.writeFillCursorTxn(txn, 'primary', 99);
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );

      // Rollback: cursor is still the pre-transaction value (7), NOT 99.
      expect(await backend.readFillCursor('primary'), 7);

      // And on commit, the value IS advanced.
      await backend.transaction((txn) async {
        await backend.writeFillCursorTxn(txn, 'primary', 55);
      });
      expect(await backend.readFillCursor('primary'), 55);
    });

    test('fill_cursor is per-destination (two destinations have '
        'independent cursors)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      expect(await backend.readFillCursor('primary'), -1);
      expect(await backend.readFillCursor('secondary'), -1);

      await writeFillCursorForTest(backend, 'primary', 10);
      expect(await backend.readFillCursor('primary'), 10);
      expect(await backend.readFillCursor('secondary'), -1);

      await writeFillCursorForTest(backend, 'secondary', 22);
      expect(await backend.readFillCursor('secondary'), 22);
      expect(await backend.readFillCursor('primary'), 10);
    });

    // Verifies: EVS-DEV-destination-drain/G
    // the fill position read inside a
    //   transaction reflects a write staged in that transaction.
    test('readFillCursorTxn sees an in-transaction write', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await writeFillCursorForTest(backend, 'primary', 3);
      final seen = await backend.transaction((txn) async {
        final before = await backend.readFillCursorTxn(txn, 'primary');
        await backend.writeFillCursorTxn(txn, 'primary', 9);
        final after = await backend.readFillCursorTxn(txn, 'primary');
        final unset = await backend.readFillCursorTxn(txn, 'other');
        return (before, after, unset);
      });
      expect(seen, (3, 9, -1));
    });

    test('writeFillCursorTxn rejects sequenceNumber < -1', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        writeFillCursorForTest(backend, 'primary', -2),
        throwsArgumentError,
      );
      // The failed write left the cursor unchanged.
      expect(await backend.readFillCursor('primary'), -1);
    });
  });
}

// -------- Records kept beside a queue --------
//
// The schedule, the replay request and the registry check record round-trip
// through the contract reads, in the same transaction and a later one, and
// roll back with their transaction.
void _registerQueueRecordTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('records beside a queue', () {
    // Verifies: EVS-DEV-destination-drain/A
    // the persisted schedule keeps its
    //   registration identity and hard-delete opt-in exactly.
    // Verifies: EVS-DEV-destination-drain/G
    // the fill compares the whole persisted
    //   schedule, registration included, so the read returns what was written.
    test('schedule round-trips registrationId and allowHardDelete', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      for (final optIn in <bool>[true, false]) {
        final schedule = DestinationSchedule(
          startDate: DateTime.utc(2026, 1, 2, 3),
          endDate: DateTime.utc(2027, 1, 2, 3),
          registrationId: 'reg-$optIn',
          allowHardDelete: optIn,
        );
        final sameTxn = await backend.transaction((txn) async {
          await backend.writeScheduleTxn(txn, 'dest-$optIn', schedule);
          return backend.readScheduleTxn(txn, 'dest-$optIn');
        });
        expect(sameTxn, schedule);
        final later = await backend.transaction(
          (txn) => backend.readScheduleTxn(txn, 'dest-$optIn'),
        );
        expect(later, schedule);
        expect(later!.registrationId, 'reg-$optIn');
        expect(later.allowHardDelete, optIn);
        expect(await backend.readSchedule('dest-$optIn'), schedule);
      }
      expect(
        await backend.transaction(
          (txn) => backend.readScheduleTxn(txn, 'absent'),
        ),
        isNull,
      );
    });

    // Verifies: EVS-DEV-destination-drain/E
    // a replay request round-trips, is
    //   overwritten, rolls back with its transaction and is cleared.
    test('replay request write, overwrite, rollback and clear', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      const first = ReplayRequest(firstActivation: true);
      final gap = ReplayRequest(gapUpper: DateTime.utc(2026, 3, 4, 5, 6, 7));
      Future<ReplayRequest?> read() => backend.transaction(
        (txn) => backend.readReplayRequestTxn(txn, 'dest'),
      );

      expect(await read(), isNull);
      final sameTxn = await backend.transaction((txn) async {
        await backend.writeReplayRequestTxn(txn, 'dest', first);
        return backend.readReplayRequestTxn(txn, 'dest');
      });
      expect(sameTxn, first);
      expect(await read(), first);

      await backend.transaction(
        (txn) => backend.writeReplayRequestTxn(txn, 'dest', gap),
      );
      expect(await read(), gap);

      await expectLater(
        backend.transaction((txn) async {
          await backend.writeReplayRequestTxn(txn, 'dest', first);
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );
      expect(await read(), gap);

      await backend.transaction(
        (txn) => backend.clearReplayRequestTxn(txn, 'dest'),
      );
      expect(await read(), isNull);
      // Clearing an absent request is a no-op.
      await backend.transaction(
        (txn) => backend.clearReplayRequestTxn(txn, 'dest'),
      );
      expect(await read(), isNull);
    });

    // Verifies: EVS-DEV-destination-drain/U
    // the registry check record commits
    //   and reads back in the same and a later transaction, a second write
    //   overwrites it, and a rolled-back write leaves the prior value.
    test('registry check write, overwrite and rollback', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a = RegistryCheck(
        op: 'setStartDate',
        destinationId: 'd1',
        outcome: 'unchanged',
        at: DateTime.utc(2026, 5, 6, 7, 8, 9, 123),
      );
      final b = RegistryCheck(
        op: 'deleteDestination',
        destinationId: 'd2',
        outcome: 'refused_pending_head',
        at: DateTime.utc(2026, 5, 6, 7, 8, 10),
      );
      Future<RegistryCheck?> read() =>
          backend.transaction(backend.readRegistryCheckTxn);

      expect(await read(), isNull);
      final sameTxn = await backend.transaction((txn) async {
        await backend.writeRegistryCheckTxn(txn, a);
        return backend.readRegistryCheckTxn(txn);
      });
      expect(sameTxn, a);
      expect(await read(), a);

      await backend.transaction((txn) => backend.writeRegistryCheckTxn(txn, b));
      expect(await read(), b);

      await expectLater(
        backend.transaction((txn) async {
          await backend.writeRegistryCheckTxn(txn, a);
          throw StateError('simulated failure');
        }),
        throwsStateError,
      );
      expect(await read(), b);
    });
  });
}

// -------- Backend-state subgroup --------
//
// schema_version round-trip via
//   backend-state KV bookkeeping is part of the StorageBackend abstraction.
void _registerBackendStateTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('backend_state', () {
    // Verifies: EVS-PRD-portability/D
    test('schema_version round-trips', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      expect(await backend.readSchemaVersion(), 0); // never written
      await backend.transaction((txn) async {
        await backend.writeSchemaVersion(txn, 7);
      });
      expect(await backend.readSchemaVersion(), 7);
    });
  });
}

// -------- findEventById subgroup --------
//
// findEventById / findEventByIdInTxn read
//   a single event from the unified log; returns null when absent; used
//   by ingest's idempotency check.
void _registerEventByIdTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('findEventById', () {
    // Verifies: EVS-PRD-event-log/D
    test('findEventById returns the stored event when present', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final appended = await _appendBuilt(
        backend,
        (seq) => _event('evt-target', seq),
      );
      final result = await backend.findEventById('evt-target');
      expect(result, isNotNull);
      expect(result!.eventId, 'evt-target');
      expect(result.sequenceNumber, appended.sequenceNumber);
      expect(result.aggregateId, appended.aggregateId);
      expect(result.eventHash, appended.eventHash);
    });

    test(
      'findEventById returns null when no event with that id exists',
      () async {
        if (!initializedOf()) return;
        final backend = backendOf();
        await _appendBuilt(backend, (seq) => _event('evt-other-1', seq));
        await _appendBuilt(backend, (seq) => _event('evt-other-2', seq));
        final result = await backend.findEventById('evt-missing');
        expect(result, isNull);
      },
    );

    test('findEventById disambiguates among many stored events', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await _appendBuilt(backend, (seq) => _event('evt-a', seq));
      final target = await _appendBuilt(
        backend,
        (seq) => _event('evt-target', seq),
      );
      await _appendBuilt(backend, (seq) => _event('evt-c', seq));
      final result = await backend.findEventById('evt-target');
      expect(result, isNotNull);
      expect(result!.sequenceNumber, target.sequenceNumber);
      expect(result.eventId, 'evt-target');
    });
  });
}

// -------- Close subgroup --------
//
// close() releases resources; subsequent
//   operations on the closed backend fail. The exception type is loose
//   (any subclass of Exception) — concrete backends raise their own
//   storage-layer error.
void _registerCloseTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('close', () {
    // Verifies: EVS-PRD-portability/D
    test('close() closes the underlying database', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        final s = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(txn, _event('ev-1', s));
      });
      await backend.close();
      // After close(), operations on the backend fail; the caller owns
      // re-opening if reads are desired post-close.
      await expectLater(backend.findAllEvents(), throwsA(isA<Exception>()));
    });
  });
}

// -------- Event version columns --------
//
// The entry-type version and the data-format version of an event keep their
// major and minor through every read path that builds a StoredEvent, in and
// out of a transaction, and the event's hash verifies after each read.
void _registerEventVersionColumnTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
  MutableSecurityContextStore Function(StorageBackend backend) securityStoreOf,
) {
  group('event version columns', () {
    // Verifies: EVS-DEV-version-compatibility/A+C
    test('an event stamped entry type 1.3 and data format 2.1 reads back '
        'exactly through every read path, and its hash verifies', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      const entryVersion = EntryTypeVersion(1, 3);
      const dataFormat = DataFormatVersion(2, 1);
      final recordedAt = DateTime.utc(2026, 5, 1, 12);

      late StoredEvent appended;
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        final record = <String, Object?>{
          'event_id': 'versioned-1',
          'aggregate_id': 'agg-versions',
          'aggregate_type': 'note',
          'entry_type': 'versioned_note',
          'entry_type_version': entryVersion.toJson(),
          'lib_format_version': dataFormat.toJson(),
          'event_type': 'finalized',
          'sequence_number': seq,
          'data': <String, Object?>{'title': 'v'},
          'metadata': <String, Object?>{
            'change_reason': 'initial',
            'provenance': <Map<String, Object?>>[
              <String, Object?>{
                'hop': 'mobile-device',
                'received_at': '2026-05-01T12:00:00.000Z',
                'identifier': 'install-A',
                'software_version': 'app@1.0.0',
              },
            ],
          },
          'initiator': const UserInitiator('u-versions').toJson(),
          'flow_token': 'flow-versions',
          'client_timestamp': recordedAt.toIso8601String(),
          'previous_event_hash': null,
        };
        record['event_hash'] = canonicalEventHash(record);
        appended = StoredEvent.fromMap(record, seq);
        await backend.appendEvent(txn, appended);
        await securityStoreOf(backend).writeInTxn(
          txn,
          EventSecurityContext(
            eventId: 'versioned-1',
            recordedAt: recordedAt,
            ipAddress: '10.0.0.1',
          ),
        );
      });

      void expectExact(StoredEvent? read, String path) {
        expect(read, isNotNull, reason: path);
        expect(read!.entryTypeVersion, entryVersion, reason: path);
        expect(read.libFormatVersion, dataFormat, reason: path);
        final map = Map<String, Object?>.from(read.toMap())
          ..remove('event_hash');
        expect(canonicalEventHash(map), appended.eventHash, reason: path);
      }

      StoredEvent? pick(Iterable<StoredEvent> events) {
        for (final e in events) {
          if (e.eventId == 'versioned-1') return e;
        }
        return null;
      }

      expectExact(
        pick(await backend.findEventsForAggregate('agg-versions')),
        'findEventsForAggregate',
      );
      expectExact(pick(await backend.findAllEvents()), 'findAllEvents');
      expectExact(await backend.findEventById('versioned-1'), 'findEventById');
      expectExact(
        pick(await backend.readEventsReverse().toList()),
        'readEventsReverse',
      );
      final audit = await backend.queryAudit(flowToken: 'flow-versions');
      expectExact(pick(audit.rows.map((r) => r.event)), 'queryAudit');
      await backend.transaction((txn) async {
        expectExact(
          pick(await backend.findEventsForAggregateInTxn(txn, 'agg-versions')),
          'findEventsForAggregateInTxn',
        );
        expectExact(
          pick(await backend.findAllEventsInTxn(txn)),
          'findAllEventsInTxn',
        );
        expectExact(
          await backend.findEventByIdInTxn(txn, 'versioned-1'),
          'findEventByIdInTxn',
        );
      });
    });
  });
}
