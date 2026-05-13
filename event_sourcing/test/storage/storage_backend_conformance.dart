// Verifies: EVS-PRD-portability/D — backend-agnostic conformance harness;
//   concrete StorageBackend implementations call this from their own test
//   files. The harness is the canonical place where the abstract
//   StorageBackend contract is exercised.
// Verifies: EVS-PRD-event-log/A — append-only writes commit atomically;
//   rolled-back txn leaves log unchanged.
// Verifies: EVS-PRD-event-log/B — sequence counter is monotonic across
//   transactions; nextSequenceNumber reserve-and-increment contract.
// Verifies: EVS-PRD-event-log/C — findEventsForAggregate returns events sorted
//   by sequence_number, isolating per-aggregate order.
// Verifies: EVS-PRD-event-log/D — findAllEvents + findAllEventsInTxn read in
//   order from any starting position (afterSequence + limit); findEventById
//   reads single events; client-timestamp + originator filters.
// Verifies: EVS-DEV-find-all-events-extended-filters/A,B,C — entry-type,
//   client-timestamp, and originator filters on findAllEvents and
//   findAllEventsInTxn; filters AND-compose.
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
import 'package:flutter_test/flutter_test.dart';

import '../test_support/fifo_entry_helpers.dart';

/// Run the backend-agnostic `StorageBackend` conformance suite against
/// the implementation produced by [factory].
///
/// [factory] is called in each test's `setUp` to produce a fresh, empty
/// backend. Returning `null` from [factory] marks every test in the suite
/// as skipped (this is how the Postgres harness gates on `PG_TEST_URL`
/// absence). The factory may also be invoked from inside an individual
/// test body — the foreign-Txn test in [_registerTransactionTests]
/// constructs a second backend instance to obtain a Txn from a
/// different backend.
///
/// [backendLabel] is the human-readable name folded into the outer group
/// title (e.g. `'sembast (memory)'`, `'postgres'`).
void runStorageBackendConformanceTests(
  Future<StorageBackend?> Function() factory, {
  required String backendLabel,
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
    _registerBackendStateTests(() => backend, () => initialized);
    _registerEventByIdTests(() => backend, () => initialized);
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
    entryTypeVersion: 1,
    libFormatVersion: 1,
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
  entryTypeVersion: 1,
  libFormatVersion: 1,
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
// Verifies: EVS-PRD-event-log/A — successful body commits all writes
//   atomically; thrown exception rolls back all writes; Txn handle is
//   invalidated when body returns or throws; a Txn from one backend
//   instance is rejected by another (defense-in-depth on the type-and-
//   identity check).
void _registerTransactionTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
  Future<StorageBackend?> Function() factory,
) {
  group('transaction', () {
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

    test('Txn cannot be used after body returns', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Txn escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(
        backend.appendEvent(escaped, _event('ev-late', 1)),
        throwsStateError,
      );
    });

    test('Txn cannot be used after body throws', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      late Txn escaped;
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

    // Defense-in-depth: a Txn handed out by a *different* backend
    // instance must be rejected when re-used against this one. The
    // type-and-identity check guards against accidentally feeding one
    // backend's transaction into another's state.
    test('foreign Txn (from a different backend) is rejected', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final other = await factory();
      if (other == null) {
        markTestSkipped('factory returned null on second invocation');
        return;
      }
      late Txn foreignTxn;
      await other.transaction((txn) async {
        foreignTxn = txn;
      });
      await other.close();

      // The foreign Txn is already invalidated by its own backend's
      // end-of-body invalidation, so the validity check fires first.
      // Even if it were still valid, the type-and-identity check would
      // catch it.
      await expectLater(
        backend.appendEvent(foreignTxn, _event('ev-foreign', 1)),
        throwsStateError,
      );
    });
  });
}

// -------- Event log subgroup --------
//
// Verifies: EVS-PRD-event-log/A,B,C,D — append-only writes; sequence
//   counter monotonicity (reserve-and-increment); per-aggregate order;
//   in-order reads from any starting position; findAllEventsInTxn coherent
//   with same-txn writes; readLatestEventHash transactional read.
void _registerEventLogTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('event log', () {
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

    // Phase-2 Prereq B, Option 1: counter is advanced by nextSequenceNumber,
    // so appendEvent rejects a mismatched sequence number rather than
    // silently advancing the counter implicitly.
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
      late Txn escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(backend.readLatestEventHash(escaped), throwsStateError);
    });

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
      late Txn escaped;
      await backend.transaction((txn) async {
        escaped = txn;
      });
      await expectLater(backend.findAllEventsInTxn(escaped), throwsStateError);
    });

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
// Verifies: EVS-DEV-find-all-events-extended-filters/A,B,C — entryType,
//   clientTimestampStart, clientTimestampEnd on findAllEvents and
//   findAllEventsInTxn; filters AND-compose; existing afterSequence +
//   limit unaffected.
void _registerFindAllEventsFilterTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('findAllEvents extended filters', () {
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
// Verifies: EVS-DEV-find-all-events-extended-filters/C — originatorHopId and
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
            aggregateId: 'agg-ev-portalP',
            hopId: 'portal-server',
            identifier: 'install-P',
            eventId: 'ev-portalP',
          ),
        );
      });
    }

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
// Verifies: EVS-PRD-portability/D — generic view-storage methods are part
//   of the StorageBackend abstraction; round-trip, missing-key-null,
//   delete, find with limit/offset, clearView, viewName isolation.
void _registerViewRowTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('generic view storage', () {
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
// Verifies: EVS-PRD-portability/D — view-target-version persistence is
//   part of the StorageBackend abstraction (round-trip, null-on-unknown,
//   readAll, clear, cross-view isolation).
void _registerViewTargetVersionTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('view_target_versions storage', () {
    test('round-trip read/write', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'demo_note',
          3,
        );
      });
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(
            txn,
            'diary_entries',
            'demo_note',
          ),
          3,
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

    test('readAll returns full map for one view', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'demo_note',
          2,
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'diary_entries',
          'epistaxis',
          5,
        );
        await backend.writeViewTargetVersionInTxn(
          txn,
          'other_view',
          'demo_note',
          1,
        );
      });
      await backend.transaction((txn) async {
        final map = await backend.readAllViewTargetVersionsInTxn(
          txn,
          'diary_entries',
        );
        expect(map, <String, int>{'demo_note': 2, 'epistaxis': 5});
      });
    });

    test('clear removes only the named view', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'view_a', 'x', 1);
        await backend.writeViewTargetVersionInTxn(txn, 'view_b', 'x', 2);
      });
      await backend.transaction((txn) async {
        await backend.clearViewTargetVersionsInTxn(txn, 'view_a');
      });
      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'view_a', 'x'),
          isNull,
        );
        expect(await backend.readViewTargetVersionInTxn(txn, 'view_b', 'x'), 2);
      });
    });

    test('idempotent overwrite', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'v', 'e', 1);
        await backend.writeViewTargetVersionInTxn(txn, 'v', 'e', 1);
        await backend.writeViewTargetVersionInTxn(txn, 'v', 'e', 2);
        expect(await backend.readViewTargetVersionInTxn(txn, 'v', 'e'), 2);
      });
    });
  });
}

// -------- FIFO subgroup --------
//
// Verifies: EVS-PRD-portability/D — FIFO persistence methods (enqueueFifo,
//   readFifoHead, listFifoEntries, appendAttempt, markFinal,
//   anyFifoWedged/wedgedFifos) are part of the StorageBackend abstraction.
//   markFinal idempotency + one-way transition rule are part of the FIFO
//   contract; drain's at-least-once delivery depends on
//   markFinal(same-status) being a no-op.
void _registerFifoTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('FIFO', () {
    // -------- enqueueFifo + validation --------

    test('enqueueFifo + readFifoHead round-trip', () async {
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
      expect(head.eventIdRange, (firstSeq: 1, lastSeq: 1));
      expect(head.finalStatus, isNull);
      expect(head.attempts, isEmpty);
      expect(head.sentAt, isNull);
      expect(head.sequenceInQueue, enqueued.sequenceInQueue);
    });

    test('enqueueFifo rejects an empty batch with ArgumentError', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.enqueueFifo(
          'primary',
          const [],
          wirePayload: wirePayloadJson(const {'k': 'v'}),
        ),
        throwsArgumentError,
      );
    });

    test('enqueueFifo assigns distinct UUID entry_ids even when the same '
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

    test('enqueueFifo with nativeEnvelope persists '
        'envelope_metadata and nulls wire_payload', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      final envelope = BatchEnvelopeMetadata(
        batchFormatVersion: '1',
        batchId: 'batch-x',
        senderHop: 'mobile-1',
        senderIdentifier: 'device-uuid',
        senderSoftwareVersion: 'diary@1.2.3',
        sentAt: DateTime.utc(2026, 4, 25, 12),
      );
      await backend.enqueueFifo('dest', [event], nativeEnvelope: envelope);
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
      expect(head.envelopeMetadata!.batchFormatVersion, '1');
      expect(head.wireFormat, BatchEnvelope.wireFormat);
      expect(
        head.transformVersion,
        isNull,
        reason: 'native rows carry no transform_version',
      );
    });

    test('enqueueFifo with wirePayload stores '
        'wire_payload, envelope_metadata is null', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      final payload = wirePayloadJson(
        const <String, Object?>{'kind': 'csv-row', 'value': 42},
        contentType: 'application/json',
        transformVersion: 'json-v1',
      );
      await backend.enqueueFifo('dest', [event], wirePayload: payload);
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

    test('enqueueFifo rejects supplying both wirePayload '
        'and nativeEnvelope', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await expectLater(
        backend.enqueueFifo(
          'dest',
          [event],
          wirePayload: wirePayloadJson(const {'k': 'v'}),
          nativeEnvelope: BatchEnvelopeMetadata(
            batchFormatVersion: '1',
            batchId: 'batch-x',
            senderHop: 'mobile-1',
            senderIdentifier: 'device-uuid',
            senderSoftwareVersion: 'diary@1.2.3',
            sentAt: DateTime.utc(2026, 4, 25, 12),
          ),
        ),
        throwsArgumentError,
      );
    });

    test('enqueueFifo rejects supplying neither wirePayload '
        'nor nativeEnvelope', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await expectLater(
        backend.enqueueFifo('dest', [event]),
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

    // -------- appendAttempt --------

    test('appendAttempt appends without changing final_status', () async {
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
      await backend.appendAttempt('primary', e1.entryId, attempt);

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
      await backend.appendAttempt('primary', e1.entryId, attempt2);
      final head2 = await backend.readFifoHead('primary');
      expect(head2?.attempts, [attempt, attempt2]);
    });

    test('appendAttempt no-ops when entry does not exist', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      // Must not throw.
      await backend.appendAttempt(
        'primary',
        'nonexistent',
        AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
      );
      // The FIFO is otherwise untouched: e1 is still pending with no attempts.
      final head = await backend.readFifoHead('primary');
      expect(head?.entryId, e1.entryId);
      expect(head?.attempts, isEmpty);
    });

    test('appendAttempt no-ops when FIFO store does not exist', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.appendAttempt(
        'ghost-dest',
        'any-entry',
        AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
      );
      // Nothing materialized in the unknown store.
      expect(await backend.readFifoHead('ghost-dest'), isNull);
    });

    // -------- markFinal --------

    test('markFinal sent retains the entry', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);

      // After marking sent, readFifoHead moves past it to the next pending.
      expect(await backend.readFifoHead('primary'), isNull);

      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final nextHead = await backend.readFifoHead('primary');
      expect(nextHead?.entryId, e2.entryId);
    });

    test('markFinal sent sets sent_at', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final before = DateTime.now().toUtc();
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      final after = DateTime.now().toUtc();

      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row, isNotNull);
      expect(row!.sentAt, isNotNull);
      expect(row.sentAt!.isAfter(before) || row.sentAt == before, isTrue);
      expect(row.sentAt!.isBefore(after) || row.sentAt == after, isTrue);
    });

    test('markFinal wedged does NOT set sent_at', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.markFinal('primary', e1.entryId, FinalStatus.wedged);

      final row = await backend.readFifoRow('primary', e1.entryId);
      expect(row, isNotNull);
      expect(row!.sentAt, isNull);
    });

    test('after markFinal sent, readFifoHead returns next pending', () async {
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

      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);

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

      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      await backend.markFinal('primary', e2.entryId, FinalStatus.wedged);
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
      // Transition the head into tombstoned via the public setFinalStatusTxn
      // method (null -> tombstoned is a legal transition per the contract);
      // this focuses the test on readFifoHead's skip-past behavior.
      await backend.transaction((txn) async {
        await backend.setFinalStatusTxn(
          txn,
          'primary',
          e1.entryId,
          FinalStatus.tombstoned,
        );
      });

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
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      await backend.transaction((txn) async {
        await backend.setFinalStatusTxn(
          txn,
          'primary',
          e2.entryId,
          FinalStatus.tombstoned,
        );
      });

      expect(await backend.readFifoHead('primary'), isNull);
    });

    test('markFinal no-ops when entry does not exist', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      // Must not throw.
      await backend.markFinal('primary', 'ghost', FinalStatus.sent);
      // e1 still at head, still pending.
      final head = await backend.readFifoHead('primary');
      expect(head?.entryId, e1.entryId);
      expect(head?.finalStatus, isNull);
    });

    test('markFinal no-ops when FIFO store does not exist', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.markFinal('ghost-dest', 'any-entry', FinalStatus.sent);
      expect(await backend.readFifoHead('ghost-dest'), isNull);
    });

    // From markfinal_idempotency_test.dart — Case 2 (idempotency).
    test('markFinal sent twice on the same row returns cleanly '
        '(no throw, row stays sent)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      await expectLater(
        backend.markFinal('primary', e1.entryId, FinalStatus.sent),
        completes,
      );
      final all = await backend.listFifoEntries('primary');
      expect(all, hasLength(1));
      expect(all.single.finalStatus, FinalStatus.sent);
    });

    // From markfinal_idempotency_test.dart — Case 3 (status mismatch with
    // explicit error-message inspection).
    test('markFinal sent then markFinal wedged throws StateError '
        'naming both statuses', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      await expectLater(
        () async =>
            backend.markFinal('primary', e1.entryId, FinalStatus.wedged),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            allOf(contains('sent'), contains('wedged')),
          ),
        ),
      );
    });

    // -------- anyFifoWedged + wedgedFifos --------

    test('anyFifoWedged true iff any FIFO is wedged', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      final a1 = await enqueueSingle(
        backend,
        'A',
        eventId: 'a1',
        sequenceNumber: 1,
      );
      await enqueueSingle(backend, 'B', eventId: 'b1', sequenceNumber: 1);

      expect(await backend.anyFifoWedged(), isFalse);

      await backend.markFinal('A', a1.entryId, FinalStatus.wedged);
      expect(await backend.anyFifoWedged(), isTrue);
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
      await backend.appendAttempt(
        'A',
        a1.entryId,
        AttemptResult(
          attemptedAt: DateTime.utc(2026, 4, 22, 12, 30),
          outcome: 'permanent',
          errorMessage: 'HTTP 400: bad request',
          httpStatus: 400,
        ),
      );
      await backend.markFinal('A', a1.entryId, FinalStatus.wedged);
      await backend.markFinal('C', c1.entryId, FinalStatus.wedged);

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
      await backend.markFinal('primary', bare.entryId, FinalStatus.wedged);

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
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      expect(await backend.anyFifoWedged(), isFalse);
      expect(await backend.wedgedFifos(), isEmpty);
    });

    // -------- Phase-2 Prereq A, Option 1: backend-owned sequence_in_queue ---

    // Verifies that the backend assigns sequence_in_queue monotonically
    // starting at 1, regardless of any caller-side sequencing concerns.
    test('enqueueFifo assigns its own monotonic sequence_in_queue '
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
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);
      final e2 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      // e2 should get sequence 2, not 1 (the slot vacated by e1 going sent
      // is NOT reused — terminal-state rows are retained forever).
      final row = await backend.readFifoRow('primary', e2.entryId);
      expect(row, isNotNull);
      expect(row!.sequenceInQueue, 2);
    });
  });
}

// -------- listFifoEntries subgroup --------
//
// Verifies: EVS-PRD-portability/D — listFifoEntries enumerates entries
//   ordered by sequence_in_queue with optional afterSequenceInQueue +
//   limit slicing; empty list on unknown destination.
void _registerListFifoEntriesTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('listFifoEntries', () {
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
// Verifies: EVS-PRD-portability/D — fill_cursor read/write (standalone and
//   transactional variants) is part of the StorageBackend abstraction;
//   default sentinel, round-trip, rollback semantics, per-destination
//   isolation, validation of legal range.
void _registerFillCursorTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('fill_cursor', () {
    test('readFillCursor returns -1 when unset', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      expect(await backend.readFillCursor('primary'), -1);
    });

    test('writeFillCursor then readFillCursor round-trips', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await backend.writeFillCursor('primary', 42);
      expect(await backend.readFillCursor('primary'), 42);

      // A second write replaces the prior value (monotonic advance is
      // caller policy; the backend contract just stores what it's given).
      await backend.writeFillCursor('primary', 100);
      expect(await backend.readFillCursor('primary'), 100);
    });

    test('writeFillCursor inside a transaction participates in '
        'atomicity (rollback confirms cursor was NOT advanced)', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      // Pre-transaction baseline.
      await backend.writeFillCursor('primary', 7);
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

      await backend.writeFillCursor('primary', 10);
      expect(await backend.readFillCursor('primary'), 10);
      expect(await backend.readFillCursor('secondary'), -1);

      await backend.writeFillCursor('secondary', 22);
      expect(await backend.readFillCursor('secondary'), 22);
      expect(await backend.readFillCursor('primary'), 10);
    });

    test('writeFillCursor rejects sequenceNumber < -1', () async {
      if (!initializedOf()) return;
      final backend = backendOf();
      await expectLater(
        backend.writeFillCursor('primary', -2),
        throwsArgumentError,
      );
      // The failed write left the cursor unchanged.
      expect(await backend.readFillCursor('primary'), -1);
    });
  });
}

// -------- Backend-state subgroup --------
//
// Verifies: EVS-PRD-portability/D — schema_version round-trip via
//   backend-state KV bookkeeping is part of the StorageBackend abstraction.
void _registerBackendStateTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('backend_state', () {
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
// Verifies: EVS-PRD-event-log/D — findEventById / findEventByIdInTxn read
//   a single event from the unified log; returns null when absent; used
//   by ingest's idempotency check.
void _registerEventByIdTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('findEventById', () {
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
// Verifies: EVS-PRD-portability/D — close() releases resources; subsequent
//   operations on the closed backend fail. The exception type is loose
//   (any subclass of Exception) — concrete backends raise their own
//   storage-layer error.
void _registerCloseTests(
  StorageBackend Function() backendOf,
  bool Function() initializedOf,
) {
  group('close', () {
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
