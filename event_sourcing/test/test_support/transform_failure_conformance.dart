// Backend-agnostic scenarios for a fill whose destination's transform keeps
// failing: the transform failure record it writes and retries under the
// destination's retry curve, the transform-failed item it enqueues once the
// retry budget is spent, and the record a recovering transform clears.
// Sembast runs them from test/sync/transform_failure_sembast_test.dart and
// Postgres from test/storage/postgres/postgres_transform_failure_test.dart.
//
// This file exposes one function, [runTransformFailureScenarios], and
// registers no `main()` of its own. Traceability lives on the individual
// tests.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show drainForTest, fillForTest;
import 'test_backends.dart';
import 'wedges_view_invariant.dart';

const Initiator _init = AutomationInitiator(
  service: 'transform-failure-scenarios',
);
const String _noteType = 'transform_failure_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'transform-failure-install',
  softwareVersion: 'test@1.0.0',
);

/// A policy with the given attempt/time bounds and no jitter, so its
/// backoff and its longest-allowed-delay are exactly [initialBackoff] at
/// every attempt count (`backoffMultiplier: 1.0`).
SyncPolicy _policy({
  Duration initialBackoff = Duration.zero,
  int maxAttempts = 1000000,
  Duration maxRetryTime = const Duration(hours: 24),
}) => SyncPolicy(
  initialBackoff: initialBackoff,
  backoffMultiplier: 1.0,
  maxBackoff: initialBackoff,
  jitterFraction: 0.0,
  maxAttempts: maxAttempts,
  maxRetryTime: maxRetryTime,
);

/// A destination whose `transform` is scripted per call: [transformImpl]
/// runs (and may throw) on every invocation; with none given, `transform`
/// succeeds with an empty payload. `canAddToBatch` always refuses, so every
/// batch this destination's fill assembles carries exactly one event —
/// batch identity in these scenarios is the single event's sequence
/// number.
class ScriptedTransformDestination extends Destination {
  ScriptedTransformDestination({this.id = 'scripted'});

  @override
  final String id;

  @override
  String get wireFormat => 'scripted-v1';

  @override
  SubscriptionFilter get filter => const SubscriptionFilter();

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      false;

  /// Invocations of `transform`, successes and failures alike.
  int transformCalls = 0;

  /// Run on every `transform` call in place of the identity default; set
  /// by a test to throw on the calls it wants to fail.
  Future<WirePayload> Function(List<StoredEvent> batch)? transformImpl;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    transformCalls += 1;
    final impl = transformImpl;
    if (impl != null) return impl(batch);
    return Future<WirePayload>.value(
      WirePayload(
        bytes: Uint8List.fromList(utf8.encode('{}')),
        contentType: 'application/json',
        transformVersion: 'v1',
      ),
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    throw UnimplementedError(
      'ScriptedTransformDestination.send is not exercised by the transform '
      'failure scenarios',
    );
  }
}

/// Always throws [error] (a `StateError` by default).
Future<WirePayload> Function(List<StoredEvent>) alwaysThrows([Error? error]) =>
    (_) async => throw error ?? StateError('scripted transform failure');

class _Process {
  _Process(this.backend, this.store, this.registry) {
    trackTestBackend(store, backend);
  }
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

class _World {
  _World(this.db);
  final QueueTestDatabase db;
  DateTime eventTime = DateTime.utc(2026, 3, 1);
  late _Process a;

  StorageBackend get backend => a.backend;
  EventStore get store => a.store;
  DestinationRegistry get registry => a.registry;

  Future<_Process> openProcess() async {
    final backend = await db.openBackend();
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
      ),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: db.securityFor(backend),
      clock: () => eventTime,
    );
    return _Process(backend, store, DestinationRegistry(eventStore: store));
  }

  Future<StoredEvent> note(String id) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    final event = await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id},
      initiator: _init,
    );
    return event!;
  }

  Future<void> activate(Destination d) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  }

  /// Activates [d] and resolves its first-activation replay request
  /// against a log holding no event this destination admits, so a
  /// scenario's later fills run through the live fill (`fillBatch`'s own
  /// batch decision) rather than through `_performReplayRequest`. A test
  /// sets a failing `transformImpl` only after this call returns.
  Future<void> activateResolved(Destination d) async {
    await activate(d);
    await fillForTest(
      d,
      backend: backend,
      clock: () => DateTime.utc(2026, 1, 1),
      policy: _policy(),
      cadence: const Duration(seconds: 15),
    );
  }
}

/// Run every transform-failure scenario against a database
/// [databaseFactory] builds fresh for each test (a null database skips the
/// test).
void runTransformFailureScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('transform failure scenarios ($label)', () {
    late _World w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = _World(db);
      w.a = await w.openProcess();
    });

    tearDown(() async {
      if (!available) return;
      await w.db.close();
    });

    // Verifies: EVS-DEV-destination-drain/Y
    // the first failure of a batch's transform records its time and the
    //   batch's sequence range, and enqueues no item.
    // Verifies: EVS-DEV-destination-retry-budget/B
    test('the first failure records one time and enqueues nothing', () async {
      if (!available) return;
      final d = ScriptedTransformDestination()..transformImpl = alwaysThrows();
      await w.activate(d);
      final e1 = await w.note('n1');
      final logged = <LibraryLogRecord>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(onLog: logged.add),
        () => fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: _policy(),
          cadence: const Duration(seconds: 15),
        ),
      );
      expect(d.transformCalls, 1);
      expect(await w.backend.readFifoHead(d.id), isNull);
      expect(await w.backend.readFillCursor(d.id), lessThan(e1.sequenceNumber));
      final record = await w.backend.transaction(
        (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
      );
      expect(record, isNotNull);
      expect(record!.failureTimes, <DateTime>[DateTime.utc(2027, 1, 1)]);
      expect(record.sequenceRange, (
        firstSeq: e1.sequenceNumber,
        lastSeq: e1.sequenceNumber,
      ));
      expect(
        logged.where(
          (r) => r.level == LibraryLogLevel.severe && r.message.contains(d.id),
        ),
        isNotEmpty,
        reason: 'the failure is logged severe',
      );
    });

    // Verifies: EVS-DEV-destination-retry-budget/B
    test('a pass before the backoff elapses reruns nothing', () async {
      if (!available) return;
      final d = ScriptedTransformDestination();
      await w.activateResolved(d);
      d.transformImpl = alwaysThrows();
      await w.note('n1');
      final t0 = DateTime.utc(2027, 1, 1);
      await fillForTest(
        d,
        backend: w.backend,
        clock: () => t0,
        policy: _policy(initialBackoff: const Duration(minutes: 5)),
        cadence: const Duration(seconds: 15),
      );
      expect(d.transformCalls, 1);
      await fillForTest(
        d,
        backend: w.backend,
        clock: () => t0.add(const Duration(minutes: 1)),
        policy: _policy(initialBackoff: const Duration(minutes: 5)),
        cadence: const Duration(seconds: 15),
      );
      expect(d.transformCalls, 1, reason: 'still within the backoff window');
      final record = await w.backend.transaction(
        (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
      );
      expect(record!.failureTimes, <DateTime>[t0]);
    });

    // Verifies: EVS-DEV-destination-drain/G
    test('a transform failure record another process wrote between the '
        "fill's reads and its commit wins over the fill's own pending "
        'record', () async {
      if (!available) return;
      final d = ScriptedTransformDestination()..transformImpl = alwaysThrows();
      await w.activateResolved(d);
      await w.note('n1');
      final policy = _policy();
      await fillForTest(
        d,
        backend: w.backend,
        clock: () => DateTime.utc(2027, 1, 1),
        policy: policy,
        cadence: const Duration(seconds: 15),
      );
      expect(d.transformCalls, 1);
      final concurrent = TransformFailureRecord(
        failureTimes: <DateTime>[DateTime.utc(2026, 6, 1)],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
      );
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterFillReads: (id) => w.backend.transaction(
            (txn) =>
                w.backend.writeTransformFailureRecordTxn(txn, id, concurrent),
          ),
        ),
        () => fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: policy,
          cadence: const Duration(seconds: 15),
        ),
      );
      expect(
        d.transformCalls,
        2,
        reason:
            'the transform runs outside the transaction, before the '
            'concurrent write is seen',
      );
      final record = await w.backend.transaction(
        (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
      );
      expect(
        record,
        concurrent,
        reason:
            "the fill's compare-and-set saw the record change since its "
            'read and wrote nothing, leaving the concurrent write in place',
      );
    });

    // Verifies: EVS-DEV-destination-drain/Y
    test('the retry budget spent by attempts enqueues a transform-failed item, '
        'advances the position and clears the record', () async {
      if (!available) return;
      final d = ScriptedTransformDestination();
      await w.activateResolved(d);
      d.transformImpl = alwaysThrows();
      final e1 = await w.note('n1');
      final policy = _policy(maxAttempts: 3);
      for (var i = 0; i < 3; i++) {
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: policy,
          cadence: const Duration(seconds: 15),
        );
      }
      expect(d.transformCalls, 3);
      final head = await w.backend.readFifoHead(d.id);
      expect(head, isNotNull);
      expect(head!.transformFailed, isTrue);
      expect(head.transformFailures, 3);
      expect(head.wirePayload, isNull);
      expect(head.envelopeMetadata, isNull);
      expect(head.wireFormat, d.wireFormat);
      expect(head.eventIds, <String>[e1.eventId]);
      expect(await w.backend.readFillCursor(d.id), e1.sequenceNumber);
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
        ),
        isNull,
      );

      // A following drain wedges it without a send.
      await drainForTest(d, registry: w.registry, policy: policy);
      final wedged = await w.backend.readFifoHead(d.id);
      expect(wedged!.finalStatus, FinalStatus.wedged);
      await expectWedgesViewMatchesQueue(w.store);
    });

    // Verifies: EVS-DEV-destination-drain/Y
    test(
      'the retry budget spent by attempts enqueues a transform-failed '
      'item and clears the record on the first-activation replay path',
      () async {
        if (!available) return;
        // The event precedes activation, so the first fill resolves the
        // first-activation replay request instead of the live fill: the
        // budget-spend enqueue and the record clear this asserts are
        // `_performReplayRequest`'s own, not `fillBatch`'s.
        final d = ScriptedTransformDestination()
          ..transformImpl = alwaysThrows();
        await w.activate(d);
        final e1 = await w.note('n1');
        final policy = _policy(maxAttempts: 3);
        for (var i = 0; i < 3; i++) {
          await fillForTest(
            d,
            backend: w.backend,
            clock: () => DateTime.utc(2027, 1, 1),
            policy: policy,
            cadence: const Duration(seconds: 15),
          );
        }
        expect(d.transformCalls, 3);
        final head = await w.backend.readFifoHead(d.id);
        expect(head, isNotNull);
        expect(head!.transformFailed, isTrue);
        expect(head.transformFailures, 3);
        expect(await w.backend.readFillCursor(d.id), e1.sequenceNumber);
        expect(
          await w.backend.transaction(
            (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
          ),
          isNull,
        );
      },
    );

    // Verifies: EVS-DEV-destination-retry-budget/B
    test('the time bound alone spends the budget', () async {
      if (!available) return;
      final d = ScriptedTransformDestination();
      await w.activateResolved(d);
      d.transformImpl = alwaysThrows();
      final e1 = await w.note('n1');
      final policy = _policy(
        maxAttempts: 1000000,
        maxRetryTime: const Duration(minutes: 10),
      );
      const cadence = Duration(minutes: 2);
      var t = DateTime.utc(2027, 1, 1);
      // No backoff, so each gap is capped at the cadence alone. Five gaps
      // of 2 minutes each sum to 10 minutes, reached at the 6th failure.
      for (var i = 0; i < 6; i++) {
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => t,
          policy: policy,
          cadence: cadence,
        );
        t = t.add(const Duration(minutes: 2));
      }
      expect(d.transformCalls, 6);
      final head = await w.backend.readFifoHead(d.id);
      expect(head, isNotNull, reason: 'the time bound alone spent the budget');
      expect(head!.transformFailed, isTrue);
      expect(head.transformFailures, 6);
      expect(await w.backend.readFillCursor(d.id), e1.sequenceNumber);
    });

    // Verifies: EVS-DEV-destination-drain/Y
    test('a transform that recovers enqueues a normal item and clears the '
        'record', () async {
      if (!available) return;
      final d = ScriptedTransformDestination();
      await w.activateResolved(d);
      var calls = 0;
      d.transformImpl = (batch) async {
        calls += 1;
        if (calls == 1) throw StateError('first call fails');
        return WirePayload(
          bytes: Uint8List.fromList(utf8.encode('{}')),
          contentType: 'application/json',
          transformVersion: 'v1',
        );
      };
      final e1 = await w.note('n1');
      final policy = _policy(initialBackoff: const Duration(minutes: 5));
      await fillForTest(
        d,
        backend: w.backend,
        clock: () => DateTime.utc(2027, 1, 1),
        policy: policy,
        cadence: const Duration(seconds: 15),
      );
      expect(await w.backend.readFifoHead(d.id), isNull);
      await fillForTest(
        d,
        backend: w.backend,
        clock: () => DateTime.utc(2027, 1, 1, 0, 6),
        policy: policy,
        cadence: const Duration(seconds: 15),
      );
      final head = await w.backend.readFifoHead(d.id);
      expect(head, isNotNull);
      expect(head!.transformFailed, isFalse);
      expect(head.wirePayload, isNotNull);
      expect(head.eventIds, <String>[e1.eventId]);
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
        ),
        isNull,
      );
    });

    // Verifies: EVS-DEV-destination-drain/Y
    test(
      'a growing batch replaces the record left by an earlier, smaller one',
      () async {
        if (!available) return;
        // _GrowingBatchDestination accepts a growing batch, so a second
        // event arriving before the first batch's failing transform is
        // retried widens the batch the record names, on the first-
        // activation replay path.
        final d = _GrowingBatchDestination()..transformImpl = alwaysThrows();
        await w.activate(d);
        final e1 = await w.note('n1');
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: _policy(),
          cadence: const Duration(seconds: 15),
        );
        Future<TransformFailureRecord?> readRecord() => w.backend.transaction(
          (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
        );
        final firstRecord = await readRecord();
        expect(firstRecord!.sequenceRange, (
          firstSeq: e1.sequenceNumber,
          lastSeq: e1.sequenceNumber,
        ));

        d.batchCapacity = 2;
        final e2 = await w.note('n2');
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: _policy(),
          cadence: const Duration(seconds: 15),
        );
        final secondRecord = await readRecord();
        expect(
          secondRecord!.sequenceRange,
          (firstSeq: e1.sequenceNumber, lastSeq: e2.sequenceNumber),
          reason: 'the record for the smaller batch is replaced',
        );
        expect(secondRecord.failureTimes.length, 1, reason: 'not appended to');
      },
    );

    // Verifies: EVS-DEV-destination-drain/Y
    // Verifies: EVS-DEV-destination-drain/E
    test(
      'a pure gap replay request leaves an unrelated live-fill record alone',
      () async {
        if (!available) return;
        final d = ScriptedTransformDestination();
        await w.activate(d);
        // Resolve the first activation cleanly, against no events, before
        // the transform ever fails: the request clears now, so the record
        // this test creates below belongs to the live fill, not to a
        // still-pending activation (which would otherwise absorb the later
        // setStartDate into its own request instead of raising a pure gap
        // request — see EVS-DEV-destination-drain/E).
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: _policy(),
          cadence: const Duration(seconds: 15),
        );
        d.transformImpl = alwaysThrows();
        final e1 = await w.note('n1');
        // No backoff: two live-fill passes at the same clock reading each
        // record one more failure, so the record's failure count (2) is the
        // signal a wrongly-cleared-and-recreated record would lose (it
        // would read back at 1, not 3, after the gap pass below).
        final policy = _policy();
        for (var i = 0; i < 2; i++) {
          await fillForTest(
            d,
            backend: w.backend,
            clock: () => DateTime.utc(2027, 1, 1),
            policy: policy,
            cadence: const Duration(seconds: 15),
          );
        }
        Future<TransformFailureRecord?> readRecord() => w.backend.transaction(
          (txn) => w.backend.readTransformFailureRecordTxn(txn, d.id),
        );
        final beforeGap = await readRecord();
        expect(beforeGap!.sequenceRange, (
          firstSeq: e1.sequenceNumber,
          lastSeq: e1.sequenceNumber,
        ));
        expect(beforeGap.failureTimes.length, 2);

        // A later, unrelated backward start-date move: the destination
        // already activated, so this records a gap-only replay request (no
        // first activation), naming a window with nothing to enqueue (the
        // fill position is still behind e1). The gap portion running, or
        // finding nothing to enqueue, must not touch e1's record, which
        // belongs to the live fill, not to this request: the same fill call
        // resolves the (empty) gap request and then falls through to retry
        // e1 live, so a wrongly cleared record would read back with one
        // fresh failure instead of three.
        await w.registry.setStartDate(
          d.id,
          DateTime.utc(2025, 6, 1),
          initiator: _init,
        );
        await fillForTest(
          d,
          backend: w.backend,
          clock: () => DateTime.utc(2027, 1, 1),
          policy: policy,
          cadence: const Duration(seconds: 15),
        );
        final afterGap = await readRecord();
        expect(
          afterGap!.sequenceRange,
          beforeGap.sequenceRange,
          reason: "the gap request must not replace e1's unrelated record",
        );
        expect(
          afterGap.failureTimes.length,
          3,
          reason:
              "the gap request's own resolution must not reset e1's "
              'accumulated failures before the live fill retries it',
        );
      },
    );
  });
}

/// A [ScriptedTransformDestination] whose batch capacity a test can widen
/// mid-run, to exercise a batch that grows across passes.
class _GrowingBatchDestination extends ScriptedTransformDestination {
  int batchCapacity = 1;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < batchCapacity;
}
