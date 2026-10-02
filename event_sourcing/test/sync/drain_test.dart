@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';
import '../test_support/native_destination.dart';
import '../test_support/queue_test_support.dart';
import '../test_support/registry_with_audit.dart';

/// Fixture — a fresh in-memory SembastBackend per test.
Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

/// Enqueue a single-event row via the batch-aware `enqueueFifoTxn`. The
/// backend mints a v4-UUID `entry_id` at enqueue time (independent of
/// the event id); callers that need to look the row up later capture the
/// returned `FifoEntry.entryId`.
Future<String> _enqueueRow(
  SembastBackend backend,
  String destId, {
  required String eventId,
  required int sequenceNumber,
}) async {
  final entry = await enqueueSingle(
    backend,
    destId,
    eventId: eventId,
    sequenceNumber: sequenceNumber,
    wirePayload: <String, Object?>{'event_id': eventId},
    wireFormat: 'fake-v1',
    transformVersion: 'fake-v1',
  );
  return entry.entryId;
}

void main() {
  group('drain()', () {
    late SembastBackend backend;
    late DestinationRegistry registry;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('drain-$dbCounter.db');
      final deps = await buildAuditedRegistryDeps(backend);
      registry = DestinationRegistry(eventStore: deps.eventStore);
    });

    tearDown(() async {
      await backend.close();
    });

    test('empty FIFO returns without calling send', () async {
      final dest = FakeDestination();
      await drainForTest(dest, registry: registry);
      expect(dest.sent, isEmpty);
    });

    // Verifies: EVS-PRD-destinations/E
    // the queued item is handed to the application-supplied send.
    test('SendOk marks head sent and advances to the next head', () async {
      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(script: [const SendOk()]);

      await drainForTest(dest, registry: registry);

      expect(dest.sent, hasLength(1));
      // After the head is marked sent, readFifoHead returns null.
      expect(await backend.readFifoHead('fake'), isNull);
    });

    test('drain loops across multiple SendOks in one call', () async {
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq);
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drainForTest(dest, registry: registry);
      expect(dest.sent, hasLength(3));
      expect(await backend.readFifoHead('fake'), isNull);
    });

    // Verifies: EVS-DEV-destination-retry-budget/C
    // Verifies: EVS-DEV-destination-retry-budget/D
    test('SendNotAttempted records no attempt, leaves the head pending, and '
        'sends nothing further in the same pass', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await _enqueueRow(backend, 'fake', eventId: 'e2', sequenceNumber: 2);
      final dest = FakeDestination(
        script: [const SendNotAttempted(reason: 'receiver asked to wait')],
      );

      await drainForTest(dest, registry: registry);

      // e2 (the trail row) was NOT attempted in the same pass.
      expect(dest.sent, hasLength(1));

      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, isNull);
      expect(head.attempts, isEmpty);
    });

    // Verifies: EVS-DEV-destination-retry-budget/C
    // With maxAttempts = 1, a SendNotAttempted does not spend the budget:
    // the next drain call sends with the item's full budget still intact,
    // and a single subsequent SendTransient reaches the bound and wedges —
    // proving the not-attempted outcome recorded no attempt at all.
    test(
      'SendNotAttempted does not spend the retry budget (maxAttempts = 1)',
      () async {
        await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
        final dest = FakeDestination(
          script: [
            const SendNotAttempted(),
            const SendTransient(error: 'still failing'),
          ],
        );
        const policy = SyncPolicy(
          initialBackoff: Duration.zero,
          backoffMultiplier: 1.0,
          maxBackoff: Duration.zero,
          jitterFraction: 0.0,
          maxAttempts: 1,
        );

        await drainForTest(dest, registry: registry, policy: policy);
        final afterFirst = await backend.readFifoHead('fake');
        expect(afterFirst!.attempts, isEmpty);
        expect(afterFirst.finalStatus, isNull);

        await drainForTest(dest, registry: registry, policy: policy);
        final afterSecond = await backend.readFifoHead('fake');
        expect(afterSecond!.attempts, hasLength(1));
        expect(afterSecond.finalStatus, FinalStatus.wedged);
      },
    );

    // When the head row's final_status is FinalStatus.wedged, drain
    // SHALL return without calling Destination.send; the row is NOT
    // re-attempted, and its trail rows are NOT attempted either.
    // Recovery from a wedged head is tombstoneAndRefill.
    test('drain halts when head is wedged, does not call send', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await setStatusForTest(backend, 'fake', e1RowId, FinalStatus.wedged);
      // Script would throw StateError if send() were invoked (see
      // FakeDestination.send); absence of such a throw confirms
      // drain did not call send. We script SendOk defensively so a
      // regression that DID call send would surface as a hasLength(1)
      // mismatch rather than an exhausted-script StateError.
      final dest = FakeDestination(script: [const SendOk()]);

      await drainForTest(dest, registry: registry);

      expect(dest.sent, isEmpty);
      // The wedged row remains wedged, unchanged.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);
    });

    // On the next loop iteration, drain reads the newly-wedged row and
    // halts at the top-of-loop check. Concretely: drain attempts e1
    // exactly once, e1 becomes wedged, e2 (the trail row) is NEVER
    // attempted, and e1 remains at the head of readFifoHead.
    test('SendPermanent marks head wedged; drain halts on '
        'next iteration; trail row is NOT attempted', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final dest = FakeDestination(
        script: [const SendPermanent(error: 'schema-skew')],
      );

      await drainForTest(dest, registry: registry);
      // Exactly one send call — e1. e2 (trail) was NOT attempted.
      expect(dest.sent, hasLength(1));

      // e1 is wedged; e2 is still pre-terminal (final_status null).
      // readFifoHead returns the wedged e1 because wedged is a
      // returnable-but-halting final_status.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);

      // e2 is still pre-terminal.
      final e2 = await backend.readFifoRow('fake', e2RowId);
      expect(e2, isNotNull);
      expect(e2!.finalStatus, isNull);
    });

    // SendTransient at maxAttempts marks the head wedged; drain halts
    // on the next iteration; the trail row is NOT attempted. Uses a
    // tiny maxAttempts policy (=1) with Duration.zero backoffs so a
    // single SendTransient trips the cap.
    test('SendTransient at maxAttempts marks head wedged; '
        'drain halts on next iteration; trail row is NOT attempted', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      const oneAttemptPolicy = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 1,
      );
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drainForTest(
        dest,
        registry: registry,
        clock: () => DateTime.utc(2026, 4, 22, 11),
        policy: oneAttemptPolicy,
      );
      // Exactly one send call — e1 tripped the cap. e2 was NOT attempted.
      expect(dest.sent, hasLength(1));

      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);

      // e2 remains pre-terminal.
      final e2 = await backend.readFifoRow('fake', e2RowId);
      expect(e2, isNotNull);
      expect(e2!.finalStatus, isNull);
    });

    // A transient attempt is appended; entry remains pending; backoff
    // gates the next drain.
    test('SendTransient appends attempt; next drain honors '
        'backoff and does not call send again', () async {
      final firstAttemptAt = DateTime.utc(2026, 4, 22, 10, 0, 5);

      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      // First drain: uses scripted "now" = firstAttemptAt.
      await drainForTest(dest, registry: registry, clock: () => firstAttemptAt);
      expect(dest.sent, hasLength(1));
      // Entry is still pending with one attempt.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.attempts, hasLength(1));
      expect(head.finalStatus, isNull);

      // Re-drain immediately after (clock = firstAttemptAt + 1s). Backoff
      // is 60s from the last attempt; 1s after is well inside the window.
      await drainForTest(
        dest,
        registry: registry,
        clock: () => firstAttemptAt.add(const Duration(seconds: 1)),
      );
      expect(dest.sent, hasLength(1)); // no new send call
    });

    test('after backoff elapses, drain calls send again', () async {
      final firstAttemptAt = DateTime.utc(2026, 4, 22, 10, 0, 5);
      // SyncPolicy.backoffFor(1) is roughly 300s (60 * 5).
      final afterBackoff = firstAttemptAt.add(
        const Duration(seconds: 300 * 2),
      ); // 10 minutes — well past

      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(
        script: [
          const SendTransient(error: 'HTTP 503', httpStatus: 503),
          const SendOk(),
        ],
      );

      await drainForTest(dest, registry: registry, clock: () => firstAttemptAt);
      expect(dest.sent, hasLength(1));

      await drainForTest(dest, registry: registry, clock: () => afterBackoff);
      expect(dest.sent, hasLength(2));
      expect(await backend.readFifoHead('fake'), isNull); // sent
    });

    // Every attempted row's final_status is either null (still
    // pre-terminal), sent, or wedged by the time drain returns. This
    // test uses three successful SendOk results so all three rows are
    // visited without triggering a halt; each send call must append
    // exactly one AttemptResult to its row.
    test('every send call appends an AttemptResult', () async {
      final rowIds = <String>{};
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        rowIds.add(
          await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq),
        );
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drainForTest(
        dest,
        registry: registry,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );

      // Inspect the raw store: each row has exactly 1 attempt.
      final db = backend.databaseForTesting;
      final raw = await StoreRef<int, Map<String, Object?>>(
        'fifo_fake',
      ).find(db);
      expect(raw, hasLength(3));
      for (final r in raw) {
        expect(rowIds.contains(r.value['entry_id']), isTrue);
        expect((r.value['attempts']! as List).length, 1);
      }
    });

    // drain attempts rows in sequence_in_queue order. Three successful
    // SendOks prove the ordering: the payloads land in the destination
    // in the same order the rows were enqueued.
    // Verifies: EVS-PRD-destinations/C
    test('strict FIFO — drain attempts e1, e2, e3 in enqueue order', () async {
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq);
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drainForTest(
        dest,
        registry: registry,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );
      // Three send calls, in the order e1, e2, e3. The WirePayload
      // content reflects the row's event_id JSON encoding; decode it
      // to confirm the drain called send in FIFO order.
      expect(dest.sent, hasLength(3));
      final orderedEventIds = dest.sent
          .map(
            (p) =>
                (jsonDecode(utf8.decode(p.bytes))
                        as Map<String, Object?>)['event_id']
                    as String,
          )
          .toList();
      expect(orderedEventIds, ['e1', 'e2', 'e3']);
    });

    // A wedged head on d1 halts d1 only; d2's queue still drains. (Here we
    // exercise the drain-loop half of the claim by calling drain separately
    // per destination.)
    // Verifies: EVS-PRD-destinations/G
    test(
      'multi-destination independence: wedge on d1 does not block d2',
      () async {
        final clockTime = DateTime.utc(2026, 4, 22, 10);
        final d1RowId = await _enqueueRow(
          backend,
          'd1',
          eventId: 'e1',
          sequenceNumber: 1,
        );
        await _enqueueRow(backend, 'd2', eventId: 'e2', sequenceNumber: 2);
        final d1 = FakeDestination(
          id: 'd1',
          script: [const SendPermanent(error: 'HTTP 400')],
        );
        final d2 = FakeDestination(id: 'd2', script: [const SendOk()]);

        await drainForTest(d1, registry: registry, clock: () => clockTime);
        await drainForTest(d2, registry: registry, clock: () => clockTime);

        expect(d1.sent, hasLength(1));
        expect(d2.sent, hasLength(1));
        // d1's row is wedged (SendPermanent); readFifoHead returns the
        // wedged row so UI surfaces can observe the wedge via this one
        // entry point.
        final d1Head = await backend.readFifoHead('d1');
        expect(d1Head, isNotNull);
        expect(d1Head!.entryId, d1RowId);
        expect(d1Head.finalStatus, FinalStatus.wedged);
        // d2's only row was sent (terminal-passable); no more rows.
        expect(await backend.readFifoHead('d2'), isNull);
      },
    );

    // drain consults the injected policy (not the defaults). Pre-seed
    // attempts[] to one below the injected cap; the next transient
    // attempt should wedge the entry.
    test('drain honors injected policy.maxAttempts', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );

      const smallPolicy = SyncPolicy(
        initialBackoff: Duration(seconds: 60),
        backoffMultiplier: 5.0,
        maxBackoff: Duration(hours: 2),
        jitterFraction: 0.1,
        maxAttempts: 3, // smaller cap than defaults.maxAttempts (20)
      );
      // Pre-load attempts: smallPolicy.maxAttempts - 1 transient records.
      for (var i = 0; i < smallPolicy.maxAttempts - 1; i++) {
        await appendAttemptForTest(
          backend,
          'fake',
          e1RowId,
          _attemptResultFactory(i),
        );
      }
      // Clock well past any backoff window.
      final longAfter = DateTime.utc(2027, 1, 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drainForTest(
        dest,
        registry: registry,
        clock: () => longAfter,
        policy: smallPolicy,
      );
      expect(dest.sent, hasLength(1));
      // With a cap of 3 and 3 total attempts, the entry is wedged.
      // readFifoHead returns the wedged row (it is a halt signal to
      // drain, not a skip-past).
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);
    });

    // Sanity-check that omitting `policy` reads the defaults (20 attempts).
    test('null policy falls back to SyncPolicy.defaults', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      // Pre-load 2 attempts: well below the default cap of 20, so a
      // transient should leave the entry pending (head still present).
      for (var i = 0; i < 2; i++) {
        await appendAttemptForTest(
          backend,
          'fake',
          e1RowId,
          _attemptResultFactory(i),
        );
      }
      final longAfter = DateTime.utc(2027, 1, 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drainForTest(dest, registry: registry, clock: () => longAfter);
      expect(dest.sent, hasLength(1));
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.finalStatus, isNull);
    });

    // An error raised by the application-supplied delivery implementation is
    // a failed attempt: the outcome is recorded, the row stays at the head,
    // and the failure does not reach the caller of the drain pass.
    // Verifies: EVS-PRD-destinations/H+I+J
    test('drain treats a thrown exception as SendTransient and records an '
        'attempt', () async {
      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = _ThrowingDestination();

      await drainForTest(
        dest,
        registry: registry,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );

      // Entry is still pending, with one attempt whose outcome is
      // "transient".
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.attempts, hasLength(1));
      expect(head.attempts.first.outcome, 'transient');
    });

    // A native item's delivery is rebuilt from its envelope metadata and
    // the events its `event_ids` name; the byte-identical resend of a
    // delivery is covered by the delivery channel drain scenarios. An item
    // whose `event_ids` name a missing event throws StateError: the FIFO
    // row outlives its underlying event log entry, and drain refuses to
    // send a partial or incorrect delivery.
    test('drain on native row with missing event throws '
        'StateError', () async {
      const init = AutomationInitiator(service: 'drain-test');
      final dest = NativeDestination(
        filter: const SubscriptionFilter(includeSystemEvents: true),
      );
      await registry.addDestination(dest, initiator: init);
      await registry.setStartDate(dest.id, DateTime.utc(2000), initiator: init);
      await fillForTest(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2100),
      );
      final queued = await backend.readFifoHead(dest.id);
      expect(queued, isNotNull, reason: 'the fill enqueued an item');
      final missing = queued!.eventIds.first;

      // Surgically delete the event from the origin event store
      // (bypasses the append-only API; test-only mutation that simulates
      // a torn / corrupted event log). The `sembast.Finder` prefix
      // disambiguates from `flutter_test`'s widget-tree `Finder`.
      final db = backend.databaseForTesting;
      final eventStore = intMapStoreFactory.store('events');
      final record = (await eventStore.find(
        db,
        finder: sembast.Finder(
          filter: sembast.Filter.equals('event_id', missing),
          limit: 1,
        ),
      )).single;
      await eventStore.record(record.key).delete(db);

      await expectLater(
        drainForTest(dest, registry: registry),
        throwsA(isA<StateError>()),
      );
      // The drain refused before sending anything and recorded no attempt:
      // the row is still the pending head with an empty attempt history.
      expect(dest.sent, isEmpty);
      final head = await backend.readFifoHead(dest.id);
      expect(head, isNotNull);
      expect(head!.entryId, queued.entryId);
      expect(head.attempts, isEmpty);
      expect(head.finalStatus, isNull);
    });
  });

  // A queue on a file-backed database outlives the backend that wrote it:
  // rows enqueued before the backend closes are drained, in order, by a
  // fresh backend opened over the same file.
  // Verifies: EVS-PRD-destinations/D
  test('queued rows survive closing and reopening the database', () async {
    final dir = await Directory.systemTemp.createTemp('drain-restart-');
    addTearDown(() => dir.delete(recursive: true));
    final path = '${dir.path}/queue.db';

    final before = SembastBackend(
      database: await databaseFactoryIo.openDatabase(path),
    );
    await buildAuditedRegistryDeps(before);
    await _enqueueRow(before, 'fake', eventId: 'e1', sequenceNumber: 1);
    await _enqueueRow(before, 'fake', eventId: 'e2', sequenceNumber: 2);
    await before.close();

    final after = SembastBackend(
      database: await databaseFactoryIo.openDatabase(path),
    );
    addTearDown(after.close);
    final deps = await buildAuditedRegistryDeps(after);
    final registry = DestinationRegistry(eventStore: deps.eventStore);

    final dest = FakeDestination(script: [const SendOk(), const SendOk()]);
    await drainForTest(dest, registry: registry);

    expect(
      dest.sent
          .map(
            (p) =>
                (jsonDecode(utf8.decode(p.bytes))
                        as Map<String, Object?>)['event_id']
                    as String,
          )
          .toList(),
      ['e1', 'e2'],
    );
    expect(await after.readFifoHead('fake'), isNull);
  });

  group('budgetSpent()', () {
    final t0 = DateTime.utc(2027, 1, 1);
    AttemptResult at(int seconds, {String outcome = 'transient'}) =>
        AttemptResult(
          attemptedAt: t0.add(Duration(seconds: seconds)),
          outcome: outcome,
        );

    // Verifies: EVS-DEV-destination-retry-budget/A
    // the attempt bound alone spends the
    //   budget, whatever the recorded times.
    test('the attempt bound spends the budget on its own', () {
      const policy = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 2,
        maxRetryTime: Duration(days: 1),
      );
      expect(budgetSpent([at(0), at(1)], policy, Duration.zero), isTrue);
      expect(budgetSpent([at(0)], policy, Duration.zero), isFalse);
    });

    // Verifies: EVS-DEV-destination-retry-budget/A
    // with no recorded attempt the time
    //   bound is never reached, whatever its value.
    test('an empty history never spends the time bound', () {
      const policy = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 1000000,
        maxRetryTime: Duration.zero,
      );
      expect(budgetSpent(const [], policy, Duration.zero), isFalse);
    });

    // Verifies: EVS-DEV-destination-retry-budget/A
    // each gap is capped at the retry
    //   curve's longest allowed delay after the earlier attempt plus the
    //   cadence, and the sum of the capped gaps decides whether the time
    //   bound is spent.
    test('gaps are capped at the curve delay plus the cadence', () {
      const policy = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 1.0,
        maxBackoff: Duration(seconds: 10),
        jitterFraction: 0.0,
        maxAttempts: 1000000,
        maxRetryTime: Duration(seconds: 50),
      );
      const cadence = Duration(seconds: 5);
      // Two gaps of 12 s, each capped at 10 + 5 = 15 s: nothing is
      // capped down, so the raw sum (24 s) governs and stays under 50 s.
      expect(budgetSpent([at(0), at(12), at(24)], policy, cadence), isFalse);
      // A single gap of 40 s, capped down to 15 s: under the 50 s bound.
      expect(budgetSpent([at(0), at(40)], policy, cadence), isFalse);
      // Four gaps of 15 s, each capped at exactly 15 s: the sum (60 s)
      // reaches the 50 s bound.
      expect(
        budgetSpent([at(0), at(15), at(30), at(45), at(60)], policy, cadence),
        isTrue,
      );
    });
  });
}

/// Scripted AttemptResult for pre-loading transient history.
AttemptResult _attemptResultFactory(int i) => AttemptResult(
  attemptedAt: DateTime.utc(2026, 1, 1).add(Duration(minutes: i)),
  outcome: 'transient',
  errorMessage: 'pre-seeded transient #$i',
  httpStatus: 503,
);

class _ThrowingDestination extends FakeDestination {
  _ThrowingDestination() : super(id: 'fake');

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    throw StateError('boom');
  }
}
