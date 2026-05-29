// Verifies: EVS-PRD-destinations/B (per-destination filter — fill_cursor
//   advances past permanently-rejected events (subscription mismatch,
//   client_timestamp < startDate) but NOT past events deferred by the upper
//   bound (client_timestamp > min(endDate, now())). The upper bound is
//   non-monotonic because endDate is mutable; the window has not yet opened
//   (future startDate) or is malformed (startDate > endDate). When endDate is
//   in the past with startDate <= endDate, fillBatch still scans in-window
//   events.)
// Verifies: EVS-PRD-destinations/C (FIFO order — cursor advances to
//   batch.last.sequenceNumber so deferred events remain re-evaluable and are
//   enqueued in sequence order when the window widens)
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

Future<StoredEvent> _appendEvent(
  SembastBackend backend, {
  required String eventId,
  required DateTime clientTimestamp,
  String entryType = 'epistaxis_event',
  String aggregateId = 'agg-1',
}) {
  return backend.transaction((txn) async {
    final seq = await backend.nextSequenceNumber(txn);
    final event = StoredEvent(
      key: 0,
      eventId: eventId,
      aggregateId: aggregateId,
      aggregateType: 'note',
      entryType: entryType,
      entryTypeVersion: 1,
      libFormatVersion: 1,
      eventType: 'finalized',
      sequenceNumber: seq,
      data: const <String, dynamic>{},
      metadata: const <String, dynamic>{},
      initiator: const UserInitiator('u'),
      clientTimestamp: clientTimestamp,
      eventHash: 'hash-$eventId',
    );
    await backend.appendEvent(txn, event);
    return event;
  });
}

void main() {
  late SembastBackend backend;
  var dbCounter = 0;

  setUp(() async {
    dbCounter += 1;
    backend = await _openBackend('fill-batch-cursor-defers-$dbCounter.db');
  });

  tearDown(() async => backend.close());

  group('cursor advance respects rejection reason', () {
    // An event whose client_timestamp exceeds the current upper bound
    // (min(endDate, now())) is deferred. fill_cursor SHALL NOT advance
    // past it. When the upper bound widens (clock advances or endDate
    // moves forward), the deferred event becomes eligible and is
    // enqueued on a subsequent fillBatch call.
    test('deferred event is not cursor-skipped; later fillBatch '
        'with widened upper enqueues it', () async {
      // T0 = the test's reference "now". endDate is set far in the future
      // so the upper bound is now (line 99-101 of fill_batch.dart picks
      // now when endDate > now). startDate is in the past so events from
      // the past pass the lower bound.
      final t0 = DateTime.utc(2026, 4, 15, 12);
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 1, 1),
        endDate: DateTime.utc(2099, 1, 1),
      );

      // e1: in-window at t0 (client_timestamp <= upper = t0).
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );
      // e2: deferred at t0 (client_timestamp > upper = t0). Future event.
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        clientTimestamp: DateTime.utc(2026, 4, 20),
      );

      final dest = FakeDestination(id: 'fake');

      // First tick at t0: upper = t0, e1 in-window, e2 deferred.
      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      // e1 enqueued, cursor at e1.seq, NOT past e2.
      final fifoAfterFirst = await backend.listFifoEntries('fake');
      expect(fifoAfterFirst, hasLength(1));
      expect(fifoAfterFirst.single.eventIds, equals([e1.eventId]));
      final cursorAfterFirst = await backend.readFillCursor('fake');
      expect(
        cursorAfterFirst,
        e1.sequenceNumber,
        reason: 'cursor must NOT advance past the deferred e2',
      );
      expect(
        cursorAfterFirst,
        lessThan(e2.sequenceNumber),
        reason: 'deferred event has higher seq than cursor; re-evaluable',
      );

      // Advance the clock past e2's client_timestamp. Now e2 is in-window.
      final t1 = DateTime.utc(2026, 5, 1);
      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t1,
      );

      final fifoAfterSecond = await backend.listFifoEntries('fake');
      expect(
        fifoAfterSecond,
        hasLength(2),
        reason:
            'previously-deferred e2 must be picked up after upper widens '
            '',
      );
      expect(
        fifoAfterSecond.map((r) => r.eventIds.single).toList(),
        equals([e1.eventId, e2.eventId]),
        reason: 'order preserved',
      );
      expect(
        await backend.readFillCursor('fake'),
        e2.sequenceNumber,
        reason: 'cursor advances to e2 once it is enqueued',
      );
    });

    // When each candidate fails the time-window or subscription filter,
    // the cursor advances past permanently-rejected events (subscription
    // mismatch) but stops at the first deferred event (upper-bound
    // rejection).
    test('inWindow.isEmpty path — cursor advances past permanent '
        'rejection but stops at first deferred event', () async {
      final t0 = DateTime.utc(2026, 4, 15, 12);
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 1, 1),
        endDate: DateTime.utc(2099, 1, 1),
      );

      // e1: subscription-rejected (entry_type 'other' not in filter).
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        entryType: 'other',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );
      // e2: deferred (client_timestamp > upper = t0).
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        entryType: 'epistaxis_event',
        clientTimestamp: DateTime.utc(2026, 4, 20),
      );
      // e3: would be in-window, but is past e2 in seq order. Without
      //     the deferred-stop rule, the cursor might advance past e3 too.
      final e3 = await _appendEvent(
        backend,
        eventId: 'e3',
        entryType: 'epistaxis_event',
        clientTimestamp: DateTime.utc(2026, 4, 12),
      );

      final dest = FakeDestination(
        id: 'bug-scenario',
        filter: const SubscriptionFilter(entryTypes: {'epistaxis_event'}),
      );

      // First tick: candidates [e1, e2, e3].
      //   e1: subscription-reject (permanent).
      //   e2: deferred (upper-bound).
      //   e3: in-window (subscription matches, client_timestamp <= upper).
      // Walk stops at e2 (first deferred). e1 contributes to cursor
      // advance (permanent), e2 and e3 stay deferred.
      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      final fifo1 = await backend.listFifoEntries('bug-scenario');
      expect(
        fifo1,
        isEmpty,
        reason:
            'no in-window events promoted: walk stopped at e2 (deferred) '
            'before reaching e3',
      );
      final cursor1 = await backend.readFillCursor('bug-scenario');
      expect(
        cursor1,
        e1.sequenceNumber,
        reason:
            'cursor advances past e1 (permanent rejection) and stops at '
            'e2 (deferred)',
      );
      expect(
        cursor1,
        lessThan(e2.sequenceNumber),
        reason: 'cursor must NOT advance past deferred e2',
      );

      // Advance clock past e2; e3 is also in-window now (was already).
      final t1 = DateTime.utc(2026, 5, 1);
      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t1,
      );

      final fifo2 = await backend.listFifoEntries('bug-scenario');
      // e2 enqueued first (lower seq); next tick will get e3.
      expect(
        fifo2.map((r) => r.eventIds.single).toList(),
        equals([e2.eventId]),
        reason:
            'first tick after upper widens: e2 enqueued (FakeDestination '
            'batchCapacity=1 limits one batch per tick).',
      );

      // Continue draining.
      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t1,
      );
      final fifo3 = await backend.listFifoEntries('bug-scenario');
      expect(
        fifo3.map((r) => r.eventIds.single).toList(),
        equals([e2.eventId, e3.eventId]),
        reason:
            'second tick: e3 enqueued. Both previously-deferred events '
            'are recovered once the upper bound widens.',
      );
    });

    // Inching endDate forward while still in the past lets each tick
    // pick up a slice of previously-deferred events. Cursor advances
    // incrementally; events are not lost. Uses batchCapacity=100 so
    // each fillBatch call drains all in-window events in one go.
    test('throttle scheme — endDate inched forward while past '
        'enqueues previously-deferred events incrementally', () async {
      final clockNow = DateTime.utc(2026, 6, 1);
      // Five events spanning Jan-May.
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 1, 10),
      );
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        clientTimestamp: DateTime.utc(2026, 2, 10),
      );
      final e3 = await _appendEvent(
        backend,
        eventId: 'e3',
        clientTimestamp: DateTime.utc(2026, 3, 10),
      );
      final e4 = await _appendEvent(
        backend,
        eventId: 'e4',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );
      final e5 = await _appendEvent(
        backend,
        eventId: 'e5',
        clientTimestamp: DateTime.utc(2026, 5, 10),
      );

      // batchCapacity=100 so each fillBatch call drains every in-window
      // event for the current schedule, simulating SyncCycle ticking
      // multiple times within one logical "step."
      final dest = FakeDestination(id: 'throttle', batchCapacity: 100);

      // Step 1: endDate = end of February. Should enqueue e1, e2 only.
      await fillBatch(
        dest,
        backend: backend,
        schedule: DestinationSchedule(
          startDate: DateTime.utc(2026, 1, 1),
          endDate: DateTime.utc(2026, 2, 28),
        ),
        clock: () => clockNow,
      );
      var fifo = await backend.listFifoEntries('throttle');
      expect(
        fifo.expand((r) => r.eventIds).toList(),
        equals([e1.eventId, e2.eventId]),
      );
      expect(
        await backend.readFillCursor('throttle'),
        e2.sequenceNumber,
        reason: 'cursor at e2; e3+ still deferred',
      );

      // Step 2: inch endDate to end of April. Should pick up e3, e4.
      await fillBatch(
        dest,
        backend: backend,
        schedule: DestinationSchedule(
          startDate: DateTime.utc(2026, 1, 1),
          endDate: DateTime.utc(2026, 4, 30),
        ),
        clock: () => clockNow,
      );
      fifo = await backend.listFifoEntries('throttle');
      expect(
        fifo.expand((r) => r.eventIds).toList(),
        equals([e1.eventId, e2.eventId, e3.eventId, e4.eventId]),
        reason: 'e3 and e4 picked up after endDate widens',
      );

      // Step 3: widen endDate to far future. e5 should land.
      await fillBatch(
        dest,
        backend: backend,
        schedule: DestinationSchedule(
          startDate: DateTime.utc(2026, 1, 1),
          endDate: DateTime.utc(2099, 1, 1),
        ),
        clock: () => clockNow,
      );
      fifo = await backend.listFifoEntries('throttle');
      expect(
        fifo.expand((r) => r.eventIds).toList(),
        equals([e1.eventId, e2.eventId, e3.eventId, e4.eventId, e5.eventId]),
        reason: 'all events ultimately enqueued',
      );
    });

    // Events not matching the destination's SubscriptionFilter are
    // permanently rejected. The cursor MAY advance past them (filter
    // is stable).
    test('subscription-rejected events advance cursor', () async {
      final t0 = DateTime.utc(2026, 4, 15, 12);
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 1, 1),
        endDate: DateTime.utc(2099, 1, 1),
      );

      // e1: subscription-rejected (entry_type 'other').
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        entryType: 'other',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );
      // e2: in-window, in subscription.
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        entryType: 'epistaxis_event',
        clientTimestamp: DateTime.utc(2026, 4, 11),
      );

      final dest = FakeDestination(
        id: 'sub',
        filter: const SubscriptionFilter(entryTypes: {'epistaxis_event'}),
      );

      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      final fifo = await backend.listFifoEntries('sub');
      expect(fifo.expand((r) => r.eventIds).toList(), equals([e2.eventId]));
      // Cursor advanced past e1 (permanent subscription rejection) AND
      // through e2 (promoted). Cursor at e2.seq.
      expect(await backend.readFillCursor('sub'), e2.sequenceNumber);
      expect(
        await backend.readFillCursor('sub'),
        greaterThan(e1.sequenceNumber),
        reason: 'cursor advanced past subscription-rejected e1',
      );
    });

    // Events below startDate are permanently rejected for the current
    // invocation; the cursor MAY advance past them. Under monotonic-
    // backward semantics, a later setStartDate(earlier) re-promotes the
    // gap window via runGapReplay (independent of fill_cursor); fillBatch
    // does not need to keep these events re-evaluable.
    test('events below startDate advance cursor', () async {
      final t0 = DateTime.utc(2026, 4, 15, 12);
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 4, 1),
        endDate: DateTime.utc(2099, 1, 1),
      );

      // e1: backdated (client_timestamp before startDate).
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 3, 31, 23),
      );
      // e2: in-window.
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        clientTimestamp: DateTime.utc(2026, 4, 5),
      );

      final dest = FakeDestination(id: 'lower');

      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      final fifo = await backend.listFifoEntries('lower');
      expect(fifo.expand((r) => r.eventIds).toList(), equals([e2.eventId]));
      expect(await backend.readFillCursor('lower'), e2.sequenceNumber);
      expect(
        await backend.readFillCursor('lower'),
        greaterThan(e1.sequenceNumber),
        reason:
            'cursor advanced past startDate-rejected e1 (lower bound is '
            'permanent for fillBatch; -C backward moves are '
            'handled by runGapReplay)',
      );
    });
  });

  group('window-state short-circuits', () {
    // A future startDate causes fillBatch to return immediately. No
    // FIFO writes, no cursor advance.
    test('future startDate causes immediate return without scan', () async {
      final t0 = DateTime.utc(2026, 4, 15);

      // Append events that would otherwise be processed.
      await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );

      final dest = FakeDestination(id: 'future-start');
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2027, 1, 1), // future
      );

      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      expect(await backend.readFifoHead('future-start'), isNull);
      expect(await backend.readFillCursor('future-start'), -1);
      expect(
        dest.transformCalls,
        0,
        reason: 'transform must NOT be called when window not yet open',
      );
    });

    // A malformed window (startDate > endDate) causes fillBatch to
    // return immediately.
    test('malformed window (startDate > endDate) early-returns', () async {
      final t0 = DateTime.utc(2026, 4, 15);

      await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 4, 10),
      );

      final dest = FakeDestination(id: 'malformed');
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 5, 1),
        endDate: DateTime.utc(2026, 3, 1), // before startDate
      );

      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      expect(await backend.readFifoHead('malformed'), isNull);
      expect(await backend.readFillCursor('malformed'), -1);
      expect(dest.transformCalls, 0);
    });

    // For a closed-past window (startDate <= endDate < now), fillBatch
    // SHALL still scan to enqueue any in-window events not yet promoted.
    // This is the throttle scheme's foundation: closed-past does not
    // freeze processing, only not-yet-opened / malformed cases do.
    test('closed-past window still scans and enqueues in-window '
        'events', () async {
      final t0 = DateTime.utc(2026, 6, 1);

      // Three events: two in-window, one past endDate.
      final e1 = await _appendEvent(
        backend,
        eventId: 'e1',
        clientTimestamp: DateTime.utc(2026, 1, 10),
      );
      final e2 = await _appendEvent(
        backend,
        eventId: 'e2',
        clientTimestamp: DateTime.utc(2026, 2, 10),
      );
      await _appendEvent(
        backend,
        eventId: 'e3',
        clientTimestamp: DateTime.utc(2026, 5, 10),
      );

      final dest = FakeDestination(id: 'closed-past', batchCapacity: 100);
      final schedule = DestinationSchedule(
        startDate: DateTime.utc(2026, 1, 1),
        endDate: DateTime.utc(2026, 3, 1), // past relative to t0
      );

      await fillBatch(
        dest,
        backend: backend,
        schedule: schedule,
        clock: () => t0,
      );

      final fifo = await backend.listFifoEntries('closed-past');
      expect(
        fifo.expand((r) => r.eventIds).toList(),
        equals([e1.eventId, e2.eventId]),
        reason:
            'closed-past window still processes in-window events; only '
            'e3 (past endDate) is deferred',
      );
      expect(await backend.readFillCursor('closed-past'), e2.sequenceNumber);
    });
  });
}
