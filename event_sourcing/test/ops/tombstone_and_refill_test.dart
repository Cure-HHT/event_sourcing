// Verifies: EVS-PRD-destinations/C+D+F
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';
import '../test_support/queue_test_support.dart';
import '../test_support/registry_with_audit.dart';

const Initiator _testInit = AutomationInitiator(service: 'test-bootstrap');

/// Fresh in-memory SembastBackend per test.
Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

/// Read every FIFO row on [destinationId] as raw maps, in
/// `sequence_in_queue` ascending order. Lets tests assert the raw
/// Sembast state without round-tripping through the FifoEntry type.
Future<List<Map<String, Object?>>> _readAllFifoRows(
  SembastBackend backend,
  String destinationId,
) async {
  final db = backend.databaseForTesting;
  final records =
      await sembast.StoreRef<int, Map<String, Object?>>(
        'fifo_$destinationId',
      ).find(
        db,
        finder: sembast.Finder(
          sortOrders: [sembast.SortOrder('sequence_in_queue')],
        ),
      );
  return records.map((r) => Map<String, Object?>.from(r.value)).toList();
}

/// Append a single event to the event log with a reserved sequence
/// number. Used by the fillBatch-reintegration test so
/// there are real events for fillBatch to re-promote after the
/// tombstone + trail sweep.
Future<StoredEvent> _appendEvent(
  SembastBackend backend, {
  required String eventId,
  required DateTime clientTimestamp,
}) {
  return backend.transaction((txn) async {
    final seq = await backend.nextSequenceNumber(txn);
    final event = StoredEvent(
      key: 0,
      eventId: eventId,
      aggregateId: 'agg-1',
      aggregateType: 'note',
      entryType: 'epistaxis_event',
      entryTypeVersion: const EntryTypeVersion(1, 0),
      libFormatVersion: const DataFormatVersion(2, 0),
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

/// Sentinel passed as [_HeadKind] to `_seedFifo` to request a
/// null-final_status head row (a still-pending drain candidate).
enum _HeadKind {
  /// Do not seed a head row. Useful for -A rejection cases
  /// where the only seeded row is a `sent` row (which readFifoHead
  /// skips, so the FIFO effectively has no head).
  none,

  /// Head row is left pre-terminal (final_status == null).
  pending,

  /// Head row transitions to FinalStatus.wedged with one seeded
  /// attempt so -B can assert attempts[] preservation.
  wedged,
}

/// Set up a destination with a seeded FIFO: an optional sent prefix
/// (rows 1..[sentCount]), an optional head row (kind controlled by
/// [headKind]), and an optional trail of null-status rows following
/// the head. Returns the head row's `entry_id` (null when
/// [headKind] == _HeadKind.none) so tests can pass it to
/// `tombstoneAndRefill`.
///
/// Destination is active (no endDate) so we exercise
/// without the unjam deactivation requirement.
Future<
  ({
    SembastBackend backend,
    DestinationRegistry registry,
    FakeDestination destination,
    String? headEntryId,
  })
>
_seedFifo(
  SembastBackend backend, {
  int sentCount = 0,
  _HeadKind headKind = _HeadKind.pending,
  int trailCount = 0,
}) async {
  final deps = await buildAuditedRegistryDeps(backend);
  final registry = DestinationRegistry(eventStore: deps.eventStore);
  final destination = FakeDestination(id: 'tombstone-dest');
  await registry.addDestination(destination, initiator: _testInit);
  await registry.setStartDate(
    destination.id,
    DateTime.utc(2026, 1, 1),
    initiator: _testInit,
  );

  var seq = 0;
  // Sent prefix rows.
  for (var i = 0; i < sentCount; i++) {
    seq += 1;
    final row = await enqueueSingle(
      backend,
      destination.id,
      eventId: 'sent-e$seq',
      sequenceNumber: seq,
    );
    await setStatusForTest(
      backend,
      destination.id,
      row.entryId,
      FinalStatus.sent,
    );
  }
  String? headEntryId;
  if (headKind != _HeadKind.none) {
    seq += 1;
    final row = await enqueueSingle(
      backend,
      destination.id,
      eventId: 'head-e$seq',
      sequenceNumber: seq,
    );
    headEntryId = row.entryId;
    if (headKind == _HeadKind.wedged) {
      // Seed one attempt so -B can assert attempts[] is
      // preserved across the wedged -> tombstoned flip.
      await appendAttemptForTest(
        backend,
        destination.id,
        headEntryId,
        AttemptResult(
          attemptedAt: DateTime.utc(2026, 4, 22, 12, seq),
          outcome: 'permanent',
          errorMessage: 'simulated failure',
          httpStatus: 500,
        ),
      );
      await setStatusForTest(
        backend,
        destination.id,
        headEntryId,
        FinalStatus.wedged,
      );
    }
  }
  // Trail rows (all pre-terminal / null final_status).
  for (var i = 0; i < trailCount; i++) {
    seq += 1;
    await enqueueSingle(
      backend,
      destination.id,
      eventId: 'trail-e$seq',
      sequenceNumber: seq,
    );
  }
  // fill_cursor tracks the last enqueued row's sequence_number so the
  // rewind is observable.
  if (seq > 0) {
    await writeFillCursorForTest(backend, destination.id, seq);
  }
  return (
    backend: backend,
    registry: registry,
    destination: destination,
    headEntryId: headEntryId,
  );
}

void main() {
  group('tombstoneAndRefill()', () {
    late SembastBackend backend;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('tombstone-$dbCounter.db');
    });

    tearDown(() async {
      await backend.close();
    });

    // FIFO. A non-head row (e.g., a pre-terminal row behind another
    // pre-terminal row) is rejected with ArgumentError before any
    // transactional mutation runs.
    test('throws ArgumentError when fifoRowId is not the head', () async {
      final setup = await _seedFifo(
        backend,
        headKind: _HeadKind.pending, // head is pre-terminal at seq_in_queue=1
        trailCount: 2,
      );
      // Trail rows exist at seq_in_queue 2 and 3; target the last
      // trail row, which is not the head.
      final rows = await _readAllFifoRows(backend, setup.destination.id);
      final trailRow = rows.last;
      final trailEntryId = trailRow['entry_id']! as String;
      expect(trailEntryId, isNot(equals(setup.headEntryId)));

      await expectLater(
        setup.registry.tombstoneAndRefill(
          setup.destination.id,
          trailEntryId,
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
      // Nothing was mutated.
      final after = await _readAllFifoRows(backend, setup.destination.id);
      expect(after.length, rows.length);
      for (final r in after) {
        expect(r['final_status'], isNull);
      }
    });

    // (readFifoHead skips it, so it can't be the head). Rejected with
    // ArgumentError.
    test('throws ArgumentError when target is sent', () async {
      final setup = await _seedFifo(
        backend,
        sentCount: 1,
        headKind: _HeadKind.none,
      );
      final rows = await _readAllFifoRows(backend, setup.destination.id);
      final sentEntryId = rows.single['entry_id']! as String;
      expect(rows.single['final_status'], FinalStatus.sent.toJson());
      await expectLater(
        setup.registry.tombstoneAndRefill(
          setup.destination.id,
          sentEntryId,
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
    });

    // (readFifoHead skips it).
    test('throws ArgumentError when target is tombstoned', () async {
      // Seed a FIFO with a wedged head, tombstone it, then try to
      // tombstone again — the second call sees the first call's
      // rewind and no remaining head pointing to the tombstoned row.
      final setup = await _seedFifo(backend, headKind: _HeadKind.wedged);
      final headEntryId = setup.headEntryId!;
      await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      // The tombstoned row is still in the store but readFifoHead
      // skips it; a second tombstone call against it must reject.
      await expectLater(
        setup.registry.tombstoneAndRefill(
          setup.destination.id,
          headEntryId,
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
    });

    // ArgumentError (readFifoHead either returns null or a different
    // entryId; either way target-is-head fails).
    test('throws ArgumentError when row does not exist', () async {
      final setup = await _seedFifo(backend, headKind: _HeadKind.wedged);
      await expectLater(
        setup.registry.tombstoneAndRefill(
          setup.destination.id,
          'does-not-exist',
          initiator: _testInit,
        ),
        throwsArgumentError,
      );
    });

    // tombstoned and the row's attempts[] is preserved verbatim.
    test('wedged head transitions to tombstoned; '
        'attempts preserved', () async {
      final setup = await _seedFifo(backend, headKind: _HeadKind.wedged);
      final headEntryId = setup.headEntryId!;
      final before = await _readAllFifoRows(backend, setup.destination.id);
      final beforeHead = before.single;
      expect(beforeHead['final_status'], FinalStatus.wedged.toJson());
      final originalAttempts = (beforeHead['attempts'] as List).toList();
      expect(originalAttempts.length, 1);

      final result = await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      expect(result.rowId, headEntryId);

      final after = await _readAllFifoRows(backend, setup.destination.id);
      expect(after.length, 1);
      final afterHead = after.single;
      expect(afterHead['final_status'], FinalStatus.tombstoned.toJson());
      // attempts[] preserved byte-for-byte.
      final afterAttempts = (afterHead['attempts'] as List).toList();
      expect(afterAttempts, equals(originalAttempts));
      // entry_id and sequence_in_queue unchanged.
      expect(afterHead['entry_id'], beforeHead['entry_id']);
      expect(afterHead['sequence_in_queue'], beforeHead['sequence_in_queue']);
    });

    // Verifies: EVS-PRD-destinations/M
    // recovery of a pending head is refused:
    //   afterwards the row is pending, the fill cursor unchanged, the trail
    //   intact, and no recovery event is appended.
    test('a pending head is refused; nothing changes', () async {
      final setup = await _seedFifo(
        backend,
        headKind: _HeadKind.pending,
        trailCount: 2,
      );
      final headEntryId = setup.headEntryId!;
      final before = await _readAllFifoRows(backend, setup.destination.id);
      final cursorBefore = await backend.readFillCursor(setup.destination.id);

      await expectLater(
        setup.registry.tombstoneAndRefill(
          setup.destination.id,
          headEntryId,
          initiator: _testInit,
        ),
        throwsStateError,
      );

      expect(await _readAllFifoRows(backend, setup.destination.id), before);
      expect(before.first['final_status'], isNull);
      expect(await backend.readFillCursor(setup.destination.id), cursorBefore);
      final recoveries = (await backend.findAllEvents()).where(
        (e) => e.entryType == kDestinationWedgeRecoveredEntryType,
      );
      expect(recoveries, isEmpty);
    });

    // is strictly greater than the target's is deleted from the FIFO.
    // The target row itself is not deleted (it is the tombstone).
    test('trail null rows after target are deleted', () async {
      final setup = await _seedFifo(
        backend,
        headKind: _HeadKind.wedged,
        trailCount: 3,
      );
      final headEntryId = setup.headEntryId!;
      final before = await _readAllFifoRows(backend, setup.destination.id);
      expect(before.length, 4); // 1 wedged head + 3 trail

      final result = await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      expect(result.deletedTrailCount, 3);

      final after = await _readAllFifoRows(backend, setup.destination.id);
      // Only the tombstoned head remains.
      expect(after.length, 1);
      expect(after.single['entry_id'], headEntryId);
      expect(after.single['final_status'], FinalStatus.tombstoned.toJson());
    });

    // sequence_in_queue gap visible in the store. With sentCount=1
    // (seq_in_queue 1), head (2) + trail (3, 4, 5), after tombstone:
    // surviving rows have seq_in_queue {1, 2} — the gap [3..5] is
    // never filled.
    test('sequence_in_queue gap is visible after trail delete', () async {
      final setup = await _seedFifo(
        backend,
        sentCount: 1,
        headKind: _HeadKind.wedged,
        trailCount: 3,
      );
      final headEntryId = setup.headEntryId!;
      await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      final after = await _readAllFifoRows(backend, setup.destination.id);
      final seqs = after.map((r) => r['sequence_in_queue']! as int).toList();
      expect(seqs, [1, 2]); // sent row + tombstoned head; trail is gone.
    });

    // target.event_id_range.first_seq - 1. With sentCount=2 (seq 1,2)
    // and a wedged head at seq 3, the rewind target is 2 (=3-1).
    test('fill_cursor rewinds to target.first_seq - 1', () async {
      final setup = await _seedFifo(
        backend,
        sentCount: 2,
        headKind: _HeadKind.wedged,
        trailCount: 2,
      );
      final headEntryId = setup.headEntryId!;
      // Pre-tombstone cursor advanced to last enqueued seq (5).
      expect(await backend.readFillCursor(setup.destination.id), 5);

      final result = await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      expect(result.rewoundTo, 2);
      expect(await backend.readFillCursor(setup.destination.id), 2);
    });

    // rewind target is target.first_seq - 1, which is 0 when the head
    // sits at seq 1 (pre-start sentinel is -1, but the canonical
    // formula is first_seq - 1, NOT max(sent) ?? -1).
    test('fill_cursor rewinds correctly when no sent rows exist', () async {
      final setup = await _seedFifo(
        backend,
        headKind: _HeadKind.wedged,
        trailCount: 2,
      );
      final headEntryId = setup.headEntryId!;
      // Pre-tombstone cursor at 3 (last enqueued seq).
      expect(await backend.readFillCursor(setup.destination.id), 3);

      final result = await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      // head sits at seq 1 (no sent prefix), so rewoundTo = 1 - 1 = 0.
      expect(result.rewoundTo, 0);
      expect(await backend.readFillCursor(setup.destination.id), 0);
    });

    // rowId, deletedTrailCount, and rewoundTo.
    test('returns TombstoneAndRefillResult with correct fields', () async {
      final setup = await _seedFifo(
        backend,
        sentCount: 2,
        headKind: _HeadKind.wedged,
        trailCount: 4,
      );
      final headEntryId = setup.headEntryId!;

      final result = await setup.registry.tombstoneAndRefill(
        setup.destination.id,
        headEntryId,
        initiator: _testInit,
      );
      expect(result, isA<TombstoneAndRefillResult>());
      expect(result.rowId, headEntryId);
      expect(result.deletedTrailCount, 4);
      expect(result.rewoundTo, 2); // head first_seq = 3, so 3-1 = 2
    });

    // tombstoneAndRefill, the next fillBatch re-promotes every event
    // covered by the tombstoned target AND by its trail into fresh
    // FIFO rows. v4-UUID `entry_id`s ensure the tombstoned audit row
    // and the fresh re-promotion rows coexist even when they cover the
    // same event_ids — their identifiers never collide.
    //
    // Setup: 9 events e1..e9 on the event log, after the registry's own
    // audit events. Enqueue three contiguous 3-event batches — the first
    // wedged (e1-e3, head), then two null (e4-e6 and e7-e9, trail).
    //
    // Contract:
    //  - fill_cursor rewinds to target.first_seq - 1, the sequence number
    //    just below e1;
    //  - e1..e9 are re-promoted into fresh FIFO rows starting from
    //    the rewound cursor;
    //  - the tombstoned audit row survives alongside the fresh rows;
    //  - every fresh row has a new UUID entryId distinct from the
    //    tombstoned row's entryId.
    test('next fillBatch re-promotes target events AND trail events', () async {
      final deps = await buildAuditedRegistryDeps(backend);
      final registry = DestinationRegistry(eventStore: deps.eventStore);
      final destination = FakeDestination(id: 'dst-f', batchCapacity: 3);
      // Register + set startDate. The activation's replay request is
      // performed by the first fill, which runs only after the recovery, so
      // it does not enqueue rows that would conflict with the controlled
      // seeding below.
      await registry.addDestination(destination, initiator: _testInit);
      await registry.setStartDate(
        destination.id,
        DateTime.utc(2026, 1, 1),
        initiator: _testInit,
      );
      // Seed 9 events on the event log.
      // The registry's audit events precede them on the log, so the
      // batches below select e1..e9 by the sequence numbers the appends
      // returned, not by position.
      final clientTs = DateTime.utc(2026, 4, 22, 10);
      final seqOf = <int>[];
      for (var i = 1; i <= 9; i++) {
        final event = await _appendEvent(
          backend,
          eventId: 'e$i',
          clientTimestamp: clientTs,
        );
        seqOf.add(event.sequenceNumber);
      }
      expect(seqOf.first, greaterThan(1), reason: 'audits precede e1');

      // Directly enqueue three 3-event batches. These land at
      // sequence_in_queue 1, 2, 3 because the FIFO is empty.
      Future<void> enqueueBatch(List<int> seqs) async {
        await backend.transaction((txn) async {
          final events = <StoredEvent>[];
          final all = await backend.findAllEventsInTxn(txn);
          for (final s in seqs) {
            events.add(all.firstWhere((e) => e.sequenceNumber == s));
          }
          final payload = wirePayloadJson({'seqs': seqs});
          await backend.enqueueFifoTxn(
            txn,
            destination.id,
            events,
            wirePayload: payload,
          );
        });
      }

      await enqueueBatch(seqOf.sublist(0, 3));
      await enqueueBatch(seqOf.sublist(3, 6));
      await enqueueBatch(seqOf.sublist(6, 9));
      // Wedge the head batch row.
      final rows0 = await _readAllFifoRows(backend, destination.id);
      expect(rows0.length, 3);
      expect(
        [for (final r in rows0) (r['event_ids']! as List).cast<String>()],
        [
          ['e1', 'e2', 'e3'],
          ['e4', 'e5', 'e6'],
          ['e7', 'e8', 'e9'],
        ],
        reason: 'the head holds e1-e3 and the trail e4-e9',
      );
      final headEntryId = rows0.first['entry_id']! as String;
      await appendAttemptForTest(
        backend,
        destination.id,
        headEntryId,
        AttemptResult(
          attemptedAt: DateTime.utc(2026, 4, 22, 12),
          outcome: 'permanent',
          errorMessage: 'simulated failure',
          httpStatus: 500,
        ),
      );
      await setStatusForTest(
        backend,
        destination.id,
        headEntryId,
        FinalStatus.wedged,
      );
      // fill_cursor at e9 (last enqueued seq) so the rewind is
      // observable.
      await writeFillCursorForTest(backend, destination.id, seqOf.last);

      // Act: tombstone + refill.
      final result = await registry.tombstoneAndRefill(
        destination.id,
        headEntryId,
        initiator: _testInit,
      );
      expect(result.deletedTrailCount, 2);
      // This positions fillBatch to walk e1..e9 again.
      expect(result.rewoundTo, seqOf.first - 1);

      // Run fillBatch enough times to drain all events. With
      // batchCapacity=3, three calls cover events 1-3, 4-6, 7-9.
      final schedule = await registry.scheduleOf(destination.id);
      for (var i = 0; i < 3; i++) {
        await fillWithScheduleForTest(
          destination,
          backend: backend,
          schedule: schedule,
          clock: () => DateTime.utc(2026, 4, 22, 13),
        );
      }

      final rows1 = await _readAllFifoRows(backend, destination.id);
      final tombstoned = rows1
          .where((r) => r['final_status'] == FinalStatus.tombstoned.toJson())
          .toList();
      expect(tombstoned.length, 1);
      expect(tombstoned.single['entry_id'], headEntryId);

      final fresh = rows1.where((r) => r['final_status'] == null).toList();
      // three 3-event batches at the destination's batchCapacity, holding
      // exactly e1..e9 in log order.
      expect(fresh.length, 3);
      expect(
        [for (final r in fresh) ...(r['event_ids']! as List).cast<String>()],
        [for (var i = 1; i <= 9; i++) 'e$i'],
      );
      final coveredIds = <String>{};
      for (final r in fresh) {
        coveredIds.addAll((r['event_ids']! as List).cast<String>());
      }
      expect(coveredIds, {for (var i = 1; i <= 9; i++) 'e$i'});
      // v4-UUID entry_id invariant: every fresh row's entry_id is a UUID
      // distinct from the tombstoned row's entry_id.
      for (final r in fresh) {
        expect(r['entry_id'], isNot(headEntryId));
      }
      // And all four rows (1 tombstoned + 3 fresh) have pairwise-
      // distinct entry_ids.
      final allEntryIds = rows1.map((r) => r['entry_id']! as String).toList();
      expect(allEntryIds.toSet().length, allEntryIds.length);
      // fill_cursor advanced through all 9 user events, and the fill then
      // advances past the registry's audit events, which the
      // destination's filter rejects, so the cursor ends at the log's last
      // event.
      final lastSeq = (await backend.findAllEvents()).last.sequenceNumber;
      expect(await backend.readFillCursor(destination.id), lastSeq);
    });
  });
}
