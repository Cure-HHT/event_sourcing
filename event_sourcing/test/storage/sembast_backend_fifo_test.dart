// Verifies: EVS-PRD-portability/D
// these tests pin sembast-specific
//   FIFO behaviors that are NOT part of the abstract StorageBackend
//   contract: (a) the on-disk lockstep between the Sembast int store-key
//   and the payload's `sequence_in_queue`; (b) sequence_in_queue's
//   never-reused property after a raw `store.delete` bypassing the
//   public API; (c) the `debugLogSink` warning emissions from
//   `markFinal`/`appendAttempt` no-op branches.
//
// The abstract StorageBackend FIFO contract (enqueueFifo, readFifoHead,
// listFifoEntries, appendAttempt, markFinal, hasFifoWedged/wedgedFifos,
// fill-cursor read/write, markFinal idempotency and one-way transitions)
// is exercised against this backend by
// `sembast_backend_conformance_test.dart` via the backend-agnostic
// conformance harness in `storage_backend_conformance.dart`.
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fifo_entry_helpers.dart';

void main() {
  group('SembastBackend FIFO (impl-internal)', () {
    late SembastBackend backend;
    var pathCounter = 0;

    setUp(() async {
      pathCounter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'fifo-$pathCounter.db',
      );
      backend = SembastBackend(database: db);
    });

    tearDown(() async {
      await backend.close();
    });

    // -------- Sembast int-key/payload lockstep --------

    // Pins that the Sembast int key equals the payload's
    // `sequence_in_queue` (they are in lockstep by design). This is a
    // sembast-internal invariant — the abstract contract does not
    // require any particular relationship between the storage-layer key
    // and the FifoEntry.sequenceInQueue field.
    test('sequence_in_queue equals the Sembast store key (lockstep)', () async {
      await enqueueSingle(backend, 'primary', eventId: 'e1', sequenceNumber: 1);
      await enqueueSingle(backend, 'primary', eventId: 'e2', sequenceNumber: 2);

      final db = backend.databaseForTesting;
      final raw = await StoreRef<int, Map<String, Object?>>(
        'fifo_primary',
      ).find(db);
      for (final record in raw) {
        expect(record.value['sequence_in_queue'], record.key);
      }
    });

    // The destination's sequence_in_queue counter is monotonic per
    // destination and NEVER reused, even when a row is deleted from the
    // underlying Sembast store. This test performs a raw `store.delete`
    // bypassing the backend API to simulate the trail-sweep deletion
    // path, then verifies the next enqueue picks up the next never-seen
    // value rather than refilling the vacated slot. The raw-store
    // intrusion is sembast-internal scaffolding; there is no public
    // delete-row API to simulate this through.
    test('sequence_in_queue is monotonic per destination, '
        'never reused', () async {
      await enqueueSingle(backend, 'primary', eventId: 'e1', sequenceNumber: 1);
      await enqueueSingle(backend, 'primary', eventId: 'e2', sequenceNumber: 2);
      await enqueueSingle(backend, 'primary', eventId: 'e3', sequenceNumber: 3);

      final db = backend.databaseForTesting;
      final store = StoreRef<int, Map<String, Object?>>('fifo_primary');
      final before = await store.find(db);
      expect(before.map((r) => r.value['sequence_in_queue']).toList(), [
        1,
        2,
        3,
      ]);

      // Raw delete of row whose sequence_in_queue is 2 (the e2 row).
      // This simulates the trail-sweep deletion path without depending
      // on that API.
      await store.record(2).delete(db);

      final afterDelete = await store.find(db);
      expect(afterDelete.map((r) => r.value['sequence_in_queue']).toList(), [
        1,
        3,
      ]);

      // Fourth enqueue MUST get sequence_in_queue 4 — NOT 2 (the deleted
      // slot) and NOT 3.
      final e4 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e4',
        sequenceNumber: 4,
      );
      final afterFirstEnqueue = await store.find(db);
      final e4Record = afterFirstEnqueue.firstWhere(
        (r) => r.value['entry_id'] == e4.entryId,
      );
      expect(e4Record.value['sequence_in_queue'], 4);
      expect(e4Record.key, 4);

      // Now delete row 4 (the max-key row). Under a buggy
      // "max(existing key) + 1" derivation, the next enqueue would
      // assign 4 again. The persisted counter prevents that: the next
      // enqueue must get 5.
      await store.record(4).delete(db);
      final e5 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e5',
        sequenceNumber: 5,
      );
      final afterSecondEnqueue = await store.find(db);
      final e5Record = afterSecondEnqueue.firstWhere(
        (r) => r.value['entry_id'] == e5.entryId,
      );
      expect(e5Record.value['sequence_in_queue'], 5);
      expect(e5Record.key, 5);
    });

    // -------- debugLogSink warning emissions --------
    //
    // The abstract StorageBackend contract documents that markFinal and
    // appendAttempt SHALL emit a warning-level diagnostic when they
    // no-op due to a missing target. The verification *hook* used here
    // (`backend.debugLogSink`) is sembast-specific — other backends may
    // expose their own diagnostic plumbing. Until the abstract contract
    // gains a portable diagnostic surface, these tests stay attached to
    // the SembastBackend impl.

    test('markFinal emits a warning that names method, '
        'entry id, and destination id when it no-ops', () async {
      final logs = <String>[];
      backend.debugLogSink = logs.add;

      await backend.markFinal('primary', 'ghost', FinalStatus.sent);

      expect(logs, hasLength(1));
      final line = logs.single;
      expect(line, contains('markFinal'));
      expect(line, contains('ghost'));
      expect(line, contains('primary'));
      expect(line, contains('drain/unjam'));
      expect(line, contains('drain/delete'));
    });

    test('appendAttempt emits a warning that names method, '
        'entry id, and destination id when it no-ops', () async {
      final logs = <String>[];
      backend.debugLogSink = logs.add;

      await backend.appendAttempt(
        'primary',
        'ghost',
        AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
      );

      expect(logs, hasLength(1));
      final line = logs.single;
      expect(line, contains('appendAttempt'));
      expect(line, contains('ghost'));
      expect(line, contains('primary'));
      expect(line, contains('drain/unjam'));
      expect(line, contains('drain/delete'));
    });

    test('no warning is emitted on a happy-path markFinal / '
        'appendAttempt', () async {
      final logs = <String>[];
      backend.debugLogSink = logs.add;

      final e1 = await enqueueSingle(
        backend,
        'primary',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.appendAttempt(
        'primary',
        e1.entryId,
        AttemptResult(attemptedAt: DateTime.utc(2026, 4, 22), outcome: 'ok'),
      );
      await backend.markFinal('primary', e1.entryId, FinalStatus.sent);

      expect(logs, isEmpty);
    });
  });
}
