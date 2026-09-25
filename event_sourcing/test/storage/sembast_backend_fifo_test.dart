// Verifies: EVS-PRD-portability/D
// these tests pin sembast-specific
//   FIFO behaviors that are NOT part of the abstract StorageBackend
//   contract: (a) the on-disk lockstep between the Sembast int store-key
//   and the payload's `sequence_in_queue`; (b) sequence_in_queue's
//   never-reused property after a raw `store.delete` bypassing the
//   public API.
//
// The abstract StorageBackend FIFO contract (enqueueFifoTxn,
// readFifoHead, listFifoEntries, appendAttemptTxn, setFinalStatusTxn,
// hasFifoWedged/wedgedFifos, fill-cursor read/write, and the exact legal
// status transitions: a repeated status, a missing item or a terminal
// item throws StateError) is exercised against this backend by
// `sembast_backend_conformance_test.dart` via the backend-agnostic
// conformance harness in `storage_backend_conformance.dart`.
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
  });
}
