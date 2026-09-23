// Test support for queue-level tests: persist a schedule the way a
// registration does, and wedge a queue head the way the drainer does. This
// file declares no tests, so it carries no citation.
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';

import 'fake_destination.dart';

/// Persist [schedule] for [destinationId], stamped with a registration id
/// (the given one, or `test-registration-<destinationId>` when the schedule
/// carries none), as a registration writes it.
Future<DestinationSchedule> persistScheduleForTest(
  StorageBackend backend,
  String destinationId,
  DestinationSchedule schedule,
) async {
  final persisted = DestinationSchedule(
    startDate: schedule.startDate,
    endDate: schedule.endDate,
    registrationId:
        schedule.registrationId ?? 'test-registration-$destinationId',
    allowHardDelete: schedule.allowHardDelete,
  );
  await backend.transaction(
    (txn) => backend.writeScheduleTxn(txn, destinationId, persisted),
  );
  return persisted;
}

/// Wedge [destinationId]'s pending head the supported way: one drain pass,
/// through [registry], whose delivery reports a permanent failure (so the
/// wedge event and the wedge record are written with the wedge). Returns
/// the wedged item's `entry_id`. Throws when the head is not pending.
///
/// The pass runs at [at] (default far in the future), so a backoff left by
/// earlier attempts does not hold the send.
Future<String> wedgeHeadForTest(
  DestinationRegistry registry,
  String destinationId, {
  String error = 'refused by the test receiver',
  DateTime? at,
}) async {
  final backend = registry.backend;
  final head = await backend.readFifoHead(destinationId);
  if (head == null || head.finalStatus != null) {
    throw StateError(
      'wedgeHeadForTest($destinationId): the head is not pending '
      '(${head?.finalStatus})',
    );
  }
  await drain(
    FakeDestination(
      id: destinationId,
      script: <SendResult>[SendPermanent(error: error)],
    ),
    registry: registry,
    clock: () => at ?? DateTime.utc(2100),
  );
  final after = await backend.readFifoRow(destinationId, head.entryId);
  if (after?.finalStatus != FinalStatus.wedged) {
    throw StateError(
      'wedgeHeadForTest($destinationId): the head did not wedge',
    );
  }
  return head.entryId;
}

/// Persist [schedule] (see [persistScheduleForTest]) and run one fill over
/// it: the fill reads the persisted schedule, so a test that describes the
/// window in memory persists it first.
Future<void> fillWithScheduleForTest(
  Destination destination, {
  required StorageBackend backend,
  required DestinationSchedule schedule,
  Source? source,
  Clock? clock,
  bool flushHeld = false,
}) async {
  await persistScheduleForTest(backend, destination.id, schedule);
  await fillBatch(
    destination,
    backend: backend,
    source: source,
    clock: clock,
    flushHeld: flushHeld,
  );
}
