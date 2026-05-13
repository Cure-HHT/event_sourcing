// Implements: EVS-PRD-destinations/A/D/F — DestinationRegistry: configures
// destinations on a deployment (A), persists schedules so state survives
// restart (D), and supports dynamic add/deactivate/delete over the
// operating lifetime (F).
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:event_sourcing/src/sync/historical_replay.dart';

/// Process-wide registry of synchronization destinations.
///
/// Under , the registry supports a dynamic lifecycle: destinations
/// may be added at any time after bootstrap, their `startDate` may be set
/// or moved earlier (monotonically non-increasing — forward movement
/// throws), their `endDate` may be mutated, and they may be deactivated
/// or hard-deleted per the per-destination `allowHardDelete` opt-in.
///
/// Every runtime mutation of registry-controlled state (add, set start
/// date, set end date, deactivate, delete, tombstoneAndRefill) emits a
/// system audit event in the SAME `backend.transaction` as the mutation
/// itself. The audit event lands or rolls back atomically with the
/// underlying mutation: a failed audit append rolls back the mutation,
/// and a failed mutation rolls back any partially-formed audit row.
///
/// The registry is bound to a `StorageBackend` for schedule / FIFO
/// persistence and to an `EventStore` for in-transaction audit emission.
/// Production code constructs a single instance during bootstrap; tests
/// construct a fresh instance per test against an in-memory
/// `SembastBackend` and a matching `EventStore`.
// time after bootstrap; duplicate id is rejected; emits a registration
// audit event atomically with the schedule write.
// non-increasing (earlier OK, equal no-op, later throws); emits a
// start_date audit event atomically with the schedule write and any
// applicable replay (historical on first activation, gap on backward
// move).
// deactivateDestination is the now() shorthand; both emit an end_date
// audit event atomically with the schedule write.
// allowHardDelete and drops the schedule + FIFO store atomically with
// a deletion audit event.
// same transaction as the mutation, so partial states cannot persist.
// operator wedge-recovery primitive; emits a wedge-recovery audit event
// atomically with the FIFO mutations.
class DestinationRegistry {
  /// Construct a registry bound to [backend] for storage persistence and
  /// [eventStore] for in-transaction audit emission. The registry does
  /// not open the database — the caller retains ownership of the
  /// backend's lifecycle.
  DestinationRegistry({required this.backend, required EventStore eventStore})
    : _eventStore = eventStore;

  /// Backend used for schedule persistence and FIFO-store drop on
  /// delete. Stored as a final field so the binding is established at
  /// construction and cannot drift.
  final StorageBackend backend;

  /// Event store used to stamp config-change audit events inside the
  /// same transaction as the underlying mutation. The store's own
  /// `Source` is reused for every audit emission.
  final EventStore _eventStore;

  final Map<String, Destination> _destinations = <String, Destination>{};
  final Map<String, DestinationSchedule> _schedules =
      <String, DestinationSchedule>{};

  /// Register [destination]. Seeds the in-memory schedule cache with a
  /// dormant `DestinationSchedule` (no `startDate`, no `endDate`) and
  /// persists that initial schedule so a subsequent process restart
  /// recovers the same dormant state. Emits a
  /// `system.destination_registered` audit event in the same
  /// transaction as the schedule write.
  ///
  /// Throws `ArgumentError` if a destination with the same id is already
  /// registered.
  // bootstrap; duplicate id rejected with ArgumentError.
  // schedule across process restart, so setStartDate's monotonic-
  // backward semantics survive bootstrap re-running addDestination
  // with the same id. Only seeds a dormant schedule when no schedule
  // is persisted.
  // transaction as the schedule write.
  Future<void> addDestination(
    Destination destination, {
    required Initiator initiator,
  }) async {
    if (_destinations.containsKey(destination.id)) {
      throw ArgumentError.value(
        destination.id,
        'destination.id',
        'destination id ${destination.id} is already registered '
            '',
      );
    }
    // Read schedule outside the txn — it's a pure read, and the
    // SembastBackend contract has no readScheduleInTxn surface. The
    // subsequent transaction body is the one that must commit
    // atomically with the audit emission.
    final persisted = await backend.readSchedule(destination.id);
    final resolved = persisted ?? const DestinationSchedule();
    await _eventStore.runTransaction((txn, collector) async {
      if (persisted == null) {
        await backend.writeScheduleTxn(txn, destination.id, resolved);
      }
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationRegisteredEntryType,
        data: <String, Object?>{
          'id': destination.id,
          'wire_format': destination.wireFormat,
          'allow_hard_delete': destination.allowHardDelete,
          'serializes_natively': destination.serializesNatively,
          'filter_entry_types': destination.filter.entryTypes?.toList(),
          'filter_event_types': destination.filter.eventTypes?.toList(),
          // predicate API. Downstream key-based queries should find the
          // key present-but-null rather than absent.
          'filter_predicate_description': null,
        },
        initiator: initiator,
      );
    });
    // Update in-memory state only after the transaction commits, so a
    // rolled-back transaction (e.g. audit append failure) leaves the
    // registry consistent with persistence.
    _destinations[destination.id] = destination;
    _schedules[destination.id] = resolved;
  }

  /// All registered destinations, in registration order. Returned list
  /// is unmodifiable so callers cannot mutate the registry by mutating
  /// the view.
  List<Destination> all() =>
      List<Destination>.unmodifiable(_destinations.values);

  /// Destination with [id], or null when no such destination has been
  /// registered. Does not consult persistence — only in-memory state.
  Destination? byId(String id) => _destinations[id];

  /// Read the current `DestinationSchedule` for [id]. Reads from the
  /// in-memory cache; the cache is populated by `addDestination` and
  /// kept current by `setStartDate` / `setEndDate`. Throws
  /// `ArgumentError` when [id] is not registered.
  // downstream fillBatch time-window filtering.
  Future<DestinationSchedule> scheduleOf(String id) async {
    final cached = _schedules[id];
    if (cached != null) return cached;
    final persisted = await backend.readSchedule(id);
    if (persisted != null) {
      _schedules[id] = persisted;
      return persisted;
    }
    throw ArgumentError.value(
      id,
      'id',
      'no destination registered with id $id',
    );
  }

  /// Assign or move [when] as the destination's `startDate`
  ///. The contract is monotonic-backward — earlier OK,
  /// equal no-op, later throws:
  ///
  /// - `current.startDate == null` (first activation): persists [when]
  ///   as the new `startDate`. If [when] is at or before `DateTime.now()`
  ///   the call triggers historical replay synchronously in the same
  ///   transaction. If [when] is in the future, no
  ///   replay runs — events accumulate in `event_log` and are batched by
  ///   `fillBatch` once the wall-clock crosses [when].
  /// - `when < current.startDate` (move earlier): persists [when] and
  ///   triggers a gap replay over `[when, current.startDate)` in the
  ///   same transaction. The gap replay walks the event log
  ///   independently of `fill_cursor` and enqueues matching events
  ///   into the destination's FIFO; the cursor is left intact.
  /// - `when == current.startDate`: no-op. Returns without writing.
  /// - `when > current.startDate`: throws `StateError`. Forward
  ///   movement is forbidden because already-shipped FIFO rows would
  ///   be retroactively orphaned by the narrower window.
  ///
  /// Emits a `system.destination_start_date_set` audit event in the
  /// same transaction as the schedule write (and replay, when
  /// applicable). The audit `data` carries `prior_start_date` (the
  /// previous value, or `null` on first activation).
  ///
  /// Throws `ArgumentError` when [id] is not registered.
  // non-increasing.
  // triggers historical replay synchronously inside the same
  // transaction as the schedule write.
  // does NOT trigger replay.
  // over [when, current.startDate) inside the same transaction;
  // fill_cursor is not regressed.
  // transaction as the schedule write and replay.
  Future<void> setStartDate(
    String id,
    DateTime when, {
    required Initiator initiator,
  }) async {
    if (!_destinations.containsKey(id)) {
      throw ArgumentError.value(
        id,
        'id',
        'no destination registered with id $id',
      );
    }
    final current = _schedules[id] ?? const DestinationSchedule();
    final priorStartDate = current.startDate;

    if (priorStartDate != null) {
      if (when.isAtSameMomentAs(priorStartDate)) {
        // No-op: caller can use this idempotently. Avoid touching the
        // schedule, replay, or audit log so a redundant boot-time
        // activation has zero observable effect.
        return;
      }
      if (when.isAfter(priorStartDate)) {
        throw StateError(
          'DestinationRegistry.setStartDate($id): forward movement '
          'forbidden — current startDate is $priorStartDate, requested '
          '$when. setStartDate is monotonically non-increasing '
          '.',
        );
      }
      // when < priorStartDate: backward move, falls through to the
      // gap-replay branch below.
    }

    final updated = DestinationSchedule(
      startDate: when,
      endDate: current.endDate,
    );
    // The schedule write, replay (historical on first activation OR
    // gap on backward move), and audit emission all commit together.
    // Running everything in the same transaction provides the
    // serialization guarantee relies on: a concurrent
    // record() serializes behind this transaction and walks candidates
    // strictly past the advanced fill_cursor (or the unchanged cursor,
    // for gap replay).
    await _eventStore.runTransaction((txn, collector) async {
      await backend.writeScheduleTxn(txn, id, updated);

      if (priorStartDate == null) {
        // First activation — historical replay over [when, now] when
        // [when] is in the past.
        if (!when.isAfter(DateTime.now())) {
          await runHistoricalReplay(
            txn,
            _destinations[id]!,
            updated,
            backend,
            source: _eventStore.source,
          );
        }
      } else {
        // Backward move — gap replay over [when, priorStartDate).
        // Skip when [when] is in the future: by the logic,
        // events with client_timestamp < priorStartDate are still
        // unreachable through the current window, but they will become
        // eligible only when the wall-clock reaches [when]; we leave
        // them to a future setStartDate(now-or-past) follow-up. In
        // practice, callers either move directly to a past date or to
        // a future date that they later update again.
        if (!when.isAfter(DateTime.now())) {
          await runGapReplay(
            txn,
            _destinations[id]!,
            backend,
            newStartDate: when,
            oldStartDate: priorStartDate,
            source: _eventStore.source,
          );
        }
      }

      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationStartDateSetEntryType,
        data: <String, Object?>{
          'id': id,
          'start_date': when.toUtc().toIso8601String(),
          'prior_start_date': priorStartDate?.toUtc().toIso8601String(),
        },
        initiator: initiator,
      );
    });
    // Update the in-memory cache only after the transaction commits, so
    // a rolled-back transaction does not leave the registry advertising
    // a schedule that was not persisted.
    _schedules[id] = updated;
  }

  /// Mutate the destination's `endDate` to [endDate] and return a
  /// `SetEndDateResult` describing the transition:
  ///
  /// - `closed` — call transitions currently-active to currently-closed.
  /// - `scheduled` — new `endDate` is in the future.
  /// - `applied` — no change in current active-vs-closed classification.
  ///
  /// Emits a `system.destination_end_date_set` audit event in the same
  /// transaction as the schedule write. The same audit entry type
  /// covers both `setEndDate` and `deactivateDestination` (the now()
  /// shorthand).
  ///
  /// Throws `ArgumentError` when [id] is not registered.
  // applied per the three-way classification.
  // transaction as the schedule write; covers deactivate as the
  // now() shorthand.
  Future<SetEndDateResult> setEndDate(
    String id,
    DateTime endDate, {
    required Initiator initiator,
  }) async {
    if (!_destinations.containsKey(id)) {
      throw ArgumentError.value(
        id,
        'id',
        'no destination registered with id $id',
      );
    }
    final now = DateTime.now();
    final current = _schedules[id] ?? const DestinationSchedule();
    final wasActive = current.isActiveAt(now);
    final updated = DestinationSchedule(
      startDate: current.startDate,
      endDate: endDate,
    );
    final isActive = updated.isActiveAt(now);

    // Classify the two endDate snapshots (pre-call and post-call) as
    // scheduled-for-future-close or not. "Scheduled" here means "has a
    // future endDate"; it is independent of whether the destination is
    // currently active or dormant.
    final wasScheduled =
        current.endDate != null && current.endDate!.isAfter(now);
    final isScheduled = endDate.isAfter(now);

    final SetEndDateResult result;
    if (wasActive && !isActive) {
      // Active → closed at or before now.
      result = SetEndDateResult.closed;
    } else if (!wasActive && isActive) {
      // Previously closed (or dormant), now has a future endDate that
      // reopens / schedules a close window.
      result = SetEndDateResult.scheduled;
    } else if (isScheduled && !wasScheduled) {
      // No active/closed transition, but the endDate is newly in the
      // future (e.g., first assignment to a dormant destination, or
      // replacing a past endDate with a future one without crossing now).
      result = SetEndDateResult.scheduled;
    } else {
      // No state change relative to now AND no new close scheduled —
      // covers past → past, future → future without crossing now, and
      // first-time past on a dormant destination.
      result = SetEndDateResult.applied;
    }

    await _eventStore.runTransaction((txn, collector) async {
      await backend.writeScheduleTxn(txn, id, updated);
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationEndDateSetEntryType,
        data: <String, Object?>{
          'id': id,
          'end_date': endDate.toUtc().toIso8601String(),
          'prior_end_date': current.endDate?.toUtc().toIso8601String(),
          'result': result.name,
        },
        initiator: initiator,
      );
    });
    _schedules[id] = updated;
    return result;
  }

  /// Set the destination's `endDate` to `DateTime.now()`, returning
  /// `SetEndDateResult.closed`. The audit event is
  /// emitted by the underlying `setEndDate` call.
  // shorthand for setEndDate; audit emission is delegated.
  Future<SetEndDateResult> deactivateDestination(
    String id, {
    required Initiator initiator,
  }) => setEndDate(id, DateTime.now(), initiator: initiator);

  /// Unregister [id] and drop its FIFO store + schedule record in one
  /// transaction. Emits a `system.destination_deleted` audit event in
  /// the same transaction as the FIFO + schedule drop. Throws
  /// `StateError` when the destination's `allowHardDelete` getter is
  /// `false` — the default, opt-out-only gate on permanent FIFO
  /// destruction.
  // allowHardDelete; atomic FIFO-store + schedule drop.
  // transaction as the FIFO + schedule drop.
  Future<void> deleteDestination(
    String id, {
    required Initiator initiator,
  }) async {
    final destination = _destinations[id];
    if (destination == null) {
      throw ArgumentError.value(
        id,
        'id',
        'no destination registered with id $id',
      );
    }
    if (!destination.allowHardDelete) {
      throw StateError(
        'DestinationRegistry.deleteDestination($id): destination '
        'allowHardDelete is false; hard deletion requires an explicit '
        'per-destination opt-in.',
      );
    }
    await _eventStore.runTransaction((txn, collector) async {
      await backend.deleteFifoStoreTxn(txn, id);
      await backend.deleteScheduleTxn(txn, id);
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationDeletedEntryType,
        data: <String, Object?>{'id': id, 'allow_hard_delete': true},
        initiator: initiator,
      );
    });
    _destinations.remove(id);
    _schedules.remove(id);
  }

  /// Operator-driven wedge recovery: tombstone the FIFO head, delete
  /// pending trail rows behind it, rewind `fill_cursor`, and emit a
  /// `system.destination_wedge_recovered` audit event — all in one
  /// `backend.transaction`. The sole code path by which a FIFO row
  /// reaches `final_status == tombstoned`.
  ///
  /// Preconditions, checked BEFORE opening the
  /// transaction so a mis-call does not hold a write lock:
  /// - The row identified by [fifoRowId] on [destinationId] SHALL exist.
  /// - The row SHALL be the current head of the destination's FIFO
  ///   (i.e., `readFifoHead(destinationId)` returns this row). Its
  ///   `final_status` is therefore either `null` (pre-terminal) or
  ///   `FinalStatus.wedged` (blocking terminal); a `sent` or
  ///   `tombstoned` target, or a non-head target, is rejected with
  ///   `ArgumentError`.
  ///
  /// Cascade inside one `StorageBackend.transaction`:
  /// - Target row flips to `FinalStatus.tombstoned`; `attempts[]` and
  ///   all other fields preserved.
  /// - Every row whose `sequence_in_queue > target.sequence_in_queue`
  ///   AND whose `final_status IS null` is deleted from the FIFO store.
  /// - `fill_cursor_<destinationId>` is rewound to
  ///   `target.event_id_range.first_seq - 1`.
  /// - A `system.destination_wedge_recovered` audit event is appended.
  ///
  /// Returns a [TombstoneAndRefillResult].
  // checked pre-transaction so ArgumentError does not hold a write lock.
  // preserves attempts[] verbatim.
  // transaction as the FIFO mutations.
  Future<TombstoneAndRefillResult> tombstoneAndRefill(
    String destinationId,
    String fifoRowId, {
    required Initiator initiator,
  }) async {
    // returns the first row whose final_status is null or wedged; sent
    // and tombstoned rows are skipped. So if the caller's target is the
    // head, it is automatically in {null, wedged}; if it is anything
    // else (does not exist, sent, tombstoned, or simply not-the-head),
    // the returned head will differ from fifoRowId and we reject.
    final head = await backend.readFifoHead(destinationId);
    if (head == null || head.entryId != fifoRowId) {
      throw ArgumentError.value(
        fifoRowId,
        'fifoRowId',
        'tombstoneAndRefill($destinationId, $fifoRowId): target is not '
            'the current head of the FIFO. readFifoHead returned '
            '${head?.entryId}.',
      );
    }
    // head.finalStatus is null or wedged here (readFifoHead contract).

    final targetFirstSeq = head.eventIdRange.firstSeq;
    final targetLastSeq = head.eventIdRange.lastSeq;
    final targetSeqInQueue = head.sequenceInQueue;

    return _eventStore.runTransaction((txn, collector) async {
      await backend.setFinalStatusTxn(
        txn,
        destinationId,
        fifoRowId,
        FinalStatus.tombstoned,
      );
      final deletedTrailCount = await backend
          .deleteNullRowsAfterSequenceInQueueTxn(
            txn,
            destinationId,
            targetSeqInQueue,
          );
      final rewoundTo = targetFirstSeq - 1;
      await backend.writeFillCursorTxn(txn, destinationId, rewoundTo);
      final result = TombstoneAndRefillResult(
        targetRowId: fifoRowId,
        deletedTrailCount: deletedTrailCount,
        rewoundTo: rewoundTo,
      );
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationWedgeRecoveredEntryType,
        data: <String, Object?>{
          'id': destinationId,
          'target_row_id': fifoRowId,
          'target_event_id_range_first_seq': targetFirstSeq,
          'target_event_id_range_last_seq': targetLastSeq,
          'deleted_trail_count': deletedTrailCount,
          'rewound_to': rewoundTo,
        },
        initiator: initiator,
      );
      return result;
    });
  }

  /// Emit a system audit event for a destination mutation inside [txn].
  ///
  /// The aggregate is stamped as `source.identifier` (the install UUID)
  /// / `system_destination` / `finalized`; the destination identity
  /// lives in `data['id']`. Every destination mutation a single install
  /// emits therefore lands in a single per-install hash-chained system
  /// aggregate. Emission uses no flow token, metadata, security,
  /// checkpoint, or change reason. dedupeByContent is left off because
  /// each destination mutation records a distinct timeline entry.
  ///
  /// `entry_type_version` is stamped by the substrate from the registry's
  /// `registeredVersion` for [entryType]; if [entryType] is not registered,
  /// `appendInTxn`'s `_validateAppendInputs` raises an `ArgumentError`
  /// inside the surrounding transaction (rolling back any prior writes).
  //   install UUID as their aggregate; destination identity moves into
  //   data.id so callers can still query "all audits about destination
  //   X" by filtering on entry_type AND data.id.
  Future<void> _emitDestinationAuditInTxn(
    Txn txn,
    PublishCollector collector, {
    required String entryType,
    required Map<String, Object?> data,
    required Initiator initiator,
  }) async {
    await _eventStore.appendInTxn(
      txn,
      collector: collector,
      entryType: entryType,
      aggregateId: _eventStore.source.identifier,
      aggregateType: 'system_destination',
      eventType: 'finalized',
      data: data,
      initiator: initiator,
      flowToken: null,
      metadata: null,
      security: null,
      checkpointReason: null,
      changeReason: null,
      dedupeByContent: false,
    );
  }
}
