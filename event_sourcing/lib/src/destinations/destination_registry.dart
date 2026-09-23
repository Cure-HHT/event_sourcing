// Implements: EVS-PRD-destinations/A+D+F
// DestinationRegistry: configures
// destinations on a deployment (A), persists schedules so state survives
// restart (D), and supports dynamic add/deactivate/delete over the
// operating lifetime (F).
// Implements: EVS-PRD-destinations/M+N+O
// recovery requires a wedged head and
//   rewinds below every item it removes; deletion is refused on a pending
//   head, retains the delivery record, and a destination registered again
//   under the same id delivers.
// Implements: EVS-DEV-destination-drain/A+E+F+U
// the registry's operations act on the
//   persisted schedule, decide every outcome (refusals and no-op outcomes
//   included) inside one transaction that writes, record replay requests
//   instead of enqueuing, and perform recovery and deletion in one
//   transaction each.
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// Outcome of one run of a registry operation's transaction body: a result
/// to return, or a refusal to throw after the transaction commits.
sealed class _Outcome<T> {
  const _Outcome();
}

final class _Done<T> extends _Outcome<T> {
  const _Done(this.value);
  final T value;
}

final class _Refused<T> extends _Outcome<T> {
  const _Refused(this.error);
  final Error error;
}

/// A destination registered in this registry, with the registration it was
/// registered under.
class _Local {
  const _Local(this.destination, this.registrationId);
  final Destination destination;
  final String registrationId;
}

/// Registry of synchronization destinations.
///
/// The registry holds the [Destination] objects this process registers
/// (their filter, transform and transport are code) and runs the operations
/// that change a destination's persisted state: registration, start and end
/// dates, operator recovery, deletion. Those operations act on the persisted
/// schedule, not on this process's in-memory destinations, so any process
/// can run them for any destination the database knows; the delivery cycle
/// fills and drains only the destinations its own registry holds.
///
/// Every operation decides its outcome inside one transaction that writes:
/// a mutation writes its records and a system audit event together (a failed
/// audit append rolls the mutation back), and a refusal, or an outcome that
/// changes nothing, writes only the database-wide registry check record and
/// throws or returns after the commit. On a backend that validates a
/// transaction against other writers only when it writes (a browser
/// database shared by several tabs), every outcome is therefore decided on
/// fresh data.
///
/// No registry operation enqueues: an operation that widens a destination's
/// window records a replay request that the delivery cycle's next fill
/// performs, under the destination registered in the process that drains.
class DestinationRegistry {
  /// Construct a registry bound to [backend] for storage persistence and
  /// [eventStore] for in-transaction audit emission. The registry does
  /// not open the database — the caller retains ownership of the
  /// backend's lifecycle.
  DestinationRegistry({required this.backend, required EventStore eventStore})
    : _eventStore = eventStore;

  /// Backend holding the destinations' schedules and queues.
  final StorageBackend backend;

  /// Event store used to stamp config-change audit events inside the
  /// same transaction as the underlying mutation. The store's own
  /// `Source` is reused for every audit emission.
  final EventStore _eventStore;

  final Map<String, _Local> _destinations = <String, _Local>{};

  /// Ids whose registration in this registry is in progress.
  final Set<String> _registering = <String>{};

  /// Runs one registry operation's transaction and throws its refusal after
  /// the commit.
  Future<T> _run<T>(
    String op,
    Future<_Outcome<T>> Function(Transaction txn, PublishCollector collector)
    body,
  ) async {
    final before = DeliveryTestHooks.current?.beforeRegistryTransaction;
    if (before != null) await before(op);
    final outcome = await _eventStore.runTransaction((txn, collector) async {
      _observeBodyRun(op);
      return body(txn, collector);
    });
    switch (outcome) {
      case _Done<T>(:final value):
        return value;
      case _Refused<T>(:final error):
        throw error;
    }
  }

  /// Reports a run of [op]'s transaction body to the `onRegistryBodyRun`
  /// test seam; an exception the seam throws is logged and does not reach
  /// the operation.
  static void _observeBodyRun(String op) {
    final seam = DeliveryTestHooks.current?.onRegistryBodyRun;
    if (seam == null) return;
    try {
      seam(op);
    } on Object catch (e, st) {
      libraryLog(
        'destination_registry',
        'the onRegistryBodyRun test seam threw',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  /// Writes the registry check record for an outcome that writes nothing
  /// else, and returns [outcome].
  Future<_Outcome<T>> _decideWithoutChange<T>(
    Transaction txn, {
    required String op,
    required String destinationId,
    required String check,
    required _Outcome<T> outcome,
  }) async {
    await backend.writeRegistryCheckTxn(
      txn,
      RegistryCheck(
        op: op,
        destinationId: destinationId,
        outcome: check,
        at: DateTime.now().toUtc(),
      ),
    );
    return outcome;
  }

  Future<_Outcome<T>> _refuseUnknown<T>(
    Transaction txn,
    String op,
    String id,
  ) => _decideWithoutChange<T>(
    txn,
    op: op,
    destinationId: id,
    check: 'refused_unknown_destination',
    outcome: _Refused<T>(
      ArgumentError.value(id, 'id', 'no destination registered with id $id'),
    ),
  );

  /// Register [destination] in this registry and record its registration.
  ///
  /// Inside one transaction: reads the persisted schedule; writes a dormant
  /// schedule when none exists, or keeps the existing dates and registration
  /// when one does (the destination is already known to the database, for
  /// example registered by another process); writes this destination's
  /// hard-delete opt-in into the schedule as the one in effect (the latest
  /// registration wins); and appends a `system.destination_registered`
  /// event recording that opt-in. A new registration's identity is the id
  /// of that event.
  ///
  /// Throws `ArgumentError`, with nothing written but the registry check
  /// record, when this registry already holds [destination]'s id under the
  /// registration the database still has, or is registering that id in
  /// another call that has not finished. When the database no longer has
  /// that registration (the destination was deleted, and perhaps registered
  /// again, elsewhere), this registry's entry is stale and is replaced.
  ///
  /// A destination deleted and registered again under the same id starts a
  /// new registration. Its refill may send again events that the prior
  /// registration's `sent` items already delivered: delivery is
  /// at-least-once.
  Future<void> addDestination(
    Destination destination, {
    required Initiator initiator,
  }) async {
    final id = destination.id;
    // The destination's configuration is consumer code: read it once, here,
    // so a body the storage re-runs never calls it.
    final wireFormat = destination.wireFormat;
    final allowHardDelete = destination.allowHardDelete;
    final serializesNatively = destination.serializesNatively;
    final filterEntryTypes = destination.filter.entryTypes?.toList();
    final filterEventTypes = destination.filter.eventTypes?.toList();
    // Reserved before the first await, so two registrations of one id in
    // this registry cannot both decide it is not yet registered.
    final reserved = _registering.add(id);
    try {
      final registrationId = await _run<String>('addDestination', (
        txn,
        collector,
      ) async {
        final persisted = await backend.readScheduleTxn(txn, id);
        final local = _destinations[id];
        if (!reserved ||
            (local != null &&
                persisted != null &&
                persisted.registrationId == local.registrationId)) {
          return _decideWithoutChange<String>(
            txn,
            op: 'addDestination',
            destinationId: id,
            check: 'refused_already_registered',
            outcome: _Refused<String>(
              ArgumentError.value(
                id,
                'destination.id',
                'destination id $id is already registered',
              ),
            ),
          );
        }
        final event = await _emitDestinationAuditInTxn(
          txn,
          collector,
          entryType: kDestinationRegisteredEntryType,
          data: <String, Object?>{
            'id': id,
            'wire_format': wireFormat,
            'allow_hard_delete': allowHardDelete,
            'serializes_natively': serializesNatively,
            'filter_entry_types': filterEntryTypes,
            'filter_event_types': filterEventTypes,
            // Predicates are not serializable; null is recorded so that
            // downstream key-based queries find the key present-but-null
            // rather than absent.
            'filter_predicate_description': null,
          },
          initiator: initiator,
        );
        final registration = persisted?.registrationId ?? event.eventId;
        await backend.writeScheduleTxn(
          txn,
          id,
          DestinationSchedule(
            startDate: persisted?.startDate,
            endDate: persisted?.endDate,
            registrationId: registration,
            allowHardDelete: allowHardDelete,
          ),
        );
        _injectAfterLastWrite(kDestinationRegisteredEntryType);
        return _Done<String>(registration);
      });
      _destinations[id] = _Local(destination, registrationId);
    } finally {
      if (reserved) _registering.remove(id);
    }
  }

  /// All destinations registered in this registry, in registration order.
  /// Returned list is unmodifiable so callers cannot mutate the registry by
  /// mutating the view.
  List<Destination> all() => List<Destination>.unmodifiable(
    _destinations.values.map((l) => l.destination),
  );

  /// Destination with [id] registered in this registry, or null. Does not
  /// consult persistence — only this registry's destinations.
  Destination? byId(String id) => _destinations[id]?.destination;

  /// Read the persisted `DestinationSchedule` for [id]. Throws
  /// `ArgumentError` when the database holds no schedule for [id].
  Future<DestinationSchedule> scheduleOf(String id) async {
    final persisted = await backend.readSchedule(id);
    if (persisted != null) return persisted;
    throw ArgumentError.value(
      id,
      'id',
      'no destination registered with id $id',
    );
  }

  /// Assign or move [when] as the destination's `startDate`. The contract
  /// is monotonic-backward — earlier OK, equal no-op, later throws:
  ///
  /// - First activation (`startDate == null`): persists [when] and records
  ///   a first-activation replay request. The delivery cycle's next fill
  ///   replays every admitted event past the fill position whose client
  ///   timestamp lies in `[when, min(endDate, now)]`, to completion.
  /// - `when < startDate` (move earlier): persists [when] and records a gap
  ///   replay request bounded by the prior start date. The next fill
  ///   enqueues the admitted events at or below the fill position whose
  ///   client timestamp lies in `[when, prior start date)`; events above
  ///   the fill position are left to the fill under the new start date. A
  ///   gap request already pending keeps its larger bound, so several moves
  ///   before a fill replay the union once. Refused (`StateError`) while
  ///   the queue head is wedged: recover the queue first.
  /// - `when == startDate`: no change; returns after writing only the
  ///   registry check record.
  /// - `when > startDate`: throws `StateError`. Forward movement is
  ///   forbidden because already-shipped queue items would be
  ///   retroactively orphaned by the narrower window.
  ///
  /// The schedule, the replay request and a
  /// `system.destination_start_date_set` audit event (carrying
  /// `prior_start_date`) commit together. The operation enqueues nothing
  /// itself.
  ///
  /// Throws `ArgumentError` when the database holds no schedule for [id].
  Future<void> setStartDate(
    String id,
    DateTime when, {
    required Initiator initiator,
  }) => _run<void>('setStartDate', (txn, collector) async {
    const op = 'setStartDate';
    final current = await backend.readScheduleTxn(txn, id);
    if (current == null) return _refuseUnknown<void>(txn, op, id);
    final priorStartDate = current.startDate;
    if (priorStartDate != null) {
      if (when.isAtSameMomentAs(priorStartDate)) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: 'unchanged',
          outcome: const _Done<void>(null),
        );
      }
      if (when.isAfter(priorStartDate)) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: 'refused_forward_move',
          outcome: _Refused<void>(
            StateError(
              'DestinationRegistry.setStartDate($id): forward movement '
              'forbidden — current startDate is $priorStartDate, requested '
              '$when. setStartDate is monotonically non-increasing.',
            ),
          ),
        );
      }
      final head = await backend.readFifoHeadTxn(txn, id);
      if (head?.finalStatus == FinalStatus.wedged) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: 'refused_wedged_head',
          outcome: _Refused<void>(
            StateError(
              'DestinationRegistry.setStartDate($id): the queue head '
              '${head!.entryId} is wedged; recover the queue '
              '(tombstoneAndRefill) before moving the start date earlier.',
            ),
          ),
        );
      }
    }
    await backend.writeScheduleTxn(
      txn,
      id,
      DestinationSchedule(
        startDate: when,
        endDate: current.endDate,
        registrationId: current.registrationId,
        allowHardDelete: current.allowHardDelete,
      ),
    );
    final existing = await backend.readReplayRequestTxn(txn, id);
    final request = priorStartDate == null
        ? ReplayRequest(firstActivation: true, gapUpper: existing?.gapUpper)
        : ReplayRequest(
            firstActivation: existing?.firstActivation ?? false,
            gapUpper:
                existing?.gapUpper ??
                ((existing?.firstActivation ?? false) ? null : priorStartDate),
          );
    await backend.writeReplayRequestTxn(txn, id, request);
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
    _injectAfterLastWrite(kDestinationStartDateSetEntryType);
    return const _Done<void>(null);
  });

  /// Mutate the destination's `endDate` to [endDate] and return a
  /// `SetEndDateResult` describing the transition:
  ///
  /// - `closed` — call transitions currently-active to currently-closed.
  /// - `scheduled` — new `endDate` is in the future.
  /// - `applied` — no change in current active-vs-closed classification.
  ///
  /// The schedule and a `system.destination_end_date_set` audit event
  /// commit together. The same audit entry type covers both `setEndDate`
  /// and `deactivateDestination` (the now() shorthand).
  ///
  /// Throws `ArgumentError` when the database holds no schedule for [id].
  Future<SetEndDateResult> setEndDate(
    String id,
    DateTime endDate, {
    required Initiator initiator,
  }) => _run<SetEndDateResult>('setEndDate', (txn, collector) async {
    final current = await backend.readScheduleTxn(txn, id);
    if (current == null) {
      return _refuseUnknown<SetEndDateResult>(txn, 'setEndDate', id);
    }
    final now = DateTime.now();
    final wasActive = current.isActiveAt(now);
    final updated = DestinationSchedule(
      startDate: current.startDate,
      endDate: endDate,
      registrationId: current.registrationId,
      allowHardDelete: current.allowHardDelete,
    );
    final isActive = updated.isActiveAt(now);

    // "Scheduled" means "has a future endDate"; it is independent of
    // whether the destination is currently active or dormant.
    final wasScheduled =
        current.endDate != null && current.endDate!.isAfter(now);
    final isScheduled = endDate.isAfter(now);

    final SetEndDateResult result;
    if (wasActive && !isActive) {
      result = SetEndDateResult.closed;
    } else if (!wasActive && isActive) {
      result = SetEndDateResult.scheduled;
    } else if (isScheduled && !wasScheduled) {
      result = SetEndDateResult.scheduled;
    } else {
      result = SetEndDateResult.applied;
    }

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
    _injectAfterLastWrite(kDestinationEndDateSetEntryType);
    return _Done<SetEndDateResult>(result);
  });

  /// Set the destination's `endDate` to `DateTime.now()`, returning
  /// `SetEndDateResult.closed`. The audit event is
  /// emitted by the underlying `setEndDate` call.
  Future<SetEndDateResult> deactivateDestination(
    String id, {
    required Initiator initiator,
  }) => setEndDate(id, DateTime.now(), initiator: initiator);

  /// Delete destination [id] and retire its queue, in one transaction.
  ///
  /// Refused (`StateError`, nothing written but the registry check record)
  /// unless the persisted hard-delete opt-in in effect is true, and while the
  /// queue head is pending: a pending head may be in delivery in the process
  /// that drains, and deleting it would lose the record of a delivery the
  /// receiver may have accepted. Wait for the head to wedge, then delete. An
  /// empty queue and a wedged head are accepted.
  ///
  /// The deletion tombstones a wedged head, deletes the pending items behind
  /// it, removes the destination's schedule, fill position and replay
  /// request, and keeps every item that was delivered, wedged or recovered
  /// (the delivery record) and the queue's sequence counter. A
  /// `system.destination_deleted` audit event records the tombstoned item,
  /// the number of pending items deleted and the opt-in it acted on. This
  /// registry forgets the destination after the commit.
  ///
  /// A destination registered again under the same id starts a new
  /// registration; its refill may send again events that this
  /// registration's `sent` items already delivered (at-least-once).
  ///
  /// Throws `ArgumentError` when the database holds no schedule for [id].
  Future<void> deleteDestination(
    String id, {
    required Initiator initiator,
  }) async {
    const op = 'deleteDestination';
    await _run<void>(op, (txn, collector) async {
      final schedule = await backend.readScheduleTxn(txn, id);
      if (schedule == null) return _refuseUnknown<void>(txn, op, id);
      if (!schedule.allowHardDelete) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: 'refused_not_opted_in',
          outcome: _Refused<void>(
            StateError(
              'DestinationRegistry.deleteDestination($id): the hard-delete '
              'opt-in in effect is false; hard deletion requires an explicit '
              'per-destination opt-in.',
            ),
          ),
        );
      }
      final head = await backend.readFifoHeadTxn(txn, id);
      if (head != null && head.finalStatus == null) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: 'refused_pending_head',
          outcome: _Refused<void>(
            StateError(
              'DestinationRegistry.deleteDestination($id): the queue head '
              '${head.entryId} is pending and may be in delivery; deletion '
              'requires an empty queue or a wedged head.',
            ),
          ),
        );
      }
      final retirement = await backend.retireQueueTxn(txn, id);
      await backend.deleteScheduleTxn(txn, id);
      await backend.clearReplayRequestTxn(txn, id);
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationDeletedEntryType,
        data: <String, Object?>{
          'id': id,
          'tombstoned_row_id': retirement.tombstonedRowId,
          'deleted_pending_count': retirement.deletedPendingCount,
          'allow_hard_delete': schedule.allowHardDelete,
        },
        initiator: initiator,
      );
      _injectAfterLastWrite(kDestinationDeletedEntryType);
      return const _Done<void>(null);
    });
    _destinations.remove(id);
  }

  /// Operator recovery of a wedged queue: tombstone the wedged head, delete
  /// the pending items behind it, rewind `fill_cursor`, and append a
  /// `system.destination_wedge_recovered` audit event — all in one
  /// transaction that first reads the head.
  ///
  /// Refused, with nothing written but the registry check record:
  /// - `ArgumentError` when the database holds no schedule for
  ///   [destinationId], or when [fifoRowId] is not the queue's current head
  ///   (absent, `sent`, `tombstoned`, or behind the head);
  /// - `StateError` when the head is pending: recovery requires a wedged
  ///   head.
  ///
  /// The fill position is rewound below the lowest event carried by any
  /// item the recovery removes — the head and every swept item, including
  /// a gap replay's items, whose events can lie below the head's — so the
  /// next fill re-evaluates each of those events against the filter and
  /// schedule of the destination the drainer registers. Events carried by
  /// `sent` items above that point are enqueued again and delivered again
  /// (at-least-once). A pending replay request survives and is bounded by
  /// the rewound position. The refill is performed by the drainer's fill.
  ///
  /// Returns a [TombstoneAndRefillResult].
  Future<TombstoneAndRefillResult> tombstoneAndRefill(
    String destinationId,
    String fifoRowId, {
    required Initiator initiator,
  }) => _run<TombstoneAndRefillResult>('tombstoneAndRefill', (
    txn,
    collector,
  ) async {
    const op = 'tombstoneAndRefill';
    final schedule = await backend.readScheduleTxn(txn, destinationId);
    if (schedule == null) {
      return _refuseUnknown<TombstoneAndRefillResult>(txn, op, destinationId);
    }
    final head = await backend.readFifoHeadTxn(txn, destinationId);
    if (head == null || head.entryId != fifoRowId) {
      return _decideWithoutChange<TombstoneAndRefillResult>(
        txn,
        op: op,
        destinationId: destinationId,
        check: 'refused_not_head',
        outcome: _Refused<TombstoneAndRefillResult>(
          ArgumentError.value(
            fifoRowId,
            'fifoRowId',
            'tombstoneAndRefill($destinationId, $fifoRowId): target is not '
                'the current head of the FIFO. The head is ${head?.entryId}.',
          ),
        ),
      );
    }
    if (head.finalStatus != FinalStatus.wedged) {
      return _decideWithoutChange<TombstoneAndRefillResult>(
        txn,
        op: op,
        destinationId: destinationId,
        check: 'refused_pending_head',
        outcome: _Refused<TombstoneAndRefillResult>(
          StateError(
            'tombstoneAndRefill($destinationId, $fifoRowId): recovery '
            'requires a wedged head; the head is pending.',
          ),
        ),
      );
    }
    final targetFirstSeq = head.sequenceRange.firstSeq;
    final targetLastSeq = head.sequenceRange.lastSeq;
    await backend.setFinalStatusTxn(
      txn,
      destinationId,
      fifoRowId,
      FinalStatus.tombstoned,
    );
    final sweep = await backend.deleteNullRowsAfterSequenceInQueueTxn(
      txn,
      destinationId,
      head.sequenceInQueue,
    );
    final swept = sweep.minFirstSeq;
    final lowest = swept != null && swept < targetFirstSeq
        ? swept
        : targetFirstSeq;
    final rewoundTo = lowest - 1;
    await backend.writeFillCursorTxn(txn, destinationId, rewoundTo);
    await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationWedgeRecoveredEntryType,
      data: <String, Object?>{
        'id': destinationId,
        'target_row_id': fifoRowId,
        'target_event_id_range_first_seq': targetFirstSeq,
        'target_event_id_range_last_seq': targetLastSeq,
        'deleted_trail_count': sweep.deletedCount,
        'rewound_to': rewoundTo,
      },
      initiator: initiator,
    );
    _injectAfterLastWrite(kDestinationWedgeRecoveredEntryType);
    return _Done<TombstoneAndRefillResult>(
      TombstoneAndRefillResult(
        targetRowId: fifoRowId,
        deletedTrailCount: sweep.deletedCount,
        rewoundTo: rewoundTo,
      ),
    );
  });

  /// Consults the `failRegistryAuditAppend` test seam after an operation's
  /// last write, inside its transaction.
  void _injectAfterLastWrite(String entryType) {
    if (DeliveryTestHooks.current?.failRegistryAuditAppend?.call(entryType) ??
        false) {
      throw InjectedFailure('registry audit append of $entryType');
    }
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
  Future<StoredEvent> _emitDestinationAuditInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String entryType,
    required Map<String, Object?> data,
    required Initiator initiator,
  }) async {
    final event = await _eventStore.appendInTxn(
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
    // dedupeByContent is off, so the append always stores an event.
    return event!;
  }
}
