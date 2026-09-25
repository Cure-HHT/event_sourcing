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
// Implements: EVS-DEV-destination-drain/H
// every destination audit the registry
//   appends carries the event type of its kind.
// Implements: EVS-PRD-destinations/P+Q+R
// _wedgeHeadInTxn marks the head wedged,
//   appends the wedge event recording the cause and writes the wedge record in
//   the caller's transaction; the event carries structured fields only, never
//   text from an attempt's outcome.
// Implements: EVS-DEV-destination-drain/D+I
// the wedge event is appended only for
//   the pending head the drainer wedges (read and checked inside the
//   transaction), with exactly the declared data keys; recovery and deletion
//   remove the wedge record in the transaction that ends the wedge.
// Implements: EVS-PRD-destinations/U
// requestHalt records an operator's halt
//   request, which the drainer honours by wedging the queue head through
//   honourHaltInTxn.
// Implements: EVS-DEV-destination-drain/N+O+P+Q+R+T
// the halt request is written and
//   cleared in the transaction of the event that opens or closes it; the
//   transaction that honours it verifies the request event in the log; every
//   wedge consumes the open request and records it; a request is refused for
//   an unknown destination, while one is open, or while the head is wedged,
//   and a cancellation with none open; deletion closes the open request and
//   names it; the halt operations act on persisted state from any registry.

part of '../event_store.dart';

/// Service name of the initiator of the wedge events the drainer appends.

const String _drainService = 'event_sourcing.drain';

/// Initiator of the wedge events the drainer appends for a permanent refusal
/// or an exhausted retry budget.
const Initiator _drainInitiator = AutomationInitiator(service: _drainService);

/// What the transaction that honours a halt request did.
@internal
enum HaltHonour {
  /// The request was open and verified; the queue head is wedged with cause
  /// operator halt.
  honoured,

  /// The request the drainer read is no longer the open one, or the queue
  /// has no pending head; nothing was written.
  changed,

  /// The stored request cites no halt request event of this destination and
  /// database in the log; the stored request was removed and nothing was
  /// wedged.
  unverified,
}

/// An open halt request whose request event the log holds.
class _VerifiedHalt {
  const _VerifiedHalt(this.request, this.event, this.purpose);
  final HaltRequest request;
  final StoredEvent event;
  final HaltPurpose purpose;
}

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
/// dates, halt requests and their cancellation, operator recovery, deletion.
/// The delivery cycle's drainer wedges a queue head through it as well (for a
/// failed delivery, or to honour a halt request), so the wedge event is
/// appended through the registry's event store. Those operations act on the persisted
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
  /// Construct a registry over [eventStore]: its operations and the
  /// delivery cycle that drains its destinations read and write the store's
  /// backend, and append their audit events through the store. The
  /// registry does not open the database; the caller retains ownership of
  /// the backend's lifecycle. `EventStore.open` registers the reserved
  /// destination audit entry types the registry and the drainer append.
  DestinationRegistry({required EventStore eventStore})
    : _eventStore = eventStore;

  /// Backend holding the destinations' schedules and queues: the event
  /// store's backend.
  StorageBackend get _backend => _eventStore._backend;

  /// Event store used to stamp config-change audit events inside the
  /// same transaction as the underlying mutation. The store's own
  /// `Source` is reused for every audit emission.
  final EventStore _eventStore;

  /// The event store this registry appends through. The delivery cycle's
  /// drainer runs its outcome transactions in it.
  @internal
  EventStore get eventStore => _eventStore;

  final Map<String, _Local> _destinations = <String, _Local>{};

  /// Ids whose registration in this registry is in progress.
  final Set<String> _registering = <String>{};

  /// Runs one registry operation's transaction and throws its refusal after
  /// the commit. For an outcome that commits, [onCommitted] updates this
  /// registry's own state before the delivery cycle is woken, so the woken
  /// pass sees the registry as the operation left it.
  Future<T> _run<T>(
    String op,
    Future<_Outcome<T>> Function(Transaction txn, PublishCollector collector)
    body, {
    void Function(T value)? onCommitted,
  }) async {
    final before = DeliveryTestHooks.current?.beforeRegistryTransaction;
    if (before != null) await before(op);
    final outcome = await _eventStore.runTransaction((txn, collector) async {
      _observeBodyRun(op);
      return body(txn, collector);
    });
    switch (outcome) {
      case _Done<T>(:final value):
        onCommitted?.call(value);
        // The drainer acts on what the operation committed at its next pass.
        _eventStore._wakeDeliveryCycle();
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
    await _backend.writeRegistryCheckTxn(
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
  /// record, when [destination]'s id is empty or contains `|` (the default
  /// destination-wedges view joins the database identity and the id with
  /// `|`), or when this registry already holds [destination]'s id under the
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
      await _run<String>(
        'addDestination',
        (txn, collector) async {
          // Implements: EVS-DEV-destination-drain/K
          // destination identifiers are non-empty and exclude '|'.
          if (id.isEmpty || id.contains('|')) {
            return _decideWithoutChange<String>(
              txn,
              op: 'addDestination',
              destinationId: id,
              check: 'refused_invalid_identifier',
              outcome: _Refused<String>(
                ArgumentError.value(
                  id,
                  'destination.id',
                  'a destination identifier must be non-empty and must not '
                      "contain '|'",
                ),
              ),
            );
          }
          final persisted = await _backend.readScheduleTxn(txn, id);
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
            eventType: kDestinationRegisteredEventType,
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
          await _backend.writeScheduleTxn(
            txn,
            id,
            DestinationSchedule(
              startDate: persisted?.startDate,
              endDate: persisted?.endDate,
              registrationId: registration,
              allowHardDelete: allowHardDelete,
            ),
          );
          _consultAuditAppendSeam(kDestinationRegisteredEntryType);
          return _Done<String>(registration);
        },
        onCommitted: (registrationId) {
          _destinations[id] = _Local(destination, registrationId);
        },
      );
    } finally {
      if (reserved) _registering.remove(id);
    }
  }

  /// The registration [id] is registered under in this registry, or null
  /// when this registry does not register it.
  @internal
  String? localRegistrationId(String id) => _destinations[id]?.registrationId;

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
    final persisted = await _backend.readSchedule(id);
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
    final current = await _backend.readScheduleTxn(txn, id);
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
      final head = await _backend.readFifoHeadTxn(txn, id);
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
    await _backend.writeScheduleTxn(
      txn,
      id,
      DestinationSchedule(
        startDate: when,
        endDate: current.endDate,
        registrationId: current.registrationId,
        allowHardDelete: current.allowHardDelete,
      ),
    );
    final existing = await _backend.readReplayRequestTxn(txn, id);
    final request = priorStartDate == null
        ? ReplayRequest(firstActivation: true, gapUpper: existing?.gapUpper)
        : ReplayRequest(
            firstActivation: existing?.firstActivation ?? false,
            gapUpper:
                existing?.gapUpper ??
                ((existing?.firstActivation ?? false) ? null : priorStartDate),
          );
    await _backend.writeReplayRequestTxn(txn, id, request);
    await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationStartDateSetEntryType,
      eventType: kDestinationStartDateSetEventType,
      data: <String, Object?>{
        'id': id,
        'start_date': when.toUtc().toIso8601String(),
        'prior_start_date': priorStartDate?.toUtc().toIso8601String(),
      },
      initiator: initiator,
    );
    _consultAuditAppendSeam(kDestinationStartDateSetEntryType);
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
    final current = await _backend.readScheduleTxn(txn, id);
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

    await _backend.writeScheduleTxn(txn, id, updated);
    await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationEndDateSetEntryType,
      eventType: kDestinationEndDateSetEventType,
      data: <String, Object?>{
        'id': id,
        'end_date': endDate.toUtc().toIso8601String(),
        'prior_end_date': current.endDate?.toUtc().toIso8601String(),
        'result': result.name,
      },
      initiator: initiator,
    );
    _consultAuditAppendSeam(kDestinationEndDateSetEntryType);
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
  /// that drains (at most one commits queue changes for the database,
  /// `EVS-PRD-destinations/V`), and deleting it would lose the record of a delivery the
  /// receiver may have accepted. Request a halt ([requestHalt]), wait for the
  /// drainer to wedge the head, then delete; the refusal message says
  /// whether a halt request is already open and not yet honoured. An empty
  /// queue and a wedged head are accepted.
  ///
  /// The deletion tombstones a wedged head, deletes the pending items behind
  /// it, removes the destination's schedule, fill position, replay request,
  /// wedge record, halt request and send fence, and keeps every item that
  /// was delivered, wedged or recovered (the delivery record) and the queue's
  /// sequence counter. A `system.destination_deleted` audit event records
  /// the tombstoned item, the number of pending items deleted, the opt-in it
  /// acted on and the halt request it closed (`closed_halt_request_event_id`,
  /// null when none was open). This registry forgets the destination after
  /// the commit.
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
      final schedule = await _backend.readScheduleTxn(txn, id);
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
      final head = await _backend.readFifoHeadTxn(txn, id);
      final halt = await _backend.readHaltRequestTxn(txn, id);
      if (head != null && head.finalStatus == null) {
        return _decideWithoutChange<void>(
          txn,
          op: op,
          destinationId: id,
          check: halt == null
              ? 'refused_pending_head'
              : 'refused_halt_not_honoured',
          outcome: _Refused<void>(
            StateError(
              halt == null
                  ? 'DestinationRegistry.deleteDestination($id): the queue '
                        'head ${head.entryId} is pending and may be in '
                        'delivery; deletion requires an empty queue or a '
                        'wedged head. Request a halt (requestHalt) and wait '
                        'for the drainer to wedge the head, then delete.'
                  : 'DestinationRegistry.deleteDestination($id): a halt is '
                        'requested (request ${halt.requestEventId}) and not '
                        'yet honoured; the queue head ${head.entryId} is '
                        'pending and may be in delivery. Wait for the drainer '
                        'to wedge the head (watch the default '
                        'destination-wedges view or wedgedFifos), then '
                        'delete.',
            ),
          ),
        );
      }
      final retirement = await _backend.retireQueueTxn(txn, id);
      await _backend.deleteScheduleTxn(txn, id);
      await _backend.clearReplayRequestTxn(txn, id);
      await _backend.clearWedgeRecordTxn(txn, id);
      await _backend.clearHaltRequestTxn(txn, id);
      await _backend.clearSendFenceTxn(txn, id);
      await _backend.clearRefillGuardTxn(txn, id);
      // Implements: EVS-PRD-destinations/T
      // a deletion appends a deletion event naming the wedged item it retires,
      //   if any.
      await _emitDestinationAuditInTxn(
        txn,
        collector,
        entryType: kDestinationDeletedEntryType,
        eventType: kDestinationDeletedEventType,
        data: <String, Object?>{
          'id': id,
          'tombstoned_row_id': retirement.tombstonedRowId,
          'deleted_pending_count': retirement.deletedPendingCount,
          'allow_hard_delete': schedule.allowHardDelete,
          'closed_halt_request_event_id': halt?.requestEventId,
        },
        initiator: initiator,
      );
      _consultAuditAppendSeam(kDestinationDeletedEntryType);
      return const _Done<void>(null);
    }, onCommitted: (_) => _destinations.remove(id));
  }

  /// Operator recovery of a wedged queue: tombstone the wedged head, delete
  /// the pending items behind it, rewind `fill_cursor`, remove the
  /// destination's wedge record, and append a
  /// `system.destination_wedge_recovered` audit event — all in one
  /// transaction that first reads the head.
  ///
  /// Refused, with nothing written but the registry check record:
  /// - `ArgumentError` when the database holds no schedule for
  ///   [destinationId], or when [fifoRowId] is not the queue's current head
  ///   (absent, `sent`, `tombstoned`, or behind the head);
  /// - `StateError` when the head is pending: recovery requires a wedged
  ///   head. The message says whether a halt request is open and not yet
  ///   honoured (wait for the drainer to wedge the head), or none is (request
  ///   a halt first).
  ///
  /// Rebuilding a healthy queue, for example after changing a destination's
  /// transform or filter, is a halt followed by this recovery, in this
  /// order:
  ///
  /// 1. [requestHalt]; its purpose says whether the new configuration is
  ///    already deployed ([HaltPurpose.pause]) or not yet
  ///    ([HaltPurpose.reconfigure]).
  /// 2. Wait for the drainer to wedge the head (the default
  ///    destination-wedges view shows the wedge, with cause operator halt).
  /// 3. For [HaltPurpose.reconfigure], deploy the new configuration so that
  ///    the process that drains registers it (restart the drainer; in a
  ///    multi-instance deployment, finish the rollout).
  /// 4. [tombstoneAndRefill], from any process.
  ///
  /// A [HaltPurpose.reconfigure] request that a permanent refusal or an
  /// exhausted budget consumed follows the same order: the wedge record
  /// keeps the purpose of the request its wedge consumed, whatever the
  /// wedge's cause.
  ///
  /// The order is checked for a head whose wedge consumed a
  /// [HaltPurpose.reconfigure] request: the recovery is refused
  /// (`StateError`, nothing written but the registry check record) while
  /// the drainer that holds the drain lock has not declared its
  /// configuration since the lock changed hands (retry after its next
  /// pass), does not serve the destination, or still declares for it the
  /// configuration recorded when the halt was honoured
  /// (`declaredConfiguration`, compared by its fingerprint). An accepted
  /// recovery leaves a refill guard naming that recorded configuration:
  /// until a drainer that declares another configuration has refilled the
  /// destination, which removes the guard, a drainer that declares the
  /// recorded one does not fill it (for example an instance of the old
  /// revision that takes the lock during a rollout). The guard stays until
  /// that fill has refilled the whole rewound range (its fill position
  /// reaches the position the recovery rewound from), so a takeover part
  /// way through the refill does not mix items built under the two
  /// configurations. When the guard stays
  /// in place (a rollout rolled back, so every instance declares the halted
  /// configuration again), restart the drainer with a changed
  /// `configurationVersion`, so its fill proceeds and removes the guard, or
  /// delete the destination. `DestinationRegistry.readDeliveryStatus` shows
  /// the guard. A wedge with no halt purpose, or purpose
  /// [HaltPurpose.pause], is recoverable at any time.
  ///
  /// The recovery event records the drain epoch, the configuration the
  /// lock holder declares for the destination and its fingerprint (null
  /// when it declares none), and the fingerprint of the refill guard it
  /// set (null when it set none).
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
    final schedule = await _backend.readScheduleTxn(txn, destinationId);
    if (schedule == null) {
      return _refuseUnknown<TombstoneAndRefillResult>(txn, op, destinationId);
    }
    final head = await _backend.readFifoHeadTxn(txn, destinationId);
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
      final halt = await _backend.readHaltRequestTxn(txn, destinationId);
      return _decideWithoutChange<TombstoneAndRefillResult>(
        txn,
        op: op,
        destinationId: destinationId,
        check: halt == null
            ? 'refused_pending_head'
            : 'refused_halt_not_honoured',
        outcome: _Refused<TombstoneAndRefillResult>(
          StateError(
            halt == null
                ? 'tombstoneAndRefill($destinationId, $fifoRowId): recovery '
                      'requires a wedged head; the head is pending. To '
                      'rebuild a healthy queue, request a halt (requestHalt) '
                      'first, wait for the drainer to wedge the head, then '
                      'recover.'
                : 'tombstoneAndRefill($destinationId, $fifoRowId): a halt is '
                      'requested (request ${halt.requestEventId}) and not yet '
                      'honoured; the head is pending. Wait for the drainer to '
                      'wedge the head (watch the default destination-wedges '
                      'view or wedgedFifos), then recover.',
          ),
        ),
      );
    }
    // Implements: EVS-DEV-destination-drain/F
    // recovery of a reconfigure halt is refused while the drain-lock holder
    //   declares the configuration recorded when the halt was honoured, or
    //   has declared none since the lock changed hands; an accepted one
    //   leaves a refill guard, recorded in the recovery event.
    final wedge = await _backend.readWedgeRecordTxn(txn, destinationId);
    final epoch = await _backend.readDrainEpochTxn(txn);
    final stored = await _backend.readDrainerDeclarationTxn(txn);
    final holder = stored != null && stored.epoch == epoch ? stored : null;
    final holderFingerprint = holder?.fingerprints[destinationId];
    final holderConfiguration = holder?.configurations[destinationId];
    String? guardFingerprint;
    if (wedge?.haltPurpose == HaltPurpose.reconfigure) {
      final recorded = wedge!.configurationFingerprint;
      final refusal = holder == null
          ? (
              'refused_no_declaration_since_lock_changed',
              'no drainer has declared its configuration since the lock '
                  'changed hands; retry after its next pass.',
            )
          : holderFingerprint == null
          ? (
              'refused_not_served',
              'no drainer serves this destination: the process that drains '
                  'does not register it. Register the new configuration in '
                  'the draining process, or delete the destination.',
            )
          : holderFingerprint == recorded
          ? (
              'refused_configuration_unchanged',
              'the drainer still declares the configuration in effect when '
                  'the halt was honoured. If the new configuration is not '
                  'deployed yet, deploy it first, and pass a new '
                  'configurationVersion to SyncCycle.start when the change is '
                  "not visible in the destination's declared fields. If the "
                  'new configuration was already deployed before the halt, '
                  'restart the drainer with a changed configurationVersion to '
                  'recover this halt; for a later rebuild under an '
                  'already-deployed configuration, request the halt with '
                  'purpose pause.',
            )
          : null;
      if (refusal != null) {
        return _decideWithoutChange<TombstoneAndRefillResult>(
          txn,
          op: op,
          destinationId: destinationId,
          check: refusal.$1,
          outcome: _Refused<TombstoneAndRefillResult>(
            StateError(
              'tombstoneAndRefill($destinationId, $fifoRowId): the head was '
              'wedged for a reconfigure halt, and ${refusal.$2}',
            ),
          ),
        );
      }
      guardFingerprint = recorded;
    }
    final targetFirstSeq = head.sequenceRange.firstSeq;
    final targetLastSeq = head.sequenceRange.lastSeq;
    final cursorBefore = await _backend.readFillCursorTxn(txn, destinationId);
    await _backend.setFinalStatusTxn(
      txn,
      destinationId,
      fifoRowId,
      FinalStatus.tombstoned,
    );
    final sweep = await _backend.deleteNullRowsAfterSequenceInQueueTxn(
      txn,
      destinationId,
      head.sequenceInQueue,
    );
    final swept = sweep.minFirstSeq;
    final lowest = swept != null && swept < targetFirstSeq
        ? swept
        : targetFirstSeq;
    final rewoundTo = lowest - 1;
    await _backend.writeFillCursorTxn(txn, destinationId, rewoundTo);
    await _backend.clearWedgeRecordTxn(txn, destinationId);
    // Implements: EVS-PRD-destinations/T
    // an operator recovery of a wedged queue appends a recovery event in the
    //   transaction that retires the wedged head.
    final recovery = await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationWedgeRecoveredEntryType,
      eventType: kDestinationWedgeRecoveredEventType,
      data: <String, Object?>{
        'id': destinationId,
        'row_id': fifoRowId,
        'target_event_id_range_first_seq': targetFirstSeq,
        'target_event_id_range_last_seq': targetLastSeq,
        'deleted_trail_count': sweep.deletedCount,
        'rewound_to': rewoundTo,
        'drainer_epoch': epoch,
        'drainer_configuration_fingerprint': holderFingerprint,
        'drainer_configuration': holderConfiguration,
        'refill_guard_fingerprint': guardFingerprint,
      },
      initiator: initiator,
    );
    if (guardFingerprint != null) {
      await _backend.writeRefillGuardTxn(
        txn,
        destinationId,
        RefillGuard(
          fingerprint: guardFingerprint,
          recoveryEventId: recovery.eventId,
          refillThrough: cursorBefore,
        ),
      );
    }
    _consultAuditAppendSeam(kDestinationWedgeRecoveredEntryType);
    return _Done<TombstoneAndRefillResult>(
      TombstoneAndRefillResult(
        rowId: fifoRowId,
        deletedTrailCount: sweep.deletedCount,
        rewoundTo: rewoundTo,
      ),
    );
  });

  /// Read the persisted delivery status of this registry's database: the
  /// declaration of the current drainer, the one delivery cycle that
  /// commits queue changes for the database (`EVS-PRD-destinations/V`): its
  /// drain epoch, the configuration it declares for each destination it
  /// serves, and the destinations it does not serve, as of its latest pass.
  /// Also its latest heartbeat, and for
  /// each persisted destination its schedule, open halt request, wedge
  /// record, refill guard and unserved reason. All of it is read in one
  /// transaction, from persisted state only, so any process can read it,
  /// including one that does not drain and registers nothing.
  ///
  /// `drainer` is null when no drainer has declared since the current drain
  /// epoch began (the lock changed hands and the new holder has not
  /// completed a pass start yet), and then no destination carries an
  /// unserved reason. `heartbeat` is the latest pass start's record
  /// whatever its epoch: compare its `epoch` with `drainer`'s to tell
  /// whether it belongs to the current drainer. The default destination-wedges view
  /// covers wedges only: a destination that is unserved or held by a refill
  /// guard is not wedged, and appears here, not in the view. On a browser
  /// database shared by several tabs the read reflects this tab's latest
  /// known revision of the database.
  // Implements: EVS-DEV-destination-drain/T
  // the persisted delivery status: each destination's schedule, open halt
  //   request, wedge record, refill guard and unserved reason, and the
  //   current drainer's declaration, read from any process.
  Future<DeliveryStatus> readDeliveryStatus() =>
      _backend.transaction((txn) async {
        final epoch = await _backend.readDrainEpochTxn(txn);
        final stored = await _backend.readDrainerDeclarationTxn(txn);
        final drainer = stored != null && stored.epoch == epoch ? stored : null;
        final heartbeat = await _backend.readDrainHeartbeatTxn(txn);
        final schedules = await _backend.listSchedulesTxn(txn);
        final destinations = <String, DestinationDeliveryStatus>{};
        for (final entry in schedules.entries) {
          final id = entry.key;
          destinations[id] = DestinationDeliveryStatus(
            schedule: entry.value,
            openHaltRequest: await _backend.readHaltRequestTxn(txn, id),
            wedge: await _backend.readWedgeRecordTxn(txn, id),
            refillGuard: await _backend.readRefillGuardTxn(txn, id),
            unserved: drainer?.unserved[id],
          );
        }
        return DeliveryStatus(
          drainer: drainer,
          heartbeat: heartbeat,
          destinations: destinations,
        );
      });

  /// Request that the drainer halt delivery on destination [destinationId],
  /// and return the identifier of the request event.
  ///
  /// The drainer honours the request at the top of its next iteration for
  /// the destination, before any further send, by wedging the queue head
  /// itself with cause [WedgeCause.operatorHalt]; a send already in flight
  /// completes first, and its outcome is recorded. Only the drainer wedges a
  /// head, so a wedged head is never in delivery.
  ///
  /// Inside one transaction: reads the persisted schedule, the open request
  /// and the queue head; appends a `system.destination_halt_requested` event
  /// recording the destination, the database identity and [purpose]; and
  /// writes the destination's halt request naming that event. The request is
  /// accepted on an empty queue, and stays open until a head exists and the
  /// drainer wedges it, or until [cancelHalt] or a deletion closes it. Any
  /// wedge consumes the open request, whatever its cause, and records it.
  ///
  /// Refused, with nothing written but the registry check record:
  /// - `ArgumentError` when the database holds no schedule for
  ///   [destinationId];
  /// - `StateError` while a request is open for the destination, or while
  ///   its queue head is wedged (the halt is already in effect).
  ///
  /// The operation acts on persisted state: the calling process need not
  /// register the destination, and the process that drains honours the
  /// request whether or not it registers the destination.
  ///
  /// [purpose] records why delivery is halted and decides the order of a
  /// rebuild (see [tombstoneAndRefill]): [HaltPurpose.reconfigure] when a new
  /// delivery configuration for the destination is still to be deployed,
  /// [HaltPurpose.pause] when it is already deployed or when delivery is
  /// only to stop.
  Future<String> requestHalt(
    String destinationId, {
    required Initiator initiator,
    required HaltPurpose purpose,
  }) => _run<String>('requestHalt', (txn, collector) async {
    const op = 'requestHalt';
    final schedule = await _backend.readScheduleTxn(txn, destinationId);
    if (schedule == null) return _refuseUnknown<String>(txn, op, destinationId);
    final open = await _backend.readHaltRequestTxn(txn, destinationId);
    if (open != null) {
      return _decideWithoutChange<String>(
        txn,
        op: op,
        destinationId: destinationId,
        check: 'refused_halt_open',
        outcome: _Refused<String>(
          StateError(
            'DestinationRegistry.requestHalt($destinationId): a halt request '
            '(${open.requestEventId}) is already open; cancel it '
            '(cancelHalt) or wait for the drainer to honour it.',
          ),
        ),
      );
    }
    final head = await _backend.readFifoHeadTxn(txn, destinationId);
    if (head?.finalStatus == FinalStatus.wedged) {
      return _decideWithoutChange<String>(
        txn,
        op: op,
        destinationId: destinationId,
        check: 'refused_wedged_head',
        outcome: _Refused<String>(
          StateError(
            'DestinationRegistry.requestHalt($destinationId): the queue head '
            '${head!.entryId} is wedged; delivery is already halted.',
          ),
        ),
      );
    }
    final event = await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationHaltRequestedEntryType,
      eventType: kDestinationHaltRequestedEventType,
      data: <String, Object?>{'id': destinationId, 'purpose': purpose.wire},
      initiator: initiator,
    );
    await _backend.writeHaltRequestTxn(
      txn,
      destinationId,
      HaltRequest(
        requestEventId: event.eventId,
        requestedAt: event.clientTimestamp,
        purpose: purpose,
        requestedBy: event.initiator.toJson(),
      ),
    );
    _consultAuditAppendSeam(kDestinationHaltRequestedEntryType);
    return _Done<String>(event.eventId);
  });

  /// Cancel destination [destinationId]'s open halt request.
  ///
  /// Inside one transaction: reads the persisted schedule and the open
  /// request, removes the request, and appends a
  /// `system.destination_halt_cancelled` event naming the request event it
  /// closes (`halt_request_event_id`). Once the drainer has honoured a
  /// request, the wedge has closed it and a cancellation is refused; the
  /// wedge is recovered with [tombstoneAndRefill].
  ///
  /// Refused, with nothing written but the registry check record:
  /// `ArgumentError` when the database holds no schedule for
  /// [destinationId]; `StateError` when no request is open.
  ///
  /// The operation acts on persisted state: the calling process need not
  /// register the destination.
  Future<void> cancelHalt(
    String destinationId, {
    required Initiator initiator,
  }) => _run<void>('cancelHalt', (txn, collector) async {
    const op = 'cancelHalt';
    final schedule = await _backend.readScheduleTxn(txn, destinationId);
    if (schedule == null) return _refuseUnknown<void>(txn, op, destinationId);
    final open = await _backend.readHaltRequestTxn(txn, destinationId);
    if (open == null) {
      return _decideWithoutChange<void>(
        txn,
        op: op,
        destinationId: destinationId,
        check: 'refused_no_halt_open',
        outcome: _Refused<void>(
          StateError(
            'DestinationRegistry.cancelHalt($destinationId): no halt request '
            'is open.',
          ),
        ),
      );
    }
    await _backend.clearHaltRequestTxn(txn, destinationId);
    await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationHaltCancelledEntryType,
      eventType: kDestinationHaltCancelledEventType,
      data: <String, Object?>{
        'id': destinationId,
        'halt_request_event_id': open.requestEventId,
      },
      initiator: initiator,
    );
    _consultAuditAppendSeam(kDestinationHaltCancelledEntryType);
    return const _Done<void>(null);
  });

  /// Honour destination [destinationId]'s open halt request inside [txn].
  /// Called only by the delivery cycle's drainer, in the transaction that
  /// honours the request [requestEventId] it read before.
  ///
  /// Reads the stored request and the queue head inside [txn]:
  /// - when the stored request is not [requestEventId] (cancelled or
  ///   replaced since the drainer read it), or the queue has no pending head,
  ///   writes nothing and returns [HaltHonour.changed];
  /// - when the log holds no `system.destination_halt_requested` event with
  ///   that identifier naming this destination and this database, removes
  ///   the stored request, wedges nothing and returns
  ///   [HaltHonour.unverified]: the log is authoritative;
  /// - otherwise wedges the head with cause [WedgeCause.operatorHalt]
  ///   through [_wedgeHeadInTxn], which consumes the request, and returns
  ///   [HaltHonour.honoured]. [maxAttempts] is the retry budget in effect,
  ///   or null when the draining process does not register the destination.
  ///   [drainerEpoch], [configuration] and [configurationFingerprint] are
  ///   recorded as [_wedgeHeadInTxn] records them.
  Future<HaltHonour> _honourHaltInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String destinationId,
    required String requestEventId,
    required int? maxAttempts,
    required int drainerEpoch,
    required Map<String, Object?>? configuration,
    required String? configurationFingerprint,
  }) async {
    final request = await _backend.readHaltRequestTxn(txn, destinationId);
    if (request == null || request.requestEventId != requestEventId) {
      return HaltHonour.changed;
    }
    final verified = await _verifiedHaltTxn(txn, destinationId, request);
    if (verified == null) {
      await _backend.clearHaltRequestTxn(txn, destinationId);
      return HaltHonour.unverified;
    }
    final head = await _backend.readFifoHeadTxn(txn, destinationId);
    if (head == null || head.finalStatus != null) return HaltHonour.changed;
    await _wedgeHeadInTxn(
      txn,
      collector,
      destinationId: destinationId,
      rowId: head.entryId,
      cause: WedgeCause.operatorHalt,
      maxAttempts: maxAttempts,
      drainerEpoch: drainerEpoch,
      configuration: configuration,
      configurationFingerprint: configurationFingerprint,
    );
    return HaltHonour.honoured;
  }

  /// [request] with its request event, when the log holds a
  /// `system.destination_halt_requested` event with its identifier that
  /// names [destinationId] and this registry's database and records a known
  /// purpose; null otherwise.
  Future<_VerifiedHalt?> _verifiedHaltTxn(
    Transaction txn,
    String destinationId,
    HaltRequest request,
  ) async {
    final event = await _backend.findEventByIdInTxn(
      txn,
      request.requestEventId,
    );
    if (event == null ||
        event.entryType != kDestinationHaltRequestedEntryType ||
        event.data['id'] != destinationId ||
        event.data['database_id'] != _eventStore.databaseId) {
      return null;
    }
    final purpose = event.data['purpose'];
    if (purpose is! String) return null;
    try {
      return _VerifiedHalt(request, event, HaltPurpose.fromWire(purpose));
    } on FormatException {
      return null;
    }
  }

  /// Wedge [destinationId]'s pending queue head [rowId] inside [txn]: mark
  /// it `wedged`, consume the destination's open halt request, append the
  /// wedge event, and write the destination's wedge record. Returns the
  /// wedge event, and the identifier an unverifiable stored halt request
  /// cited when the wedge removed one (null otherwise), for the caller to
  /// log once the transaction commits. Called only by the delivery cycle's
  /// drainer, in the transaction that decides the wedge.
  ///
  /// Reads the head, the wedge record and the halt request inside [txn] and
  /// throws [StateError], writing nothing, when the queue has no head, when
  /// [rowId] is not the head, when the head is not pending, when a wedge
  /// record exists (a pending head means no wedge is open), or when the
  /// evidence does not support [cause]: a [WedgeCause.permanentRefusal]
  /// needs a last attempt that reported a permanent failure, a
  /// [WedgeCause.retryBudgetExhausted] an attempt count at or above
  /// [maxAttempts], and a [WedgeCause.operatorHalt] an open halt request
  /// whose request event the log holds for this destination and database,
  /// on a head whose last attempt did not report a permanent failure (that
  /// head is wedged for the refusal). Throws [ArgumentError] when
  /// [maxAttempts] is below one, or null for
  /// [WedgeCause.retryBudgetExhausted].
  ///
  /// A wedge of any cause consumes the open halt request: it removes the
  /// stored request and records `halt_request_event_id`,
  /// `halt_requested_by` (the request event's initiator) and `halt_purpose`
  /// from the request event. A stored request whose event the log does not
  /// hold is removed and not recorded.
  ///
  /// For a permanent refusal or an exhausted budget the event's attempt
  /// fields (`attempt_count`, `last_outcome`, `http_status`) are read from
  /// the item's attempts as they stand in [txn], the final attempt included
  /// when the caller recorded it earlier in [txn]; an operator halt records
  /// them as null. `max_attempts` is [maxAttempts], the retry budget in
  /// effect, null when the draining process does not register the
  /// destination (no budget is in effect for it there). No text from an attempt's outcome
  /// enters the event. The wedge event of an operator halt names the request
  /// event as its initiator's triggering event.
  ///
  /// [drainerEpoch] is the drain epoch of the lock the drainer holds, and
  /// [configuration] and [configurationFingerprint] the configuration the
  /// drainer declares for the destination and its fingerprint (null when the
  /// draining process does not register the destination); the event records
  /// them as `drainer_epoch`, `configuration` and
  /// `configuration_fingerprint`, and the wedge record keeps the epoch and
  /// the fingerprint.
  Future<({StoredEvent wedgeEvent, String? discardedHaltRequestEventId})>
  _wedgeHeadInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String destinationId,
    required String rowId,
    required WedgeCause cause,
    required int? maxAttempts,
    required int drainerEpoch,
    required Map<String, Object?>? configuration,
    required String? configurationFingerprint,
  }) async {
    _observeBodyRun('wedgeHeadInTxn');
    if (maxAttempts == null && cause == WedgeCause.retryBudgetExhausted) {
      throw ArgumentError.value(
        maxAttempts,
        'maxAttempts',
        'a wedge of cause ${cause.wire} needs the retry budget in effect',
      );
    }
    if (maxAttempts != null && maxAttempts < 1) {
      throw ArgumentError.value(
        maxAttempts,
        'maxAttempts',
        'the retry budget must be at least one attempt',
      );
    }
    final head = await _backend.readFifoHeadTxn(txn, destinationId);
    if (head == null || head.entryId != rowId) {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): the item is not the queue '
        'head; the head is ${head?.entryId}.',
      );
    }
    if (head.finalStatus != null) {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): the head is '
        '${head.finalStatus!.toJson()}, not pending.',
      );
    }
    final open = await _backend.readWedgeRecordTxn(txn, destinationId);
    if (open != null) {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): the head is pending but a '
        'wedge record names item ${open.rowId}; a pending head means no '
        'wedge is open.',
      );
    }
    final attempts = head.attempts;
    final last = attempts.isEmpty ? null : attempts.last;
    if (cause == WedgeCause.permanentRefusal && last?.outcome != 'permanent') {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): cause ${cause.wire} but '
        'the last recorded attempt reported ${last?.outcome ?? 'nothing'}.',
      );
    }
    if (cause == WedgeCause.retryBudgetExhausted &&
        attempts.length < maxAttempts!) {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): cause ${cause.wire} but '
        'the item records ${attempts.length} attempts, below the budget '
        '$maxAttempts.',
      );
    }
    if (cause == WedgeCause.operatorHalt && last?.outcome == 'permanent') {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): cause ${cause.wire} but '
        'the last recorded attempt reported a permanent failure; the head '
        'is wedged for that refusal.',
      );
    }
    final stored = await _backend.readHaltRequestTxn(txn, destinationId);
    final halt = stored == null
        ? null
        : await _verifiedHaltTxn(txn, destinationId, stored);
    if (cause == WedgeCause.operatorHalt && halt == null) {
      throw StateError(
        'wedgeHeadInTxn($destinationId, $rowId): cause ${cause.wire} but no '
        'halt request the log holds is open for the destination.',
      );
    }
    await _backend.setFinalStatusTxn(
      txn,
      destinationId,
      rowId,
      FinalStatus.wedged,
    );
    // Implements: EVS-DEV-destination-drain/O
    // a wedge of any cause consumes the open halt request and records its
    //   identifier, requester and purpose.
    if (stored != null) {
      await _backend.clearHaltRequestTxn(txn, destinationId);
    }
    final halted = cause == WedgeCause.operatorHalt;
    // The append is where an injected wedge-event failure takes effect.
    _consultAuditAppendSeam(kDestinationWedgedEntryType);
    final wedgeEvent = await _emitDestinationAuditInTxn(
      txn,
      collector,
      entryType: kDestinationWedgedEntryType,
      eventType: kDestinationWedgedEventType,
      data: <String, Object?>{
        'id': destinationId,
        // `database_id` is added by the audit emitter.
        'row_id': rowId,
        'event_ids': List<String>.of(head.eventIds),
        'first_seq': head.sequenceRange.firstSeq,
        'last_seq': head.sequenceRange.lastSeq,
        'sequence_in_queue': head.sequenceInQueue,
        'cause': cause.wire,
        'attempt_count': halted ? null : attempts.length,
        'max_attempts': maxAttempts,
        'last_outcome': halted ? null : last?.outcome,
        'http_status': !halted && last?.outcome == 'transient'
            ? last?.httpStatus
            : null,
        'wire_format': head.wireFormat,
        'transform_version': head.transformVersion,
        'halt_request_event_id': halt?.event.eventId,
        'halt_requested_by': halt?.event.initiator.toJson(),
        'halt_purpose': halt?.purpose.wire,
        'drainer_epoch': drainerEpoch,
        'configuration_fingerprint': configurationFingerprint,
        'configuration': configuration,
      },
      initiator: halted
          ? AutomationInitiator(
              service: _drainService,
              triggeringEventId: halt!.event.eventId,
            )
          : _drainInitiator,
    );
    await _backend.writeWedgeRecordTxn(
      txn,
      destinationId,
      WedgeRecord(
        rowId: rowId,
        wedgeEventId: wedgeEvent.eventId,
        cause: cause,
        haltPurpose: halt?.purpose,
        drainerEpoch: drainerEpoch,
        configurationFingerprint: configurationFingerprint,
      ),
    );
    if (DeliveryTestHooks.current?.afterWedgeHeadInTxn?.call(destinationId) ??
        false) {
      throw InjectedFailure('after the wedge of $destinationId');
    }
    return (
      wedgeEvent: wedgeEvent,
      discardedHaltRequestEventId: stored != null && halt == null
          ? stored.requestEventId
          : null,
    );
  }

  /// Consults the `failRegistryAuditAppend` test seam for an audit of
  /// [entryType], inside the transaction that appends it: after a registry
  /// operation's last write, and for the drainer's wedge in place of the
  /// wedge event's append (after the `wedged` status write).
  void _consultAuditAppendSeam(String entryType) {
    if (DeliveryTestHooks.current?.failRegistryAuditAppend?.call(entryType) ??
        false) {
      throw InjectedFailure('registry audit append of $entryType');
    }
  }

  /// Emit a system audit event for a destination mutation inside [txn].
  ///
  /// The aggregate is stamped as `source.identifier` (the install UUID)
  /// / [kDestinationAuditAggregateType], and the event type is [eventType],
  /// the per-kind event type paired with [entryType] (for example
  /// [kDestinationDeletedEventType]), so a declarative filter or projection
  /// tells the kinds apart by event type. The destination identity lives in
  /// `data['id']`, and the identity of the database that appends the event
  /// (`EventStore.databaseId`) in `data['database_id']`, which the emitter
  /// adds. Every destination mutation a single install emits therefore lands
  /// in a single per-install hash-chained system aggregate. Emission uses no
  /// flow token, metadata, security, checkpoint, or change reason.
  /// dedupeByContent is left off because each destination mutation records
  /// a distinct timeline entry.
  ///
  /// `entry_type_version` is stamped by the substrate from the registry's
  /// `registeredVersion` for [entryType]; `EventStore.open` registers every
  /// destination audit entry type.
  // Implements: EVS-DEV-destination-drain/K
  // every destination audit event the library appends carries the identity
  //   of the database that appends it.
  Future<StoredEvent> _emitDestinationAuditInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String entryType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
  }) async {
    final event = await _eventStore._appendReservedInTxn(
      txn,
      collector,
      entryType: entryType,
      aggregateId: _eventStore.source.identifier,
      aggregateType: kDestinationAuditAggregateType,
      eventType: eventType,
      data: <String, Object?>{...data, 'database_id': _eventStore.databaseId},
      initiator: initiator,
    );
    // dedupeByContent is off, so the append always stores an event.
    return event!;
  }
}

/// The registry's wedge of a destination's queue head inside [txn], as the
/// drainer runs it, for the library's own tests of the wedge's refusals:
/// the wedge is private to the event store's Dart library, and the
/// drainer, which shares that library, is its only production caller. In
/// a build with assertions disabled it throws [StateError] before it
/// touches [txn].
// Implements: EVS-PRD-storage-barrier/J
// the test-only entry point to the wedge refuses in a build with assertions
//   disabled, so it changes nothing the library writes there.
@internal
@visibleForTesting
Future<({StoredEvent wedgeEvent, String? discardedHaltRequestEventId})>
wedgeHeadInTxnForTest(
  DestinationRegistry registry,
  Transaction txn,
  PublishCollector collector, {
  required String destinationId,
  required String rowId,
  required WedgeCause cause,
  required int? maxAttempts,
  required int drainerEpoch,
  required Map<String, Object?>? configuration,
  required String? configurationFingerprint,
}) async {
  var assertionsEnabled = false;
  assert(() {
    assertionsEnabled = true;
    return true;
  }(), 'records that assertions are enabled');
  if (!assertionsEnabled) {
    throw StateError(
      'wedgeHeadInTxnForTest is test-only and refuses in a build with '
      'assertions disabled',
    );
  }
  return registry._wedgeHeadInTxn(
    txn,
    collector,
    destinationId: destinationId,
    rowId: rowId,
    cause: cause,
    maxAttempts: maxAttempts,
    drainerEpoch: drainerEpoch,
    configuration: configuration,
    configurationFingerprint: configurationFingerprint,
  );
}
