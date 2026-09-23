// Implements: EVS-PRD-destinations/C
// (FIFO order — each cycle runs fillBatch
//   then drain per destination so events are promoted into the queue and
//   shipped in the order they were appended)
// Implements: EVS-PRD-destinations/E
// (pluggable delivery — SyncCycle drives
//   drain() which calls Destination.send, the application-supplied transport)
// Implements: EVS-PRD-destinations/F
// (dynamic registration — registry.all() is
//   called per cycle so destinations added or removed since the last cycle are
//   reflected in the current run without restart)
// Implements: EVS-DEV-destination-drain/J
// (the cycle refuses a retry budget below
//   one: a static policy when the cycle is built, a resolved one by filling
//   and draining nothing in that cycle)
// Implements: EVS-DEV-destination-drain/T
// (each pass compares the persisted
//   schedules with the destinations its registry holds: it honours halt
//   requests on every persisted destination, fills and sends none that its
//   registry does not hold or that storage no longer knows, and reports
//   each of them)
// Implements: EVS-DEV-destination-drain/E
// (delivery fills and sends only the
//   destinations registered in the draining process)
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// Why a delivery cycle does not serve a destination: it neither fills nor
/// sends it.
enum UnservedReason {
  /// The database holds a schedule for the destination, and the cycle's
  /// registry does not register it (it is registered by another process,
  /// or was removed from this process's code). The cycle still honours a
  /// halt request on it, so an operator can halt it and then delete it.
  notRegisteredHere,

  /// The cycle's registry holds the destination, and the database holds no
  /// schedule for it (another process deleted it).
  deletedInStorage,
}

/// Top-level sync orchestrator.
///
/// One `SyncCycle` instance lives for the process lifetime. Its [call]
/// method is the single entry point that every trigger — app-lifecycle
/// resume, an application-owned foreground timer, connectivity-restored event,
/// post-`record()` fire-and-forget, FCM message receipt — routes into.
/// Centralizing on one entry point is how the reentrancy guard works:
/// concurrent triggers race into [call] but only one drives the cycle.
///
/// The retry policy is given statically (`policy:`) or resolved at the
/// start of each cycle (`policyResolver:`). Its attempt budget must be at
/// least one: a static policy below one is refused with an
/// [ArgumentError], and a cycle whose resolved policy is below one logs
/// the refusal and fills and drains nothing.
///
/// Per-destination work in one cycle is fillBatch → drain. fillBatch
/// promotes events appended since the last cycle from the event log into
/// the destination's FIFO; drain ships them. Both run inside
/// [_fillAndDrainOrSwallow], which catches per-destination failures so
/// one bad destination cannot starve the others.
///
/// A destination is code (its filter, transform and transport), so the
/// cycle fills and sends only the destinations its registry holds. Each
/// pass reads every persisted schedule and compares: a destination the
/// database knows and the registry does not hold, and one the registry
/// holds and the database no longer knows, are neither filled nor sent, and
/// are reported in [unserved] and logged once while they stay unserved. A
/// halt requested on a destination the registry does not hold is honoured
/// all the same; honouring a halt needs no transport.
// fillBatch+drain, post-drain inbound poll, single-isolate reentrancy
// guard, no background isolate.
class SyncCycle {
  // (null falls back to SyncPolicy.defaults inside drain()), or a
  // SyncPolicy Function()? policyResolver invoked once per call() for
  // hot-swap scenarios. The two are mutually exclusive (D).
  SyncCycle({
    required DestinationRegistry registry,
    Source? source,
    Clock? clock,
    SyncPolicy? policy,
    SyncPolicy? Function()? policyResolver,
  }) : _registry = registry,
       _source = source,
       _clock = clock,
       _policy = policy,
       _policyResolver = policyResolver {
    if (policy != null && policyResolver != null) {
      throw ArgumentError(
        'SyncCycle: supply at most one of policy / policyResolver',
      );
    }
    if (policy != null) checkRetryBudget(policy);
  }

  /// The registry whose destinations the cycle fills and drains. Fill and
  /// drain both act on the backend of the registry's event store, so they
  /// can never operate on two different backends.
  final DestinationRegistry _registry;

  /// Source identity for fillBatch. Required when any registered
  /// destination has `serializesNatively == true` (native destinations
  /// stamp the batch envelope with this identity); optional otherwise.
  /// fillBatch itself raises ArgumentError if a native destination needs
  /// it but none was supplied.
  final Source? _source;
  final Clock? _clock;
  final SyncPolicy? _policy;
  final SyncPolicy? Function()? _policyResolver;

  bool _inFlight = false;

  Map<String, UnservedReason> _unserved = const <String, UnservedReason>{};

  /// The destinations the latest pass did not serve, and why: those the
  /// database knows and this cycle's registry does not hold, and those the
  /// registry holds and the database no longer knows. Empty before the
  /// first pass, and after a pass that could not read the persisted
  /// schedules (that pass honours no halt on a destination its registry
  /// does not hold).
  Map<String, UnservedReason> get unserved =>
      Map<String, UnservedReason>.unmodifiable(_unserved);

  /// Set when a forced [call] (`flushHeld: true`) arrives while a cycle is
  /// already running. The running cycle checks this after each pass and runs
  /// one more forced pass, so a forced flush is never dropped by the
  /// reentrancy guard (the event it wanted shipped is still held otherwise).
  bool _pendingForce = false;

  /// True while a prior [call] invocation has not yet completed. Exposed
  /// for tests to assert the guard's internal state.
  bool get isInFlight => _inFlight;

  /// Run one drain-and-poll cycle. Returns immediately (without side
  /// effects) when a prior [call] is still running.
  ///
  /// [flushHeld] forces this cycle to bypass fillBatch's single-event
  /// `maxAccumulateTime` hold, so a lone matching event ships now instead of
  /// waiting for the coalescing window (or a later trigger) to elapse. If a
  /// prior cycle is in flight, the forced request is not dropped: it is
  /// recorded and the running cycle runs one additional forced pass once it
  /// finishes. Ordinary (unforced) triggers still coalesce as before.
  // inbound poll + reentrancy guard + per-cycle policy resolution.
  Future<void> call({bool flushHeld = false}) async {
    if (_inFlight) {
      // A cycle is already running. A forced flush must not be dropped by the
      // guard — mark it pending so the running cycle runs one more forced pass.
      if (flushHeld) _pendingForce = true;
      return;
    }
    _inFlight = true;
    try {
      var force = flushHeld;
      while (true) {
        _pendingForce = false;
        // Resolve once per cycle, after the reentrancy guard. The same
        // SyncPolicy value is forwarded to every destination's drain in
        // this cycle.
        final cyclePolicy = _policyResolver != null
            ? _policyResolver()
            : _policy;
        if (cyclePolicy != null && cyclePolicy.maxAttempts < 1) {
          // A resolved budget below one is refused: the cycle fills and
          // drains nothing under it. call() is triggered after appends and
          // must not throw, so the refusal is logged.
          libraryLog(
            'sync_cycle',
            'the policy resolver returned a retry budget of '
                '${cyclePolicy.maxAttempts}; a budget must be at least one '
                'attempt, so this cycle fills and drains nothing',
            level: LibraryLogLevel.severe,
          );
          break;
        }

        final pass = await _passTargets();
        // A thrown exception from one destination's fill or drain does
        // not cancel the others. See `_fillAndDrainOrSwallow` for the
        // per-destination exception handling. The two lists come from one
        // snapshot of the registry and are disjoint, so no destination is
        // drained and honoured at once.
        await Future.wait(<Future<void>>[
          ...pass.served.map(
            (d) => _fillAndDrainOrSwallow(d, cyclePolicy, force),
          ),
          ...pass.haltOnly.map(_honourHaltOrSwallow),
        ]);
        await pollInbound();

        // A forced call that arrived mid-cycle: run one more pass with the
        // hold bypassed so the event it wanted flushed actually ships.
        if (!_pendingForce) break;
        force = true;
      }
    } finally {
      _inFlight = false;
    }
  }

  /// What this pass does, decided from one snapshot of the registry and one
  /// read of the persisted schedules: the registered destinations it fills
  /// and sends (`served`), and the persisted destinations the registry does
  /// not hold, on which it only honours a halt request (`haltOnly`). Records
  /// the destinations it does not serve in [unserved] and logs each once per
  /// reason while it persists.
  ///
  /// When the schedules cannot be read, the pass serves every registered
  /// destination (a fill reads its own schedule), honours no halt on any
  /// other, and reports nothing unserved.
  Future<({List<Destination> served, List<String> haltOnly})>
  _passTargets() async {
    final registered = _registry.all();
    final Map<String, Object> persisted;
    try {
      if (DeliveryTestHooks.current?.failListSchedules?.call() ?? false) {
        throw const InjectedFailure('the read of the persisted schedules');
      }
      persisted = await _registry.backend.listSchedules();
    } catch (e, st) {
      libraryLog(
        'sync_cycle',
        'reading the persisted destination schedules failed; this pass '
            'fills and sends the registered destinations and honours no halt '
            'on any other',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
      _unserved = const <String, UnservedReason>{};
      return (served: registered, haltOnly: const <String>[]);
    }
    final registeredIds = <String>{for (final d in registered) d.id};
    final unserved = <String, UnservedReason>{
      for (final id in persisted.keys)
        if (!registeredIds.contains(id)) id: UnservedReason.notRegisteredHere,
      for (final id in registeredIds)
        if (!persisted.containsKey(id)) id: UnservedReason.deletedInStorage,
    };
    for (final entry in unserved.entries) {
      if (_unserved[entry.key] == entry.value) continue;
      libraryLog(
        'sync_cycle',
        entry.value == UnservedReason.notRegisteredHere
            ? 'destination ${entry.key} is persisted but not registered in '
                  "this cycle's registry; it is not filled or sent here, and "
                  'a halt requested on it is still honoured'
            : "destination ${entry.key} is registered in this cycle's "
                  'registry but has no persisted schedule (deleted by another '
                  'process); it is not filled or sent',
        level: LibraryLogLevel.warning,
      );
    }
    _unserved = unserved;
    return (
      served: <Destination>[
        for (final d in registered)
          if (persisted.containsKey(d.id)) d,
      ],
      haltOnly: <String>[
        for (final id in persisted.keys)
          if (!registeredIds.contains(id)) id,
      ],
    );
  }

  Future<void> _honourHaltOrSwallow(String destinationId) async {
    try {
      await honourHaltById(destinationId, registry: _registry);
    } catch (e, st) {
      libraryLog(
        'sync_cycle',
        'honouring a halt request failed for destination $destinationId',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  Future<void> _fillAndDrainOrSwallow(
    Destination destination,
    SyncPolicy? cyclePolicy,
    bool flushHeld,
  ) async {
    // Step 1: promote events appended since the last cycle into this
    // destination's FIFO, after performing any replay a registry operation
    // requested. The fill reads the persisted schedule itself.
    try {
      await fillBatch(
        destination,
        backend: _registry.backend,
        source: _source,
        clock: _clock,
        flushHeld: flushHeld,
      );
    } catch (e, st) {
      // Swallow — one destination's fill failure must
      // not cancel another's drain. The drain step still runs because
      // any FIFO rows enqueued by a prior cycle are still drainable.
      // A failed fill wrote nothing (its transaction rolled back) and has
      // no per-attempt record — without this log, a destination whose
      // fill fails on every cycle would silently stop receiving new FIFO
      // rows.
      libraryLog(
        'sync_cycle',
        'fillBatch failed for destination ${destination.id}',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
    // Step 2: ship whatever sits at the FIFO head. Even if step 1 fell
    // through with an exception, drain may still have rows from earlier.
    try {
      await drain(
        destination,
        registry: _registry,
        clock: _clock,
        policy: cyclePolicy,
      );
    } catch (e, st) {
      // Per the contract, one destination's failure does not cancel
      // another's drain. We swallow here so Future.wait does not abort.
      // A send's own failure never reaches here: drain records it as the
      // attempt's outcome, and a wedge that reports failure is followed by
      // a transaction that reads the head again and records the attempt
      // alone when the wedge did not commit. An exception that escapes
      // drain means an outcome transaction reported failure, so whether it
      // committed is not known here: the next pass reads the head again
      // (a wedged head ends the pass; a pending head's recorded attempts
      // decide its status) before any send. The log line below is the
      // only record of the failure.
      libraryLog(
        'sync_cycle',
        'drain failed for destination ${destination.id}',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  /// Inbound-tombstone hook, invoked once per cycle after every outbound
  /// drain completes. The body is a no-op: remote-authored tombstones
  /// (deletions initiated by another party) do not propagate inbound.
  /// The capability is recorded in `spec/roadmap/sync.md`.
  Future<void> pollInbound() async {
    // Intentionally empty.
  }
}
