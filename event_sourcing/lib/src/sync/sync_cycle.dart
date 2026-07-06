// Implements: EVS-PRD-destinations/C (FIFO order — each cycle runs fillBatch
//   then drain per destination so events are promoted into the queue and
//   shipped in the order they were appended)
// Implements: EVS-PRD-destinations/E (pluggable delivery — SyncCycle drives
//   drain() which calls Destination.send, the application-supplied transport)
// Implements: EVS-PRD-destinations/F (dynamic registration — registry.all() is
//   called per cycle so destinations added or removed since the last cycle are
//   reflected in the current run without restart)
import 'dart:developer' as developer;

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';

/// Top-level sync orchestrator.
///
/// One `SyncCycle` instance lives for the process lifetime. Its [call]
/// method is the single entry point that every trigger — app-lifecycle
/// resume, the 15-minute foreground timer, connectivity-restored event,
/// post-`record()` fire-and-forget, FCM message receipt — routes into.
/// Centralizing on one entry point is how the reentrancy guard works:
/// concurrent triggers race into [call] but only one drives the cycle.
///
/// Per-destination work in one cycle is fillBatch → drain. fillBatch
/// promotes events appended since the last cycle from the event log into
/// the destination's FIFO; drain ships them. Both run inside
/// [_fillAndDrainOrSwallow], which catches per-destination failures so
/// one bad destination cannot starve the others.
// fillBatch+drain, post-drain inbound poll, single-isolate reentrancy
// guard, no background isolate.
class SyncCycle {
  // (null falls back to SyncPolicy.defaults inside drain()), or a
  // SyncPolicy Function()? policyResolver invoked once per call() for
  // hot-swap scenarios. The two are mutually exclusive (D).
  SyncCycle({
    required StorageBackend backend,
    required DestinationRegistry registry,
    Source? source,
    Clock? clock,
    SyncPolicy? policy,
    SyncPolicy? Function()? policyResolver,
  }) : _backend = backend,
       _registry = registry,
       _source = source,
       _clock = clock,
       _policy = policy,
       _policyResolver = policyResolver {
    if (policy != null && policyResolver != null) {
      throw ArgumentError(
        'SyncCycle: supply at most one of policy / policyResolver',
      );
    }
  }

  final StorageBackend _backend;
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

        final destinations = _registry.all();
        // A thrown exception from one destination's fill or drain does
        // not cancel the others. See `_fillAndDrainOrSwallow` for the
        // per-destination exception handling.
        await Future.wait(
          destinations.map(
            (d) => _fillAndDrainOrSwallow(d, cyclePolicy, force),
          ),
        );
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

  Future<void> _fillAndDrainOrSwallow(
    Destination destination,
    SyncPolicy? cyclePolicy,
    bool flushHeld,
  ) async {
    // Step 1: promote events appended since the last cycle into this
    // destination's FIFO. Without this, drain has nothing to read past
    // what runHistoricalReplay enqueued at activation time, and every
    // post-activation event is stranded in the event log.
    try {
      final schedule = await _registry.scheduleOf(destination.id);
      await fillBatch(
        destination,
        backend: _backend,
        schedule: schedule,
        source: _source,
        clock: _clock,
        flushHeld: flushHeld,
      );
    } catch (e, st) {
      // Swallow — one destination's fill failure must
      // not cancel another's drain. The drain step still runs because
      // any FIFO rows enqueued by a prior cycle are still drainable.
      // Unlike drain (which records each attempt into the entry's
      // attempts[].error_message before throwing), fillBatch has no
      // per-attempt audit surface — without this log, a destination
      // whose fill fails on every cycle would silently stop receiving
      // new FIFO rows.
      developer.log(
        'fillBatch failed for destination ${destination.id}',
        name: 'sync_cycle',
        error: e,
        stackTrace: st,
      );
    }
    // Step 2: ship whatever sits at the FIFO head. Even if step 1 fell
    // through with an exception, drain may still have rows from earlier.
    try {
      await drain(
        destination,
        backend: _backend,
        clock: _clock,
        policy: cyclePolicy,
      );
    } catch (e, st) {
      // Per the contract, one destination's failure does not cancel
      // another's drain. We swallow here so Future.wait does not abort;
      // the drain loop itself has already recorded the attempt via its
      // internal try/catch on `destination.send`, so the exception is
      // not silently lost — it is still surfaced via the entry's
      // `attempts[].error_message`. The log line below adds an
      // operator-visible signal for failures that escape that internal
      // catch (programming bugs in drain itself).
      developer.log(
        'drain failed for destination ${destination.id}',
        name: 'sync_cycle',
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
