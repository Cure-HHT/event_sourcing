// Implements: EVS-DEV-event-store-open/G+H+J+K+L+M
// the boot's progress, reported to an optional observer that decides
//   nothing: phases, units counted before each phase's work, elapsed time;
//   the observer is not awaited, what it throws is logged, and a call from
//   it, or from work it started, into an event store or a shipped storage
//   backend while the boot runs is refused.
import 'dart:async';
import 'dart:math' as math;

import 'package:event_sourcing/src/logging.dart';
import 'package:meta/meta.dart';

/// A phase of the boot of `EventStore.open`, in the order the boot runs
/// them.
enum BootPhase {
  /// The open registers its data generation and decides, before writing
  /// anything, whether this build may open the database. It counts no units
  /// (`done` and `total` are 0). It is reported when the open starts, so it
  /// is also the phase while the open waits for the boot lock. A boot
  /// transaction that the backend runs again reports this phase again, and
  /// the phases after it, when its next run starts; between a discarded run
  /// and the next one (the backend's retry delay and its wait for the boot
  /// lock) the last report of the discarded run stands.
  checks,

  /// Snapshot promotion: the re-derivation of the rows of views whose
  /// stored target versions lag the registered entry-type versions.
  promotion,

  /// View catch-up: the re-derivation of views that are behind the log for
  /// an entry type in their interest.
  catchUp,

  /// The boot committed and the open is about to return the store. It counts
  /// no units (`done` and `total` are 0).
  complete,
}

/// One report of the progress of the boot of `EventStore.open`, delivered
/// to the observer passed as `onBootProgress`.
///
/// [done] and [total] count the units of work of [phase]. The total is
/// counted before the phase's work starts and does not change while the
/// phase runs; [done] starts at 0, never falls within the phase, and equals
/// [total] at the phase's last report. A unit is one aggregate whose row an
/// aggregate view re-derives, or one event of the log a table view's refold
/// reads (a table view is refolded whole, over the events the log holds when
/// the phase starts). The phases [BootPhase.checks] and [BootPhase.complete]
/// count no units.
///
/// The two kinds of unit cost different amounts of work: re-deriving an
/// aggregate reads every event of that aggregate, reading one event of a
/// table refold reads one. A phase that re-derives both an aggregate view
/// and a table view adds both kinds into one [total], so `done / total` is
/// then a fraction of the units, not of the phase's time, and an estimate
/// of the time left extrapolated from it is approximate.
///
/// [elapsed] is the time since the open began, read from a monotonic clock.
@immutable
class BootProgress {
  const BootProgress({
    required this.phase,
    required this.done,
    required this.total,
    required this.elapsed,
  });

  /// The phase the boot is in.
  final BootPhase phase;

  /// The units of [phase] finished so far.
  final int done;

  /// The units [phase] has to do, counted before it started.
  final int total;

  /// The time since the open began.
  final Duration elapsed;

  @override
  bool operator ==(Object other) =>
      other is BootProgress &&
      other.phase == phase &&
      other.done == done &&
      other.total == total &&
      other.elapsed == elapsed;

  @override
  int get hashCode => Object.hash(phase, done, total, elapsed);

  @override
  String toString() =>
      'BootProgress(${phase.name} $done/$total, '
      '${elapsed.inMilliseconds} ms)';
}

/// The units a phase finishes between two reports: a phase reports when it
/// starts, each time it has finished this many more units, and when it ends.
@internal
const int kBootProgressChunk = 500;

/// Private zone key marking code that runs from a boot progress observer.
final Object _observerScopeKey = Object();

/// Whether the boot an observer reports on is still running.
class _ObserverScope {
  bool bootRunning = true;
}

/// Throws [StateError] when called, directly or from work it started in its
/// zone, by the boot progress observer of an `EventStore.open` whose boot is
/// still running. [operation] names what was called. The rule is uniform:
/// it holds for every event store and backend, whichever database they are
/// over, because the observer only records.
@internal
void refuseCallFromBootProgressObserver(String operation) {
  final scope = Zone.current[_observerScopeKey];
  if (scope is _ObserverScope && scope.bootRunning) {
    throw StateError(
      '$operation was called from a boot progress observer while the boot '
      'of EventStore.open runs. The observer runs inside the boot and must '
      'not call back into an event store; record the progress and act on it '
      'after the open returns.',
    );
  }
}

/// Reports the progress of one open's boot to its observer.
///
/// Every report calls the observer synchronously, in an error zone that
/// catches and logs whatever it throws, synchronously or from work it
/// started, and that marks the code running in it so that a call back into
/// an event store or a shipped storage backend while the boot runs is
/// refused ([refuseCallFromBootProgressObserver]). The boot does not await a
/// future the observer returns; the observer's synchronous work runs inside
/// the boot and delays it.
///
/// The zone outlives the boot: work the observer started keeps running in
/// it, and an error that work raises later is logged here too, not
/// delivered to the caller's zone. A future created in the zone that fails
/// delivers its error to this zone, so code in another error zone awaiting
/// it does not complete.
@internal
class BootProgressReporter {
  BootProgressReporter(this._observer);

  final void Function(BootProgress progress)? _observer;
  final Stopwatch _stopwatch = Stopwatch()..start();
  final _ObserverScope _scope = _ObserverScope();
  int _bodyRuns = 0;

  /// Reports [phase] at [done] of [total] units.
  void report(BootPhase phase, int done, int total) {
    final observer = _observer;
    if (observer == null) return;
    final progress = BootProgress(
      phase: phase,
      done: done,
      total: total,
      elapsed: _stopwatch.elapsed,
    );
    final scope = _scope;
    runZonedGuarded(
      () => observer(progress),
      (error, stackTrace) => libraryLog(
        'event_store',
        scope.bootRunning
            ? 'the boot progress observer threw; the boot continues'
            : 'the boot progress observer, or work it started, threw after '
                  'the boot finished',
        level: LibraryLogLevel.severe,
        error: error,
        stackTrace: stackTrace,
      ),
      zoneValues: <Object, Object>{_observerScopeKey: scope},
    );
  }

  /// Marks the start of a run of the boot transaction's body. The open
  /// reported [BootPhase.checks] before the first run; each later run
  /// reports it again, since that run decides again from what it reads.
  void beginBodyRun() {
    if (_bodyRuns > 0) report(BootPhase.checks, 0, 0);
    _bodyRuns += 1;
  }

  /// Starts [phase] over [total] units. A phase with no units reports
  /// nothing.
  BootPhaseProgress startPhase(BootPhase phase, int total) =>
      BootPhaseProgress._(this, phase, total);

  /// Marks the boot finished: from here on, calls the observer started are
  /// no longer refused.
  void bootFinished() => _scope.bootRunning = false;
}

/// The progress of one phase of the boot.
@internal
class BootPhaseProgress {
  BootPhaseProgress._(this._reporter, this.phase, this.total) {
    if (total > 0) _emit();
  }

  final BootProgressReporter _reporter;

  /// The phase this progress reports.
  final BootPhase phase;

  /// The units of the phase, counted before it started.
  final int total;

  int _done = 0;
  int _reported = 0;

  /// Records [units] more units finished; reports when a chunk's worth has
  /// finished since the last report. Never exceeds [total].
  void add(int units) {
    if (total == 0 || units <= 0) return;
    _done = math.min(total, _done + units);
    if (_done - _reported >= kBootProgressChunk) _emit();
  }

  /// Ends the phase: every unit is done, reported unless already reported.
  void end() {
    if (total == 0) return;
    _done = total;
    if (_reported != _done) _emit();
  }

  void _emit() {
    _reported = _done;
    _reporter.report(phase, _done, total);
  }
}
