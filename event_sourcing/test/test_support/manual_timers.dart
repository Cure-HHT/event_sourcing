// Test support: timers a test fires by hand, installed through the
// `timerFactory` test seam, so the delivery cycle's cadence, heartbeat and
// drain-lock retries run only when the test says. This file declares no
// tests, so it carries no citation.
import 'dart:async';

import 'package:flutter_test/flutter_test.dart';

/// Creates timers that fire only when [fire] is called.
final class ManualTimers {
  final List<ManualTimer> _timers = <ManualTimer>[];

  /// The `timerFactory` seam: a periodic timer that fires on [fire]. Its
  /// callback runs in the zone that created it, as `Timer.periodic`'s does.
  Timer create(Duration period, void Function(Timer timer) callback) {
    final timer = ManualTimer._(
      period,
      Zone.current.bindUnaryCallback(callback),
    );
    _timers.add(timer);
    return timer;
  }

  /// The timers created so far that are still active.
  List<ManualTimer> get active => <ManualTimer>[
    for (final t in _timers)
      if (t.isActive) t,
  ];

  /// Fires every timer active now once, in creation order, then lets the
  /// work they started run.
  Future<void> fire() async {
    for (final timer in active) {
      timer._fire();
    }
    await pumpEventQueue();
  }
}

/// A timer of [ManualTimers].
final class ManualTimer implements Timer {
  ManualTimer._(this.period, this._callback);

  /// The period it was created with.
  final Duration period;
  final void Function(Timer timer) _callback;
  bool _active = true;
  int _tick = 0;

  void _fire() {
    if (!_active) return;
    _tick += 1;
    _callback(this);
  }

  @override
  void cancel() => _active = false;

  @override
  bool get isActive => _active;

  @override
  int get tick => _tick;
}

/// Timers that never fire: `timerFactory` for a test in which only
/// triggers may start a pass.
Timer neverFiringTimer(Duration period, void Function(Timer timer) callback) =>
    ManualTimers().create(period, callback);
