// The demo server listens before it opens its event store, so a platform's
// probes reach it during a long boot: `/livez` answers 200 as soon as the
// process listens, and `/health` answers 503 with the boot's progress until
// the server is ready to serve, then 200.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:shelf/shelf.dart';

/// Where the server is in its start-up.
enum BootStatus {
  /// Listening; the event store is opening.
  booting,

  /// The event store is open and the routes are served.
  ready,

  /// The open failed; the process is about to exit.
  failed,
}

/// Tracks the boot of the server's event store from the reports of
/// `onBootProgress`, and answers the health probe from them.
///
/// The observer only records: [record] stores the report and returns. The
/// endpoint computes the rest when it is asked. During [BootPhase.checks]
/// (which also covers the wait for the boot lock another instance's boot
/// holds) no further report arrives, so the time shown is read from this
/// tracker's own clock rather than echoed from the last report. The
/// percentage is `done / total` of the phase in progress; the estimate of
/// the time left extrapolates the phase's own elapsed time, and is
/// approximate when a phase re-derives both aggregate and table views.
/// Readiness is reached when the server's bootstrap has returned
/// ([markReady]), not when the boot reports [BootPhase.complete], which
/// means only that the event store opened.
class BootHealth {
  BootHealth({DateTime Function()? now})
    : _now = now ?? DateTime.now,
      _startedAt = (now ?? DateTime.now)();

  final DateTime Function() _now;
  final DateTime _startedAt;

  BootStatus _status = BootStatus.booting;
  BootProgress? _last;
  Duration? _phaseStartElapsed;
  Object? _error;

  /// Where the server is in its start-up.
  BootStatus get status => _status;

  /// The latest boot report, or null before the first.
  BootProgress? get last => _last;

  /// The observer to pass as `onBootProgress`: records [progress].
  void record(BootProgress progress) {
    final previous = _last;
    if (previous == null ||
        previous.phase != progress.phase ||
        progress.done < previous.done) {
      // A new phase, or a boot run the backend started again (it reports
      // from its checks again, so the done count can fall).
      _phaseStartElapsed = progress.elapsed;
    }
    _last = progress;
  }

  /// The server's bootstrap returned: serve.
  void markReady() => _status = BootStatus.ready;

  /// The open failed with [error].
  void markFailed(Object error) {
    _status = BootStatus.failed;
    _error = error;
  }

  /// The body `/health` answers with.
  Map<String, Object?> body() {
    switch (_status) {
      case BootStatus.ready:
        return <String, Object?>{'status': 'ready'};
      case BootStatus.failed:
        return <String, Object?>{'status': 'failed', 'error': '$_error'};
      case BootStatus.booting:
        final last = _last;
        final phase = last?.phase ?? BootPhase.checks;
        return <String, Object?>{
          'status': 'booting',
          'phase': phase.name,
          'percent': _percent(last),
          'eta_s': _etaSeconds(last),
          'elapsed_s': _now().difference(_startedAt).inMilliseconds / 1000,
        };
    }
  }

  /// 200 when ready, 503 otherwise.
  int get statusCode => _status == BootStatus.ready ? 200 : 503;

  static int? _percent(BootProgress? last) {
    if (last == null || last.total <= 0) return null;
    return (last.done * 100 / last.total).floor();
  }

  double? _etaSeconds(BootProgress? last) {
    final phaseStartElapsed = _phaseStartElapsed;
    if (last == null ||
        last.total <= 0 ||
        last.done <= 0 ||
        phaseStartElapsed == null) {
      return null;
    }
    // The time the phase took for the units done so far, on the boot's own
    // clock, scaled to the units left.
    final inPhase = last.elapsed - phaseStartElapsed;
    final perUnit = inPhase.inMicroseconds / last.done;
    final remaining = perUnit * (last.total - last.done);
    return (remaining / Duration.microsecondsPerSecond * 10).round() / 10;
  }
}

/// The server's front handler: answers `/livez` and `/health` from the
/// moment the process listens, and every other route through [ready] once
/// [health] is ready (503 until then).
Handler bootGate(BootHealth health, Handler? Function() ready) {
  return (Request request) async {
    switch (request.url.path) {
      case 'livez':
        return Response.ok('ok');
      case 'health':
        return Response(
          health.statusCode,
          body: jsonEncode(health.body()),
          headers: const <String, String>{'content-type': 'application/json'},
        );
    }
    final handler = health.status == BootStatus.ready ? ready() : null;
    if (handler == null) {
      return Response(
        503,
        body: jsonEncode(health.body()),
        headers: const <String, String>{'content-type': 'application/json'},
      );
    }
    return handler(request);
  };
}
