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
/// endpoint computes the rest when it is asked. Both boot phases count no
/// units, so the probe body carries the phase name and the elapsed time
/// only, read from this tracker's own clock rather than echoed from the
/// last report. Readiness is reached when the server's bootstrap has
/// returned ([markReady]), not when the boot reports [BootPhase.complete],
/// which means only that the event store opened.
class BootHealth {
  BootHealth({DateTime Function()? now})
    : _now = now ?? DateTime.now,
      _startedAt = (now ?? DateTime.now)();

  final DateTime Function() _now;
  final DateTime _startedAt;

  BootStatus _status = BootStatus.booting;
  BootProgress? _last;
  Object? _error;

  /// Where the server is in its start-up.
  BootStatus get status => _status;

  /// The latest boot report, or null before the first.
  BootProgress? get last => _last;

  /// The observer to pass as `onBootProgress`: records [progress].
  void record(BootProgress progress) => _last = progress;

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
          'elapsed_s': _now().difference(_startedAt).inMilliseconds / 1000,
        };
    }
  }

  /// 200 when ready, 503 otherwise.
  int get statusCode => _status == BootStatus.ready ? 200 : 503;
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
