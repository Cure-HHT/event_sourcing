// The demo server's HTTP host: it listens before the event store opens, so
// a platform's startup and liveness probes (`/livez`) and its readiness
// probe (`/health`) reach it during a long boot, and it serves the demo's
// routes once the bootstrap has returned.

import 'dart:async';
import 'dart:io';

import 'package:action_permissions_demo/server/boot_health.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

/// The listening demo server.
class DemoServerHost {
  DemoServerHost._(this.http, this.health);

  /// Binds [port] on [host] (0 picks a free port) and answers the probes at
  /// once: `/livez` 200, `/health` 503 with the boot's progress. Every other
  /// route answers 503 until [serve].
  static Future<DemoServerHost> listen({
    required int port,
    String host = 'localhost',
    BootHealth? health,
  }) async {
    final boot = health ?? BootHealth();
    late final DemoServerHost self;
    final http = await shelf_io.serve(
      bootGate(boot, () => self._handler),
      host,
      port,
    );
    return self = DemoServerHost._(http, boot);
  }

  /// The bound server.
  final HttpServer http;

  /// The boot the health probe reports; pass [BootHealth.record] as the
  /// event store's `onBootProgress`.
  final BootHealth health;

  Handler? _handler;

  /// The bootstrap returned: serve [routes] and report ready.
  void serve(DemoRoutes routes) {
    _handler = routes.handler;
    health.markReady();
  }

  /// The boot failed, or the server can no longer serve, with [error]:
  /// `/health` answers 503 with `{status: failed}` and every other route
  /// 503. The server then stops listening and exits.
  void fail(Object error) => health.markFailed(error);

  /// Stops listening.
  Future<void> close() => http.close(force: true);

  /// Watches the server's delivery [cycle].
  ///
  /// Logs the cycle's state through [log]: at once, then as sampled every
  /// 500 ms, so a change that reverts within one sample (a lock lost and
  /// re-acquired at once) can go unlogged; the drain epoch in
  /// `readDeliveryStatus().heartbeat` records every acquisition.
  ///
  /// When the cycle stops for good (its [SyncCycle.stopCause] is set: the
  /// storage backend was fenced or closed, or its database handle cannot
  /// commit), the instance can no longer commit to its database: the watch
  /// logs the cause, marks the server failed, so `/health` answers 503 and
  /// the platform's readiness probe stops routing traffic to it, and calls
  /// [onStoppedForGood], which stops the process. A cycle that is closed
  /// (the server shutting down) changes nothing.
  ///
  /// Returns the sampling timer, to cancel when the server stops.
  Timer watchCycle(
    SyncCycle cycle, {
    required void Function(String line) log,
    required Future<void> Function(Object cause) onStoppedForGood,
  }) {
    var last = cycle.state;
    log('delivery cycle: ${last.name}');
    late final Timer timer;
    unawaited(
      cycle.stopped.then((_) async {
        final cause = cycle.stopCause;
        if (cause == null) return;
        timer.cancel();
        log('delivery cycle stopped: $cause');
        fail(cause);
        await onStoppedForGood(cause);
      }),
    );
    return timer = Timer.periodic(const Duration(milliseconds: 500), (_) {
      final now = cycle.state;
      if (now == last) return;
      last = now;
      log('delivery cycle: ${now.name}');
    });
  }
}
