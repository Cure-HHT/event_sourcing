// The demo server listens before its event store opens: `/livez` answers
// 200 from the moment it listens, `/health` answers 503 with the boot's
// phase, percentage and estimate while the event store boots, and 200 once
// the bootstrap has returned and the routes are served (not when the boot
// reports its completion); every other route answers 503 until then.
// App-side behaviour: carries no requirement citation.
@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:action_permissions_demo/server/boot_health.dart';
import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:action_permissions_demo/server/demo_server_host.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'support/demo_bootstrap.dart';

BootProgress _p(BootPhase phase, int done, int total, int ms) => BootProgress(
  phase: phase,
  done: done,
  total: total,
  elapsed: Duration(milliseconds: ms),
);

Future<(int, String)> _get(HttpClient client, int port, String path) async {
  final request = await client.get('localhost', port, path);
  final response = await request.close();
  return (response.statusCode, await utf8.decodeStream(response));
}

void main() {
  group('BootHealth', () {
    test('reports checks with its own wall clock while no report arrives', () {
      var now = DateTime.utc(2026, 9, 1, 12);
      final health = BootHealth(now: () => now);
      health.record(_p(BootPhase.checks, 0, 0, 5));
      now = now.add(const Duration(seconds: 42));
      expect(health.statusCode, 503);
      expect(health.body(), <String, Object?>{
        'status': 'booting',
        'phase': 'checks',
        'percent': null,
        'eta_s': null,
        'elapsed_s': 42.0,
      });
    });

    test('reports a phase percentage and an estimate from the phase\'s own '
        'elapsed time', () {
      final health = BootHealth(now: () => DateTime.utc(2026))
        ..record(_p(BootPhase.checks, 0, 0, 0))
        ..record(_p(BootPhase.promotion, 0, 1000, 2000))
        ..record(_p(BootPhase.promotion, 250, 1000, 7000));
      final body = health.body();
      expect(body['phase'], 'promotion');
      expect(body['percent'], 25);
      // 250 units took 5 s, so the 750 left take about 15 s.
      expect(body['eta_s'], 15.0);
    });

    test('a boot run started again restarts the phase estimate', () {
      final health = BootHealth(now: () => DateTime.utc(2026))
        ..record(_p(BootPhase.promotion, 0, 100, 0))
        ..record(_p(BootPhase.promotion, 50, 100, 1000))
        ..record(_p(BootPhase.checks, 0, 0, 1500))
        ..record(_p(BootPhase.promotion, 0, 100, 1600))
        ..record(_p(BootPhase.promotion, 10, 100, 1700));
      expect(health.body()['percent'], 10);
      expect(health.body()['eta_s'], 0.9);
    });

    test('the completion report alone is not ready', () {
      final health = BootHealth()..record(_p(BootPhase.complete, 0, 0, 10));
      expect(health.statusCode, 503);
      expect(health.body()['phase'], 'complete');
      health.markReady();
      expect(health.statusCode, 200);
      expect(health.body(), <String, Object?>{'status': 'ready'});
    });

    test('a failed boot stays unready', () {
      final health = BootHealth()..markFailed(StateError('refused'));
      expect(health.statusCode, 503);
      expect(health.body()['status'], 'failed');
      expect(health.body()['error'], contains('refused'));
    });
  });

  test('the host answers /livez and /health across a boot', () async {
    final host = await DemoServerHost.listen(port: 0);
    addTearDown(host.close);
    final client = HttpClient();
    addTearDown(client.close);
    final port = host.http.port;

    // Listening, before the event store opens.
    expect(await _get(client, port, '/livez'), (200, 'ok'));
    var health = await _get(client, port, '/health');
    expect(health.$1, 503);
    expect(jsonDecode(health.$2), containsPair('phase', 'checks'));
    expect((await _get(client, port, '/healthz')).$1, 503);

    // The observer records every report; the probe body is read at each.
    final seen = <Map<String, Object?>>[];
    final components = await bootstrapDemoServer(
      backend: SembastBackend(
        database: await newDatabaseFactoryMemory().openDatabase('boot.db'),
      ),
      idempotencyStore: DemoIdempotencyStore(),
      permissionsYaml: validPermissionsYaml,
      usersYaml: validUsersYaml,
      installIdentifier: '00000000-0000-4000-8000-0000000000b0',
      onBootProgress: (progress) {
        host.health.record(progress);
        seen.add(host.health.body());
      },
    );
    addTearDown(components.eventStore.close);
    expect(seen.map((b) => b['phase']), <String>['checks', 'complete']);
    expect(seen.map((b) => b['status']).toSet(), <String>{'booting'});

    // The event store opened, but the server is not ready until it serves.
    health = await _get(client, port, '/health');
    expect(health.$1, 503);
    expect(jsonDecode(health.$2), containsPair('phase', 'complete'));
    expect((await _get(client, port, '/livez')).$1, 200);

    host.serve(
      DemoRoutes(
        components: components,
        projection: PollingDemoStateProjection(components: components),
      ),
    );
    health = await _get(client, port, '/health');
    expect(health, (200, jsonEncode(<String, Object?>{'status': 'ready'})));
    expect(await _get(client, port, '/healthz'), (200, 'ok'));
    expect((await _get(client, port, '/livez')).$1, 200);
  });

  test('a delivery cycle that stops for good drops readiness', () async {
    final host = await DemoServerHost.listen(port: 0);
    addTearDown(host.close);
    final client = HttpClient();
    addTearDown(client.close);
    final port = host.http.port;
    final components = await bootstrapDemoServer(
      backend: SembastBackend(
        database: await newDatabaseFactoryMemory().openDatabase('fatal.db'),
      ),
      idempotencyStore: DemoIdempotencyStore(),
      permissionsYaml: validPermissionsYaml,
      usersYaml: validUsersYaml,
      installIdentifier: '00000000-0000-4000-8000-0000000000b1',
    );
    final cycle = await SyncCycle.start(
      registry: components.destinations,
      cadence: const Duration(milliseconds: 50),
    );
    addTearDown(cycle.close);
    host.serve(
      DemoRoutes(
        components: components,
        projection: PollingDemoStateProjection(components: components),
      ),
    );
    final lines = <String>[];
    final fatal = <Object>[];
    final watch = host.watchCycle(
      cycle,
      log: lines.add,
      onStoppedForGood: (cause) async => fatal.add(cause),
    );
    addTearDown(watch.cancel);
    expect(cycle.state, SyncCycleState.running);
    expect((await _get(client, port, '/health')).$1, 200);

    // The cycle's storage backend goes away under it (a fenced Postgres
    // backend takes the same path): the cycle stops for good.
    await components.eventStore.close();
    await cycle.stopped.timeout(const Duration(seconds: 10));
    expect(cycle.stopCause, isNotNull);
    await Future<void>.delayed(Duration.zero);

    expect(fatal, <Object>[cycle.stopCause!]);
    expect(host.health.status, BootStatus.failed);
    final health = await _get(client, port, '/health');
    expect(health.$1, 503);
    expect(jsonDecode(health.$2), containsPair('status', 'failed'));
    expect((await _get(client, port, '/healthz')).$1, 503);
    expect(lines, contains(startsWith('delivery cycle stopped: ')));
  });

  test('a closed delivery cycle does not drop readiness', () async {
    final host = await DemoServerHost.listen(port: 0);
    addTearDown(host.close);
    final components = await bootstrapDemoServer(
      backend: SembastBackend(
        database: await newDatabaseFactoryMemory().openDatabase('closed.db'),
      ),
      idempotencyStore: DemoIdempotencyStore(),
      permissionsYaml: validPermissionsYaml,
      usersYaml: validUsersYaml,
      installIdentifier: '00000000-0000-4000-8000-0000000000b2',
    );
    addTearDown(components.eventStore.close);
    final cycle = await SyncCycle.start(registry: components.destinations);
    host.serve(
      DemoRoutes(
        components: components,
        projection: PollingDemoStateProjection(components: components),
      ),
    );
    final fatal = <Object>[];
    final watch = host.watchCycle(
      cycle,
      log: (_) {},
      onStoppedForGood: (cause) async => fatal.add(cause),
    );
    addTearDown(watch.cancel);
    await cycle.close();
    await Future<void>.delayed(Duration.zero);
    expect(fatal, isEmpty);
    expect(host.health.status, BootStatus.ready);
  });
}
