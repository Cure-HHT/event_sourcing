// test/demo_routes_test.dart
// Verifies: EVS-PRD-action-dispatch/A
// Verifies: EVS-PRD-permissions-as-events/B
// Verifies: EVS-PRD-event-log/A/C
import 'dart:convert';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf.dart';

import 'support/demo_bootstrap.dart';

Future<Response> _post(
  DemoRoutes routes,
  String path,
  Map<String, Object?> body,
) async {
  final req = Request(
    'POST',
    Uri.parse('http://localhost$path'),
    body: jsonEncode(body),
    headers: <String, String>{'content-type': 'application/json'},
  );
  return routes.handler(req);
}

Future<Response> _get(DemoRoutes routes, String path) async {
  final req = Request('GET', Uri.parse('http://localhost$path'));
  return routes.handler(req);
}

Future<Map<String, Object?>> _readJson(Response r) async {
  final text = await r.readAsString();
  return jsonDecode(text) as Map<String, Object?>;
}

/// Run the routes test suite against the [factory]-supplied backend
/// pair. The [label] disambiguates test names when multiple flavors run
/// in the same `flutter test` invocation.
void runDemoRoutesTests(DemoBackendFactory factory, {required String label}) {
  Future<DemoRoutes> makeRoutes(String installId) async {
    final backends = await factory();
    final components = await bootstrapDemoServer(
      backend: backends.backend,
      idempotencyStore: backends.idempotencyStore,
      permissionsYaml: validPermissionsYaml,
      usersYaml: validUsersYaml,
      installIdentifier: installId,
    );
    return DemoRoutes(
      components: components,
      projection: PollingDemoStateProjection(components: components),
    );
  }

  group('DemoRoutes ($label)', () {
    test('GET /healthz returns ok', () async {
      final routes = await makeRoutes('00000000-0000-4000-8000-000000000020');
      final r = await _get(routes, '/healthz');
      expect(r.statusCode, 200);
      expect(await r.readAsString(), 'ok');
    });

    test(
      'POST /session/start: known userId returns role + site + permissions',
      () async {
        final routes = await makeRoutes('00000000-0000-4000-8000-000000000021');
        final r = await _post(routes, '/session/start', <String, Object?>{
          'userId': 'green-user-1',
        });
        expect(r.statusCode, 200);
        final body = await _readJson(r);
        final response = SessionStartResponse.fromJson(body);
        expect(response.principalUserId, 'green-user-1');
        expect(response.principalRole, 'GreenTeam');
        expect(response.principalActiveSite, 'green-workspace');
        expect(response.snapshotPermissions, contains('help.ask'));
        expect(response.snapshotPermissions, contains('notes.write.green'));
        expect(response.snapshotPermissions, contains('buttons.press.green'));
      },
    );

    test(
      'POST /session/start: unknown userId returns Anon role + empty permissions',
      () async {
        final routes = await makeRoutes('00000000-0000-4000-8000-000000000022');
        final r = await _post(routes, '/session/start', <String, Object?>{
          'userId': 'who-dis',
        });
        final body = await _readJson(r);
        final response = SessionStartResponse.fromJson(body);
        expect(response.principalUserId, isNull);
        expect(response.principalRole, 'Anon');
        expect(response.snapshotPermissions, isEmpty);
      },
    );

    test(
      'POST /dispatch: PressGreenButton happy-path returns success',
      () async {
        final routes = await makeRoutes('00000000-0000-4000-8000-000000000023');
        final r = await _post(routes, '/dispatch', <String, Object?>{
          'actionName': 'PressGreenButtonAction',
          'rawInput': <String, Object?>{},
          'userId': 'green-user-1',
        });
        expect(r.statusCode, 200);
        final body = await _readJson(r);
        final response = DispatchResponse.fromJson(body);
        expect(response, isA<DispatchResponseSuccess>());
        final success = response as DispatchResponseSuccess;
        expect(success.emittedEventIds, hasLength(1));
      },
    );

    test(
      'POST /dispatch: BlueTeam pressing green is authorization_denied',
      () async {
        final routes = await makeRoutes('00000000-0000-4000-8000-000000000024');
        final r = await _post(routes, '/dispatch', <String, Object?>{
          'actionName': 'PressGreenButtonAction',
          'rawInput': <String, Object?>{},
          'userId': 'blue-user',
        });
        final body = await _readJson(r);
        final response = DispatchResponse.fromJson(body);
        expect(response, isA<DispatchResponseDenied>());
        final denied = response as DispatchResponseDenied;
        expect(denied.denialKind, 'authorization_denied');
        expect(denied.permissionDenied, 'buttons.press.green');
      },
    );

    test(
      'POST /dispatch: unknown action returns unknown_action denied',
      () async {
        final routes = await makeRoutes('00000000-0000-4000-8000-000000000025');
        final r = await _post(routes, '/dispatch', <String, Object?>{
          'actionName': 'NoSuchAction',
          'rawInput': <String, Object?>{},
          'userId': 'green-user-1',
        });
        final body = await _readJson(r);
        final response = DispatchResponse.fromJson(body);
        expect(response, isA<DispatchResponseDenied>());
        final denied = response as DispatchResponseDenied;
        expect(denied.denialKind, 'unknown_action');
        expect(denied.requestedName, 'NoSuchAction');
      },
    );

    test('GET /_demo/inspect returns InspectSnapshot JSON', () async {
      final routes = await makeRoutes('00000000-0000-4000-8000-000000000026');
      final r = await _get(routes, '/_demo/inspect');
      expect(r.statusCode, 200);
      final body = await _readJson(r);
      final snap = InspectSnapshot.fromJson(body);
      expect(snap.directory, hasLength(3));
      expect(snap.matrixGrants, hasLength(9));
    });

    test('lastTrace updates after a dispatch', () async {
      final routes = await makeRoutes('00000000-0000-4000-8000-000000000027');
      expect(routes.lastTrace(), isNull);
      await _post(routes, '/dispatch', <String, Object?>{
        'actionName': 'PressGreenButtonAction',
        'rawInput': <String, Object?>{},
        'userId': 'green-user-1',
      });
      final trace = routes.lastTrace();
      expect(trace, isNotNull);
      expect(trace!.actionName, 'PressGreenButtonAction');
      expect(trace.stages.last, 'return_success');
    });
  });
}

Future<DemoBackends> _sembastFactory() async {
  final db = await databaseFactoryMemory.openDatabase('demo');
  return DemoBackends(
    backend: SembastBackend(database: db),
    idempotencyStore: DemoIdempotencyStore(),
  );
}

void main() {
  runDemoRoutesTests(_sembastFactory, label: 'sembast (memory)');
}
