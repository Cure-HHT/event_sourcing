// Verifies: EVS-DEV-postgres-backend/D — end-to-end PostgresBackend exercise
//   via the action_permissions demo server. Action dispatch over HTTP
//   writes an event into the `events` table and the role-permission
//   matrix view rows into `view_rows` on a Postgres instance.
//
// Gated on PG_TEST_URL. Drops + recreates the `public` schema in setUp
// so each test runs against a deterministic empty database — matches
// the discipline used by the StorageBackend conformance harness.

@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:postgres/postgres.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

String? _pgTestUrl() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) return null;
  return url;
}

const String _permissionsYaml = '''
roles:
  - Admin
  - GreenTeam
  - BlueTeam
grants:
  Admin:
    - users.provision
  GreenTeam:
    - help.ask
    - notes.write.green
    - buttons.press.green
    - buttons.press.red
  BlueTeam:
    - help.ask
    - notes.write.blue
    - buttons.press.blue
    - buttons.press.red
''';

const String _usersYaml = '''
users:
  - userId: admin-user
    role: Admin
    activeSite: null
  - userId: green-user-1
    role: GreenTeam
    activeSite: green-workspace
  - userId: blue-user
    role: BlueTeam
    activeSite: blue-workspace
''';

void main() {
  final url = _pgTestUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping postgres integration test');
    });
    return;
  }

  group('action_permissions_demo on Postgres', () {
    late PostgresBackend backend;
    late PostgresIdempotencyStore idempotencyStore;
    late DemoServerComponents components;
    late HttpServer server;
    late Uri baseUri;

    setUp(() async {
      // Drop+recreate `public` schema for fresh test isolation. Split
      // into two execute calls because postgres v3.5 rejects multi-
      // statement strings in Session.execute (same discipline as the
      // StorageBackend conformance harness).
      final endpoint = _endpointFromUrl(url);
      final tmp = await Connection.open(
        endpoint,
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await tmp.execute('DROP SCHEMA public CASCADE');
      await tmp.execute('CREATE SCHEMA public');
      await tmp.close();

      backend = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
      idempotencyStore = PostgresIdempotencyStore.over(backend.pool);

      components = await bootstrapDemoServer(
        backend: backend,
        idempotencyStore: idempotencyStore,
        permissionsYaml: _permissionsYaml,
        usersYaml: _usersYaml,
        installIdentifier: '00000000-0000-4000-8000-0000000000aa',
      );
      expect(
        components.policyErrors,
        isEmpty,
        reason: 'policy seed should validate cleanly against the demo YAML',
      );

      final routes = DemoRoutes(
        components: components,
        projection: PollingDemoStateProjection(
          components: components,
          lastTraceProvider: () => null,
        ),
      );
      server = await shelf_io.serve(routes.handler, 'localhost', 0);
      baseUri = Uri(
        scheme: 'http',
        host: server.address.host,
        port: server.port,
      );
    });

    tearDown(() async {
      await server.close();
      await backend.close();
    });

    test('end-to-end: PressGreenButton dispatch writes event to Postgres '
        'events table', () async {
      // Capture pre-dispatch baseline so the assertion below isolates
      // the event the dispatch produced from seeded bootstrap events.
      final eventsBefore = await backend.findAllEvents();

      final dispatch =
          await _post(baseUri.resolve('/dispatch'), <String, Object?>{
            'actionName': 'PressGreenButtonAction',
            'rawInput': <String, Object?>{},
            'userId': 'green-user-1',
          });
      expect(
        dispatch.statusCode,
        200,
        reason:
            'dispatch should succeed; status was ${dispatch.statusCode}, '
            'body=${dispatch.body}',
      );

      final response = DispatchResponse.fromJson(
        jsonDecode(dispatch.body) as Map<String, Object?>,
      );
      expect(response, isA<DispatchResponseSuccess>());
      final success = response as DispatchResponseSuccess;
      expect(success.emittedEventIds, hasLength(1));
      final emittedId = success.emittedEventIds.single;

      // The emitted event MUST be persisted in Postgres directly. We
      // round-trip via `findEventById` (which the substrate routes to
      // a SELECT against the `events` table) so the assertion proves
      // the row landed in Postgres — not just in the in-memory event
      // bus.
      final fetched = await backend.findEventById(emittedId);
      expect(fetched, isNotNull, reason: 'event not found in events table');
      expect(fetched!.eventType, 'green_button_pressed');
      expect(fetched.entryType, 'green_button_press');

      // Total event count grew by exactly one (the dispatch's emitted
      // event). Seeded bootstrap events come from setUp.
      final eventsAfter = await backend.findAllEvents();
      expect(
        eventsAfter.length,
        eventsBefore.length + 1,
        reason: 'expected exactly one new event in events table after dispatch',
      );
    });

    test(
      'bootstrap seed lands role_permission_grants view rows in Postgres',
      () async {
        // The role_permission_grants view is materialized from the
        // permission_granted events the bootstrap seed appends. Reading
        // it through StorageBackend.findViewRows proves the substrate
        // wrote rows into the `view_rows` table on Postgres.
        final rows = await backend.findViewRows('role_permission_grants');
        expect(rows, hasLength(9));
        final pairs = rows
            .map((r) => '${r['role']}:${r['permissionName']}')
            .toSet();
        expect(pairs, contains('GreenTeam:help.ask'));
        expect(pairs, contains('Admin:users.provision'));
        expect(pairs, contains('BlueTeam:buttons.press.blue'));
      },
    );

    test('dispatch failure path: BlueTeam pressing green is recorded as '
        'authorization_denied event in Postgres', () async {
      final dispatch =
          await _post(baseUri.resolve('/dispatch'), <String, Object?>{
            'actionName': 'PressGreenButtonAction',
            'rawInput': <String, Object?>{},
            'userId': 'blue-user',
          });
      expect(dispatch.statusCode, 200);

      final response = DispatchResponse.fromJson(
        jsonDecode(dispatch.body) as Map<String, Object?>,
      );
      expect(response, isA<DispatchResponseDenied>());
      final denied = response as DispatchResponseDenied;
      expect(denied.denialKind, 'authorization_denied');

      // The dispatcher writes an `action_denial` event for every
      // denied dispatch. Verify it landed in events.
      final denials = (await backend.findAllEvents())
          .where((e) => e.entryType == 'action_denial')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.single.aggregateType, isNotEmpty);
    });
  });
}

/// Minimal `POST <uri>` helper that returns the response body as a
/// string. We use `dart:io` directly rather than `package:http` to keep
/// the test surface narrow.
Future<_HttpResponse> _post(Uri uri, Map<String, Object?> body) async {
  final client = HttpClient();
  try {
    final request = await client.postUrl(uri);
    request.headers.contentType = ContentType.json;
    request.write(jsonEncode(body));
    final response = await request.close();
    final responseBody = await response.transform(utf8.decoder).join();
    return _HttpResponse(response.statusCode, responseBody);
  } finally {
    client.close();
  }
}

class _HttpResponse {
  const _HttpResponse(this.statusCode, this.body);
  final int statusCode;
  final String body;
}

Endpoint _endpointFromUrl(String url) {
  final uri = Uri.parse(url);
  final userInfoParts = uri.userInfo.isEmpty
      ? const <String>[]
      : uri.userInfo.split(':');
  return Endpoint(
    host: uri.host,
    port: uri.port == 0 ? 5432 : uri.port,
    database: uri.pathSegments.isEmpty ? '' : uri.pathSegments.first,
    username: userInfoParts.isEmpty ? null : userInfoParts.first,
    password: userInfoParts.length < 2
        ? null
        : userInfoParts.sublist(1).join(':'),
  );
}
