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
      final endpoint = PostgresBackend.endpointFromUrl(url);
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

    // Scenario A: idempotency replay short-circuits to cached result.
    //
    // PressGreenButtonAction declares Idempotency.none, so we cannot use it
    // here — the dispatcher ignores keys on `.none` actions and every
    // press writes a fresh event. PressRedAlarmAction declares
    // Idempotency.required, so the dispatcher consults the
    // PostgresIdempotencyStore on each dispatch and replays the cached
    // outcome on a repeated `(actionName, principalId, idempotencyKey)`
    // tuple.
    //
    // The seed YAML grants `buttons.press.red` to GreenTeam, so
    // `green-user-1` is authorized for the action.
    test('idempotency replay: duplicate PressRedAlarm dispatch is '
        'short-circuited to the cached outcome on Postgres', () async {
      const idemKey = 'red-alarm-test-key-001';
      final body = <String, Object?>{
        'actionName': 'PressRedAlarmAction',
        'rawInput': <String, Object?>{'reason': 'smoke detected'},
        'userId': 'green-user-1',
        'idempotencyKey': idemKey,
      };

      // First dispatch: 200 success, one red_alarm event lands.
      final eventsBefore = await backend.findAllEvents();
      final first = await _post(baseUri.resolve('/dispatch'), body);
      expect(
        first.statusCode,
        200,
        reason: 'first dispatch should succeed; body=${first.body}',
      );
      final firstResponse = DispatchResponse.fromJson(
        jsonDecode(first.body) as Map<String, Object?>,
      );
      expect(firstResponse, isA<DispatchResponseSuccess>());
      final firstSuccess = firstResponse as DispatchResponseSuccess;
      expect(firstSuccess.emittedEventIds, hasLength(1));
      final firstEmittedId = firstSuccess.emittedEventIds.single;

      // Second dispatch with the same key: 200, but the dispatcher should
      // serve the cached outcome (no new event written).
      final second = await _post(baseUri.resolve('/dispatch'), body);
      expect(
        second.statusCode,
        200,
        reason: 'second dispatch should also return 200; body=${second.body}',
      );
      final secondResponse = DispatchResponse.fromJson(
        jsonDecode(second.body) as Map<String, Object?>,
      );
      expect(
        secondResponse,
        isA<DispatchResponseIdempotencyHit>(),
        reason: 'expected idempotency-hit wire variant on replay',
      );
      final hit = secondResponse as DispatchResponseIdempotencyHit;
      expect(
        hit.priorEventIds,
        equals(<String>[firstEmittedId]),
        reason: 'cached outcome should reference the original event id',
      );

      // Direct Postgres assertion: exactly one red_alarm event landed
      // in the `events` table — the second dispatch did NOT append.
      final eventsAfter = await backend.findAllEvents();
      expect(
        eventsAfter.length,
        eventsBefore.length + 1,
        reason:
            'expected exactly one new event after two identical '
            'dispatches (the second should short-circuit)',
      );
      final redAlarms = eventsAfter
          .where((e) => e.entryType == 'red_alarm')
          .toList();
      expect(redAlarms, hasLength(1));
      expect(redAlarms.single.eventType, 'red_alarm_pressed');

      // Inspector-parity assertion: listEntries() on the Postgres
      // idempotency store returns the cached entry. This goes through
      // the same code path as the demo's `/_demo/inspect` endpoint.
      final entries = await components.idempotencyStore.listEntries();
      expect(entries, hasLength(1));
      final entry = entries.single;
      expect(entry.actionName, 'PressRedAlarmAction');
      expect(entry.idempotencyKey, idemKey);
      expect(entry.emittedEventIds, equals(<String>[firstEmittedId]));
      // Principal id is the UserPrincipal's userId on this dispatcher.
      expect(entry.principalId, 'green-user-1');
    });

    // Scenario C: multi-action sequence with monotonic verification.
    //
    // Dispatch a mix of authorized and denied actions through HTTP and
    // verify the Postgres `events` table records each one with a
    // contiguous, monotonic sequence_number. Both successes and denials
    // emit an event per the action-dispatch contract.
    //
    // Mix (10 dispatches):
    //  - 5x PressGreenButtonAction by green-user-1 (all succeed)
    //  - 3x PressGreenButtonAction by blue-user    (all denied —
    //      BlueTeam lacks buttons.press.green)
    //  - 2x PressRedAlarmAction by green-user-1    (succeed — GreenTeam
    //      has buttons.press.red; idempotency.required so each needs a
    //      distinct key)
    test('multi-action sequence: 10 dispatches produce 10 contiguous '
        'monotonic events on Postgres', () async {
      final eventsBefore = await backend.findAllEvents();
      final baselineMaxSeq = eventsBefore.isEmpty
          ? 0
          : eventsBefore
                .map((e) => e.sequenceNumber)
                .reduce((a, b) => a > b ? a : b);

      // Build the dispatch script.
      final dispatches = <Map<String, Object?>>[
        for (var i = 0; i < 5; i++)
          <String, Object?>{
            'actionName': 'PressGreenButtonAction',
            'rawInput': <String, Object?>{},
            'userId': 'green-user-1',
          },
        for (var i = 0; i < 3; i++)
          <String, Object?>{
            'actionName': 'PressGreenButtonAction',
            'rawInput': <String, Object?>{},
            'userId': 'blue-user',
          },
        for (var i = 0; i < 2; i++)
          <String, Object?>{
            'actionName': 'PressRedAlarmAction',
            'rawInput': <String, Object?>{'reason': 'drill-$i'},
            'userId': 'green-user-1',
            // Distinct keys so each lands a fresh red_alarm event rather
            // than serving the cached outcome from the prior dispatch.
            'idempotencyKey': 'red-drill-$i',
          },
      ];
      expect(dispatches, hasLength(10));

      var successCount = 0;
      var denialCount = 0;
      for (final body in dispatches) {
        final res = await _post(baseUri.resolve('/dispatch'), body);
        expect(
          res.statusCode,
          200,
          reason: 'dispatch HTTP failed: ${res.body}',
        );
        final parsed = DispatchResponse.fromJson(
          jsonDecode(res.body) as Map<String, Object?>,
        );
        if (parsed is DispatchResponseSuccess) {
          successCount++;
        } else if (parsed is DispatchResponseDenied) {
          denialCount++;
        } else {
          fail('unexpected response variant: $parsed');
        }
      }
      expect(successCount, 7);
      expect(denialCount, 3);

      // Direct Postgres reads. The 10 dispatches must land 10 new rows
      // in the events table.
      final eventsAfter = await backend.findAllEvents();
      final newEvents = eventsAfter
          .where((e) => e.sequenceNumber > baselineMaxSeq)
          .toList();
      expect(
        newEvents,
        hasLength(10),
        reason:
            'expected 10 new events from 10 dispatches '
            '(success and denial both emit one event each)',
      );

      // Monotonic + contiguous: findAllEvents returns events ordered by
      // sequence_number ASC; the new tail must be a contiguous run.
      for (var i = 1; i < newEvents.length; i++) {
        expect(
          newEvents[i].sequenceNumber,
          newEvents[i - 1].sequenceNumber + 1,
          reason:
              'sequence numbers must be contiguous and monotonic '
              'across dispatches; gap between #${i - 1} and #$i',
        );
      }

      // Outcome breakdown: 7 successes split across green_button_press
      // (5) and red_alarm (2); 3 denials all flow through the
      // action_denial entry type with eventType=authorization_denied.
      final greenPresses = newEvents
          .where((e) => e.entryType == 'green_button_press')
          .toList();
      final redAlarms = newEvents
          .where((e) => e.entryType == 'red_alarm')
          .toList();
      final denials = newEvents
          .where((e) => e.entryType == 'action_denial')
          .toList();
      expect(greenPresses, hasLength(5));
      expect(redAlarms, hasLength(2));
      expect(denials, hasLength(3));
      for (final d in denials) {
        expect(d.eventType, 'authorization_denied');
        expect(d.aggregateType, 'action_attempt');
      }

      // Each successful press is its own aggregate (the action picks a
      // fresh v4 UUID per invocation). So the green presses live across
      // 5 distinct aggregateIds and the red alarms across 2.
      expect(greenPresses.map((e) => e.aggregateId).toSet(), hasLength(5));
      expect(redAlarms.map((e) => e.aggregateId).toSet(), hasLength(2));

      // role_permission_grants view rows are unaffected by the action
      // dispatches above (it only reads role_permission_grant events,
      // and the seed already produced those). Confirm the count stayed
      // stable at 9 — same as the bootstrap-seed assertion above.
      final viewRows = await backend.findViewRows('role_permission_grants');
      expect(viewRows, hasLength(9));

      // Only the 2 PressRedAlarmAction dispatches consume idempotency
      // slots; the 5 green-button successes and 3 denials never touch
      // the idempotency store (Idempotency.none for green; denials
      // short-circuit before Stage 9). Verify the postgres
      // `idempotency` table holds exactly those 2 entries.
      final idemEntries = await components.idempotencyStore.listEntries();
      expect(idemEntries, hasLength(2));
      expect(
        idemEntries.map((e) => e.idempotencyKey).toSet(),
        equals(<String>{'red-drill-0', 'red-drill-1'}),
      );
      for (final e in idemEntries) {
        expect(e.actionName, 'PressRedAlarmAction');
        expect(e.principalId, 'green-user-1');
        expect(e.emittedEventIds, hasLength(1));
      }
    });
  });

  // Scenario B: persistence across backend restart.
  //
  // This test is intentionally a sibling of the group above (not a
  // member) so it bypasses the per-test schema-reset setUp/tearDown.
  // The whole point of the scenario is that state survives backend
  // close/reopen against the same `evs_demo` database — which would
  // be invisible if setUp dropped the schema between halves.
  //
  // We do our own one-shot schema reset INSIDE the test body so the
  // test is still hermetic with respect to leftover state from earlier
  // runs.
  group('action_permissions_demo on Postgres — restart durability', () {
    test('persistence across backend close+reopen: events and view rows '
        'survive on the same Postgres database', () async {
      // Helper to drop+recreate the public schema. Mirrors setUp() above.
      Future<void> resetSchema() async {
        final endpoint = PostgresBackend.endpointFromUrl(url);
        final tmp = await Connection.open(
          endpoint,
          settings: const ConnectionSettings(sslMode: SslMode.disable),
        );
        await tmp.execute('DROP SCHEMA public CASCADE');
        await tmp.execute('CREATE SCHEMA public');
        await tmp.close();
      }

      // Start clean.
      await resetSchema();

      // --- Phase 1: open backend, dispatch some actions, snapshot state.
      final backend1 = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
      );
      final idem1 = PostgresIdempotencyStore.over(backend1.pool);
      final components1 = await bootstrapDemoServer(
        backend: backend1,
        idempotencyStore: idem1,
        permissionsYaml: _permissionsYaml,
        usersYaml: _usersYaml,
        installIdentifier: '00000000-0000-4000-8000-0000000000aa',
      );
      expect(components1.policyErrors, isEmpty);

      final routes1 = DemoRoutes(
        components: components1,
        projection: PollingDemoStateProjection(
          components: components1,
          lastTraceProvider: () => null,
        ),
      );
      final server1 = await shelf_io.serve(routes1.handler, 'localhost', 0);
      final base1 = Uri(
        scheme: 'http',
        host: server1.address.host,
        port: server1.port,
      );

      // 3 successful green-button presses by green-user-1.
      for (var i = 0; i < 3; i++) {
        final r = await _post(base1.resolve('/dispatch'), <String, Object?>{
          'actionName': 'PressGreenButtonAction',
          'rawInput': <String, Object?>{},
          'userId': 'green-user-1',
        });
        expect(r.statusCode, 200, reason: 'dispatch $i failed: ${r.body}');
        final parsed = DispatchResponse.fromJson(
          jsonDecode(r.body) as Map<String, Object?>,
        );
        expect(parsed, isA<DispatchResponseSuccess>());
      }
      // Plus one PressRedAlarm to populate the idempotency table — so we
      // can verify it survives the restart too.
      final redRes = await _post(base1.resolve('/dispatch'), <String, Object?>{
        'actionName': 'PressRedAlarmAction',
        'rawInput': <String, Object?>{'reason': 'restart-test'},
        'userId': 'green-user-1',
        'idempotencyKey': 'restart-key-1',
      });
      expect(redRes.statusCode, 200);

      // Snapshot pre-close state directly from Postgres.
      final eventsPhase1 = await backend1.findAllEvents();
      final viewRowsPhase1 = await backend1.findViewRows(
        'role_permission_grants',
      );
      final idemPhase1 = await idem1.listEntries();

      // We expect at least 4 dispatched events on top of bootstrap seeds.
      // The seed appends ~12 events (9 role grants + 3 user_provisioned),
      // plus 4 dispatched (3 green + 1 red) = 16. We don't hard-code the
      // exact bootstrap count; the load-bearing invariant is that the
      // post-reopen reads return the SAME list, byte-equal in identity
      // fields.
      expect(
        eventsPhase1.where((e) => e.entryType == 'green_button_press'),
        hasLength(3),
      );
      expect(
        eventsPhase1.where((e) => e.entryType == 'red_alarm'),
        hasLength(1),
      );

      // Tear down phase 1 — close server, then close backend. From here
      // on, backend1 must NOT be touched (it is closed).
      await server1.close();
      await backend1.close();

      // --- Phase 2: open a FRESH PostgresBackend against the SAME URL.
      //   No schema reset. State must survive.
      final backend2 = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
      );
      addTearDown(backend2.close);
      final idem2 = PostgresIdempotencyStore.over(backend2.pool);

      // Read events + view rows from the NEW backend instance.
      final eventsPhase2 = await backend2.findAllEvents();
      final viewRowsPhase2 = await backend2.findViewRows(
        'role_permission_grants',
      );
      final idemPhase2 = await idem2.listEntries();

      // Same number of events.
      expect(
        eventsPhase2.length,
        eventsPhase1.length,
        reason: 'event count must survive backend close+reopen',
      );

      // Same sequence numbers in same order. findAllEvents returns
      // ordered by sequence_number ASC.
      expect(
        eventsPhase2.map((e) => e.sequenceNumber).toList(),
        equals(eventsPhase1.map((e) => e.sequenceNumber).toList()),
        reason: 'sequence numbers must be stable across reopen',
      );

      // Same event identity (eventId) and entry-type sequence — these
      // are the load-bearing observable facts a downstream consumer
      // would see.
      expect(
        eventsPhase2.map((e) => e.eventId).toList(),
        equals(eventsPhase1.map((e) => e.eventId).toList()),
        reason: 'event ids must match exactly across reopen',
      );
      expect(
        eventsPhase2.map((e) => e.entryType).toList(),
        equals(eventsPhase1.map((e) => e.entryType).toList()),
      );

      // role_permission_grants view rows survived.
      expect(viewRowsPhase2, hasLength(9));
      expect(
        viewRowsPhase2.length,
        viewRowsPhase1.length,
        reason: 'role_permission_grants view rows must survive reopen',
      );
      final pairsPhase1 = viewRowsPhase1
          .map((r) => '${r['role']}:${r['permissionName']}')
          .toSet();
      final pairsPhase2 = viewRowsPhase2
          .map((r) => '${r['role']}:${r['permissionName']}')
          .toSet();
      expect(pairsPhase2, equals(pairsPhase1));

      // Idempotency entry from the PressRedAlarm dispatch survived.
      expect(idemPhase2, hasLength(1));
      expect(
        idemPhase2.length,
        idemPhase1.length,
        reason: 'idempotency entries must survive reopen',
      );
      expect(idemPhase2.single.idempotencyKey, 'restart-key-1');
      expect(idemPhase2.single.actionName, 'PressRedAlarmAction');
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
