// The demo server's operator routes under /demo/delivery/: the delivery
// status and the wedges view, a halt the drainer honours, a cancellation, a
// recovery, a refusal the next send makes (accepted only by the process
// whose cycle drains), the registry's refusals as 409 responses, a
// malformed body as 400, and a 403, with nothing written, for a caller
// whose role holds no `delivery.operate` grant in the log; a grant appended
// to the log, or revoked from it, changes the decision. The halt event
// names the calling user as its initiator. App-side behaviour over the library's operations:
// carries no requirement citation. Runs on Sembast here and on Postgres
// from `demo_routes_postgres_test.dart`.
import 'dart:convert';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:action_permissions_demo/server/demo_routes.dart';
import 'package:action_permissions_demo/server/demo_state_projection.dart';
import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf.dart';

import 'support/demo_bootstrap.dart';
import 'support/gated_log_destination.dart';

const String _admin = 'admin-user';
const String _green = 'green-user-1';

class _World {
  _World(this.components, this.routes, this.cycle, this.destination);

  final DemoServerComponents components;
  final DemoRoutes routes;
  final SyncCycle cycle;
  final GatedLogDestination destination;

  StorageReader get reader => components.eventStore.reader;

  Future<Response> post(String route, Map<String, Object?> body) =>
      postRaw(route, jsonEncode(body));

  Future<Response> postRaw(
    String route,
    String body, {
    DemoRoutes? through,
  }) async => (through ?? routes).handler(
    Request(
      'POST',
      Uri.parse('http://localhost/demo/delivery/$route'),
      body: body,
      headers: const <String, String>{'content-type': 'application/json'},
    ),
  );

  /// Appends a grant event, or a revocation, of `delivery.operate` for
  /// [role], the way the permissions seed records a grant.
  Future<void> grant(String role, {bool revoke = false}) async {
    await components.eventStore.append(
      entryType: 'role_permission_grant',
      aggregateType: 'role_permission_grant',
      aggregateId: '$role:${deliveryOperatePermission.name}',
      eventType: revoke ? 'permission_revoked' : 'permission_granted',
      data: PermissionGrantedPayload(
        role: role,
        permissionName: deliveryOperatePermission.name,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'delivery_routes_test'),
    );
  }

  Future<Response> status(String? userId) async => routes.handler(
    Request(
      'GET',
      Uri.parse(
        'http://localhost/demo/delivery/status'
        '${userId == null ? '' : '?userId=$userId'}',
      ),
    ),
  );

  Future<StoredEvent> note(String id) async =>
      (await components.eventStore.append(
        entryType: 'demo_note',
        aggregateId: id,
        aggregateType: 'demo_note',
        eventType: 'note_written',
        data: <String, Object?>{'text': id},
        initiator: const UserInitiator(_green),
      ))!;

  /// Everything a refused or forbidden call must leave as it was: the log
  /// and the persisted delivery status.
  Future<String> snapshot() async {
    final events = await reader.findAllEvents();
    final status = await components.destinations.readDeliveryStatus();
    final rows = await reader.listFifoEntries(logDestinationId);
    return <Object?>[
      <String>[for (final e in events) e.eventId],
      '$status',
      <Object?>[for (final r in rows) r.toJson()],
    ].toString();
  }
}

Future<Map<String, Object?>> _json(Response r) async =>
    jsonDecode(await r.readAsString()) as Map<String, Object?>;

/// Runs the operator route tests over the [factory]-supplied backend pair.
void runDeliveryRoutesTests(
  DemoBackendFactory factory, {
  required String label,
}) {
  group('delivery routes ($label)', () {
    late _World w;

    setUp(() async {
      final backends = await factory();
      final destination = GatedLogDestination();
      final components = await bootstrapDemoServer(
        storage: backends.storage,
        idempotencyStore: backends.idempotencyStore,
        permissionsYaml: validPermissionsYaml,
        usersYaml: validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-0000000000d1',
        deliveryDestination: destination,
      );
      // A one-hour cadence: the tests run the passes they assert on.
      final cycle = await SyncCycle.start(
        registry: components.destinations,
        cadence: const Duration(hours: 1),
      );
      w = _World(
        components,
        DemoRoutes(
          components: components,
          projection: PollingDemoStateProjection(components: components),
          deliveryState: () => cycle.state,
        ),
        cycle,
        destination,
      );
      addTearDown(() async {
        destination.release();
        await cycle.close();
        await components.eventStore.close();
      });
      await cycle();
    });

    test('GET status reports the drainer, the demo destination and no '
        'wedge', () async {
      final r = await w.status(_admin);
      expect(r.statusCode, 200);
      final body = await _json(r);
      expect(body['drainer'], isNotNull);
      final destinations = body['destinations']! as Map<String, Object?>;
      expect(destinations.keys, <String>[logDestinationId]);
      final server = destinations[logDestinationId]! as Map<String, Object?>;
      expect(server['open_halt_request'], isNull);
      expect(server['wedge'], isNull);
      expect(server['unserved'], isNull);
      expect(body['wedges'], isEmpty);
      expect(w.destination.delivered, isNotEmpty);
    });

    test('a halt is honoured by the drainer, shows in the wedges view and '
        'is recovered', () async {
      final halted = await w.post('halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'purpose': 'pause',
      });
      expect(halted.statusCode, 200);
      final requestEventId =
          (await _json(halted))['halt_request_event_id']! as String;
      final request = (await w.reader.findAllEvents(
        entryType: kDestinationHaltRequestedEntryType,
      )).single;
      expect(request.eventId, requestEventId);
      expect(request.initiator, const UserInitiator(_admin));

      // A second request is the registry's refusal: the first is still
      // open, or the pass its commit woke has already honoured it.
      final again = await w.post('halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'purpose': 'pause',
      });
      expect(again.statusCode, 409);
      expect(
        (await _json(again))['error'],
        anyOf(contains('already open'), contains('already halted')),
      );

      await w.note('n1');
      await w.cycle();
      final head = (await w.reader.readFifoHead(logDestinationId))!;
      expect(head.finalStatus, FinalStatus.wedged);

      final status = await _json(await w.status(_admin));
      final wedges = (status['wedges']! as List<Object?>)
          .cast<Map<String, Object?>>();
      expect(wedges, hasLength(1));
      expect(wedges.single['id'], logDestinationId);
      expect(wedges.single['cause'], WedgeCause.operatorHalt.wire);
      expect(wedges.single['row_id'], head.entryId);
      expect(
        (status['destinations']! as Map<String, Object?>)[logDestinationId],
        containsPair('wedge', isNotNull),
      );

      w.destination.delivered.clear();
      final recovered = await w.post('recover', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'rowId': head.entryId,
      });
      expect(recovered.statusCode, 200);
      expect((await _json(recovered))['row_id'], head.entryId);
      final recovery = (await w.reader.findAllEvents(
        entryType: kDestinationWedgeRecoveredEntryType,
      )).single;
      expect(recovery.initiator, const UserInitiator(_admin));
      expect((await _json(await w.status(_admin)))['wedges'], isEmpty);

      // Delivery resumes: the refill delivers the halted note.
      await w.cycle();
      expect(w.destination.delivered.join(), contains('n1'));
    });

    test('a cancellation closes the open request; a second is refused with '
        '409', () async {
      await w.post('halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'purpose': 'reconfigure',
      });
      final cancelled = await w.post('cancel-halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
      });
      expect(cancelled.statusCode, 200);
      final cancellation = (await w.reader.findAllEvents(
        entryType: kDestinationHaltCancelledEntryType,
      )).single;
      expect(cancellation.initiator, const UserInitiator(_admin));

      // Let the passes the two operations woke finish.
      await w.cycle();
      final before = await w.snapshot();
      final again = await w.post('cancel-halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
      });
      expect(again.statusCode, 409);
      expect((await _json(again))['error'], contains('no halt request'));
      expect(await w.snapshot(), before);
    });

    test('a recovery of a pending head is refused with 409', () async {
      w.destination.hold = true;
      await w.note('held');
      final pass = w.cycle();
      await w.destination.sendStarted;
      final head = (await w.reader.readFifoHead(logDestinationId))!;
      expect(head.finalStatus, isNull);

      final before = await w.snapshot();
      final refused = await w.post('recover', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'rowId': head.entryId,
      });
      expect(refused.statusCode, 409);
      expect(
        (await _json(refused))['error'],
        contains('requires a wedged head; the head is pending'),
      );
      expect(await w.snapshot(), before);
      w.destination.release();
      await pass;
    });

    test('an unknown destination is refused with 409', () async {
      final refused = await w.post('halt', <String, Object?>{
        'userId': _admin,
        'destinationId': 'nowhere',
        'purpose': 'pause',
      });
      expect(refused.statusCode, 409);
      expect((await _json(refused))['error'], contains('nowhere'));
    });

    test('refuse-next makes the drainer wedge the head for a permanent '
        'refusal', () async {
      final r = await w.post('refuse-next', <String, Object?>{
        'userId': _admin,
      });
      expect(r.statusCode, 200);
      await w.note('refused');
      await w.cycle();
      final wedges =
          ((await _json(await w.status(_admin)))['wedges']! as List<Object?>)
              .cast<Map<String, Object?>>();
      expect(wedges.single['cause'], WedgeCause.permanentRefusal.wire);
    });

    test('refuse-next on a process that does not drain is refused with 409 '
        'and arms nothing', () async {
      for (final state in <SyncCycleState? Function()>[
        () => SyncCycleState.standby,
        () => SyncCycleState.stopped,
        () => null,
      ]) {
        final notDraining = DemoRoutes(
          components: w.components,
          projection: PollingDemoStateProjection(components: w.components),
          deliveryState: state() == null ? null : () => state()!,
        );
        final before = await w.snapshot();
        final r = await w.postRaw(
          'refuse-next',
          jsonEncode(<String, Object?>{'userId': _admin}),
          through: notDraining,
        );
        expect(r.statusCode, 409, reason: '${state()}');
        expect((await _json(r))['error'], contains('does not drain'));
        expect(w.destination.refusesNext, isFalse);
        expect(await w.snapshot(), before);
      }
    });

    test('an unknown halt purpose is refused with 409', () async {
      final before = await w.snapshot();
      final r = await w.post('halt', <String, Object?>{
        'userId': _admin,
        'destinationId': logDestinationId,
        'purpose': 'bogus',
      });
      expect(r.statusCode, 409);
      expect(
        (await _json(r))['error'],
        contains('must be pause or reconfigure'),
      );
      expect(await w.snapshot(), before);
    });

    for (final body in <String>[
      'not json',
      '[]',
      '{"userId": 5}',
      '{"userId": "admin-user", "destinationId": 7}',
    ]) {
      test(
        'a malformed body ($body) is a 400, and nothing is written',
        () async {
          final before = await w.snapshot();
          final r = await w.postRaw('halt', body);
          expect(r.statusCode, 400);
          expect(await w.snapshot(), before);
        },
      );
    }

    test(
      'the decision follows the grant in the log, not the role name',
      () async {
        // GreenTeam holds no grant: 403.
        expect((await w.status(_green)).statusCode, 403);
        // A grant appended to the log admits the green user.
        await w.grant('GreenTeam');
        expect((await w.status(_green)).statusCode, 200);
        // A revocation of Admin's grant refuses the admin.
        await w.grant('Admin', revoke: true);
        expect((await w.status(_admin)).statusCode, 403);
      },
    );

    for (final caller in <String?>[_green, 'nobody', null]) {
      test('a caller without the delivery.operate grant ($caller) gets 403 '
          'on every route, and nothing is written', () async {
        final refusing = w.destination.refusesNext;
        final before = await w.snapshot();
        final responses = <Response>[
          await w.status(caller),
          await w.post('halt', <String, Object?>{
            'userId': caller,
            'destinationId': logDestinationId,
            'purpose': 'pause',
          }),
          await w.post('cancel-halt', <String, Object?>{
            'userId': caller,
            'destinationId': logDestinationId,
          }),
          await w.post('recover', <String, Object?>{
            'userId': caller,
            'destinationId': logDestinationId,
            'rowId': 'x',
          }),
          await w.post('refuse-next', <String, Object?>{'userId': caller}),
        ];
        expect(
          <int>[for (final r in responses) r.statusCode],
          <int>[403, 403, 403, 403, 403],
        );
        expect(await w.snapshot(), before);
        expect(w.destination.refusesNext, refusing);
      });
    }
  });
}

void main() {
  var n = 0;
  runDeliveryRoutesTests(() async {
    final db = await databaseFactoryMemory.openDatabase(
      'delivery-routes-${n++}.db',
    );
    return DemoBackends(
      backend: SembastBackend(database: db),
      idempotencyStore: DemoIdempotencyStore(),
    );
  }, label: 'sembast (memory)');
}
