// Verifies: EVS-PRD-destinations/V
// two demo server instances booted the way the server boots share one
//   Postgres database, each in its own isolate with its own sessions: one
//   delivery cycle drains and the other stands by; once the draining
//   instance closes its cycle, the other takes over and delivers.
// Verifies: EVS-PRD-destinations/U
// a halt requested through the standing-by instance's registry is honoured
//   by the draining instance, which wedges the head for an operator halt.
// Verifies: EVS-PRD-destinations/S+T
// the standing-by instance reads the wedge from the default
//   destination-wedges view and its delivery status, and its recovery
//   removes the row; delivery resumes on the draining instance.
//
// Gated on PG_TEST_URL. Drops and recreates the `public` schema, so it runs
// one file at a time like every Postgres test.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:postgres/postgres.dart';

import 'support/demo_bootstrap.dart';

const Duration _cadence = Duration(milliseconds: 200);
const Duration _patience = Duration(seconds: 30);

Future<void> _resetAndProvision(String url) async {
  final tmp = await Connection.open(
    PostgresBackend.endpointFromUrl(url),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
  await tmp.execute('DROP SCHEMA public CASCADE');
  await tmp.execute('CREATE SCHEMA public');
  await tmp.close();
  await PostgresBackend.provision(url, sslMode: SslMode.disable);
}

Future<DemoServerComponents> _boot(
  PostgresBackend backend,
  String installIdentifier,
  LogDestination destination,
) => bootstrapDemoServer(
  backend: backend,
  idempotencyStore: PostgresIdempotencyStore.forBackend(backend),
  permissionsYaml: validPermissionsYaml,
  usersYaml: validUsersYaml,
  installIdentifier: installIdentifier,
  deliveryDestination: destination,
);

/// Waits until [condition] holds, polling; fails after [_patience].
Future<void> _until(
  FutureOr<bool> Function() condition, {
  required String what,
}) async {
  final deadline = DateTime.now().add(_patience);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out waiting for $what');
    await Future<void>.delayed(const Duration(milliseconds: 25));
  }
}

/// The second server instance, in its own isolate: boots the way the server
/// boots, starts its delivery cycle, reports its state and deliveries, and
/// runs the registry operations the test sends it.
Future<void> _instanceB(List<Object?> args) async {
  final url = args[0]! as String;
  final toTest = args[1]! as SendPort;
  final commands = ReceivePort();
  final backend = await PostgresBackend.open(
    url: url,
    sslMode: SslMode.disable,
  );
  final destination = LogDestination(
    sink: (line) => toTest.send(<String, Object?>{'delivered': line}),
  );
  final c = await _boot(
    backend,
    'bbbb0002-0000-4000-8000-0000000000b2',
    destination,
  );
  final cycle = await SyncCycle.start(
    registry: c.destinations,
    cadence: _cadence,
  );
  var reported = cycle.state;
  toTest.send(<String, Object?>{
    'ready': commands.sendPort,
    'state': reported.name,
  });
  final watch = Timer.periodic(const Duration(milliseconds: 20), (_) {
    if (cycle.state == reported) return;
    reported = cycle.state;
    toTest.send(<String, Object?>{'state': reported.name});
  });
  await for (final message in commands) {
    final command = message! as Map<String, Object?>;
    final reply = command['reply']! as SendPort;
    try {
      switch (command['op']) {
        case 'halt':
          reply.send(
            await c.destinations.requestHalt(
              logDestinationId,
              initiator: const UserInitiator('admin-user'),
              purpose: HaltPurpose.pause,
            ),
          );
        case 'wedges':
          reply.send(
            await backend.findViewRows(defaultDestinationWedgesSpec.viewName),
          );
        case 'status':
          final status = await c.destinations.readDeliveryStatus();
          final own = status.destinations[logDestinationId]!;
          reply.send(<String, Object?>{
            'drainer_epoch': status.drainer?.epoch,
            'wedge_row': own.wedge?.rowId,
            'wedge_cause': own.wedge?.cause.wire,
            'open_halt': own.openHaltRequest?.requestEventId,
          });
        case 'recover':
          final result = await c.destinations.tombstoneAndRefill(
            logDestinationId,
            command['rowId']! as String,
            initiator: const UserInitiator('admin-user'),
          );
          reply.send(result.rowId);
        case 'close':
          watch.cancel();
          await cycle.close();
          await c.eventStore.close();
          reply.send('closed');
          commands.close();
      }
    } on Object catch (e) {
      reply.send(<String, Object?>{'error': '$e'});
    }
  }
}

/// The test's handle on instance B.
class _B {
  _B._();

  final ReceivePort _port = ReceivePort();
  final Completer<SendPort> _ready = Completer<SendPort>();
  late final SendPort _commands;
  Isolate? _isolate;

  /// B's delivery cycle state, as B last reported it.
  String state = '';

  /// The lines B's demo destination delivered.
  final List<String> delivered = <String>[];

  static Future<_B> spawn(String url) async {
    final b = _B._();
    b._port.listen((message) {
      final m = message! as Map<Object?, Object?>;
      if (m['state'] case final String s) b.state = s;
      if (m['delivered'] case final String line) b.delivered.add(line);
      if (m['ready'] case final SendPort p) b._ready.complete(p);
    });
    b._isolate = await Isolate.spawn(_instanceB, <Object?>[
      url,
      b._port.sendPort,
    ]);
    b._commands = await b._ready.future.timeout(_patience);
    return b;
  }

  Future<Object?> call(String op, [Map<String, Object?> args = const {}]) {
    final reply = ReceivePort();
    _commands.send(<String, Object?>{
      'op': op,
      'reply': reply.sendPort,
      ...args,
    });
    return reply.first.timeout(_patience).whenComplete(reply.close);
  }

  Future<void> close() async {
    final isolate = _isolate;
    if (isolate == null) return;
    _isolate = null;
    await call('close');
    isolate.kill();
    _port.close();
  }
}

void main() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping the two-instance tests');
    });
    return;
  }

  test('one instance drains and the other stands by; a halt and a recovery '
      'issued through the standing-by instance are honoured by the drainer; '
      'closing the drainer hands delivery over', () async {
    await _resetAndProvision(url);
    final deliveredA = <String>[];
    final backendA = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
    );
    final a = await _boot(
      backendA,
      'aaaa0002-0000-4000-8000-0000000000a2',
      LogDestination(sink: deliveredA.add),
    );
    final cycleA = await SyncCycle.start(
      registry: a.destinations,
      cadence: _cadence,
    );
    addTearDown(() async {
      await cycleA.close();
      await a.eventStore.close();
    });
    expect(cycleA.state, SyncCycleState.running);

    final b = await _B.spawn(url);
    addTearDown(b.close);
    expect(b.state, 'standby');
    await _until(() => deliveredA.isNotEmpty, what: "A's first delivery");

    // The halt is requested through B; A, the drainer, honours it.
    final requestEventId = await b.call('halt') as String;
    await _until(() async {
      final head = await backendA.readFifoHead(logDestinationId);
      return head?.finalStatus == FinalStatus.wedged;
    }, what: 'the drainer to honour the halt');
    final wedge = (await backendA.findAllEvents(
      entryType: kDestinationWedgedEntryType,
    )).single;
    expect(wedge.data['cause'], WedgeCause.operatorHalt.wire);
    expect(wedge.data['halt_request_event_id'], requestEventId);
    expect(b.state, 'standby');
    expect(b.delivered, isEmpty);

    // B reads the wedge from the view and from the delivery status.
    final head = (await backendA.readFifoHead(logDestinationId))!;
    final rows = (await b.call('wedges'))! as List<Object?>;
    expect(rows, hasLength(1));
    final row = rows.single! as Map<Object?, Object?>;
    expect(row['cause'], WedgeCause.operatorHalt.wire);
    expect(row['row_id'], head.entryId);
    final status = (await b.call('status'))! as Map<Object?, Object?>;
    expect(status['wedge_row'], head.entryId);
    expect(status['wedge_cause'], WedgeCause.operatorHalt.wire);
    expect(status['open_halt'], isNull);
    expect(status['drainer_epoch'], isNotNull);

    // B recovers; delivery resumes on A.
    final deliveredBefore = deliveredA.length;
    expect(
      await b.call('recover', <String, Object?>{'rowId': head.entryId}),
      head.entryId,
    );
    expect((await b.call('wedges'))! as List<Object?>, isEmpty);
    await _until(
      () => deliveredA.length > deliveredBefore,
      what: 'delivery to resume on the drainer',
    );
    expect(b.delivered, isEmpty);

    // A stops draining: B takes over and delivers what A appends next.
    await cycleA.close();
    await _until(() => b.state == 'running', what: 'B to take over');
    await a.eventStore.append(
      entryType: 'demo_note',
      aggregateId: 'after-handover',
      aggregateType: 'demo_note',
      eventType: 'note_written',
      data: const <String, Object?>{'text': 'after-handover'},
      initiator: const UserInitiator('green-user-1'),
    );
    await _until(
      () => b.delivered.any((l) => l.contains('after-handover')),
      what: 'B to deliver after the hand-over',
    );
    expect(deliveredA.any((l) => l.contains('after-handover')), isFalse);
  }, timeout: const Timeout(Duration(minutes: 3)));
}
