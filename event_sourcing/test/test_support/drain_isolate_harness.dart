// Test support: a second drainer process for the Postgres drain-lock tests,
// run in a spawned isolate with its own PostgresBackend, event store,
// registry and delivery cycle. It reports on a SendPort and accepts
// commands; the test asserts only on its messages and on database reads.
// This file declares no tests, so it carries no citation.
import 'dart:async';
import 'dart:convert';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// The entry type the spawned drainer and the tests append.
const String harnessNoteType = 'lock_note';

/// The entry types every store of the drain-lock tests registers, so the
/// spawned drainer and the test's stores run one generation.
EntryTypeRegistry harnessEntryTypes() {
  final registry = EntryTypeRegistry();
  for (final d in kSystemEntryTypes) {
    registry.register(d);
  }
  registry.register(
    const EntryTypeDefinition(
      id: harnessNoteType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: harnessNoteType,
    ),
  );
  return registry;
}

/// A destination that reports the event ids of every send, optionally
/// waits for [gate] before answering, and answers SendOk.
class HarnessReceiver extends Destination {
  HarnessReceiver(this.id, {this.onSent, this.gate});

  @override
  final String id;

  /// Called with the event ids of every send, before it answers.
  final void Function(List<String> eventIds)? onSent;

  /// Awaited by every send after it reported.
  Future<void> Function()? gate;

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{harnessNoteType});

  @override
  String get wireFormat => 'harness-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool get allowHardDelete => true;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async => WirePayload(
    bytes: Uint8List.fromList(
      utf8.encode(
        jsonEncode(<String, Object?>{
          'event_ids': <String>[for (final e in batch) e.eventId],
        }),
      ),
    ),
    contentType: 'application/json',
    transformVersion: 'harness-v1',
  );

  @override
  Future<SendResult> send(WirePayload payload) async {
    final body = jsonDecode(utf8.decode(payload.bytes)) as Map<String, Object?>;
    final ids = (body['event_ids']! as List<Object?>).cast<String>();
    onSent?.call(ids);
    final g = gate;
    if (g != null) await g();
    return const SendOk();
  }
}

/// A drainer running in a spawned isolate.
final class SpawnedDrainer {
  SpawnedDrainer._(this._isolate, this._commands, this._messages);

  final Isolate _isolate;
  final SendPort _commands;
  final Stream<Map<String, Object?>> _messages;
  final List<Map<String, Object?>> _seen = <Map<String, Object?>>[];

  /// The latest state it reported.
  String? state;

  /// The epoch and lock-session pid it reported after its latest
  /// acquisition.
  int? epoch;
  int? pid;

  /// The event ids its receiver was sent, in order.
  final List<String> sent = <String>[];

  /// Its warning and severe log lines.
  final List<String> log = <String>[];

  /// Spawns a drainer over the provisioned database at [url] that
  /// registers [destinations] and starts a delivery cycle with [cadence].
  /// Its backend's probe runs every hour, so its lock session is not
  /// probed during a test. [hooks] names the seams it installs:
  /// `holdFirstBump` holds its first epoch raise between the write and the
  /// commit until [commitBump]; `gateSends` holds every send until
  /// [releaseSends].
  static Future<SpawnedDrainer> spawn(
    String url, {
    Duration cadence = const Duration(milliseconds: 100),
    List<String> destinations = const <String>['x'],
    Set<String> hooks = const <String>{},
  }) async {
    final fromIsolate = ReceivePort();
    final isolate = await Isolate.spawn(_drainerMain, <Object?>[
      fromIsolate.sendPort,
      url,
      cadence.inMilliseconds,
      destinations,
      hooks.toList(),
    ]);
    final messages = fromIsolate
        .map((m) => Map<String, Object?>.from(m as Map))
        .asBroadcastStream();
    final ready = await messages
        .firstWhere((m) => m['type'] == 'commands')
        .timeout(const Duration(seconds: 10));
    final drainer = SpawnedDrainer._(
      isolate,
      ready['port']! as SendPort,
      messages,
    );
    messages.listen((m) {
      drainer._seen.add(m);
      switch (m['type']) {
        case 'state':
          drainer.state = m['value']! as String;
        case 'epoch':
          drainer
            ..epoch = m['value']! as int
            ..pid = m['pid']! as int;
        case 'sent':
          drainer.sent.addAll((m['ids']! as List<Object?>).cast<String>());
        case 'log':
          drainer.log.add('${m['message']} ${m['error'] ?? ''}');
        case 'closed':
          fromIsolate.close();
      }
    });
    return drainer;
  }

  /// Waits, at most 10 s, for a message of [type] (one already received
  /// counts) that [where] accepts.
  Future<Map<String, Object?>> next(
    String type, {
    bool Function(Map<String, Object?> m)? where,
  }) async {
    bool accepts(Map<String, Object?> m) =>
        m['type'] == type && (where == null || where(m));
    for (final m in _seen) {
      if (accepts(m)) return m;
    }
    return _messages.firstWhere(accepts).timeout(const Duration(seconds: 10));
  }

  /// Waits for a state message naming [value].
  Future<void> reaches(String value) =>
      next('state', where: (m) => m['value'] == value);

  /// Lets a held epoch raise commit.
  void commitBump() => _commands.send(<String, Object?>{'cmd': 'commitBump'});

  /// Lets the held sends answer.
  void releaseSends() =>
      _commands.send(<String, Object?>{'cmd': 'releaseSends'});

  /// Appends a note through its store.
  void appendEvent(String aggregateId) => _commands.send(<String, Object?>{
    'cmd': 'appendEvent',
    'id': aggregateId,
  });

  /// Closes its cycle, store and backend, and ends the isolate.
  Future<void> close() async {
    if (_seen.any((m) => m['type'] == 'closed')) return;
    _commands.send(<String, Object?>{'cmd': 'close'});
    try {
      await next('closed');
    } on TimeoutException {
      _isolate.kill(priority: Isolate.immediate);
    }
  }
}

Future<void> _drainerMain(List<Object?> args) async {
  final out = args[0]! as SendPort;
  final url = args[1]! as String;
  final cadence = Duration(milliseconds: args[2]! as int);
  final destinations = (args[3]! as List<Object?>).cast<String>();
  final hookNames = (args[4]! as List<Object?>).cast<String>().toSet();
  final commands = ReceivePort();
  out.send(<String, Object?>{'type': 'commands', 'port': commands.sendPort});

  final bumpCommitted = Completer<void>();
  var held = false;
  final sendsReleased = Completer<void>();
  final hooks = DeliveryTestHooks(
    onLog: (record) {
      if (record.level.value < 900) return;
      out.send(<String, Object?>{
        'type': 'log',
        'message': '${record.name}: ${record.message}',
        'error': record.error?.toString(),
      });
    },
    insideEpochBumpBeforeCommit: hookNames.contains('holdFirstBump')
        ? () async {
            if (held) return;
            held = true;
            out.send(<String, Object?>{'type': 'bumpPending'});
            await bumpCommitted.future;
          }
        : null,
  );
  await runWithDeliveryTestHooks(hooks, () async {
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      lockHeartbeat: const Duration(hours: 1),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: harnessEntryTypes(),
      source: const Source(
        hopId: 'spawned',
        identifier: 'spawned-install',
        softwareVersion: 'harness',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );
    final registry = DestinationRegistry(eventStore: store);
    for (final id in destinations) {
      await registry.addDestination(
        HarnessReceiver(
          id,
          onSent: (ids) =>
              out.send(<String, Object?>{'type': 'sent', 'ids': ids}),
          gate: hookNames.contains('gateSends')
              ? () => sendsReleased.future
              : null,
        ),
        initiator: const AutomationInitiator(service: 'harness'),
      );
    }
    final cycle = await SyncCycle.start(registry: registry, cadence: cadence);
    SyncCycleState? reported;
    var reportedEpoch = -1;
    final watch = Timer.periodic(const Duration(milliseconds: 5), (_) async {
      final state = cycle.state;
      if (state != reported) {
        reported = state;
        out.send(<String, Object?>{'type': 'state', 'value': state.name});
      }
      if (state == SyncCycleState.running) {
        final epoch = await backend.transaction(backend.readDrainEpochTxn);
        if (epoch != null && epoch != reportedEpoch) {
          reportedEpoch = epoch;
          final pid = (await backend.lockSessionForTest()).pid;
          out.send(<String, Object?>{
            'type': 'epoch',
            'value': epoch,
            'pid': pid,
          });
        }
      }
    });
    await for (final message in commands) {
      final m = Map<String, Object?>.from(message as Map);
      switch (m['cmd']) {
        case 'commitBump':
          if (!bumpCommitted.isCompleted) bumpCommitted.complete();
        case 'releaseSends':
          if (!sendsReleased.isCompleted) sendsReleased.complete();
        case 'appendEvent':
          await store.append(
            entryType: harnessNoteType,
            aggregateId: m['id']! as String,
            aggregateType: 'note',
            eventType: 'noted',
            data: const <String, Object?>{},
            initiator: const UserInitiator('spawned'),
          );
        case 'close':
          watch.cancel();
          if (!sendsReleased.isCompleted) sendsReleased.complete();
          await cycle.close(timeout: const Duration(seconds: 5));
          await store.close();
          out.send(<String, Object?>{'type': 'closed'});
          commands.close();
      }
    }
  });
}
