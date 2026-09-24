// Test support for the browser tests: tab models over one IndexedDB
// database. Each tab model opens the database through its own
// independently built sembast_web factory, so it holds its own sembast
// `Database`, as a browser tab does. Two tab models in one page receive no
// revision notice from each other (sembast_web posts them on one
// BroadcastChannel per page, which never delivers to its own sender), so
// each sees the other's commits only when it commits a write: the worst
// case of real tabs. This file declares no tests, so it carries no
// citation.
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
import 'package:sembast/sembast.dart' as sembast;
// ignore: implementation_imports, builds a second independent factory to model a second tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

/// The note entry type every tab model registers.
const String kWebNote = 'web_note';

/// The initiator of the tab models' registry operations.
const Initiator kWebInit = AutomationInitiator(service: 'web-tabs');

/// The initiator of the tab models' halt requests.
const Initiator kWebOperator = UserInitiator('web-operator');

/// A fresh IndexedDB database name.
String freshWebName(String prefix) =>
    '$prefix-${DateTime.now().microsecondsSinceEpoch}.db';

/// Opens [name] through a factory of its own, as one tab would.
Future<sembast.Database> openTabDatabase(
  String name, {
  sembast.DatabaseMode? mode,
}) => DatabaseFactoryWeb(
  JdbFactoryWeb(idbFactoryWeb),
).openDatabase(name, mode: mode);

/// One tab model: its backend, event store and destination registry.
final class WebTab {
  WebTab(this.name, this.backend, this.store)
    : registry = DestinationRegistry(eventStore: store);

  /// The IndexedDB name of the tab's database.
  final String name;
  final SembastBackend backend;
  final EventStore store;
  final DestinationRegistry registry;

  /// Registers [d] in this tab's registry, and activates it from
  /// 2026-01-01 when [activate] is set.
  Future<void> register(Destination d, {bool activate = true}) async {
    await registry.addDestination(d, initiator: kWebInit);
    if (activate) {
      await registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: kWebInit,
      );
    }
  }

  /// Appends a note; returns its event id.
  Future<String> note(String id) async {
    final event = await store.append(
      entryType: kWebNote,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'noted',
      data: <String, Object?>{'id': id},
      initiator: const UserInitiator('web-user'),
    );
    return event!.eventId;
  }

  Future<void> close() => store.close();
}

/// Reads [read] through a backend over a fresh read-only handle of [name],
/// so it sees every tab's commits. The handle is read-only because a
/// sembast_web open that may write compacts the database, which leaves a
/// tab model that has not seen the latest commits unable to commit again
/// (see `sembast_web_compaction_test.dart`).
Future<T> readFresh<T>(
  String name,
  Future<T> Function(SembastBackend backend) read,
) async {
  final database = await openTabDatabase(
    name,
    mode: sembast.DatabaseMode.readOnly,
  );
  final backend = SembastBackend(database: database);
  try {
    return await read(backend);
  } finally {
    await database.close();
  }
}

/// The drain epoch [name] stores, read through a fresh handle.
Future<int?> freshEpoch(String name) =>
    readFresh(name, (b) => b.transaction(b.readDrainEpochTxn));

/// Opens an event store over [name] as one tab would, registering
/// [kWebNote] at [noteVersion].
Future<WebTab> openWebTab(
  String name, {
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
  sembast.Database? database,
  ProjectionRegistry? projections,
  PromoterRegistry? promoters,
}) async {
  final backend = SembastBackend(
    database: database ?? await openTabDatabase(name),
  );
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes.register(
    EntryTypeDefinition(
      id: kWebNote,
      registeredVersion: noteVersion,
      name: kWebNote,
    ),
  );
  try {
    final store = await EventStore.open(
      storage: backend,
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'web-hop',
        identifier: 'web-install',
        softwareVersion: 'web-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: projections,
      promoters: promoters,
    );
    return WebTab(name, backend, store);
  } catch (_) {
    await backend.close();
    rethrow;
  }
}

/// A destination that records the event ids of every send it receives,
/// optionally waits on [gate] before answering, and answers with
/// [outcome] (SendOk by default).
final class WebReceiver extends Destination {
  WebReceiver({required this.id, this.batchCapacity = 1})
    : filter = const SubscriptionFilter(entryTypes: <String>{kWebNote});

  /// The most events one queue item carries.
  final int batchCapacity;

  @override
  final String id;

  @override
  final SubscriptionFilter filter;

  @override
  bool get allowHardDelete => true;

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  String get wireFormat => 'web-receiver-v1';

  /// The event ids of every send that started, in order.
  final List<List<String>> started = <List<String>>[];

  /// The event ids of every send that returned, in order.
  final List<List<String>> received = <List<String>>[];

  /// Awaited by every send after it is recorded as started.
  Future<void> Function()? gate;

  /// The outcome of the n-th send (from 0).
  SendResult Function(int n) outcome = (_) => const SendOk();

  /// Every event id sent, in order.
  List<String> get sentIds => <String>[for (final b in received) ...b];

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < batchCapacity;

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
    transformVersion: 'web-receiver-v1',
  );

  @override
  Future<SendResult> send(WirePayload payload) async {
    final body = jsonDecode(utf8.decode(payload.bytes)) as Map<String, Object?>;
    final ids = (body['event_ids']! as List<Object?>).cast<String>();
    final n = started.length;
    started.add(ids);
    final g = gate;
    if (g != null) await g();
    received.add(ids);
    return outcome(n);
  }
}

/// Polls [condition] every 5 ms until it holds, failing after [bound].
Future<void> until(
  FutureOr<bool> Function() condition, {
  Duration bound = const Duration(seconds: 10),
  String reason = 'the condition',
}) async {
  final deadline = DateTime.now().add(bound);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('$reason did not hold within $bound');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

/// No backoff, a budget of five attempts.
const SyncPolicy kWebPolicy = SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: 5,
);
