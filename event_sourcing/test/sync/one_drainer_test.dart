// Verifies: EVS-PRD-destinations/V
// the refusal of a second delivery cycle is keyed by the database: two
//   Sembast databases in one isolate each run a cycle and deliver, closing
//   one leaves the other delivering, and a cycle in another isolate over
//   another database is unaffected.
// Verifies: EVS-DEV-destination-drain-lock/A
// on Sembast the drain lock's scope is one open database handle in one
//   isolate.
import 'dart:isolate';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/delivery_cycle_conformance.dart' show Receiver, until;
import '../test_support/registry_with_audit.dart';

const String _noteType = 'one_drainer_note';

Future<({EventStore store, DestinationRegistry registry, Receiver receiver})>
_pane(String name) async {
  final db = await newDatabaseFactoryMemory().openDatabase(name);
  final backend = SembastBackend(database: db);
  final deps = await buildAuditedRegistryDeps(
    backend,
    callerEntryTypes: const <EntryTypeDefinition>[
      EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
      ),
    ],
  );
  final registry = DestinationRegistry(eventStore: deps.eventStore);
  final receiver = Receiver(id: 'x', entryTypes: const <String>{_noteType});
  await registry.addDestination(
    receiver,
    initiator: const AutomationInitiator(service: 'test'),
  );
  await registry.setStartDate(
    'x',
    DateTime.utc(2000),
    initiator: const AutomationInitiator(service: 'test'),
  );
  return (store: deps.eventStore, registry: registry, receiver: receiver);
}

Future<String> _note(EventStore store, String id) async => (await store.append(
  entryType: _noteType,
  aggregateId: id,
  aggregateType: 'note',
  eventType: 'noted',
  data: const <String, Object?>{},
  initiator: const UserInitiator('u'),
))!.eventId;

/// Opens its own in-memory database in a spawned isolate, starts a cycle
/// over it, and reports the cycle's state.
Future<void> _otherIsolate(SendPort out) async {
  final pane = await _pane('other-isolate.db');
  final cycle = await SyncCycle.start(
    registry: pane.registry,
    cadence: const Duration(hours: 1),
  );
  out.send(cycle.state.name);
  await cycle.close();
}

void main() {
  test('two databases in one isolate each run a cycle', () async {
    final first = await _pane('one-drainer-a.db');
    final second = await _pane('one-drainer-b.db');
    final a = await SyncCycle.start(
      registry: first.registry,
      cadence: const Duration(hours: 1),
    );
    final b = await SyncCycle.start(
      registry: second.registry,
      cadence: const Duration(hours: 1),
    );
    addTearDown(() async {
      await a.close();
      await b.close();
    });
    expect(a.state, SyncCycleState.running);
    expect(b.state, SyncCycleState.running);
    final idA = await _note(first.store, 'a');
    final idB = await _note(second.store, 'b');
    await until(() => first.receiver.sentIds.contains(idA), reason: 'A');
    await until(() => second.receiver.sentIds.contains(idB), reason: 'B');
    await a.close();
    final later = await _note(second.store, 'b2');
    await until(
      () => second.receiver.sentIds.contains(later),
      reason: 'B after A closed',
    );
    expect(b.state, SyncCycleState.running);
  });

  test('a cycle in another isolate over another database', () async {
    final pane = await _pane('this-isolate.db');
    final cycle = await SyncCycle.start(
      registry: pane.registry,
      cadence: const Duration(hours: 1),
    );
    addTearDown(cycle.close);
    final port = ReceivePort();
    await Isolate.spawn(_otherIsolate, port.sendPort);
    final state = await port.first.timeout(const Duration(seconds: 10));
    port.close();
    expect(state, 'running');
    expect(cycle.state, SyncCycleState.running);
  });
}
