// Release probe for the library's test seams.
//
// Installs seams whose log observer and failure injection would both fire,
// then runs a registry operation and one delivery pass over an in-memory
// Sembast database. Run without assertions (`dart run --no-enable-asserts`,
// or a `dart compile exe` executable) it must print an empty list of fired
// seams and a completed delivery, and exit 0. Run with assertions enabled
// the same body reports the seams that fired, and the process exits 1.
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:sembast/sembast_memory.dart';

/// Outcome of one probe run.
class ProbeOutcome {
  ProbeOutcome({
    required this.firedSeams,
    required this.delivered,
    required this.storedEndDate,
    required this.registryEndDate,
    required this.endDateSetEvents,
    required this.sequenceAdvance,
  });

  /// Seams that fired, in order.
  final List<String> firedSeams;

  /// Payloads the healthy destination received.
  final int delivered;

  /// The healthy destination's stored end date after the registry
  /// operation (ISO-8601), or null.
  final String? storedEndDate;

  /// The healthy destination's end date as the registry reports it after
  /// the registry operation (ISO-8601), or null.
  final String? registryEndDate;

  /// `system.destination_end_date_set` events in the log after the
  /// registry operation.
  final int endDateSetEvents;

  /// How far the registry operation advanced the sequence counter.
  final int sequenceAdvance;

  /// True when no seam fired and the pass delivered the one event.
  bool get passed => firedSeams.isEmpty && delivered == 1;

  Map<String, Object?> toJson() => <String, Object?>{
    'fired_seams': firedSeams,
    'delivered': delivered,
    'stored_end_date': storedEndDate,
    'registry_end_date': registryEndDate,
    'end_date_set_events': endDateSetEvents,
    'sequence_advance': sequenceAdvance,
    'passed': passed,
  };
}

class _ProbeDestination extends Destination {
  _ProbeDestination(this.id, {this.failTransform = false});

  @override
  final String id;

  /// When true, `transform` throws, so the delivery cycle logs a fill
  /// failure for this destination.
  final bool failTransform;

  int sends = 0;

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'probe_event'});

  @override
  String get wireFormat => 'probe-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async {
    if (failTransform) throw StateError('probe transform failure');
    return WirePayload(
      bytes: Uint8List.fromList(
        utf8.encode(jsonEncode(<String, Object?>{'n': batch.length})),
      ),
      contentType: 'application/json',
      transformVersion: 'probe-1',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    sends += 1;
    return const SendOk();
  }
}

/// Runs the probe body: seams installed around a registry operation and a
/// delivery pass.
Future<ProbeOutcome> runHooksReleaseProbe() async {
  final db = await newDatabaseFactoryMemory().openDatabase('probe.db');
  final backend = SembastBackend(database: db);
  final healthy = _ProbeDestination('probe_healthy');
  final broken = _ProbeDestination('probe_broken', failTransform: true);
  final bundle = await bootstrapEventStore(
    backend: backend,
    source: const Source(
      hopId: 'probe',
      identifier: 'probe-install',
      softwareVersion: 'probe@1',
    ),
    entryTypes: const <EntryTypeDefinition>[
      EntryTypeDefinition(
        id: 'probe_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Probe event',
      ),
    ],
    destinations: <Destination>[healthy, broken],
  );
  const initiator = AutomationInitiator(service: 'hooks-release-probe');
  final start = DateTime.utc(2000);
  for (final d in <Destination>[healthy, broken]) {
    await bundle.destinations.setStartDate(d.id, start, initiator: initiator);
  }
  await bundle.eventStore.append(
    entryType: 'probe_event',
    aggregateId: 'probe-1',
    aggregateType: 'Probe',
    eventType: 'finalized',
    data: const <String, Object?>{'n': 1},
    initiator: initiator,
  );

  final fired = <String>[];
  final hooks = DeliveryTestHooks(
    onLog: (record) => fired.add('onLog ${record.name}: ${record.message}'),
    failRegistryAuditAppend: (entryType) {
      fired.add('failRegistryAuditAppend $entryType');
      return true;
    },
  );
  late final String? storedEndDate;
  late final String? registryEndDate;
  late final int endDateSetEvents;
  late final int sequenceAdvance;
  await runWithDeliveryTestHooks(hooks, () async {
    final sequenceBefore = await backend.readSequenceCounter();
    try {
      await bundle.destinations.setEndDate(
        healthy.id,
        DateTime.utc(2999),
        initiator: initiator,
      );
    } on Object catch (e) {
      fired.add('registry operation failed: $e');
    }
    sequenceAdvance = await backend.readSequenceCounter() - sequenceBefore;
    storedEndDate = (await backend.readSchedule(
      healthy.id,
    ))?.endDate?.toIso8601String();
    registryEndDate = (await bundle.destinations.scheduleOf(
      healthy.id,
    )).endDate?.toIso8601String();
    endDateSetEvents = (await backend.findAllEvents(
      entryType: 'system.destination_end_date_set',
    )).length;
    final cycle = SyncCycle(
      backend: backend,
      registry: bundle.destinations,
      source: bundle.eventStore.source,
    );
    await cycle();
  });
  await backend.close();
  return ProbeOutcome(
    firedSeams: fired,
    delivered: healthy.sends,
    storedEndDate: storedEndDate,
    registryEndDate: registryEndDate,
    endDateSetEvents: endDateSetEvents,
    sequenceAdvance: sequenceAdvance,
  );
}

Future<void> main() async {
  final outcome = await runHooksReleaseProbe();
  stdout.writeln(jsonEncode(outcome.toJson()));
  exit(outcome.passed ? 0 : 1);
}
