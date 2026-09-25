// Verifies: EVS-PRD-ingest/A
// ingest path admits events from another
//   deployment into the local log (system audit events bridged cross-hop)
// Verifies: EVS-PRD-ingest/B
// upstream identity preserved; the stored copy
//   of an ingested audit keeps the originator's event id, aggregate,
//   initiator and provenance entries, and the receiver hop it appends
//   records the upstream event hash as its arrival hash
//
//   path that lands a wire-side audit event in `event_log` and stamps
//   receiver provenance on it. It SHALL NOT mutate the receiver's
//   `DestinationRegistry`, the receiver's `EntryTypeRegistry`, or any
//   per-destination FIFO state. Configuration on the receiver remains
//   driven exclusively by the receiver's local API calls (e.g. its own
//   `addDestination`, `setStartDate`, `setEndDate`, `deleteDestination`,
//   `tombstoneAndRefill`). Bridged system audit events are stored for
//   forensic / cross-hop observability only — they do not trigger any
//   side effect on the receiver's runtime state.
//
// Strategy: bootstrap two `EventStoreBundle` instances with distinct
//   `Source.identifier` values, one acting as ORIGINATOR and one as
//   RECEIVER. The originator emits real system audit events as a side
//   effect of its own configuration calls (`addDestination`,
//   `setStartDate`, `tombstoneAndRefill`). Those audit events are read
//   off the originator's event log and re-shipped to the receiver via
//   `EventStore.ingestEvent`. The test then asserts the receiver's
//   registries / FIFOs are byte-identical pre vs post ingest, and that
//   the audit was nonetheless stored in the receiver's `event_log`.
//
// Using two real bootstrapped datastores (rather than hand-rolling
//   StoredEvent + Chain 1 hash + receiver Chain 2 stamping) keeps the
//   test focused on the invariant under test — receiver passivity —
//   without re-implementing the wire format.

import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/queue_test_support.dart';
import '../test_support/test_backends.dart';

// ---------------------------------------------------------------------------
// Test fixture helpers
// ---------------------------------------------------------------------------

var _dbCounter = 0;

class _Fixture {
  _Fixture({required this.datastore, required this.backend});
  final EventStoreBundle datastore;
  final SembastBackend backend;
  Future<void> close() => backend.close();
}

const EntryTypeDefinition _demoNoteDef = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Demo Note',
);

Future<_Fixture> _bootstrapDatastore({
  required String hopId,
  required String identifier,
  String softwareVersion = 'pkg@1.0.0',
  List<EntryTypeDefinition> entryTypes = const <EntryTypeDefinition>[],
  List<Destination> destinations = const <Destination>[],
}) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-passive-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final datastore = await bootstrapEventStore(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: softwareVersion,
    ),
    entryTypes: entryTypes,
    destinations: destinations,
  );
  trackTestBackend(datastore.eventStore, backend);
  return _Fixture(datastore: datastore, backend: backend);
}

/// Minimal Destination test double sufficient for `addDestination` and
/// for FIFO promotion via `fillBatch`. Not exported from lib because
/// `FakeDestination` (under test/test_support) ships its own SendResult
/// scripting and we do not need that machinery here — the FIFO is
/// populated by `fillBatch` and never drained.
class _NoopDestination extends Destination {
  _NoopDestination({required this.id});
  @override
  final String id;
  @override
  final String wireFormat = 'noop-v1';
  @override
  final SubscriptionFilter filter = const SubscriptionFilter();
  @override
  final Duration maxAccumulateTime = Duration.zero;
  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;
  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    if (batch.isEmpty) {
      throw ArgumentError('_NoopDestination($id).transform: empty batch');
    }
    final json = jsonEncode(<String, Object?>{
      'event_ids': batch.map((e) => e.eventId).toList(),
    });
    return Future<WirePayload>.value(
      WirePayload(
        bytes: Uint8List.fromList(utf8.encode(json)),
        contentType: 'application/json',
        transformVersion: 'noop-v1',
      ),
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    throw StateError(
      '_NoopDestination($id).send: tests must not drain this FIFO; '
      'the rows exist for a passivity comparison only.',
    );
  }
}

/// Start [f]'s `shared-dest` destination, append [count] notes and fill
/// its queue with them, one note per row.
Future<void> _queueNotes(
  _Fixture f,
  String install, {
  required int count,
}) async {
  await f.datastore.destinations.setStartDate(
    'shared-dest',
    DateTime.utc(2020, 1, 1),
    initiator: const AutomationInitiator(service: 'test'),
  );
  for (var i = 0; i < count; i++) {
    await f.datastore.eventStore.append(
      entryType: 'demo_note',
      aggregateId: 'agg-$install-$i',
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{
        'answers': <String, Object?>{'k': 'v'},
      },
      initiator: const UserInitiator('u'),
    );
  }
  await fillWithScheduleForTest(
    f.datastore.destinations.byId('shared-dest')!,
    backend: f.backend,
    schedule: await f.datastore.destinations.scheduleOf('shared-dest'),
    source: Source(
      hopId: 'hop',
      identifier: install,
      softwareVersion: 'pkg@1.0.0',
    ),
    clock: () => DateTime.now().toUtc().add(const Duration(days: 1)),
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('EventStore ingest path — receiver-stays-passive invariant '
      '(-E', () {
    //   `system.destination_registered` audit MUST NOT add a destination
    //   to the receiver's `DestinationRegistry`. The destination listed
    //   in the audit's `data.id` is the originator's destination, not
    //   the receiver's; configuration on the receiver remains driven by
    //   its own local `addDestination` calls. The audit MUST still be
    //   stored in the receiver's `event_log` (-F admission +
    //   ingest-path write).
    test('ingesting system.destination_registered does NOT '
        'mutate DestinationRegistry on the receiver', () async {
      final originator = await _bootstrapDatastore(
        hopId: 'mobile-device',
        identifier: 'install-mobile',
        destinations: <Destination>[_NoopDestination(id: 'OriginatorPrimary')],
      );
      final receiver = await _bootstrapDatastore(
        hopId: 'control-server',
        identifier: 'install-control',
        destinations: <Destination>[_NoopDestination(id: 'ReceiverPrimary')],
      );

      try {
        // Snapshot receiver's destination registry pre-ingest.
        final preDestIds =
            receiver.datastore.destinations.all().map((d) => d.id).toList()
              ..sort();
        expect(
          preDestIds,
          equals(<String>['ReceiverPrimary']),
          reason: 'sanity: receiver was bootstrapped with one destination',
        );

        // Trigger originator to emit a fresh `destination_registered`
        // audit by adding a second destination locally.
        await originator.datastore.destinations.addDestination(
          _NoopDestination(id: 'OriginatorSecondary'),
          initiator: const AutomationInitiator(service: 'test'),
        );

        // Read the just-emitted audit off originator's event log.
        final originatorEvents = await originator.backend.findAllEvents();
        final auditEvent = originatorEvents.firstWhere(
          (e) =>
              e.entryType == kDestinationRegisteredEntryType &&
              e.data['id'] == 'OriginatorSecondary',
          orElse: () => throw StateError(
            'originator did not emit a destination_registered audit '
            'with data.id="OriginatorSecondary"',
          ),
        );

        // Ingest the bridged audit at the receiver. ingestEvent goes
        // through the same `_ingestOneInTxn` code path as ingestBatch,
        // so the invariant tested here covers both ingest entry points
        // (single-event and batch).
        final outcome = await receiver.datastore.eventStore.ingestEvent(
          auditEvent,
        );
        expect(outcome.outcome, equals(IngestOutcome.ingested));

        // INVARIANT: receiver's destination registry is byte-identical.
        final postDestIds =
            receiver.datastore.destinations.all().map((d) => d.id).toList()
              ..sort();
        expect(
          postDestIds,
          equals(preDestIds),
          reason:
              'receiver DestinationRegistry MUST NOT be mutated by an '
              'ingested system.destination_registered audit '
              '(-E',
        );
        expect(
          receiver.datastore.destinations.byId('OriginatorSecondary'),
          isNull,
          reason:
              'an originator destination id (data.id="OriginatorSecondary") '
              'MUST NOT appear in the receiver registry just because the '
              'receiver ingested the audit',
        );

        // The audit IS stored in the receiver's event_log.
        final receiverEvents = await receiver.backend.findAllEvents();
        final stored = receiverEvents
            .where(
              (e) =>
                  e.entryType == kDestinationRegisteredEntryType &&
                  e.data['id'] == 'OriginatorSecondary',
            )
            .toList();
        expect(
          stored,
          hasLength(1),
          reason:
              'bridged audit MUST be stored in the receiver event_log '
              'for cross-hop observability (-F admission)',
        );
        final copy = stored.single;
        expect(copy.eventId, auditEvent.eventId);
        expect(copy.aggregateId, auditEvent.aggregateId);
        expect(copy.initiator, auditEvent.initiator);
        final upstreamProvenance =
            auditEvent.metadata['provenance'] as List<Object?>;
        final copyProvenance = copy.metadata['provenance'] as List<Object?>;
        expect(
          copyProvenance.take(upstreamProvenance.length).toList(),
          equals(upstreamProvenance),
          reason: 'the upstream provenance entries are kept unchanged',
        );
        expect(copyProvenance, hasLength(upstreamProvenance.length + 1));
        final receiverHop = copyProvenance.last as Map<Object?, Object?>;
        expect(
          receiverHop['hop'],
          'control-server',
          reason: 'the receiver appends its own hop',
        );
        expect(
          receiverHop['arrival_hash'],
          auditEvent.eventHash,
          reason: 'the receiver hop records the upstream event hash',
        );

        // Nothing is persisted for the originator's destination either.
        expect(
          await receiver.backend.readSchedule('OriginatorSecondary'),
          isNull,
          reason: 'no schedule is persisted for the originator destination',
        );
        expect(
          await receiver.backend.listFifoEntries('OriginatorSecondary'),
          isEmpty,
          reason: 'no queue exists for the originator destination',
        );
      } finally {
        await originator.close();
        await receiver.close();
      }
    });

    //   `system.entry_type_registry_initialized` audit MUST NOT mutate
    //   the receiver's `EntryTypeRegistry`. The originator's registry
    //   shape (encoded inside the audit's `data.registry` map) is the
    //   originator's runtime contract, not the receiver's; the
    //   receiver's registry remains exactly the set of types its own
    //   `bootstrapEventStore` call registered.
    test('ingesting system.entry_type_registry_initialized '
        'does NOT mutate EntryTypeRegistry on the receiver', () async {
      // Originator bootstraps with one user entry type registered;
      // receiver bootstraps with NO user entry types. After the
      // originator's bootstrap, its event_log holds an
      // `entry_type_registry_initialized` audit naming `demo_note`.
      // That audit, when ingested by the receiver, must NOT cause
      // `demo_note` to register on the receiver.
      final originator = await _bootstrapDatastore(
        hopId: 'mobile-device',
        identifier: 'install-mobile',
        entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
      );
      final receiver = await _bootstrapDatastore(
        hopId: 'control-server',
        identifier: 'install-control',
      );

      try {
        // Snapshot receiver's entry-type registry pre-ingest. The set
        // is exactly the reserved system entry types auto-registered by
        // bootstrap (10 original system audits + 2 substrate-internal
        // lib-version boot events + 2 added by the entry-type-version-
        // substrate-owned work: ingest-audit and view_snapshot_promoted).
        final preIds = receiver.datastore.entryTypes
            .all()
            .map((d) => d.id)
            .toSet();
        expect(
          preIds.contains('demo_note'),
          isFalse,
          reason:
              'sanity: receiver did NOT register demo_note (it was '
              'registered on the originator only)',
        );
        expect(
          preIds.length,
          equals(kReservedSystemEntryTypeIds.length),
          reason:
              'sanity: receiver registry contains exactly the reserved '
              'system entry types pre-ingest',
        );

        // Read originator's bootstrap-emitted audit.
        final originatorEvents = await originator.backend.findAllEvents();
        final auditEvent = originatorEvents.firstWhere(
          (e) => e.entryType == kEntryTypeRegistryInitializedEntryType,
          orElse: () => throw StateError(
            'originator did not emit an entry_type_registry_initialized '
            'audit at bootstrap',
          ),
        );
        // Sanity: the audit's data.registry includes demo_note.
        final auditRegistry = Map<String, Object?>.from(
          auditEvent.data['registry'] as Map,
        );
        expect(
          auditRegistry.containsKey('demo_note'),
          isTrue,
          reason:
              'sanity: audit payload should reference originator-side '
              'demo_note registration',
        );

        // Ingest the bridged registry-init audit on the receiver.
        final outcome = await receiver.datastore.eventStore.ingestEvent(
          auditEvent,
        );
        expect(outcome.outcome, equals(IngestOutcome.ingested));

        // INVARIANT: receiver's entry-type registry is byte-identical.
        final postIds = receiver.datastore.entryTypes
            .all()
            .map((d) => d.id)
            .toSet();
        expect(
          postIds,
          equals(preIds),
          reason:
              'receiver EntryTypeRegistry MUST NOT be mutated by an '
              'ingested system.entry_type_registry_initialized audit '
              '(-E',
        );
        expect(
          receiver.datastore.entryTypes.byId('demo_note'),
          isNull,
          reason:
              'demo_note (an originator-only entry type) MUST NOT '
              'appear in the receiver registry just because the '
              'receiver ingested the registry-init audit',
        );

        // The audit IS stored in the receiver's event_log.
        final receiverEvents = await receiver.backend.findAllEvents();
        final stored = receiverEvents
            .where(
              (e) =>
                  e.entryType == kEntryTypeRegistryInitializedEntryType &&
                  e.aggregateId == 'install-mobile',
            )
            .toList();
        expect(
          stored,
          hasLength(1),
          reason:
              'bridged registry-init audit MUST be stored in the '
              'receiver event_log under the originator install '
              'aggregate (-F admission)',
        );
      } finally {
        await originator.close();
        await receiver.close();
      }
    });

    //   `system.destination_wedge_recovered` audit MUST NOT touch the
    //   receiver's per-destination queue state. The receiver drains a
    //   destination under the same id the audit names, and its own head is
    //   wedged, so a library that applied the bridged recovery would have a
    //   queue to act on: it would retire the receiver's wedged head, clear
    //   its wedge record or rewind its fill position.
    test('ingesting system.destination_wedge_recovered '
        'does NOT mutate FIFO state on the receiver', () async {
      const init = AutomationInitiator(service: 'test');
      final originator = await _bootstrapDatastore(
        hopId: 'mobile-device',
        identifier: 'install-mobile',
        entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
        destinations: <Destination>[_NoopDestination(id: 'shared-dest')],
      );
      final receiver = await _bootstrapDatastore(
        hopId: 'control-server',
        identifier: 'install-control',
        entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
        destinations: <Destination>[_NoopDestination(id: 'shared-dest')],
      );

      try {
        await _queueNotes(originator, 'install-mobile', count: 1);
        await _queueNotes(receiver, 'install-control', count: 2);

        // The receiver's own head is wedged: exactly the state a bridged
        // recovery would act on if the library applied it.
        final receiverWedgedRowId = await wedgeHeadForTest(
          receiver.datastore.destinations,
          'shared-dest',
        );
        final preReceiverFifo = await receiver.backend.listFifoEntries(
          'shared-dest',
        );
        expect(
          preReceiverFifo,
          hasLength(2),
          reason: 'sanity: the receiver queue holds its two notes',
        );
        expect(preReceiverFifo.first.entryId, receiverWedgedRowId);
        expect(preReceiverFifo.first.finalStatus, FinalStatus.wedged);
        final preWedgeRecord = await receiver.backend.transaction(
          (txn) => receiver.backend.readWedgeRecordTxn(txn, 'shared-dest'),
        );
        expect(
          preWedgeRecord,
          isNotNull,
          reason: 'sanity: the receiver holds a wedge record',
        );
        final preFillCursor = await receiver.backend.readFillCursor(
          'shared-dest',
        );
        final preSchedule = await receiver.backend.readSchedule('shared-dest');

        // Trigger originator's wedge recovery — emits a real
        // `system.destination_wedge_recovered` audit naming 'shared-dest'.
        final origHeadRowId = await wedgeHeadForTest(
          originator.datastore.destinations,
          'shared-dest',
        );
        await originator.datastore.destinations.tombstoneAndRefill(
          'shared-dest',
          origHeadRowId,
          initiator: init,
        );
        final auditEvent = (await originator.backend.findAllEvents(
          entryType: kDestinationWedgeRecoveredEntryType,
        )).single;
        expect(auditEvent.data['id'], 'shared-dest');
        expect(
          auditEvent.data['row_id'],
          origHeadRowId,
          reason: 'the recovery audit names the recovered originator row',
        );

        // Ingest at receiver.
        final outcome = await receiver.datastore.eventStore.ingestEvent(
          auditEvent,
        );
        expect(outcome.outcome, equals(IngestOutcome.ingested));

        // INVARIANT: the receiver's queue, wedge record, fill position and
        // schedule are unchanged. FifoEntry equality covers every field, so
        // row insertion, deletion, status flip, attempt growth or
        // re-ordering all fail the comparison.
        expect(
          await receiver.backend.listFifoEntries('shared-dest'),
          equals(preReceiverFifo),
          reason:
              'receiver queue MUST NOT change on ingest of a bridged '
              'wedge-recovery audit',
        );
        expect(
          await receiver.backend.transaction(
            (txn) => receiver.backend.readWedgeRecordTxn(txn, 'shared-dest'),
          ),
          equals(preWedgeRecord),
          reason: 'the receiver wedge record stays in place',
        );
        expect(
          await receiver.backend.readFillCursor('shared-dest'),
          preFillCursor,
          reason: 'the receiver fill position is not rewound',
        );
        expect(
          await receiver.backend.readSchedule('shared-dest'),
          equals(preSchedule),
        );
        expect(
          (await receiver.backend.wedgedFifos()).toList(),
          hasLength(1),
          reason: 'the receiver destination is still wedged',
        );

        // The audit IS stored in the receiver's event_log.
        expect(
          await receiver.backend.findAllEvents(
            entryType: kDestinationWedgeRecoveredEntryType,
          ),
          hasLength(1),
          reason:
              'bridged wedge-recovery audit MUST be stored in the '
              'receiver event_log (-F admission)',
        );
      } finally {
        await originator.close();
        await receiver.close();
      }
    });

    // Verifies: EVS-PRD-destinations/L
    // the wedge record and the queue are
    //   the receiver's own persisted state: ingesting a peer's wedge event
    //   that names a destination id the receiver also drains leaves the
    //   receiver's wedge record absent and its head pending.
    // Verifies: EVS-DEV-destination-drain/D
    // only the receiver's own drainer
    //   wedges its head; a bridged wedge event wedges nothing.
    test('ingesting system.destination_wedged does NOT wedge the receiver '
        'or write its wedge record', () async {
      final originator = await _bootstrapDatastore(
        hopId: 'mobile-device',
        identifier: 'install-mobile',
        entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
        destinations: <Destination>[_NoopDestination(id: 'shared-dest')],
      );
      final receiver = await _bootstrapDatastore(
        hopId: 'control-server',
        identifier: 'install-control',
        entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
        destinations: <Destination>[_NoopDestination(id: 'shared-dest')],
      );
      try {
        await _queueNotes(originator, 'install-mobile', count: 1);
        await _queueNotes(receiver, 'install-control', count: 1);
        await wedgeHeadForTest(
          originator.datastore.destinations,
          'shared-dest',
        );
        final wedgeEvent = (await originator.backend.findAllEvents(
          entryType: kDestinationWedgedEntryType,
        )).single;
        expect(wedgeEvent.data['id'], 'shared-dest');

        final headBefore = (await receiver.backend.readFifoHead(
          'shared-dest',
        ))!;
        expect(headBefore.finalStatus, isNull);

        final outcome = await receiver.datastore.eventStore.ingestEvent(
          wedgeEvent,
        );
        expect(outcome.outcome, equals(IngestOutcome.ingested));

        final record = await receiver.backend.transaction(
          (txn) => receiver.backend.readWedgeRecordTxn(txn, 'shared-dest'),
        );
        expect(record, isNull, reason: 'no wedge record on the receiver');
        final headAfter = (await receiver.backend.readFifoHead('shared-dest'))!;
        expect(headAfter.entryId, headBefore.entryId);
        expect(headAfter.finalStatus, isNull, reason: 'the head stays pending');
        expect(headAfter.attempts, isEmpty);
        expect(await receiver.backend.wedgedFifos(), isEmpty);
        expect(
          await receiver.backend.findAllEvents(
            entryType: kDestinationWedgedEntryType,
          ),
          hasLength(1),
          reason: 'the bridged wedge event is stored in the receiver log',
        );
      } finally {
        await originator.close();
        await receiver.close();
      }
    });
  });
}
