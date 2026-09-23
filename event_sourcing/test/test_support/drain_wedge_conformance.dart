// Backend-agnostic scenarios for the wedge event the drainer appends: its
// data fields for each cause, its atomicity with the wedged status and the
// wedge record, the fallback that records the attempt alone when the wedge
// transaction fails, the status derivation before any send (a lowered
// budget, a recorded refusal, a fallback across a restart), the absence of
// attempt text, routing to destinations, the guards on the wedge, and two
// destinations wedging in one cycle. Sembast runs them from
// test/sync/drain_wedge_event_test.dart and Postgres from
// test/storage/postgres/postgres_drain_wedge_event_test.dart.
//
// This file exposes [runDrainWedgeScenarios] and the agreement helper
// [expectWedgeRecordMatchesLog], and registers no `main()` of its own.
// Traceability lives on the individual tests.
import 'dart:async';
import 'dart:convert';
import 'dart:math' show Random;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/event_hash.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show wedgeHeadForTest;
import 'wedges_view_invariant.dart';

const Initiator _init = AutomationInitiator(service: 'wedge-scenarios');
const String _noteType = 'wedge_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'wedge-install',
  softwareVersion: 'test@1.0.0',
);

/// The keys a wedge event's data carries, every one of them on every
/// wedge.
const Set<String> declaredWedgeEventKeys = <String>{
  'id',
  'database_id',
  'row_id',
  'event_ids',
  'first_seq',
  'last_seq',
  'sequence_in_queue',
  'cause',
  'attempt_count',
  'max_attempts',
  'last_outcome',
  'http_status',
  'wire_format',
  'transform_version',
  'halt_request_event_id',
  'halt_requested_by',
  'halt_purpose',
  'drainer_epoch',
  'configuration_fingerprint',
  'configuration',
};

/// A policy with no backoff and an attempt budget of [maxAttempts].
SyncPolicy budget(int maxAttempts) => SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: maxAttempts,
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// Every destination audit event this store's install appended naming
/// [destinationId], in log order.
Future<List<StoredEvent>> _destinationAudits(
  EventStore store,
  String destinationId,
) async => <StoredEvent>[
  for (final e in await store.backend.findAllEvents())
    if (e.aggregateType == 'system_destination' &&
        e.aggregateId == store.source.identifier &&
        e.data['id'] == destinationId)
      e,
];

/// Asserts [destinationId]'s wedge record equals the open wedge the log
/// records: the latest wedge event this install appended for it, unless a
/// recovery or a deletion followed; and that the item it names is wedged.
/// Then asserts the view-queue invariant ([expectWedgesViewMatchesQueue]).
Future<void> expectWedgeRecordMatchesLog(
  EventStore store,
  String destinationId,
) async {
  await _expectWedgeRecordMatchesLog(store, destinationId);
  await expectWedgesViewMatchesQueue(store);
}

Future<void> _expectWedgeRecordMatchesLog(
  EventStore store,
  String destinationId,
) async {
  final backend = store.backend;
  StoredEvent? open;
  for (final audit in await _destinationAudits(store, destinationId)) {
    switch (audit.eventType) {
      case kDestinationWedgedEventType:
        open = audit;
      case kDestinationWedgeRecoveredEventType:
      case kDestinationDeletedEventType:
        open = null;
    }
  }
  final record = await backend.transaction(
    (txn) => backend.readWedgeRecordTxn(txn, destinationId),
  );
  if (open == null) {
    expect(record, isNull, reason: 'no open wedge in the log');
    return;
  }
  expect(
    record,
    WedgeRecord(
      rowId: open.data['row_id'] as String,
      wedgeEventId: open.eventId,
      cause: WedgeCause.fromWire(open.data['cause'] as String),
    ),
    reason: 'the record names the open wedge',
  );
  final row = await backend.readFifoRow(destinationId, record!.rowId);
  expect(row?.finalStatus, FinalStatus.wedged);
}

/// Asserts the log's hash chain is intact: contiguous sequence numbers,
/// each event linked to its predecessor's hash, and each hash reproduced
/// from the event's content.
Future<void> expectChainIntact(StorageBackend backend) async {
  final events = await backend.findAllEvents();
  String? previous;
  for (var i = 0; i < events.length; i++) {
    final e = events[i];
    expect(e.sequenceNumber, i + 1, reason: 'sequence numbers are contiguous');
    expect(e.previousEventHash, previous, reason: 'event ${e.eventId} links');
    expect(canonicalEventHash(e.toMap()), e.eventHash, reason: e.eventId);
    previous = e.eventHash;
  }
}

class _Process {
  _Process(this.backend, this.store, this.registry);
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

class _World {
  _World(this.db);
  final QueueTestDatabase db;
  DateTime eventTime = DateTime.utc(2026, 3, 1);
  late _Process a;

  StorageBackend get backend => a.backend;
  EventStore get store => a.store;
  DestinationRegistry get registry => a.registry;

  Future<_Process> openProcess() async {
    final backend = await db.openBackend();
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
      ),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: db.securityFor(backend),
      clock: () => eventTime,
    );
    return _Process(backend, store, DestinationRegistry(eventStore: store));
  }

  Future<StoredEvent> note(String id) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    final event = await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id},
      initiator: _init,
    );
    return event!;
  }

  Future<void> activate(Destination d, {DestinationRegistry? on}) async {
    final r = on ?? registry;
    await r.addDestination(d, initiator: _init);
    await r.setStartDate(d.id, DateTime.utc(2026, 1, 1), initiator: _init);
  }

  Future<void> fillAll(Destination d, {StorageBackend? on}) async {
    final b = on ?? backend;
    for (var i = 0; i < 20; i++) {
      final before = (await b.listFifoEntries(d.id)).length;
      final cursorBefore = await b.readFillCursor(d.id);
      await fillBatch(d, backend: b, source: _source, clock: _fillNow);
      final after = (await b.listFifoEntries(d.id)).length;
      if (after == before && await b.readFillCursor(d.id) == cursorBefore) {
        return;
      }
    }
  }

  Future<List<StoredEvent>> wedgeEvents({String? destinationId}) async =>
      <StoredEvent>[
        for (final e in await backend.findAllEvents())
          if (e.entryType == kDestinationWedgedEntryType &&
              (destinationId == null || e.data['id'] == destinationId))
            e,
      ];

  Future<WedgeRecord?> wedgeRecord(String destId) =>
      backend.transaction((txn) => backend.readWedgeRecordTxn(txn, destId));

  /// Everything a wedge could change about [destId], and the log.
  Future<Map<String, Object?>> snapshot(String destId) async => {
    'rows': <Object?>[
      for (final r in await backend.listFifoEntries(destId)) r.toJson(),
    ],
    'record': (await wedgeRecord(destId))?.toJson(),
    'events': <String>[
      for (final e in await backend.findAllEvents()) e.eventId,
    ],
  };
}

/// Run every drain-wedge scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
///
/// [retriesConflictingTransactions] says whether the backend runs two
/// overlapping transactions concurrently and re-runs the loser of a
/// conflict (Postgres), rather than running them one after the other
/// (Sembast). [processesShareDatabaseHandle] says whether every backend
/// the database opens shares one database handle, so that closing one
/// process's backend closes the others' (the Sembast in-memory database).
void runDrainWedgeScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
  required bool retriesConflictingTransactions,
  required bool processesShareDatabaseHandle,
}) {
  group('drain wedge scenarios ($label)', () {
    late _World w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = _World(db);
      w.a = await w.openProcess();
    });

    tearDown(() async {
      if (!available) return;
      await w.db.close();
    });

    /// Register and activate [d], append [notes] notes and fill.
    Future<FifoEntry> queued(FakeDestination d, {int notes = 1}) async {
      await w.activate(d);
      for (var i = 0; i < notes; i++) {
        await w.note('${d.id}-n$i');
      }
      await w.fillAll(d);
      return (await w.backend.readFifoHead(d.id))!;
    }

    // ------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------

    group('fields', () {
      // Verifies: EVS-DEV-destination-drain/I
      // a permanent refusal's wedge event
      //   carries exactly the declared keys, each with its value from the
      //   queue item, its attempts and the budget in effect; http_status is
      //   null for a permanent refusal, and the keys with no source are null.
      // Verifies: EVS-PRD-destinations/Q
      // the event records the cause
      //   permanent_refusal.
      // Verifies: EVS-PRD-destinations/P
      // one event, the item wedged and the
      //   attempt recorded.
      test('a permanent refusal', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'refused')],
        );
        final head = await queued(d);
        await drain(d, registry: w.registry, policy: budget(7));
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts.single.outcome, 'permanent');
        final event = (await w.wedgeEvents()).single;
        expect(event.data.keys.toSet(), declaredWedgeEventKeys);
        expect(event.data, <String, Object?>{
          'id': 'x',
          'database_id': w.store.databaseId,
          'row_id': head.entryId,
          'event_ids': head.eventIds,
          'first_seq': head.sequenceRange.firstSeq,
          'last_seq': head.sequenceRange.lastSeq,
          'sequence_in_queue': head.sequenceInQueue,
          'cause': 'permanent_refusal',
          'attempt_count': 1,
          'max_attempts': 7,
          'last_outcome': 'permanent',
          'http_status': null,
          'wire_format': head.wireFormat,
          'transform_version': head.transformVersion,
          'halt_request_event_id': null,
          'halt_requested_by': null,
          'halt_purpose': null,
          'drainer_epoch': null,
          'configuration_fingerprint': null,
          'configuration': null,
        });
        expect(event.entryType, kDestinationWedgedEntryType);
        expect(event.eventType, kDestinationWedgedEventType);
        expect(event.aggregateType, 'system_destination');
        expect(event.aggregateId, _source.identifier);
        expect(
          event.initiator,
          const AutomationInitiator(service: 'event_sourcing.drain'),
        );
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/I
      // an exhausted budget's wedge event
      //   carries the transient outcome's numeric status and the budget.
      // Verifies: EVS-PRD-destinations/Q
      // the event records the cause
      //   retry_budget_exhausted.
      test('an exhausted budget with a numeric status', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            for (var i = 0; i < 3; i++)
              const SendTransient(error: 'busy', httpStatus: 503),
          ],
        );
        final head = await queued(d);
        for (var i = 0; i < 3; i++) {
          await drain(d, registry: w.registry, policy: budget(3));
          await expectWedgeRecordMatchesLog(w.store, 'x');
        }
        expect(d.sent, hasLength(3));
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(3));
        final event = (await w.wedgeEvents()).single;
        expect(event.data.keys.toSet(), declaredWedgeEventKeys);
        expect(event.data['cause'], 'retry_budget_exhausted');
        expect(event.data['attempt_count'], 3);
        expect(event.data['max_attempts'], 3);
        expect(event.data['last_outcome'], 'transient');
        expect(event.data['http_status'], 503);
        expect(event.data['row_id'], head.entryId);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/I
      // http_status is null when the
      //   transient outcome reported none.
      test('an exhausted budget with no numeric status', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendTransient(error: 'busy')],
        );
        await queued(d);
        await drain(d, registry: w.registry, policy: budget(1));
        final event = (await w.wedgeEvents()).single;
        expect(event.data['cause'], 'retry_budget_exhausted');
        expect(event.data['http_status'], isNull);
        expect(event.data['attempt_count'], 1);
        expect(event.data['max_attempts'], 1);
      });
    });

    // ------------------------------------------------------------------
    // Atomicity
    // ------------------------------------------------------------------

    group('atomicity', () {
      Future<void> expectFallbackEndState(
        FifoEntry head,
        List<LibraryLogRecord> log,
      ) async {
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(
          row.finalStatus,
          isNull,
          reason: 'the wedged status rolled back',
        );
        expect(row.attempts, hasLength(1), reason: 'the attempt alone');
        expect(row.attempts.single.outcome, 'permanent');
        expect(await w.wedgeEvents(), isEmpty);
        expect(await w.wedgeRecord('x'), isNull);
        await expectChainIntact(w.backend);
        await expectWedgeRecordMatchesLog(w.store, 'x');
        final drainLog = <LibraryLogRecord>[
          for (final r in log)
            if (r.name == 'event_sourcing.drain') r,
        ];
        expect(drainLog, hasLength(2));
        expect(
          drainLog.first.message,
          contains('reported failure'),
          reason: 'the wedge failure is logged before the fallback runs',
        );
        expect(drainLog.first.error, isA<InjectedFailure>());
        expect(drainLog.first.stackTrace, isNotNull);
        expect(drainLog.last.message, contains('recorded alone'));
      }

      // Verifies: EVS-PRD-destinations/P
      // a failure after the wedge's writes
      //   rolls back the wedged status, the event and the record together.
      // Verifies: EVS-DEV-destination-drain/J
      // the attempt then commits alone and
      //   the pass ends.
      test('a failure after the wedge event rolls the wedge back', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add, afterWedgeHeadInTxn: (id) => true),
          () => drain(d, registry: w.registry),
        );
        await expectFallbackEndState(head, log);
      });

      // Verifies: EVS-PRD-destinations/P
      // a failure of the wedge event's
      //   append rolls back the wedged status written before it.
      // Verifies: EVS-DEV-destination-drain/J
      // the attempt then commits alone.
      test('a failed wedge event append rolls back the status write', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        final log = <LibraryLogRecord>[];
        final consulted = <String>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onLog: log.add,
            failRegistryAuditAppend: (entryType) {
              consulted.add(entryType);
              return entryType == kDestinationWedgedEntryType;
            },
            afterWedgeHeadInTxn: (id) {
              consulted.add('afterWedgeHeadInTxn');
              return false;
            },
          ),
          () => drain(d, registry: w.registry),
        );
        expect(consulted, <String>[kDestinationWedgedEntryType]);
        await expectFallbackEndState(head, log);
      });

      // Verifies: EVS-PRD-destinations/P
      // on success the wedged status, the
      //   attempt, the event and the wedge record are all present.
      test('success commits the status, attempt, event and record', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        final consulted = <String>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            afterWedgeHeadInTxn: (id) {
              consulted.add(id);
              return false;
            },
          ),
          () => drain(d, registry: w.registry),
        );
        expect(consulted, <String>['x']);
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(1));
        final event = (await w.wedgeEvents()).single;
        expect(
          await w.wedgeRecord('x'),
          WedgeRecord(
            rowId: head.entryId,
            wedgeEventId: event.eventId,
            cause: WedgeCause.permanentRefusal,
          ),
        );
        await expectChainIntact(w.backend);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/C
      // a wedge transaction that committed
      //   but reported failure is not followed by a second attempt: the
      //   fallback reads the head, finds it wedged under the wedge record,
      //   and records nothing.
      // Verifies: EVS-PRD-destinations/P
      // the committed wedge keeps its one
      //   event, and the log line names the reported failure.
      test('a wedge that committed but reported failure records nothing '
          'more', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onLog: log.add,
            afterWedgeTransaction: (id) => true,
          ),
          () => drain(d, registry: w.registry),
        );
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(1), reason: 'no second attempt');
        expect(await w.wedgeEvents(), hasLength(1));
        await expectWedgeRecordMatchesLog(w.store, 'x');
        await expectChainIntact(w.backend);
        final drainLog = <LibraryLogRecord>[
          for (final r in log)
            if (r.name == 'event_sourcing.drain') r,
        ];
        expect(drainLog, hasLength(2));
        expect(drainLog.first.message, contains('reported failure'));
        expect(drainLog.first.error, isA<InjectedFailure>());
        expect(drainLog.last.message, contains('committed before'));
        await drain(d, registry: w.registry);
        expect(d.sent, hasLength(1), reason: 'the wedged head is not resent');
      });

      // Verifies: EVS-DEV-destination-drain/C
      // when the fallback's own transaction
      //   fails too, the wedge's failure is already logged and the
      //   fallback's failure escapes, leaving no attempt.
      test('a failing fallback leaves the wedge failure logged', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        final log = <LibraryLogRecord>[];
        Object? escaped;
        try {
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              onLog: log.add,
              afterWedgeHeadInTxn: (id) => true,
              failOutcomeTransaction: (id, outcome) => true,
            ),
            () => drain(d, registry: w.registry),
          );
        } on Object catch (e) {
          escaped = e;
        }
        expect(
          escaped,
          isA<InjectedFailure>().having(
            (f) => f.point,
            'point',
            'drain outcome transaction of x',
          ),
        );
        final drainLog = <LibraryLogRecord>[
          for (final r in log)
            if (r.name == 'event_sourcing.drain') r,
        ];
        expect(drainLog, hasLength(1));
        expect(drainLog.single.message, contains('reported failure'));
        expect(
          drainLog.single.error,
          isA<InjectedFailure>().having(
            (f) => f.point,
            'point',
            'after the wedge of x',
          ),
        );
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, isNull);
        expect(row.attempts, isEmpty);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });
    });

    // ------------------------------------------------------------------
    // Status derivation before any send
    // ------------------------------------------------------------------

    group('status derivation', () {
      // Verifies: EVS-DEV-destination-drain/J
      // a budget lowered below the recorded
      //   attempt count wedges the head at the next pass, without a send,
      //   recording the attempts and the budget in effect.
      // Verifies: EVS-DEV-destination-drain/I
      // max_attempts is the budget in effect
      //   at the wedge.
      test('a lowered budget wedges without a send', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            for (var i = 0; i < 3; i++) const SendTransient(error: 'busy'),
          ],
        );
        final head = await queued(d);
        for (var i = 0; i < 3; i++) {
          await drain(d, registry: w.registry, policy: budget(5));
        }
        expect(d.sent, hasLength(3));
        expect(await w.wedgeEvents(), isEmpty);
        await drain(d, registry: w.registry, policy: budget(2));
        expect(d.sent, hasLength(3), reason: 'no send at the lowered budget');
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(3), reason: 'no attempt is added');
        final event = (await w.wedgeEvents()).single;
        expect(event.data['cause'], 'retry_budget_exhausted');
        expect(event.data['attempt_count'], 3);
        expect(event.data['max_attempts'], 2);
        expect(event.data['last_outcome'], 'transient');
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // a recorded permanent refusal on a
      //   pending head (the fallback's end state) wedges at the next pass
      //   with no send.
      test('a recorded refusal wedges without a send', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drain(d, registry: w.registry),
        );
        expect((await w.backend.readFifoHead('x'))!.finalStatus, isNull);
        await drain(d, registry: w.registry);
        expect(d.sent, hasLength(1));
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(1));
        final event = (await w.wedgeEvents()).single;
        expect(event.data['cause'], 'permanent_refusal');
        expect(event.data['attempt_count'], 1);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // an attempt that spends the budget
      //   whose wedge fails is recorded alone; the next pass wedges with no
      //   send.
      test('a spent budget recorded alone wedges without a send', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            const SendTransient(error: 'busy'),
            const SendTransient(error: 'busy'),
          ],
        );
        await queued(d);
        await drain(d, registry: w.registry, policy: budget(2));
        await expectWedgeRecordMatchesLog(w.store, 'x');
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drain(d, registry: w.registry, policy: budget(2)),
        );
        await expectWedgeRecordMatchesLog(w.store, 'x');
        final pending = (await w.backend.readFifoHead('x'))!;
        expect(pending.finalStatus, isNull);
        expect(pending.attempts, hasLength(2));
        await drain(d, registry: w.registry, policy: budget(2));
        expect(d.sent, hasLength(2));
        final event = (await w.wedgeEvents()).single;
        expect(event.data['cause'], 'retry_budget_exhausted');
        expect(event.data['attempt_count'], 2);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // a failing derivation transaction
      //   ends the pass after one try: no send, no event, a log line.
      test('a failing derivation ends the pass after one try', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drain(d, registry: w.registry),
        );
        var tries = 0;
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onLog: log.add,
            afterWedgeHeadInTxn: (id) {
              tries += 1;
              return true;
            },
          ),
          () => drain(d, registry: w.registry),
        );
        expect(tries, 1);
        expect(d.sent, hasLength(1));
        expect(await w.wedgeEvents(), isEmpty);
        expect((await w.backend.readFifoHead('x'))!.finalStatus, isNull);
        expect(
          log.where((r) => r.message.contains('recorded attempts')),
          hasLength(1),
        );
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // a derived wedge that committed but
      //   reported failure ends the pass; the head stays wedged with one
      //   event and is not sent again.
      test('a derived wedge that reported failure after committing stays '
          'wedged', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drain(d, registry: w.registry),
        );
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onLog: log.add,
            afterWedgeTransaction: (id) => true,
          ),
          () => drain(d, registry: w.registry),
        );
        expect(
          log.where((r) => r.message.contains('recorded attempts')),
          hasLength(1),
        );
        expect(
          (await w.backend.readFifoHead('x'))!.finalStatus,
          FinalStatus.wedged,
        );
        await drain(d, registry: w.registry);
        expect(d.sent, hasLength(1));
        expect(await w.wedgeEvents(), hasLength(1));
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // while the wedge keeps failing, no
      //   later cycle sends the head again, other destinations still drain,
      //   and once the failure stops the next cycle wedges with one event.
      // Verifies: EVS-PRD-destinations/G
      // a failing wedge on one destination
      //   leaves delivery on another running.
      test('a wedge that keeps failing never sends again', () async {
        if (!available) return;
        final x = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final y = FakeDestination(
          id: 'y',
          script: <SendResult>[for (var i = 0; i < 4; i++) const SendOk()],
        );
        await w.activate(x);
        await w.activate(y);
        final cycle = SyncCycle(
          registry: w.registry,
          clock: _fillNow,
          policy: budget(20),
        );
        final hooks = DeliveryTestHooks(afterWedgeHeadInTxn: (id) => id == 'x');
        await w.note('n0');
        await runWithDeliveryTestHooks(hooks, cycle.call);
        await expectWedgeRecordMatchesLog(w.store, 'x');
        expect(x.sent, hasLength(1));
        final pending = (await w.backend.readFifoHead('x'))!;
        expect(pending.finalStatus, isNull);
        expect(pending.attempts.single.outcome, 'permanent');
        for (var i = 1; i <= 3; i++) {
          await w.note('n$i');
          await runWithDeliveryTestHooks(hooks, cycle.call);
          await expectWedgeRecordMatchesLog(w.store, 'x');
          await expectWedgeRecordMatchesLog(w.store, 'y');
        }
        expect(x.sent, hasLength(1), reason: 'the head is never sent again');
        expect(y.sent, hasLength(4), reason: 'y delivers every note');
        expect(await w.wedgeEvents(), isEmpty);
        await cycle.call();
        expect(x.sent, hasLength(1));
        expect(await w.wedgeEvents(destinationId: 'x'), hasLength(1));
        expect(
          (await w.backend.readFifoHead('x'))!.finalStatus,
          FinalStatus.wedged,
        );
        await expectWedgeRecordMatchesLog(w.store, 'x');
        await expectWedgeRecordMatchesLog(w.store, 'y');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // an attempt recorded alone before a
      //   restart is wedged by a cycle over a new store on the same database
      //   before any send, with exactly one wedge event.
      test('a recorded refusal is wedged after a restart', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => SyncCycle(registry: w.registry, clock: _fillNow).call(),
        );
        expect(d.sent, hasLength(1));
        await expectWedgeRecordMatchesLog(w.store, 'x');
        // The first process stops: its store and backend close, unless
        // every process shares one database handle (closing it would close
        // the database the second process opens).
        if (!processesShareDatabaseHandle) await w.store.close();
        w.a = await w.openProcess();
        final restarted = FakeDestination(id: 'x');
        await w.registry.addDestination(restarted, initiator: _init);
        await SyncCycle(registry: w.registry, clock: _fillNow).call();
        expect(restarted.sent, isEmpty, reason: 'no send after the restart');
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, hasLength(1));
        final events = await w.wedgeEvents();
        expect(events, hasLength(1));
        expect(events.single.data['cause'], 'permanent_refusal');
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/J
      // the budget a policy resolver returns
      //   is the budget in effect: lowered below the recorded attempt count,
      //   the next cycle wedges the head without a send.
      // Verifies: EVS-DEV-destination-drain/I
      // max_attempts records the resolved
      //   budget.
      test(
        'a budget the policy resolver lowers wedges at the next cycle',
        () async {
          if (!available) return;
          final d = FakeDestination(
            id: 'x',
            script: <SendResult>[
              for (var i = 0; i < 3; i++) const SendTransient(error: 'busy'),
            ],
          );
          final head = await queued(d);
          var current = budget(5);
          final cycle = SyncCycle(
            registry: w.registry,
            clock: _fillNow,
            policyResolver: () => current,
          );
          for (var i = 0; i < 3; i++) {
            await cycle();
            await expectWedgeRecordMatchesLog(w.store, 'x');
          }
          expect(d.sent, hasLength(3));
          expect(await w.wedgeEvents(), isEmpty);
          current = budget(2);
          await cycle();
          expect(d.sent, hasLength(3), reason: 'no send at the lowered budget');
          final row = (await w.backend.readFifoRow('x', head.entryId))!;
          expect(row.finalStatus, FinalStatus.wedged);
          expect(row.attempts, hasLength(3));
          final event = (await w.wedgeEvents()).single;
          expect(event.data['cause'], 'retry_budget_exhausted');
          expect(event.data['attempt_count'], 3);
          expect(event.data['max_attempts'], 2);
          await expectWedgeRecordMatchesLog(w.store, 'x');
        },
      );

      // Verifies: EVS-DEV-destination-drain/I
      // max_attempts is the static policy's
      //   budget, and the default budget (20) when the cycle has no policy
      //   or its resolver returns none.
      test(
        'the budget a cycle records: static, default, resolved to none',
        () async {
          if (!available) return;
          Future<int?> wedgeBudget(String id, SyncCycle Function() make) async {
            final d = FakeDestination(
              id: id,
              script: <SendResult>[const SendPermanent(error: 'no')],
            );
            await queued(d);
            await make().call();
            await expectWedgeRecordMatchesLog(w.store, id);
            return (await w.wedgeEvents(
                  destinationId: id,
                )).single.data['max_attempts']
                as int?;
          }

          expect(
            await wedgeBudget(
              'static',
              () => SyncCycle(
                registry: w.registry,
                clock: _fillNow,
                policy: budget(4),
              ),
            ),
            4,
          );
          expect(
            await wedgeBudget(
              'none',
              () => SyncCycle(registry: w.registry, clock: _fillNow),
            ),
            SyncPolicy.defaults.maxAttempts,
          );
          expect(
            await wedgeBudget(
              'resolved-none',
              () => SyncCycle(
                registry: w.registry,
                clock: _fillNow,
                policyResolver: () => null,
              ),
            ),
            20,
          );
        },
      );

      // Verifies: EVS-DEV-destination-drain/J
      // a static policy with a budget below
      //   one is refused when the cycle is built.
      test('a static budget below one is refused', () async {
        if (!available) return;
        for (final n in <int>[0, -1]) {
          expect(
            () => SyncCycle(registry: w.registry, policy: UncheckedPolicy(n)),
            throwsA(isA<ArgumentError>()),
          );
        }
      });

      // Verifies: EVS-DEV-destination-drain/J
      // a resolved budget below one is
      //   refused: the cycle logs the refusal, sends nothing and writes
      //   nothing, where it would otherwise wedge every pending head
      //   before its first send.
      test('a resolved budget below one sends and writes nothing', () async {
        if (!available) return;
        for (final n in <int>[0, -1]) {
          final id = 'budget$n';
          final d = FakeDestination(
            id: id,
            script: <SendResult>[const SendOk()],
          );
          await queued(d);
          await w.note('$id-late');
          final before = await w.snapshot(id);
          final log = <LibraryLogRecord>[];
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(onLog: log.add),
            SyncCycle(
              registry: w.registry,
              clock: _fillNow,
              policyResolver: () => UncheckedPolicy(n),
            ).call,
          );
          expect(d.sent, isEmpty);
          expect(await w.snapshot(id), before, reason: 'nothing written');
          expect(
            log.where(
              (r) =>
                  r.level == LibraryLogLevel.severe &&
                  r.message.contains('retry budget of $n'),
            ),
            hasLength(1),
          );
          await expectWedgeRecordMatchesLog(w.store, id);
        }
      });

      // Verifies: EVS-DEV-destination-drain/J
      // the drain itself refuses a budget
      //   below one before anything is read or sent.
      test('the drain refuses a budget below one', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: <SendResult>[]);
        await queued(d);
        final before = await w.snapshot('x');
        await expectLater(
          drain(d, registry: w.registry, policy: UncheckedPolicy(0)),
          throwsA(isA<ArgumentError>()),
        );
        expect(d.sent, isEmpty);
        expect(await w.snapshot('x'), before);
      });
    });

    // ------------------------------------------------------------------
    // No attempt text
    // ------------------------------------------------------------------

    group('no attempt text', () {
      // Verifies: EVS-PRD-destinations/R
      // an error that echoes the payload is
      //   recorded on the item's attempts and appears nowhere in the event.
      test('a refusal that echoes the payload', () async {
        if (!available) return;
        const echoed = 'refused payload {"secret":"s3cr3t-value"}';
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: echoed)],
        );
        final head = await queued(d);
        await drain(d, registry: w.registry);
        final event = (await w.wedgeEvents()).single;
        final encoded = jsonEncode(event.toMap());
        expect(encoded, isNot(contains('s3cr3t-value')));
        expect(encoded, isNot(contains('refused payload')));
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.attempts.single.errorMessage, echoed);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-PRD-destinations/R
      // a throwing send's message and stack
      //   trace are recorded on the item's attempts and appear nowhere in
      //   the event.
      test('a throwing send', () async {
        if (!available) return;
        final d = _ThrowingDestination('x');
        await w.activate(d);
        await w.note('n0');
        await w.fillAll(d);
        final head = (await w.backend.readFifoHead('x'))!;
        await drain(d, registry: w.registry, policy: budget(1));
        final event = (await w.wedgeEvents()).single;
        expect(event.data['cause'], 'retry_budget_exhausted');
        final encoded = jsonEncode(event.toMap());
        expect(encoded, isNot(contains('boom-secret')));
        expect(encoded, isNot(contains('_ThrowingDestination')));
        expect(encoded, isNot(contains('#0')));
        final attempt = (await w.backend.readFifoRow(
          'x',
          head.entryId,
        ))!.attempts.single;
        expect(attempt.errorMessage, contains('boom-secret'));
        expect(attempt.errorMessage, contains('_ThrowingDestination'));
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });
    });

    // ------------------------------------------------------------------
    // Routing
    // ------------------------------------------------------------------

    // Verifies: EVS-PRD-destinations/B
    // the wedge event is a reserved system
    //   event: a destination that does not opt into system events never
    //   receives it, and one that opts in does.
    test('routing: only a destination opted into system events receives '
        'the wedge event', () async {
      if (!available) return;
      final x = FakeDestination(
        id: 'x',
        script: <SendResult>[const SendPermanent(error: 'no')],
      );
      final withSystem = FakeDestination(
        id: 'audit',
        filter: const SubscriptionFilter(includeSystemEvents: true),
      );
      final withoutSystem = FakeDestination(id: 'plain');
      for (final d in <Destination>[x, withSystem, withoutSystem]) {
        await w.activate(d);
      }
      await w.note('n0');
      await w.fillAll(x);
      await drain(x, registry: w.registry);
      final wedge = (await w.wedgeEvents()).single;
      await w.fillAll(withSystem);
      await w.fillAll(withoutSystem);
      Future<Set<String>> carried(String destId) async => <String>{
        for (final r in await w.backend.listFifoEntries(destId)) ...r.eventIds,
      };
      expect(await carried('audit'), contains(wedge.eventId));
      expect(await carried('plain'), isNot(contains(wedge.eventId)));
      expect(await carried('plain'), isNotEmpty);
      for (final id in <String>['x', 'audit', 'plain']) {
        await expectWedgeRecordMatchesLog(w.store, id);
      }
    });

    // ------------------------------------------------------------------
    // Guards
    // ------------------------------------------------------------------

    group('guards', () {
      // Verifies: EVS-PRD-destinations/P
      // a delivery and a transient failure
      //   below the budget append no wedge event and write no record.
      // Verifies: EVS-DEV-destination-drain/D
      // only a wedge appends a wedge event.
      test('SendOk and a transient below the budget append none', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            const SendTransient(error: 'busy'),
            const SendOk(),
          ],
        );
        await queued(d);
        await drain(d, registry: w.registry, policy: budget(3));
        expect((await w.backend.readFifoHead('x'))!.attempts, hasLength(1));
        await drain(d, registry: w.registry, policy: budget(3));
        expect(await w.backend.readFifoHead('x'), isNull);
        expect(await w.wedgeEvents(), isEmpty);
        expect(await w.wedgeRecord('x'), isNull);
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/D
      // a wedged head is never sent again
      //   and gains no second wedge event.
      test('a wedged head is never sent again', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            const SendPermanent(error: 'no'),
            const SendOk(),
          ],
        );
        await queued(d, notes: 2);
        await drain(d, registry: w.registry);
        await expectWedgeRecordMatchesLog(w.store, 'x');
        await drain(d, registry: w.registry);
        await expectWedgeRecordMatchesLog(w.store, 'x');
        await SyncCycle(registry: w.registry, clock: _fillNow).call();
        expect(d.sent, hasLength(1));
        expect(await w.wedgeEvents(), hasLength(1));
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/D
      // wedgeHeadInTxn refuses a wedged
      //   head, an item that is not the head and a queue with no head, and
      //   writes nothing.
      test('wedgeHeadInTxn refuses anything but the pending head', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d, notes: 2);
        final second = (await w.backend.listFifoEntries('x'))[1];
        Future<void> refused(
          String destId,
          String rowId, {
          WedgeCause cause = WedgeCause.permanentRefusal,
          int maxAttempts = 3,
          Matcher error = const TypeMatcher<StateError>(),
        }) async {
          final before = await w.snapshot(destId);
          await expectLater(
            w.store.runTransaction(
              (txn, collector) => w.registry.wedgeHeadInTxn(
                txn,
                collector,
                destinationId: destId,
                rowId: rowId,
                cause: cause,
                maxAttempts: maxAttempts,
              ),
            ),
            throwsA(error),
          );
          expect(await w.snapshot(destId), before);
        }

        await refused('x', second.entryId);
        await refused('x', 'no-such-row');
        await refused('empty', 'no-such-row');
        // The pending head's recorded attempts do not support the cause:
        // no attempt recorded yet.
        await refused(
          'x',
          head.entryId,
          error: isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('the last recorded attempt reported nothing'),
          ),
        );
        await refused(
          'x',
          head.entryId,
          cause: WedgeCause.retryBudgetExhausted,
          error: isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('below the budget'),
          ),
        );
        await refused(
          'x',
          head.entryId,
          cause: WedgeCause.retryBudgetExhausted,
          maxAttempts: 0,
          error: isA<ArgumentError>(),
        );
        await drain(d, registry: w.registry);
        await expectWedgeRecordMatchesLog(w.store, 'x');
        // The wedged head is refused by the pending check itself, before
        // the status write would refuse a repeated status.
        await refused(
          'x',
          head.entryId,
          error: isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('not pending'),
          ),
        );
        expect(await w.wedgeEvents(), hasLength(1));
        await expectWedgeRecordMatchesLog(w.store, 'x');
      });

      // Verifies: EVS-DEV-destination-drain/I
      // a wedge record beside a pending head
      //   names no open wedge the log records: wedgeHeadInTxn refuses to
      //   overwrite it and writes nothing.
      test(
        'wedgeHeadInTxn refuses a pending head with a wedge record',
        () async {
          if (!available) return;
          final d = FakeDestination(
            id: 'x',
            script: <SendResult>[const SendPermanent(error: 'no')],
          );
          final head = await queued(d);
          // A recorded refusal on the pending head, as the fallback leaves.
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
            () => drain(d, registry: w.registry),
          );
          const stale = WedgeRecord(
            rowId: 'stale-row',
            wedgeEventId: 'stale-event',
            cause: WedgeCause.permanentRefusal,
          );
          await w.backend.transaction(
            (txn) => w.backend.writeWedgeRecordTxn(txn, 'x', stale),
          );
          final before = await w.snapshot('x');
          await expectLater(
            w.store.runTransaction(
              (txn, collector) => w.registry.wedgeHeadInTxn(
                txn,
                collector,
                destinationId: 'x',
                rowId: head.entryId,
                cause: WedgeCause.permanentRefusal,
                maxAttempts: 3,
              ),
            ),
            throwsA(
              isA<StateError>().having(
                (e) => e.message,
                'message',
                contains('wedge record names item stale-row'),
              ),
            ),
          );
          expect(await w.snapshot('x'), before);
          expect(await w.wedgeRecord('x'), stale);
        },
      );

      // Verifies: EVS-DEV-destination-drain/M
      // an event store opened over a registry that lacks the wedge entry
      //   type registers it, so a registry over the store can wedge a head.
      test(
        'a store opened without the wedge entry type registers it',
        () async {
          if (!available) return;
          final entryTypes = EntryTypeRegistry();
          for (final d in kSystemEntryTypes) {
            if (d.id != kDestinationWedgedEntryType) entryTypes.register(d);
          }
          final backend = await w.db.openBackend();
          final store = await EventStore.openForTest(
            storage: backend,
            entryTypes: entryTypes,
            source: _source,
            securityContexts: w.db.securityFor(backend),
          );
          expect(
            identical(
              store.entryTypes.byId(kDestinationWedgedEntryType),
              kSystemEntryTypes.firstWhere(
                (d) => d.id == kDestinationWedgedEntryType,
              ),
            ),
            isTrue,
          );
          final registry = DestinationRegistry(eventStore: store);
          final d = FakeDestination(id: 'x');
          await w.activate(d, on: registry);
          await w.note('x-n0');
          await w.fillAll(d, on: backend);
          await wedgeHeadForTest(registry, 'x');
          expect(await w.wedgeEvents(destinationId: 'x'), hasLength(1));
          await expectWedgeRecordMatchesLog(store, 'x');
        },
      );
    });

    // ------------------------------------------------------------------
    // Two destinations in one cycle
    // ------------------------------------------------------------------

    // Verifies: EVS-PRD-destinations/G
    // two destinations wedging in the same
    //   cycle (their transactions run concurrently) each get their wedge
    //   event, and the chain stays intact.
    // Verifies: EVS-PRD-destinations/P
    // each wedge commits with its event; a
    //   live listener sees exactly one wedge event per destination.
    // Verifies: EVS-PRD-event-log/G
    // on a backend that re-runs the loser of
    //   a conflict, the two wedge transactions overlap and one is run again;
    //   only the committed run's event is recorded and published.
    test('two destinations wedging in one cycle', () async {
      if (!available) return;
      // Both sends return together, so the two wedge transactions start
      // together and overlap.
      var arrived = 0;
      final bothSent = Completer<void>();
      Future<void> barrier() {
        arrived += 1;
        if (arrived == 2) bothSent.complete();
        return bothSent.future;
      }

      final x = FakeDestination(
        id: 'x',
        blockBeforeSend: barrier,
        script: <SendResult>[const SendPermanent(error: 'no')],
      );
      final y = FakeDestination(
        id: 'y',
        blockBeforeSend: barrier,
        script: <SendResult>[const SendPermanent(error: 'no')],
      );
      final wedgeRuns = <String>[];
      await w.activate(x);
      await w.activate(y);
      await w.note('n0');
      final seen = <StoredEvent>[];
      final sub = w.store
          .subscribe<StoredEvent>(
            const SubscriptionFilter(
              entryTypes: <String>{},
              includeSystemEvents: true,
              eventTypes: <String>{kDestinationWedgedEventType},
            ),
            const Events(),
          )
          .listen((u) {
            if (u is Delta<StoredEvent>) seen.add(u.value);
          });
      try {
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onRegistryBodyRun: (op) {
              if (op == 'wedgeHeadInTxn') wedgeRuns.add(op);
            },
          ),
          SyncCycle(registry: w.registry, clock: _fillNow).call,
        );
        await pumpEventQueue();
      } finally {
        await sub.cancel();
      }
      if (retriesConflictingTransactions) {
        expect(
          wedgeRuns.length,
          greaterThan(2),
          reason:
              'the overlapping wedge transactions conflict and one runs '
              'again',
        );
      } else {
        expect(wedgeRuns, hasLength(2), reason: 'one run per wedge');
      }
      final events = await w.wedgeEvents();
      expect(events.map((e) => e.data['id']).toSet(), <String>{'x', 'y'});
      expect(events, hasLength(2));
      expect(seen.map((e) => e.data['id']).toList()..sort(), <String>[
        'x',
        'y',
      ]);
      expect(
        seen.map((e) => e.eventId).toSet(),
        events.map((e) => e.eventId).toSet(),
      );
      await expectChainIntact(w.backend);
      await expectWedgeRecordMatchesLog(w.store, 'x');
      await expectWedgeRecordMatchesLog(w.store, 'y');
    });

    // ------------------------------------------------------------------
    // Recovery and deletion end the wedge record
    // ------------------------------------------------------------------

    // Verifies: EVS-DEV-destination-drain/F
    // the recovery removes the wedge record
    //   in its transaction.
    // Verifies: EVS-DEV-destination-drain/A
    // the deletion removes the wedge record
    //   in its transaction.
    test('recovery and deletion remove the wedge record', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'x',
        allowHardDelete: true,
        script: <SendResult>[
          const SendPermanent(error: 'no'),
          const SendPermanent(error: 'no'),
        ],
      );
      final head = await queued(d);
      await drain(d, registry: w.registry);
      expect(await w.wedgeRecord('x'), isNotNull);
      await expectWedgeRecordMatchesLog(w.store, 'x');
      await w.registry.tombstoneAndRefill('x', head.entryId, initiator: _init);
      expect(await w.wedgeRecord('x'), isNull);
      await expectWedgeRecordMatchesLog(w.store, 'x');
      await w.fillAll(d);
      await drain(d, registry: w.registry);
      expect(await w.wedgeRecord('x'), isNotNull);
      await expectWedgeRecordMatchesLog(w.store, 'x');
      await w.registry.deleteDestination('x', initiator: _init);
      expect(await w.wedgeRecord('x'), isNull);
      await expectWedgeRecordMatchesLog(w.store, 'x');
    });
  });
}

/// A destination whose send throws.
class _ThrowingDestination extends FakeDestination {
  _ThrowingDestination(String id) : super(id: id);

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    throw StateError('boom-secret from _ThrowingDestination');
  }
}

/// A retry policy that skips the constructor's check of its budget, so a
/// test can hand the delivery cycle a budget below one as a build without
/// assertions would.
class UncheckedPolicy implements SyncPolicy {
  UncheckedPolicy(this.maxAttempts);

  @override
  final int maxAttempts;

  @override
  Duration get initialBackoff => Duration.zero;

  @override
  double get backoffMultiplier => 1.0;

  @override
  Duration get maxBackoff => Duration.zero;

  @override
  double get jitterFraction => 0.0;

  @override
  Duration backoffFor(int attemptCount, {Random? random}) => Duration.zero;
}
