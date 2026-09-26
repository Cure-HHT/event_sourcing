// Backend-agnostic scenarios for the operator halt: requesting and
// cancelling a halt, the drainer honouring it by wedging the queue head,
// the pre-send fence, every wedge consuming an open request, deletion
// closing one, the destinations a delivery cycle does not serve, the
// agreement between the stored request and the log, the log invariant the
// library maintains, rollback of every transaction that opens, closes or
// honours a request, and the ingest refusals of the halt events. Sembast
// runs them from test/sync/operator_halt_test.dart and Postgres from
// test/storage/postgres/postgres_operator_halt_test.dart.
//
// This file exposes [runOperatorHaltScenarios], the agreement helper
// [expectHaltRequestMatchesLog] and the log-invariant helper
// [expectHaltLogInvariant], and registers no `main()` of its own.
// Traceability lives on the individual tests.
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show wedgeHeadInTxnForTest;
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'destination_wedges_view_conformance.dart' show forgedEvent;
import 'drain_wedge_conformance.dart'
    show budget, declaredWedgeEventKeys, expectWedgeRecordMatchesLog;
import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart';
import 'test_backends.dart';
import 'wedges_view_invariant.dart';

const Initiator _init = AutomationInitiator(service: 'halt-scenarios');
const Initiator _operator = UserInitiator('operator-1');
const String _noteType = 'halt_note';
const Source _source = Source(
  hopId: 'server',
  identifier: 'halt-install',
  softwareVersion: 'test@1.0.0',
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// The halt request the log leaves open for each destination of [store]'s
/// own database, by destination: the latest `destination_halt_requested`
/// that no cancellation, wedge or deletion naming it followed.
Future<Map<String, StoredEvent>> _openRequestsInLog(EventStore store) async {
  final open = <String, StoredEvent>{};
  for (final e in await store.reader.findAllEvents()) {
    if (e.aggregateType != kDestinationAuditAggregateType) continue;
    if (e.data['database_id'] != store.databaseId) continue;
    final id = e.data['id']! as String;
    switch (e.eventType) {
      case kDestinationHaltRequestedEventType:
        open[id] = e;
      case kDestinationHaltCancelledEventType:
        if (open[id]?.eventId == e.data['halt_request_event_id']) {
          open.remove(id);
        }
      case kDestinationWedgedEventType:
        if (open[id]?.eventId == e.data['halt_request_event_id']) {
          open.remove(id);
        }
      case kDestinationDeletedEventType:
        if (open[id]?.eventId == e.data['closed_halt_request_event_id']) {
          open.remove(id);
        }
    }
  }
  return open;
}

/// Asserts the stored halt request of every destination in [destinationIds]
/// equals the open request derived from [store]'s log: absent when the log
/// leaves none open, otherwise naming the request event, its purpose, its
/// requester and its time.
Future<void> expectHaltRequestMatchesLog(
  EventStore store,
  Iterable<String> destinationIds,
) async {
  final backend = testBackendOf(store);
  final open = await _openRequestsInLog(store);
  for (final id in destinationIds) {
    final stored = await backend.transaction(
      (txn) => backend.readHaltRequestTxn(txn, id),
    );
    final event = open[id];
    if (event == null) {
      expect(stored, isNull, reason: 'no open halt request for $id');
      continue;
    }
    expect(
      stored,
      HaltRequest(
        requestEventId: event.eventId,
        requestedAt: event.clientTimestamp,
        purpose: HaltPurpose.fromWire(event.data['purpose']! as String),
        requestedBy: event.initiator.toJson(),
      ),
      reason: 'the stored request names the open request of $id',
    );
  }
}

/// Asserts the halt-request relation over [store]'s log, restricted to
/// events naming [store]'s own database: every event that closes a request
/// names one that is open for the same destination, and a request is
/// recorded only while none is open for that destination.
Future<void> expectHaltLogInvariant(EventStore store) async {
  final open = <String, String>{};
  final closed = <String>{};
  for (final e in await store.reader.findAllEvents()) {
    if (e.aggregateType != kDestinationAuditAggregateType) continue;
    if (e.data['database_id'] != store.databaseId) continue;
    final id = e.data['id']! as String;
    String? closes;
    switch (e.eventType) {
      case kDestinationHaltRequestedEventType:
        expect(
          open[id],
          isNull,
          reason: 'request ${e.eventId} recorded while one is open for $id',
        );
        open[id] = e.eventId;
        continue;
      case kDestinationHaltCancelledEventType:
        closes = e.data['halt_request_event_id'] as String?;
        expect(closes, isNotNull, reason: 'cancellation ${e.eventId}');
      case kDestinationWedgedEventType:
        closes = e.data['halt_request_event_id'] as String?;
      case kDestinationDeletedEventType:
        closes = e.data['closed_halt_request_event_id'] as String?;
    }
    if (closes == null) continue;
    expect(
      closed,
      isNot(contains(closes)),
      reason: '${e.eventId} closes request $closes a second time',
    );
    expect(
      open[id],
      closes,
      reason: '${e.eventId} closes $closes, which is not the open request',
    );
    closed.add(closes);
    open.remove(id);
  }
}

class _Process {
  _Process(this.backend, this.store, this.registry) {
    trackTestBackend(store, backend);
  }
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
    final entryTypes = EntryTypeRegistry()
      ..register(
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
      await fillForTest(d, backend: b, source: _source, clock: _fillNow);
      final after = (await b.listFifoEntries(d.id)).length;
      if (after == before && await b.readFillCursor(d.id) == cursorBefore) {
        return;
      }
    }
  }

  Future<List<StoredEvent>> events(String entryType) =>
      backend.findAllEvents(entryType: entryType);

  /// The drain epoch the database stores.
  Future<int?> drainEpoch() =>
      backend.transaction((txn) => backend.readDrainEpochTxn(txn));

  Future<List<StoredEvent>> wedgeEvents() =>
      events(kDestinationWedgedEntryType);

  Future<HaltRequest?> halt(String destId) =>
      backend.transaction((txn) => backend.readHaltRequestTxn(txn, destId));

  Future<SendFence?> fence(String destId) =>
      backend.transaction((txn) => backend.readSendFenceTxn(txn, destId));

  Future<WedgeRecord?> wedgeRecord(String destId) =>
      backend.transaction((txn) => backend.readWedgeRecordTxn(txn, destId));

  Future<RegistryCheck?> check() =>
      backend.transaction(backend.readRegistryCheckTxn);

  /// Everything an operation could change about [destId], and the log,
  /// except the registry check record.
  Future<Map<String, Object?>> snapshot(String destId) async => {
    'rows': <Object?>[
      for (final r in await backend.listFifoEntries(destId)) r.toJson(),
    ],
    'schedule': (await backend.readSchedule(destId))?.toJson(),
    'cursor': await backend.readFillCursor(destId),
    'halt': (await halt(destId))?.toJson(),
    'fence': (await fence(destId))?.toJson(),
    'wedge_record': (await wedgeRecord(destId))?.toJson(),
    'events': <String>[
      for (final e in await backend.findAllEvents()) e.eventId,
    ],
    'refill_guard': (await backend.transaction(
      (txn) => backend.readRefillGuardTxn(txn, destId),
    ))?.toJson(),
    // The drain lock's records change with every acquisition the harness
    // or a delivery cycle makes, not with a registry operation.
    'state_keys': <String>[
      for (final k in await db.backendStateKeys())
        if (k != 'registry_check' && !drainLockRecordKeys.contains(k)) k,
    ]..sort(),
  };

  /// The agreement checks every scenario runs after each operation: the
  /// stored halt requests and wedge records of [destIds] against the log,
  /// the view against the queues, and the log invariant.
  Future<void> agree(Iterable<String> destIds) async {
    await expectHaltRequestMatchesLog(store, destIds);
    for (final id in destIds) {
      await expectWedgeRecordMatchesLog(store, id);
    }
    await expectHaltLogInvariant(store);
  }
}

/// The `backend_state` records a stored event, or a stored security
/// finding, advances, left out of a snapshot compared beside the findings.
const Set<String> _advancedByEveryEvent = <String>{
  'sequence_counter',
  'latest_authored_sequence',
  'security_finding_held',
};

/// Run every operator-halt scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).

void runOperatorHaltScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('operator halt scenarios ($label)', () {
    late _World w;
    var available = false;
    final cycles = <TestCycle>[];

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
      cycles.clear();
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

    Future<String> requestHalt(
      String destId, {
      HaltPurpose purpose = HaltPurpose.pause,
      DestinationRegistry? on,
    }) => (on ?? w.registry).requestHalt(
      destId,
      initiator: _operator,
      purpose: purpose,
    );

    TestCycle cycleOver(DestinationRegistry registry, {SyncPolicy? policy}) {
      final cycle = TestCycle(registry, clock: _fillNow, policy: policy);
      cycles.add(cycle);
      return cycle;
    }

    // ------------------------------------------------------------------
    // Honouring a halt
    // ------------------------------------------------------------------

    group('honour', () {
      // Verifies: EVS-PRD-destinations/U
      // a halt requested on a healthy destination is honoured by wedging
      //   the queue head before any send.
      // Verifies: EVS-DEV-destination-drain/N
      // only the drainer marks the head wedged; the request is read and
      //   cleared in the transaction that honours it, the wedge cites the
      //   request event as its trigger and records the requester.
      // Verifies: EVS-PRD-destinations/Q
      // the wedge event records the cause operator_halt.
      test('a halt on a healthy destination wedges the head', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        final head = await queued(d);
        final request = await requestHalt('x');
        await w.agree(<String>['x']);
        await drainForTest(d, registry: w.registry, policy: budget(7));
        expect(d.sent, isEmpty, reason: 'no send');
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, isEmpty, reason: 'no attempt is appended');
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'operator_halt');
        expect(
          wedge.initiator,
          AutomationInitiator(
            service: 'event_sourcing.drain',
            triggeringEventId: request,
          ),
        );
        expect(wedge.data['halt_requested_by'], _operator.toJson());
        expect(await w.halt('x'), isNull, reason: 'the request is consumed');
        final rows = await wedgesViewRows(w.backend);
        expect(
          rows['${w.store.databaseId}|x']?['halt_requested_by'],
          _operator.toJson(),
        );
        expect(
          await w.wedgeRecord('x'),
          WedgeRecord(
            rowId: head.entryId,
            wedgeEventId: wedge.eventId,
            cause: WedgeCause.operatorHalt,
            haltPurpose: HaltPurpose.pause,
            drainerEpoch: await w.drainEpoch(),
          ),
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/I
      // an operator-halt wedge carries exactly the declared keys: the item
      //   fields, cause operator_halt, the budget in effect, null attempt
      //   fields, and the request's identifier, requester and purpose.
      test('operator-halt wedge fields', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        final head = await queued(d);
        final request = await requestHalt(
          'x',
          purpose: HaltPurpose.reconfigure,
        );
        await drainForTest(d, registry: w.registry, policy: budget(7));
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
          'cause': 'operator_halt',
          'attempt_count': null,
          'max_attempts': 7,
          'last_outcome': null,
          'http_status': null,
          'wire_format': head.wireFormat,
          'transform_version': head.transformVersion,
          'halt_request_event_id': request,
          'halt_requested_by': _operator.toJson(),
          'halt_purpose': 'reconfigure',
          'drainer_epoch': await w.drainEpoch(),
          'configuration_fingerprint': null,
          'configuration': null,
        });
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-PRD-destinations/U
      // a head waiting out its backoff is halted at the next pass without
      //   waiting for the backoff and without a send.
      test('a head in backoff is halted without a send', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendTransient(error: 'busy')],
        );
        await queued(d);
        const slow = SyncPolicy(
          initialBackoff: Duration(days: 1),
          backoffMultiplier: 1.0,
          maxBackoff: Duration(days: 1),
          jitterFraction: 0.0,
          maxAttempts: 5,
        );
        await drainForTest(d, registry: w.registry, policy: slow);
        expect(d.sent, hasLength(1));
        await requestHalt('x');
        await drainForTest(d, registry: w.registry, policy: slow);
        expect(d.sent, hasLength(1), reason: 'the backoff is not waited out');
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'operator_halt');
        expect(wedge.data['attempt_count'], isNull);
        await w.agree(<String>['x']);
      });

      for (final another in <bool>[false, true]) {
        // Verifies: EVS-DEV-destination-drain/N
        // a halt committed between the loop-top read and the send, by this
        //   registry or through another backend on the same database, is
        //   seen by the pre-send fence: the send is not started, and the
        //   head is wedged operator_halt in the same pass.
        // Verifies: EVS-PRD-destinations/U
        // no delivery attempt completes after the request.
        test('a halt committed before the fence stops the send'
            '${another ? ' (another backend)' : ''}', () async {
          if (!available) return;
          final d = FakeDestination(
            id: 'x',
            script: <SendResult>[const SendOk()],
          );
          await queued(d);
          final other = another ? (await w.openProcess()).registry : null;
          var fired = false;
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              beforeSendFence: (id) async {
                if (fired) return;
                fired = true;
                await requestHalt('x', on: other);
              },
            ),
            () => drainForTest(d, registry: w.registry, policy: budget(3)),
          );
          expect(fired, isTrue);
          expect(d.sent, isEmpty);
          expect((await w.wedgeEvents()).single.data['cause'], 'operator_halt');
          await w.agree(<String>['x']);
        });
      }

      // Verifies: EVS-DEV-destination-drain/N
      // a halt committed after the loop-top read found none open, on a
      //   fresh head, is honoured before the send.
      test('a halt committed after the loop-top read stops the send', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        await queued(d);
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            afterHaltLoopTopRead: (id) async {
              if (fired) return;
              fired = true;
              await requestHalt('x');
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(fired, isTrue);
        expect(d.sent, isEmpty);
        expect((await w.wedgeEvents()).single.data['cause'], 'operator_halt');
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a head delivered by an out-of-library writer before the fence: the
      //   fence finds the head changed, starts no send, and the drainer
      //   returns to the head read.
      test('a head changed before the fence is not sent', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        final head = await queued(d);
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              await w.backend.transaction(
                (txn) => w.backend.setFinalStatusTxn(
                  txn,
                  'x',
                  head.entryId,
                  FinalStatus.sent,
                ),
              );
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(fired, isTrue);
        expect(d.sent, isEmpty);
        expect(await w.fence('x'), isNull, reason: 'the fence wrote nothing');
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a head delivered by an out-of-library writer before the fence, with
      //   a second item behind it: the fence finds another item at the head,
      //   the payload built from the first is never sent, and the second is
      //   sent from a payload built from it.
      test('a head replaced before the fence is not sent', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          batchCapacity: 1,
          script: <SendResult>[const SendOk()],
        );
        final head = await queued(d, notes: 2);
        final entries = await w.backend.listFifoEntries('x');
        expect(entries, hasLength(2));
        final second = entries[1];
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              await w.backend.transaction(
                (txn) => w.backend.setFinalStatusTxn(
                  txn,
                  'x',
                  head.entryId,
                  FinalStatus.sent,
                ),
              );
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(fired, isTrue);
        expect(d.sent, hasLength(1), reason: 'only the second item is sent');
        final sentEvents =
            (jsonDecode(utf8.decode(d.sent.single.bytes))
                    as Map<String, Object?>)['event_ids']!
                as List<Object?>;
        expect(sentEvents, second.eventIds);
        expect(sentEvents, isNot(contains(head.eventIds.single)));
        final first = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(first.attempts, isEmpty, reason: 'the first was never sent');
        expect(
          (await w.backend.readFifoRow('x', second.entryId))!.finalStatus,
          FinalStatus.sent,
        );
        expect((await w.fence('x'))?.entryId, second.entryId);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a head wedged by an out-of-library writer before the fence: the
      //   fence finds the head no longer pending, starts no send, records no
      //   attempt, and the drainer stops at the wedged head.
      test('a head wedged before the fence is not sent', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        final head = await queued(d);
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              await w.backend.transaction(
                (txn) => w.backend.setFinalStatusTxn(
                  txn,
                  'x',
                  head.entryId,
                  FinalStatus.wedged,
                ),
              );
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(fired, isTrue);
        expect(d.sent, isEmpty);
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.wedged);
        expect(row.attempts, isEmpty);
        expect(await w.fence('x'), isNull, reason: 'the fence wrote nothing');
      });

      // Verifies: EVS-DEV-destination-drain/N
      // an attempt recorded on the head before the fence: the payload built
      //   from the earlier head is not sent; the drainer re-reads the head
      //   and sends a payload built from it.
      test('a head whose attempts changed before the fence', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        final head = await queued(d);
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              await w.backend.transaction(
                (txn) => w.backend.appendAttemptTxn(
                  txn,
                  'x',
                  head.entryId,
                  AttemptResult(
                    attemptedAt: DateTime.utc(2026, 1, 1),
                    outcome: 'transient',
                    errorMessage: 'out of library',
                  ),
                ),
              );
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(5)),
        );
        expect(fired, isTrue);
        expect(d.sent, hasLength(1), reason: 'one send, after the re-read');
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.sent);
        expect(row.attempts, hasLength(2));
        expect(
          await w.fence('x'),
          isA<SendFence>()
              .having((f) => f.entryId, 'entryId', head.entryId)
              .having((f) => f.attemptCount, 'attemptCount', 1),
          reason: 'the fence that proceeded saw the recorded attempt',
        );
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a deletion attempted before the fence on a pending head is refused,
      //   and the send proceeds and records its attempt.
      test('a deletion before the fence is refused', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk()],
        );
        final head = await queued(d);
        Object? refusal;
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              try {
                await w.registry.deleteDestination('x', initiator: _operator);
              } on Object catch (e) {
                refusal = e;
              }
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(refusal, isA<StateError>());
        expect(d.sent, hasLength(1));
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, FinalStatus.sent);
        expect(row.attempts.single.outcome, 'ok');
      });

      // Verifies: EVS-DEV-destination-drain/N
      // the fence writes a record naming the entry and its attempt count
      //   before each send; a fence that finds an open request leaves the
      //   record unchanged.
      test('the send fence record', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendTransient(error: 'busy')],
        );
        final head = await queued(d, notes: 2);
        await drainForTest(d, registry: w.registry, policy: budget(5));
        final first = await w.fence('x');
        expect(first?.entryId, head.entryId);
        expect(first?.attemptCount, 0);
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              if (fired) return;
              fired = true;
              await requestHalt('x');
            },
          ),
          () => drainForTest(
            d,
            registry: w.registry,
            policy: budget(5),
            clock: () => DateTime.utc(2100),
          ),
        );
        expect(fired, isTrue);
        expect(await w.fence('x'), first, reason: 'unchanged by the refusal');
        expect(d.sent, hasLength(1));
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a halt requested while a send is in flight lets that send complete
      //   as sent; the next head is wedged operator_halt, and no StateError
      //   is raised.
      // Verifies: EVS-PRD-destinations/U
      // at most the one attempt whose pre-send check preceded the request
      //   completes after it.
      test('a halt during a blocked send', () async {
        if (!available) return;
        final gate = Completer<void>();
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk(), const SendOk()],
          blockBeforeSend: () => gate.future,
        );
        final head = await queued(d, notes: 2);
        final pass = drainForTest(d, registry: w.registry, policy: budget(3));
        while (d.sent.isEmpty) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        await requestHalt('x');
        gate.complete();
        await pass;
        expect(d.sent, hasLength(1));
        final rows = await w.backend.listFifoEntries('x');
        expect(rows.first.entryId, head.entryId);
        expect(rows.first.finalStatus, FinalStatus.sent);
        expect(rows[1].finalStatus, FinalStatus.wedged);
        expect((await w.wedgeEvents()).single.data['cause'], 'operator_halt');
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/O
      // a send that ends in a permanent refusal while a request is open
      //   wedges once, with cause permanent_refusal, and consumes the
      //   request.
      // Verifies: EVS-DEV-destination-drain/J
      // the recovered queue refills and the refilled head is sent.
      test('a halt during a send that is refused', () async {
        if (!available) return;
        final gate = Completer<void>();
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
          blockBeforeSend: () => gate.future,
        );
        final head = await queued(d);
        final pass = drainForTest(d, registry: w.registry, policy: budget(3));
        while (d.sent.isEmpty) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        final request = await requestHalt('x');
        gate.complete();
        await pass;
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'permanent_refusal');
        expect(wedge.data['halt_request_event_id'], request);
        expect(wedge.data['halt_requested_by'], _operator.toJson());
        expect(wedge.data['halt_purpose'], 'pause');
        expect(await w.halt('x'), isNull);
        await w.agree(<String>['x']);
        await w.registry.tombstoneAndRefill(
          'x',
          head.entryId,
          initiator: _operator,
        );
        d.enqueueScript(const SendOk());
        await w.fillAll(d);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        expect(
          (await w.backend.listFifoEntries('x')).last.finalStatus,
          FinalStatus.sent,
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/J
      // the status derivation runs before any halt honour: a permanent
      //   attempt committed alone with a request open wedges with cause
      //   permanent_refusal and no send.
      // Verifies: EVS-DEV-destination-drain/O
      // that wedge cites the open request.
      test('a recorded refusal with an open request', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.finalStatus, isNull);
        expect(row.attempts.single.outcome, 'permanent');
        final request = await requestHalt('x');
        await drainForTest(d, registry: w.registry, policy: budget(3));
        expect(d.sent, hasLength(1), reason: 'no further send');
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'permanent_refusal');
        expect(wedge.data['halt_request_event_id'], request);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // an operator-halt wedge over a head whose last attempt reported a
      //   permanent failure is refused and writes nothing: the recorded
      //   refusal is the head's cause.
      test('an operator-halt wedge over a recorded refusal', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        final head = await queued(d);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        await requestHalt('x');
        final before = await w.snapshot('x');
        for (final budgetInEffect in <int?>[3, null]) {
          await expectLater(
            w.store.runTransaction(
              (txn, collector) => wedgeHeadInTxnForTest(
                w.registry,
                txn,
                collector,
                destinationId: 'x',
                rowId: head.entryId,
                cause: WedgeCause.operatorHalt,
                maxAttempts: budgetInEffect,
                drainerEpoch: 1,
                configuration: null,
                configurationFingerprint: null,
              ),
            ),
            throwsA(
              isA<StateError>().having(
                (e) => e.message,
                'message',
                contains('permanent'),
              ),
            ),
          );
        }
        expect(await w.snapshot('x'), before);
      });

      // Verifies: EVS-DEV-destination-drain/O
      // an exhausted budget with a request open wedges with cause
      //   retry_budget_exhausted and consumes the request.
      // Verifies: EVS-DEV-destination-drain/J
      // the wedge records the budget in effect and the attempts.
      test('an exhausted budget with an open request', () async {
        if (!available) return;
        var block = false;
        final gate = Completer<void>();
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            for (var i = 0; i < 3; i++) const SendTransient(error: 'busy'),
          ],
          blockBeforeSend: () => block ? gate.future : Future<void>.value(),
        );
        await queued(d);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await drainForTest(d, registry: w.registry, policy: budget(3));
        block = true;
        final pass = drainForTest(d, registry: w.registry, policy: budget(3));
        while (d.sent.length < 3) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        final request = await requestHalt(
          'x',
          purpose: HaltPurpose.reconfigure,
        );
        gate.complete();
        await pass;
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'retry_budget_exhausted');
        expect(wedge.data['attempt_count'], 3);
        expect(wedge.data['max_attempts'], 3);
        expect(wedge.data['halt_request_event_id'], request);
        expect(wedge.data['halt_requested_by'], _operator.toJson());
        expect(wedge.data['halt_purpose'], 'reconfigure');
        expect(await w.halt('x'), isNull);
        expect(
          (await w.wedgeRecord('x'))?.haltPurpose,
          HaltPurpose.reconfigure,
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/O
      // a budget lowered below the recorded attempts with a request open
      //   wedges at the status derivation, before any halt honour or send,
      //   with cause retry_budget_exhausted citing the request.
      test('a lowered budget with an open request', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            for (var i = 0; i < 3; i++) const SendTransient(error: 'busy'),
          ],
        );
        await queued(d);
        for (var i = 0; i < 3; i++) {
          await drainForTest(d, registry: w.registry, policy: budget(5));
        }
        final request = await requestHalt('x');
        var resolved = budget(2);
        final cycle = TestCycle(
          w.registry,
          clock: _fillNow,
          policyResolver: () => resolved,
        );
        await cycle();
        expect(d.sent, hasLength(3), reason: 'no send');
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'retry_budget_exhausted');
        expect(wedge.data['max_attempts'], 2);
        expect(wedge.data['halt_request_event_id'], request);
        resolved = budget(5);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a request cancelled and replaced between the loop-top read and the
      //   honouring transaction: no wedge cites the cancelled request, and
      //   the new one is honoured in the next iteration.
      test('a request replaced before it is honoured', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        await queued(d);
        final r1 = await requestHalt('x');
        String? r2;
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            afterHaltLoopTopRead: (id) async {
              if (fired) return;
              fired = true;
              await w.registry.cancelHalt('x', initiator: _operator);
              r2 = await requestHalt('x');
            },
          ),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(fired, isTrue);
        expect(d.sent, isEmpty);
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['halt_request_event_id'], r2);
        expect(wedge.data['halt_request_event_id'], isNot(r1));
        await w.agree(<String>['x']);
      });
    });

    // ------------------------------------------------------------------
    // Stale or forged working copy
    // ------------------------------------------------------------------

    group('a stored request the log does not hold', () {
      Future<void> forgedCase(
        String label,
        Future<String> Function() citedId,
      ) async {
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        await queued(d);
        final cited = await citedId();
        await w.backend.transaction(
          (txn) => w.backend.writeHaltRequestTxn(
            txn,
            'x',
            HaltRequest(
              requestEventId: cited,
              requestedAt: DateTime.utc(2026, 5, 1),
              purpose: HaltPurpose.pause,
              requestedBy: _operator.toJson(),
            ),
          ),
        );
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(await w.wedgeEvents(), isEmpty, reason: label);
        expect(await w.halt('x'), isNull, reason: 'the record is cleared');
        final errors = <LibraryLogRecord>[
          for (final r in log)
            if (r.level == LibraryLogLevel.severe) r,
        ];
        expect(errors, hasLength(1), reason: label);
        expect(errors.single.message, contains(cited));
        expect(errors.single.message, contains('x'));
        expect(d.sent, hasLength(1), reason: 'the head is sent in that pass');
        expect(
          (await w.backend.listFifoEntries('x')).single.finalStatus,
          FinalStatus.sent,
        );
      }

      // Verifies: EVS-DEV-destination-drain/N
      // a stored request citing an event the log does not hold wedges
      //   nothing: the record is cleared, one error is logged, and delivery
      //   continues.
      test('citing a missing event', () async {
        if (!available) return;
        await forgedCase('missing', () async => 'no-such-event');
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a stored request citing an event of another type wedges nothing.
      test('citing a registration event', () async {
        if (!available) return;
        await forgedCase('registration', () async {
          return (await w.events(
            kDestinationRegisteredEntryType,
          )).first.eventId;
        });
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a stored request citing a halt request for another destination
      //   wedges nothing.
      test('citing a request for another destination', () async {
        if (!available) return;
        await forgedCase('other destination', () async {
          await w.activate(FakeDestination(id: 'y'));
          return requestHalt('y');
        });
      });

      // Verifies: EVS-DEV-destination-drain/O
      // a wedge that finds a stored request the log does not hold removes
      //   it and records no request; the removal is logged once, after the
      //   wedge commits, and a wedge that rolls back logs nothing.
      test('a wedge over a stored request the log does not hold', () async {
        if (!available) return;
        Future<void> writeForged() => w.backend.transaction(
          (txn) => w.backend.writeHaltRequestTxn(
            txn,
            'x',
            HaltRequest(
              requestEventId: 'no-such-event',
              requestedAt: DateTime.utc(2026, 5, 1),
              purpose: HaltPurpose.pause,
              requestedBy: _operator.toJson(),
            ),
          ),
        );
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
          // The record appears while the send is in flight, after the
          // fence.
          blockBeforeSend: writeForged,
        );
        await queued(d);
        final log = <LibraryLogRecord>[];
        List<LibraryLogRecord> cited() => <LibraryLogRecord>[
          for (final r in log)
            if (r.message.contains('no-such-event')) r,
        ];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add, afterWedgeHeadInTxn: (id) => true),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(await w.wedgeEvents(), isEmpty);
        expect(cited(), isEmpty, reason: 'the wedge rolled back');
        expect((await w.halt('x'))?.requestEventId, 'no-such-event');
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'permanent_refusal');
        expect(wedge.data['halt_request_event_id'], isNull);
        expect(await w.halt('x'), isNull);
        expect(cited(), hasLength(1));
        expect(cited().single.level, LibraryLogLevel.severe);
        expect(d.sent, hasLength(1));
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a stored request citing a halt request for this destination that
      //   another database appended (held here by ingest) wedges nothing.
      test('citing a request of another database', () async {
        if (!available) return;
        await forgedCase('other database', () async {
          final peer = forgedEvent(
            entryType: kDestinationHaltRequestedEntryType,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kDestinationHaltRequestedEventType,
            data: <String, Object?>{
              'id': 'x',
              'database_id': 'peer-db',
              'purpose': HaltPurpose.pause.wire,
            },
          );
          await w.store.ingestEvent(peer);
          final held = await w.backend.findEventById(peer.eventId);
          expect(held?.data['database_id'], 'peer-db');
          return peer.eventId;
        });
      });
    });

    // ------------------------------------------------------------------
    // requestHalt and cancelHalt
    // ------------------------------------------------------------------

    group('request and cancel', () {
      // Verifies: EVS-DEV-destination-drain/P
      // two concurrent requests on one destination: exactly one is
      //   recorded.
      // Verifies: EVS-DEV-destination-drain/Q
      // the second is refused while the first is open.
      test('two concurrent requests', () async {
        if (!available) return;
        final b = await w.openProcess();
        await queued(FakeDestination(id: 'x'));
        final results = await Future.wait(<Future<Object?>>[
          requestHalt('x').then<Object?>((v) => v, onError: (Object e) => e),
          requestHalt(
            'x',
            on: b.registry,
          ).then<Object?>((v) => v, onError: (Object e) => e),
        ]);
        expect(results.whereType<String>(), hasLength(1));
        expect(results.whereType<StateError>(), hasLength(1));
        expect(
          await w.events(kDestinationHaltRequestedEntryType),
          hasLength(1),
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // every registry operation that commits invokes its event store's
      //   delivery-cycle trigger once after the commit, a halt request, a
      //   cancellation, a recovery, an end date and a deletion included; a
      //   refusal invokes nothing.
      test('committed registry operations wake the delivery cycle', () async {
        if (!available) return;
        var wakes = 0;
        final backend = await w.db.openBackend();
        final store = await EventStore.openForTest(
          storage: backend,
          entryTypes: EntryTypeRegistry()
            ..register(
              const EntryTypeDefinition(
                id: _noteType,
                registeredVersion: EntryTypeVersion(1, 0),
                name: _noteType,
              ),
            ),
          source: _source,
          securityContexts: w.db.securityFor(backend),
        );
        trackTestBackend(store, backend);
        final registry = DestinationRegistry(eventStore: store);
        final counting = DeliveryTestHooks(onDeliveryWake: (_) => wakes += 1);
        Future<void> wakesOnce(
          String op,
          Future<Object?> Function() run,
        ) async {
          final before = wakes;
          await runWithDeliveryTestHooks(counting, run);
          expect(wakes - before, 1, reason: op);
        }

        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: const <SendResult>[],
        );
        await wakesOnce(
          'addDestination',
          () => registry.addDestination(d, initiator: _init),
        );
        await wakesOnce(
          'setStartDate',
          () => registry.setStartDate(
            'x',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
        );
        await wakesOnce(
          'requestHalt',
          () => registry.requestHalt(
            'x',
            initiator: _operator,
            purpose: HaltPurpose.pause,
          ),
        );
        final refusedAt = wakes;
        await expectLater(
          runWithDeliveryTestHooks(
            counting,
            () => registry.requestHalt(
              'x',
              initiator: _operator,
              purpose: HaltPurpose.pause,
            ),
          ),
          throwsStateError,
        );
        expect(wakes, refusedAt, reason: 'a refusal wakes nothing');
        await wakesOnce(
          'cancelHalt',
          () => registry.cancelHalt('x', initiator: _operator),
        );
        await store.append(
          entryType: _noteType,
          aggregateId: 'n1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: <String, Object?>{'id': 'n1'},
          initiator: _init,
        );
        await fillForTest(
          d,
          backend: backend,
          source: _source,
          clock: _fillNow,
        );
        final head = (await backend.readFifoHead('x'))!;
        await registry.requestHalt(
          'x',
          initiator: _operator,
          purpose: HaltPurpose.pause,
        );
        await drainForTest(d, registry: registry, policy: budget(3));
        expect(
          (await backend.readFifoHead('x'))?.finalStatus,
          FinalStatus.wedged,
        );
        await wakesOnce(
          'tombstoneAndRefill',
          () => registry.tombstoneAndRefill(
            'x',
            head.entryId,
            initiator: _operator,
          ),
        );
        await wakesOnce(
          'setEndDate',
          () => registry.setEndDate(
            'x',
            DateTime.utc(2026, 6, 1),
            initiator: _operator,
          ),
        );
        await wakesOnce(
          'deleteDestination',
          () => registry.deleteDestination('x', initiator: _operator),
        );
      });

      // Verifies: EVS-DEV-destination-drain/Q
      // a request on an empty queue is accepted and stays open across empty
      //   cycles and a restart; the first head enqueued afterwards is wedged
      //   operator_halt before any send.
      test('a request on an empty queue waits for a head', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk()],
        );
        await w.activate(d);
        final request = await requestHalt('x');
        final cycle = cycleOver(w.registry);
        for (var i = 0; i < 3; i++) {
          await cycle();
        }
        expect((await w.halt('x'))?.requestEventId, request);
        final b = await w.openProcess();
        await b.registry.addDestination(d, initiator: _init);
        expect(
          (await b.backend.transaction(
            (txn) => b.backend.readHaltRequestTxn(txn, 'x'),
          ))?.requestEventId,
          request,
        );
        await w.note('x-first');
        await cycleOver(b.registry)();
        expect(d.sent, isEmpty);
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['cause'], 'operator_halt');
        expect(wedge.data['halt_request_event_id'], request);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/Q
      // cancelling on an empty queue closes the request and names it.
      test('a cancellation on an empty queue', () async {
        if (!available) return;
        await w.activate(FakeDestination(id: 'x'));
        final request = await requestHalt('x');
        await w.registry.cancelHalt('x', initiator: _operator);
        final cancel = (await w.events(
          kDestinationHaltCancelledEntryType,
        )).single;
        expect(cancel.data['halt_request_event_id'], request);
        expect(cancel.data['id'], 'x');
        expect(cancel.data['database_id'], w.store.databaseId);
        expect(cancel.initiator, _operator);
        expect(await w.halt('x'), isNull);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-PRD-destinations/U
      // a halt request and its cancellation are recorded as events.
      // Verifies: EVS-DEV-destination-drain/Q
      // the request event carries exactly the destination, the database
      //   identity and the purpose, the cancellation event exactly the
      //   destination, the database identity and the request it closes,
      //   and the stored request mirrors the request event.
      test('a request records its purpose and requester', () async {
        if (!available) return;
        await w.activate(FakeDestination(id: 'x'));
        final request = await requestHalt(
          'x',
          purpose: HaltPurpose.reconfigure,
        );
        final event = (await w.events(
          kDestinationHaltRequestedEntryType,
        )).single;
        expect(event.eventId, request);
        expect(event.data, <String, Object?>{
          'id': 'x',
          'database_id': w.store.databaseId,
          'purpose': 'reconfigure',
        });
        expect(event.initiator, _operator);
        await w.agree(<String>['x']);
        await w.registry.cancelHalt('x', initiator: _operator);
        final cancel = (await w.events(
          kDestinationHaltCancelledEntryType,
        )).single;
        expect(cancel.data, <String, Object?>{
          'id': 'x',
          'database_id': w.store.databaseId,
          'halt_request_event_id': request,
        });
        await w.agree(<String>['x']);
      });

      Future<void> refused(
        Future<Object?> Function() op, {
        required Matcher throws,
        required String opName,
        required String outcome,
        String destId = 'x',
      }) async {
        final before = await w.snapshot(destId);
        await expectLater(op(), throwsA(throws));
        expect(await w.snapshot(destId), before);
        final written = await w.check();
        expect(written?.op, opName);
        expect(written?.destinationId, destId);
        expect(written?.outcome, outcome);
      }

      // Verifies: EVS-DEV-destination-drain/Q
      // a second request while one is open is refused, with nothing written
      //   but the registry check record.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal commits only the check record and throws after it.
      test('a second request while one is open', () async {
        if (!available) return;
        await queued(FakeDestination(id: 'x'));
        await requestHalt('x');
        await refused(
          () => requestHalt('x'),
          throws: isA<StateError>(),
          opName: 'requestHalt',
          outcome: 'refused_halt_open',
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/Q
      // a request on a wedged head is refused.
      test('a request on a wedged head', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        await queued(d);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await refused(
          () => requestHalt('x'),
          throws: isA<StateError>(),
          opName: 'requestHalt',
          outcome: 'refused_wedged_head',
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/Q
      // a cancellation with no request open is refused.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal commits only the check record and throws after it.
      test('a cancellation with none open', () async {
        if (!available) return;
        await w.activate(FakeDestination(id: 'x'));
        await refused(
          () => w.registry.cancelHalt('x', initiator: _operator),
          throws: isA<StateError>(),
          opName: 'cancelHalt',
          outcome: 'refused_no_halt_open',
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/Q
      // a request or a cancellation for an unknown destination throws
      //   ArgumentError with nothing written but the check record.
      test('an unknown destination', () async {
        if (!available) return;
        await refused(
          () => requestHalt('nope'),
          throws: isA<ArgumentError>(),
          opName: 'requestHalt',
          outcome: 'refused_unknown_destination',
          destId: 'nope',
        );
        await refused(
          () => w.registry.cancelHalt('nope', initiator: _operator),
          throws: isA<ArgumentError>(),
          opName: 'cancelHalt',
          outcome: 'refused_unknown_destination',
          destId: 'nope',
        );
      });
    });

    // ------------------------------------------------------------------
    // Recovery and deletion
    // ------------------------------------------------------------------

    group('recovery and deletion', () {
      // Verifies: EVS-PRD-destinations/M
      // a recovery while a halt is requested and not yet honoured is
      //   refused, telling the operator to wait for the wedge; once the
      //   drainer honours the request the same recovery succeeds.
      test('recovery waits for the drainer to honour the halt', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        final head = await queued(d);
        await requestHalt('x');
        final before = await w.snapshot('x');
        await expectLater(
          w.registry.tombstoneAndRefill(
            'x',
            head.entryId,
            initiator: _operator,
          ),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              allOf(contains('halt'), contains('not yet honoured')),
            ),
          ),
        );
        expect(await w.snapshot('x'), before);
        expect((await w.check())?.outcome, 'refused_halt_not_honoured');
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await w.registry.tombstoneAndRefill(
          'x',
          head.entryId,
          initiator: _operator,
        );
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-PRD-destinations/M
      // with no request open, the refusal of a pending head says to request
      //   a halt first.
      test('recovery of a pending head names the halt', () async {
        if (!available) return;
        final head = await queued(FakeDestination(id: 'x'));
        await expectLater(
          w.registry.tombstoneAndRefill(
            'x',
            head.entryId,
            initiator: _operator,
          ),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains('request a halt'),
            ),
          ),
        );
      });

      // Verifies: EVS-DEV-destination-drain/A
      // a deletion of a pending head with a halt requested and not yet
      //   honoured is refused, telling the operator to wait for the drainer
      //   to wedge the head, with nothing written but the check record;
      //   with none requested the refusal says to request a halt.
      test('deletion waits for the drainer to honour the halt', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: const <SendResult>[],
        );
        await queued(d);
        await expectLater(
          w.registry.deleteDestination('x', initiator: _operator),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains('Request a halt'),
            ),
          ),
        );
        expect((await w.check())?.outcome, 'refused_pending_head');
        await requestHalt('x');
        final before = await w.snapshot('x');
        await expectLater(
          w.registry.deleteDestination('x', initiator: _operator),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              allOf(
                contains('not yet honoured'),
                isNot(contains('Request a halt')),
              ),
            ),
          ),
        );
        expect(await w.snapshot('x'), before);
        expect((await w.check())?.outcome, 'refused_halt_not_honoured');
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await w.registry.deleteDestination('x', initiator: _operator);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/R
      // deleting a destination with an open request on an empty queue
      //   closes the request and names it; registering it again does not
      //   wedge its first head.
      test('deletion closes an open request', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk()],
        );
        await w.activate(d);
        final request = await requestHalt('x');
        await w.registry.deleteDestination('x', initiator: _operator);
        final deleted = (await w.events(kDestinationDeletedEntryType)).single;
        expect(deleted.data['closed_halt_request_event_id'], request);
        expect(deleted.data['tombstoned_row_id'], isNull);
        expect(await w.halt('x'), isNull);
        await w.agree(<String>['x']);
        await w.activate(d);
        await w.note('x-after');
        await w.fillAll(d);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        expect(d.sent, hasLength(1));
        expect(await w.wedgeEvents(), isEmpty);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-PRD-destinations/T
      // deleting a destination whose head an operator halt wedged
      //   tombstones and names the head.
      // Verifies: EVS-DEV-destination-drain/R
      // with no request open, the deletion closes none.
      test('deletion after an operator halt', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: const <SendResult>[],
        );
        final head = await queued(d);
        await requestHalt('x');
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await w.registry.deleteDestination('x', initiator: _operator);
        final deleted = (await w.events(kDestinationDeletedEntryType)).single;
        expect(deleted.data['tombstoned_row_id'], head.entryId);
        expect(deleted.data['closed_halt_request_event_id'], isNull);
        await w.agree(<String>['x']);
      });
    });

    // ------------------------------------------------------------------
    // Unserved destinations
    // ------------------------------------------------------------------

    group('unserved destinations', () {
      // Verifies: EVS-DEV-destination-drain/T
      // a destination persisted by another registry and absent from the
      //   cycle's registry is reported notRegisteredHere, is not filled,
      //   and is logged once; a halt requested on it is honoured with no
      //   send, recording no budget and no configuration fingerprint; the
      //   other registry then deletes it.
      // Verifies: EVS-DEV-destination-drain/E
      // delivery fills and sends only the destinations registered in the
      //   draining process.
      // Verifies: EVS-DEV-destination-drain/I
      // a wedge honouring a halt on a destination the draining process does
      //   not register records max_attempts and configuration_fingerprint
      //   as null.
      test('a destination another registry registers', () async {
        if (!available) return;
        final other = DestinationRegistry(eventStore: w.store);
        final remote = FakeDestination(
          id: 'remote',
          allowHardDelete: true,
          script: const <SendResult>[],
        );
        await w.activate(remote, on: other);
        await w.note('r1');
        final log = <LibraryLogRecord>[];
        final cycle = cycleOver(w.registry);
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add),
          () async {
            await cycle();
            await cycle();
          },
        );
        expect(cycle.unserved, <String, UnservedReason>{
          'remote': UnservedReason.notRegisteredHere,
        });
        expect(await w.backend.listFifoEntries('remote'), isEmpty);
        expect(
          log.where((r) => r.message.contains('remote')).toList(),
          hasLength(1),
          reason: 'logged once while it persists',
        );
        // A test fill seeds a pending head, as the registering process's
        // drainer would.
        await w.fillAll(remote);
        final head = (await w.backend.readFifoHead('remote'))!;
        final request = await requestHalt('remote', on: other);
        await cycle();
        expect(remote.sent, isEmpty);
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['row_id'], head.entryId);
        expect(wedge.data['cause'], 'operator_halt');
        expect(wedge.data['halt_request_event_id'], request);
        expect(wedge.data['max_attempts'], isNull);
        expect(wedge.data['configuration_fingerprint'], isNull);
        expect(wedge.data['wire_format'], head.wireFormat);
        expect(wedge.data['transform_version'], head.transformVersion);
        await w.agree(<String>['remote']);
        await other.deleteDestination('remote', initiator: _operator);
        await cycle();
        expect(cycle.unserved, isEmpty);
        await w.agree(<String>['remote']);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // honouring a halt on a destination its process does not register,
      //   the cycle first gives the head the status a permanent last
      //   attempt calls for: the wedge records the refusal, not an operator
      //   halt, and consumes the request.
      // Verifies: EVS-DEV-destination-drain/J
      // the status derivation runs before any halt honour, on a destination
      //   the draining process does not register as on one it does.
      // Verifies: EVS-DEV-destination-drain/I
      // a wedge on a destination the draining process does not register
      //   records max_attempts as null and the attempt fields of the item.
      test('a recorded refusal on an unregistered destination', () async {
        if (!available) return;
        final other = DestinationRegistry(eventStore: w.store);
        final remote = FakeDestination(
          id: 'remote',
          script: <SendResult>[const SendPermanent(error: 'no')],
        );
        await w.activate(remote, on: other);
        await w.note('r1');
        await w.fillAll(remote);
        final head = (await w.backend.readFifoHead('remote'))!;
        // The registering process's wedge does not commit: the permanent
        // attempt is recorded alone and the head stays pending.
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drainForTest(remote, registry: other, policy: budget(3)),
        );
        final row = (await w.backend.readFifoRow('remote', head.entryId))!;
        expect(row.finalStatus, isNull);
        expect(row.attempts.single.outcome, 'permanent');
        final request = await requestHalt('remote', on: other);
        await cycleOver(w.registry)();
        expect(remote.sent, hasLength(1), reason: 'no further send');
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['row_id'], head.entryId);
        expect(wedge.data['cause'], 'permanent_refusal');
        expect(wedge.data['attempt_count'], 1);
        expect(wedge.data['last_outcome'], 'permanent');
        expect(wedge.data['max_attempts'], isNull);
        expect(wedge.data['halt_request_event_id'], request);
        expect(await w.halt('remote'), isNull);
        await w.agree(<String>['remote']);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // a pass that cannot read the persisted schedules honours no halt on
      //   a destination its registry does not hold and reports nothing
      //   unserved, rather than acting on an earlier pass's findings; the
      //   next pass that reads them honours the halt.
      test('a pass that cannot list the schedules', () async {
        if (!available) return;
        final other = DestinationRegistry(eventStore: w.store);
        final remote = FakeDestination(
          id: 'remote',
          script: const <SendResult>[],
        );
        await w.activate(remote, on: other);
        await w.note('r1');
        await w.fillAll(remote);
        final cycle = cycleOver(w.registry);
        await cycle();
        expect(cycle.unserved, <String, UnservedReason>{
          'remote': UnservedReason.notRegisteredHere,
        });
        await requestHalt('remote', on: other);
        final log = <LibraryLogRecord>[];
        final honourReads = <String>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onLog: log.add,
            failListSchedules: () => true,
            afterHaltLoopTopRead: (id) async => honourReads.add(id),
          ),
          cycle.call,
        );
        expect(honourReads, isEmpty, reason: 'no halt honour that pass');
        expect(await w.wedgeEvents(), isEmpty);
        expect(cycle.unserved, isEmpty);
        expect(
          log.where((r) => r.level == LibraryLogLevel.severe).toList(),
          hasLength(1),
        );
        await cycle();
        expect((await w.wedgeEvents()).single.data['cause'], 'operator_halt');
        await w.agree(<String>['remote']);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // a pass woken by a registration or a deletion sees the registry as
      //   the operation left it: the destination is neither reported
      //   unserved nor logged as such.
      test('a woken pass sees the registry the operation left', () async {
        if (!available) return;
        final backend = await w.db.openBackend();
        final store = await EventStore.openForTest(
          storage: backend,
          entryTypes: EntryTypeRegistry(),
          source: _source,
          securityContexts: w.db.securityFor(backend),
        );
        trackTestBackend(store, backend);
        final registry = DestinationRegistry(eventStore: store);
        var passes = 0;
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: log.add, onInboundPoll: () => passes += 1),
          () async {
            final cycle = await SyncCycle.start(
              registry: registry,
              clock: _fillNow,
              cadence: const Duration(hours: 1),
            );
            try {
              // Waits for the pass the operation's trigger started.
              Future<void> settle(int before) async {
                while (passes == before) {
                  await Future<void>.delayed(const Duration(milliseconds: 1));
                }
                await pumpEventQueue();
              }

              var before = passes;
              await registry.addDestination(
                FakeDestination(id: 'x', allowHardDelete: true),
                initiator: _init,
              );
              await settle(before);
              expect(cycle.unserved, isEmpty, reason: 'after the registration');
              before = passes;
              await registry.deleteDestination('x', initiator: _operator);
              await settle(before);
              expect(cycle.unserved, isEmpty, reason: 'after the deletion');
            } finally {
              await cycle.close();
            }
          },
        );
        expect(
          log.where((r) => r.name == 'event_sourcing.sync_cycle').toList(),
          isEmpty,
        );
      });

      // Verifies: EVS-DEV-destination-drain/T
      // a destination the cycle's registry holds but another registry
      //   deleted is reported deletedInStorage and is not filled.
      test('a destination deleted by another registry', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: const <SendResult>[],
        );
        await w.activate(d);
        final other = DestinationRegistry(eventStore: w.store);
        await other.deleteDestination('x', initiator: _operator);
        await w.note('after-delete');
        final cycle = cycleOver(w.registry);
        await cycle();
        expect(cycle.unserved, <String, UnservedReason>{
          'x': UnservedReason.deletedInStorage,
        });
        expect(await w.backend.listFifoEntries('x'), isEmpty);
        expect(d.transformCalls, 0);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // the registry's halt and cancel operations act on persisted state
      //   from a registry that registers nothing; on an unknown id each
      //   throws ArgumentError and writes nothing but the check record.
      test('halt and cancel from a registry that registers nothing', () async {
        if (!available) return;
        await w.activate(FakeDestination(id: 'x'));
        final bare = DestinationRegistry(eventStore: w.store);
        await bare.requestHalt(
          'x',
          initiator: _operator,
          purpose: HaltPurpose.pause,
        );
        await w.agree(<String>['x']);
        await bare.cancelHalt('x', initiator: _operator);
        await w.agree(<String>['x']);
        final before = await w.snapshot('nope');
        await expectLater(
          bare.requestHalt(
            'nope',
            initiator: _operator,
            purpose: HaltPurpose.pause,
          ),
          throwsA(isA<ArgumentError>()),
        );
        await expectLater(
          bare.cancelHalt('nope', initiator: _operator),
          throwsA(isA<ArgumentError>()),
        );
        expect(await w.snapshot('nope'), before);
      });

      // Verifies: EVS-DEV-destination-drain/T
      // a halt requested through another backend on the same database is
      //   honoured by this process's cycle.
      test('a halt requested through another backend', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        await queued(d);
        final b = await w.openProcess();
        final request = await requestHalt('x', on: b.registry);
        await cycleOver(w.registry, policy: budget(3))();
        expect(d.sent, isEmpty);
        final wedge = (await w.wedgeEvents()).single;
        expect(wedge.data['halt_request_event_id'], request);
        await w.agree(<String>['x']);
      });
    });

    // ------------------------------------------------------------------
    // Rollback
    // ------------------------------------------------------------------

    group('rollback', () {
      // Verifies: EVS-DEV-destination-drain/N
      // a request whose event append fails leaves no stored request and no
      //   event.
      test('a request that does not commit', () async {
        if (!available) return;
        await queued(FakeDestination(id: 'x'));
        final before = await w.snapshot('x');
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) =>
                  t == kDestinationHaltRequestedEntryType,
            ),
            () => requestHalt('x'),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot('x'), before);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/N
      // a cancellation whose event append fails leaves the stored request
      //   unchanged and no event.
      test('a cancellation that does not commit', () async {
        if (!available) return;
        await queued(FakeDestination(id: 'x'));
        await requestHalt('x');
        final before = await w.snapshot('x');
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) =>
                  t == kDestinationHaltCancelledEntryType,
            ),
            () => w.registry.cancelHalt('x', initiator: _operator),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot('x'), before);
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/O
      // a halt honour whose transaction fails leaves the head pending, the
      //   request open, and no event or wedge record; the next pass honours
      //   it.
      test('a halt honour that does not commit', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', script: const <SendResult>[]);
        await queued(d);
        await requestHalt('x');
        final before = await w.snapshot('x');
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
          () => drainForTest(d, registry: w.registry, policy: budget(3)),
        );
        expect(await w.snapshot('x'), before);
        expect(d.sent, isEmpty);
        await w.agree(<String>['x']);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        expect((await w.wedgeEvents()).single.data['cause'], 'operator_halt');
        await w.agree(<String>['x']);
      });

      // Verifies: EVS-DEV-destination-drain/R
      // a deletion whose audit append fails leaves the stored request, the
      //   wedge record, the send fence, every item and the schedule
      //   unchanged, and no event. The open request beside a wedged head is
      //   written through the internal writer, so that every record the
      //   deletion removes is present at once.
      test('a deletion that does not commit', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[
            const SendOk(),
            const SendPermanent(error: 'no'),
          ],
        );
        await queued(d, notes: 4);
        await drainForTest(d, registry: w.registry, policy: budget(3));
        await w.backend.transaction(
          (txn) => w.backend.writeHaltRequestTxn(
            txn,
            'x',
            HaltRequest(
              requestEventId: 'written-directly',
              requestedAt: DateTime.utc(2026, 5, 1),
              purpose: HaltPurpose.pause,
              requestedBy: _operator.toJson(),
            ),
          ),
        );
        final before = await w.snapshot('x');
        expect(before['halt'], isNotNull);
        expect(before['fence'], isNotNull);
        expect(before['wedge_record'], isNotNull);
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) => t == kDestinationDeletedEntryType,
            ),
            () => w.registry.deleteDestination('x', initiator: _operator),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot('x'), before);
      });
    });

    // ------------------------------------------------------------------
    // Ingest of the halt events
    // ------------------------------------------------------------------

    group('ingest of the halt events', () {
      final types = <String, String>{
        kDestinationHaltRequestedEntryType: kDestinationHaltRequestedEventType,
        kDestinationHaltCancelledEntryType: kDestinationHaltCancelledEventType,
      };
      final malformed = <String, Map<String, Object?>>{
        'missing data.id': <String, Object?>{'database_id': 'peer-db'},
        'an empty data.id': <String, Object?>{
          'id': '',
          'database_id': 'peer-db',
        },
        'a numeric data.id': <String, Object?>{
          'id': 7,
          'database_id': 'peer-db',
        },
        'missing data.database_id': <String, Object?>{'id': 'x'},
        'an empty data.database_id': <String, Object?>{
          'id': 'x',
          'database_id': '',
        },
        'a numeric data.database_id': <String, Object?>{
          'id': 'x',
          'database_id': 42,
        },
      };

      Uint8List batchOf(List<StoredEvent> events) => BatchEnvelope(
        batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
        batchId: 'halt-batch-${events.first.eventId}',
        senderHop: 'mobile-device',
        senderIdentifier: 'peer-install',
        senderSoftwareVersion: 'test@1.0.0',
        sentAt: DateTime.utc(2026, 9, 1, 12),
        events: <Map<String, Object?>>[
          for (final e in events) Map<String, Object?>.from(e.toMap()),
        ],
      ).encode();

      final paths =
          <String, Future<void> Function(EventStore, List<StoredEvent>)>{
            'ingestBatch': (store, events) async {
              await store.ingestBatch(
                batchOf(events),
                wireFormat: BatchEnvelope.wireFormat,
              );
            },
            'ingestEvent': (store, events) async {
              for (final e in events) {
                await store.ingestEvent(e);
              }
            },
          };

      /// The snapshot of [destId] with the security findings left out of
      /// the log, and without the records every stored event advances (the
      /// sequence counter and the latest authored sequence) or a stored
      /// finding sets (whether a security finding is held).
      Future<Map<String, Object?>> besideFindings(String destId) async {
        final findings = <String>{
          for (final e in await w.events(kSecurityFindingEntryType)) e.eventId,
        };
        final snapshot = await w.snapshot(destId);
        return <String, Object?>{
          ...snapshot,
          'events': <Object?>[
            for (final id in snapshot['events']! as List)
              if (!findings.contains(id)) id,
          ],
          'state_keys': <Object?>[
            for (final k in snapshot['state_keys']! as List)
              if (!_advancedByEveryEvent.contains(k)) k,
          ],
        };
      }

      /// The reasons of the `event_malformed` findings recorded about the
      /// record of [event].
      Future<List<Object?>> malformedReasons(
        StoredEvent event,
      ) async => <Object?>[
        for (final f in await w.events(kSecurityFindingEntryType))
          if (f.data['kind'] == 'event_malformed' &&
              ((f.data['evidence']! as Map)['record']! as Map)['event_id'] ==
                  event.eventId)
            (f.data['evidence']! as Map)['reason'],
      ];

      for (final type in types.entries) {
        for (final path in paths.entries) {
          for (final c in malformed.entries) {
            // Verifies: EVS-DEV-destination-drain/L
            // a halt event whose destination identifier or database identity
            //   is missing, empty or not a string is stored as no event and
            //   kept in a finding naming audit_identity_invalid.
            test('${path.key} keeps ${type.value} with ${c.key} in a '
                'finding', () async {
              if (!available) return;
              final before = await besideFindings('x');
              final bad = forgedEvent(
                entryType: type.key,
                aggregateType: kDestinationAuditAggregateType,
                eventType: type.value,
                data: c.value,
              );
              await path.value(w.store, <StoredEvent>[bad]);
              expect(await malformedReasons(bad), <String>[
                'audit_identity_invalid',
              ]);
              expect(await besideFindings('x'), before);
            });
          }

          // Verifies: EVS-DEV-destination-drain/L
          // a halt event a peer originated that names the receiver's own
          //   database is stored as no event and kept in a finding naming
          //   audit_identity_invalid; the receiver's halt state is unchanged.
          test('${path.key} keeps a peer ${type.value} naming the receiver '
              'database in a finding', () async {
            if (!available) return;
            await w.activate(FakeDestination(id: 'x'));
            final before = await besideFindings('x');
            final forged = forgedEvent(
              entryType: type.key,
              aggregateType: kDestinationAuditAggregateType,
              eventType: type.value,
              data: <String, Object?>{
                'id': 'x',
                'database_id': w.store.databaseId,
                'purpose': 'pause',
                'halt_request_event_id': 'r',
              },
            );
            await path.value(w.store, <StoredEvent>[forged]);
            expect(await malformedReasons(forged), <String>[
              'audit_identity_invalid',
            ]);
            expect(await besideFindings('x'), before);
            await w.agree(<String>['x']);
          });

          // Verifies: EVS-DEV-destination-drain/L
          // a peer's halt event is ingested, and a redelivery of it is
          //   accepted as a duplicate.
          test('${path.key} accepts a redelivered ${type.value}', () async {
            if (!available) return;
            final peer = forgedEvent(
              entryType: type.key,
              aggregateType: kDestinationAuditAggregateType,
              eventType: type.value,
              data: <String, Object?>{
                'id': 'x',
                'database_id': 'peer-db',
                'purpose': 'pause',
                'halt_request_event_id': 'r',
              },
            );
            await path.value(w.store, <StoredEvent>[peer]);
            await path.value(w.store, <StoredEvent>[peer]);
            expect(await w.events(type.key), hasLength(1));
            expect(await w.halt('x'), isNull, reason: 'no local request');
          });
        }
      }
    });
  });
}
