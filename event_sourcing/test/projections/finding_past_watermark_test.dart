// Two builds share one Sembast backend: build B registers no view, build A
// registers one aggregate view and has its catch-up paused, so only this
// test's own transactions move the copy's watermark.
//
// A security finding, a sender-succession event, or an event of a forked
// database sharing a held fork_unrecorded finding's predecessor hash each
// extend the marks a view's rows carry without necessarily matching the
// view's interest or being a security finding event itself. Build B stores
// each kind while holding no copy of the view; build A's copy reports
// converging once one lands past its watermark, and A's next local append
// neither folds into the copy nor advances its watermark past it.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionChannel, SenderSuccessionData;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/deliveries.dart';
import '../test_support/ingest_chain_findings_conformance.dart' show chained;
import '../test_support/ingest_record_findings_conformance.dart'
    show resealed, sealedRecord;

const String _kType = 'finding_note';
const String _kOtherType = 'finding_note_other';

const AggregateProjectionSpec _kViewSpec = AggregateProjectionSpec(
  viewName: 'finding_past_watermark_view',
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// Fails every catch-up step so build A's copy of the view, once behind,
/// stays converging for as long as the hook is installed
/// (EVS-DEV-view-convergence/Q backs the driver off rather than closing the
/// store).
void _pauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure('paused for a finding-past-watermark test');

var _dbCounter = 0;

Future<sembast.Database> _openDb() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'finding-past-watermark-$_dbCounter.db',
  );
}

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kOtherType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kOtherType,
      ),
    );
}

/// Build B: opens first, naming no view, so its boot has nothing of the
/// view's fingerprint to consider for deletion.
Future<EventStore> _openWithoutView(SembastBackend backend) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: _entryTypes(),
      source: const Source(
        hopId: 'peer-hop',
        identifier: 'peer-install',
        softwareVersion: 'event_sourcing_test@0.0.0',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: ProjectionRegistry(),
    );

/// Build A: opens second, registering the view under test, over the same
/// backend build B opened.
Future<EventStore> _openWithView(
  SembastBackend backend, {
  DeliveryTestHooks? hooks,
}) {
  Future<EventStore> open() => EventStore.openForTest(
    storage: backend,
    entryTypes: _entryTypes(),
    source: const Source(
      hopId: 'test-server',
      identifier: 'test-instance-1',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: ProjectionRegistry()..register(_kViewSpec),
  );
  return hooks == null ? open() : runWithDeliveryTestHooks(hooks, open);
}

Future<void> _appendNote(EventStore store, String aggregateId) => store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: 'note',
  eventType: 'finalized',
  data: <String, Object?>{'title': aggregateId},
  initiator: const UserInitiator('u1'),
);

Future<ViewCopyStatus> _progress(EventStore store) async =>
    (await store.reader.viewProgress()).singleWhere(
      (p) => p.viewName == _kViewSpec.viewName,
    );

void main() {
  group('a security finding past the watermark', () {
    // Verifies: EVS-DEV-converging-view-reads/B
    // Verifies: EVS-DEV-converging-view-reads/D
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-materializer/G
    test("is reported converging with the finding's aggregate unsettled, and "
        'a further append neither folds into the copy nor advances its '
        'watermark past the finding', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);

      final b = await _openWithoutView(backend);
      addTearDown(b.close);
      final a = await _openWithView(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(a.close);

      await _appendNote(a, 'agg-x');
      final beforeRead = await a.reader.readViewRowsByKeys(
        _kViewSpec.viewName,
        {'agg-x'},
      );
      expect(beforeRead.state, ViewConvergenceState.current);
      expect(beforeRead.rows['agg-x'], isA<SettledRow>());

      // Build B records a finding naming agg-x; B does not register the
      // view, so nothing folds this event into it, and it lands past
      // the copy's watermark.
      await b.runTransaction((txn, collector) async {
        await recordFindingInTxnForTest(
          b,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.hashMismatch,
          evidence: const <String, Object?>{
            'event_id': 'fpw-tampered-event',
            'carried_hash': 'fpw-carried-hash',
            'recomputed_hash': 'fpw-recomputed-hash',
          },
          aggregates: const <String>['agg-x'],
        );
      });

      final duringRead = await a.reader.readViewRowsByKeys(
        _kViewSpec.viewName,
        {'agg-x'},
      );
      expect(duringRead.state, ViewConvergenceState.converging);
      expect(duringRead.rows['agg-x'], isA<PendingRow>());

      final progressBefore = await _progress(a);
      await _appendNote(a, 'agg-y');
      final progressAfter = await _progress(a);
      expect(progressAfter.watermark, progressBefore.watermark);

      final afterRead = await a.reader.readViewRowsByKeys(_kViewSpec.viewName, {
        'agg-x',
        'agg-y',
      });
      expect(afterRead.state, ViewConvergenceState.converging);
      expect(afterRead.rows['agg-x'], isA<PendingRow>());
      expect(afterRead.rows['agg-y'], isA<PendingRow>());
    });
  });

  group('a sender-succession event past the watermark', () {
    // Verifies: EVS-DEV-converging-view-reads/D
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-materializer/G
    test("that extends a received finding's reach, stored by a build that "
        'does not register the view and matches no interest, is reported '
        'converging until catch-up folds it, and a further append neither '
        'folds into the copy nor advances its watermark past it', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final b = await _openWithoutView(backend);
      addTearDown(b.close);
      final a = await _openWithView(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(a.close);

      const predecessorId = 'fpw-pred-db';
      const recorderId = 'fpw-recorder-db';

      // A ingests one event of predecessorId's origin chain, creating
      // agg-x's row in A's copy of the view.
      await deliverTo(a, <Map<String, Object?>>[
        chained(predecessorId, 1, aggregateId: 'agg-x'),
      ]);

      // A ingests a received position_reused finding naming
      // predecessorId, at a threshold agg-x's event already meets. No
      // succession event is held yet, so the finding's chain database
      // does not resolve and it marks nothing yet; A folds it inline
      // (the view is current), moving the watermark past it.
      const findingId = 'fpw-received-position-reused';
      final finding = sealedRecord(
        databaseId: recorderId,
        entryType: kSecurityFindingEntryType,
        aggregateType: 'security_finding',
        eventType: kSecurityFindingRecordedEventType,
        aggregateId: findingId,
        data: <String, Object?>{
          'finding_id': findingId,
          'kind': 'position_reused',
          'evidence': <String, Object?>{
            'database_id': predecessorId,
            'origin_sequence_number': 1,
          },
          'aggregates': const <String>[],
          'detector': <String, Object?>{
            'database_id': recorderId,
            'role': 'ingest',
            'library_version': '0.0.0',
          },
        },
      );
      final findingDelivery = await deliverTo(a, <Map<String, Object?>>[
        finding,
      ]);
      final findingAck = findingDelivery.response as ReceiverAcknowledgement;

      final beforeSuccession = await a.reader.readViewRowsByKeys(
        _kViewSpec.viewName,
        {'agg-x'},
      );
      expect(beforeSuccession.state, ViewConvergenceState.current);
      expect(
        (beforeSuccession.rows['agg-x']! as SettledRow).data[r'$integrity'],
        <String, Object?>{'security_findings': <String>[]},
      );

      // Build B delivers the succession event that makes recorderId's
      // lineage include predecessorId, extending the received finding's
      // reach to agg-x. B does not register the view, so this event
      // never folds into it.
      const successionData = SenderSuccessionData(
        id: 'fpw-destination',
        registrationId: 'fpw-registration',
        databaseId: recorderId,
        predecessorDatabaseId: predecessorId,
        predecessorChannels: <SenderSuccessionChannel>[],
      );
      final successionRecord = sealedRecord(
        databaseId: recorderId,
        entryType: kDestinationSenderSucceededEntryType,
        aggregateType: kDestinationAuditAggregateType,
        eventType: kDestinationSenderSucceededEventType,
        data: successionData.toJson(),
      );
      // The finding and the succession event share one originating
      // database (recorderId), so they share one delivery channel; the
      // succession is delivered as the next number on the channel the
      // finding's delivery to build A already advanced (the receiver's
      // channel state lives in the backend the two builds share, not in
      // either build's own process).
      await deliverTo(
        b,
        <Map<String, Object?>>[successionRecord],
        channel: testChannel(recorderId),
        number: findingAck.record.deliveryNumber + 1,
        link: findingAck.record.deliveryHash,
      );

      // A's next local append matches the view's interest. The succession
      // event changed agg-x's marks (it resolves the received finding's
      // chain database), so a correct copy treats it as one it folds and
      // reads as converging until catch-up folds it.
      final progressBefore = await _progress(a);
      await _appendNote(a, 'agg-y');
      final progressAfter = await _progress(a);
      expect(progressAfter.watermark, progressBefore.watermark);

      final afterRead = await a.reader.readViewRowsByKeys(_kViewSpec.viewName, {
        'agg-x',
      });
      expect(afterRead.state, ViewConvergenceState.converging);
      expect(afterRead.rows['agg-x'], isNot(isA<SettledRow>()));

      // A build whose catch-up runs unpaused replays the gap and applies
      // the refresh (EVS-DEV-converging-view-reads/D; EVS-PRD-materializer/G).
      await a.close();
      final resumed = await _openWithView(backend);
      addTearDown(resumed.close);
      await pumpEventQueue(times: 100);
      final resumedRead = await resumed.reader.readViewRowsByKeys(
        _kViewSpec.viewName,
        {'agg-x'},
      );
      expect(resumedRead.state, ViewConvergenceState.current);
      expect(
        (resumedRead.rows['agg-x']! as SettledRow).data[r'$integrity'],
        isNot(<String, Object?>{'security_findings': <String>[]}),
      );
    });
  });

  group('an event of a forked database past the watermark', () {
    // Verifies: EVS-DEV-converging-view-reads/D
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-materializer/E
    // Verifies: EVS-PRD-materializer/G
    test("that shares a held fork_unrecorded finding's predecessor hash, "
        'stored by a build that does not register the view and matches no '
        'interest, is reported converging until catch-up folds it, and a '
        'further append neither folds into the copy nor advances its '
        'watermark past it', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final b = await _openWithoutView(backend);
      addTearDown(b.close);
      final a = await _openWithView(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(a.close);

      const forkedDb = 'fpw-forked-db';

      // A ingests a predecessor and an ordinary successor of it; both
      // match the view's interest and fold inline.
      final predecessor = chained(forkedDb, 1);
      final chain1 = chained(forkedDb, 2, previous: predecessor);
      await deliverTo(a, <Map<String, Object?>>[predecessor, chain1]);

      // A ingests a second successor of the predecessor at another
      // position. Ingest detects the fork against the already-held
      // chain1 and records a fork_unrecorded finding it holds as
      // authored, naming the predecessor's hash; being a security
      // finding event, it folds inline and the copy stays current.
      final fork = chained(forkedDb, 5, previous: predecessor);
      final forkDelivery = await deliverTo(a, <Map<String, Object?>>[fork]);
      final forkAck = forkDelivery.response as ReceiverAcknowledgement;
      final forkAggregateId = fork['aggregate_id']! as String;

      final beforeGap = await a.reader.readViewRowsByKeys(_kViewSpec.viewName, {
        forkAggregateId,
      });
      expect(beforeGap.state, ViewConvergenceState.current);
      expect(
        (beforeGap.rows[forkAggregateId]! as SettledRow).data[r'$integrity'],
        isNot(<String, Object?>{'security_findings': <String>[]}),
      );

      // Build B delivers a third successor sharing the predecessor's
      // hash, of an entry type outside the view's interest. Ingest
      // detects the same fork again but the finding it would record is
      // already held as authored, so no new event is appended: this
      // event is neither a security finding nor a succession event, yet
      // it lowers nothing new about the fork's threshold and still
      // shares its predecessor hash. B does not register the view, so
      // this event never folds into it.
      final secondFork = resealed(
        sealedRecord(databaseId: forkedDb, entryType: _kOtherType),
        <String, Object?>{
          'sequence_number': 8,
          'previous_event_hash': predecessor['event_hash'],
        },
      );
      await deliverTo(
        b,
        <Map<String, Object?>>[secondFork],
        channel: testChannel(forkedDb),
        number: forkAck.record.deliveryNumber + 1,
        link: forkAck.record.deliveryHash,
      );

      // A's next local append matches the view's interest. A correct
      // copy treats the third successor as one it folds for its marks
      // refresh and reads as converging until catch-up folds it.
      final progressBefore = await _progress(a);
      await _appendNote(a, 'agg-y');
      final progressAfter = await _progress(a);
      expect(progressAfter.watermark, progressBefore.watermark);

      final afterRead = await a.reader.readViewRowsByKeys(_kViewSpec.viewName, {
        forkAggregateId,
      });
      expect(afterRead.state, ViewConvergenceState.converging);

      // A build whose catch-up runs unpaused replays the gap; the fork's
      // aggregate keeps its mark from a replay of the log
      // (EVS-DEV-converging-view-reads/D; EVS-PRD-materializer/E&G).
      await a.close();
      final resumed = await _openWithView(backend);
      addTearDown(resumed.close);
      await pumpEventQueue(times: 100);
      final resumedRead = await resumed.reader.readViewRowsByKeys(
        _kViewSpec.viewName,
        {forkAggregateId},
      );
      expect(resumedRead.state, ViewConvergenceState.current);
      expect(
        (resumedRead.rows[forkAggregateId]! as SettledRow).data[r'$integrity'],
        isNot(<String, Object?>{'security_findings': <String>[]}),
      );
    });
  });
}
