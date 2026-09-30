// Two builds share one Sembast backend: build B registers no view, build A
// registers one aggregate view and has its catch-up paused, so only this
// test's own transactions move the copy's watermark.
//
// A received (not held-as-authored) finding that names an aggregate
// directly, rather than through a chain database's threshold, extends its
// reach the moment the finding's own originating database itself authors an
// event of that aggregate: the finding's lineage trivially contains its own
// database. IntegrityMarks.changesOtherMarks must treat such a gap event —
// one outside the view's interest, and neither a finding nor a succession
// event itself — as changing marks other than by identity, mirroring
// _Evaluation.forEvent's received-finding lineage branch; the tests below
// cite the assertions they verify.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
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
  viewName: 'finding_lineage_past_watermark_view',
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// Fails every catch-up step so build A's copy of the view, once behind,
/// stays converging for as long as the hook is installed
/// (EVS-DEV-view-convergence/Q backs the driver off rather than closing the
/// store).
void _pauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure(
      'paused for a finding-lineage-past-watermark test',
    );

var _dbCounter = 0;

Future<sembast.Database> _openDb() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'finding-lineage-past-watermark-$_dbCounter.db',
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
  group(
    'a received finding naming an aggregate directly, past the watermark',
    () {
      // Verifies: EVS-DEV-converging-view-reads/D
      // Verifies: EVS-DEV-view-convergence/E
      // Verifies: EVS-PRD-materializer/G
      test("reports converging once the finding's own originating database "
          "authors an event of the named aggregate outside the view's "
          'interest, and a further append neither folds into the copy nor '
          'advances its watermark past it', () async {
        final db = await _openDb();
        final backend = SembastBackend(database: db);
        final b = await _openWithoutView(backend);
        addTearDown(b.close);
        final a = await _openWithView(
          backend,
          hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
        );
        addTearDown(a.close);

        const earlyDb = 'flw-early-db';
        const recorderId = 'flw-recorder-db';

        // A ingests one event of agg-x authored by a database outside the
        // finding's lineage; it matches the view's interest and folds
        // inline, creating agg-x's row.
        await deliverTo(a, <Map<String, Object?>>[
          chained(earlyDb, 1, aggregateId: 'agg-x'),
        ]);

        // A ingests a received finding naming agg-x directly, originated by
        // recorderId. recorderId's lineage does not yet reach earlyDb (and
        // agg-x has no event of recorderId's own authorship yet), so the
        // finding does not mark agg-x. Being a security finding event, it
        // folds inline regardless of interest and the copy stays current.
        const findingId = 'flw-received-hash-mismatch';
        final finding = sealedRecord(
          databaseId: recorderId,
          entryType: kSecurityFindingEntryType,
          aggregateType: 'security_finding',
          eventType: kSecurityFindingRecordedEventType,
          aggregateId: findingId,
          data: <String, Object?>{
            'finding_id': findingId,
            'kind': 'hash_mismatch',
            'evidence': <String, Object?>{
              'event_id': 'flw-tampered-event',
              'carried_hash': 'flw-carried-hash',
              'recomputed_hash': 'flw-recomputed-hash',
            },
            'aggregates': const <String>['agg-x'],
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

        final beforeGap = await a.reader.readViewRowsByKeys(
          _kViewSpec.viewName,
          {'agg-x'},
        );
        expect(beforeGap.state, ViewConvergenceState.current);
        expect(
          (beforeGap.rows['agg-x']! as SettledRow).data[r'$integrity'],
          <String, Object?>{'security_findings': <String>[]},
        );

        // Build B delivers an event of agg-x authored directly by
        // recorderId — the finding's own originating database, trivially in
        // its own lineage — of an entry type the view's interest does not
        // match. B does not register the view, so this event never folds
        // into it. It is neither a finding nor a succession event, so the
        // currency scan's changesOtherMarks must recognize it through the
        // received-finding lineage branch alone.
        final gapEvent = resealed(
          chained(recorderId, 1, aggregateId: 'agg-x'),
          <String, Object?>{'entry_type': _kOtherType},
        );
        await deliverTo(
          b,
          <Map<String, Object?>>[gapEvent],
          channel: testChannel(recorderId),
          number: findingAck.record.deliveryNumber + 1,
          link: findingAck.record.deliveryHash,
        );

        // A's next local append matches the view's interest. The gap event
        // changed agg-x's marks (it resolves the received finding's
        // lineage), so a correct copy treats it as one it must fold and
        // reads as converging until catch-up folds it.
        final progressBefore = await _progress(a);
        await _appendNote(a, 'agg-y');
        final progressAfter = await _progress(a);
        expect(progressAfter.watermark, progressBefore.watermark);

        final afterRead = await a.reader.readViewRowsByKeys(
          _kViewSpec.viewName,
          {'agg-x'},
        );
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
    },
  );

  group('a held position_reused finding whose chain database authors an event '
      'at its threshold position', () {
    // Verifies: EVS-DEV-converging-view-reads/D
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-materializer/E
    // Verifies: EVS-PRD-materializer/G
    test('stored by a build that does not register the view and matches '
        'no interest, is reported converging until catch-up folds it, and '
        'a further append neither folds into the copy nor advances its '
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

      const chainDb = 'flw-threshold-chain-db';

      // A ingests the chain database's first event, below the finding's
      // threshold; it matches the view's interest and folds inline,
      // creating agg-x's row with no marks.
      final firstRecord = chained(chainDb, 1, aggregateId: 'agg-x');
      final firstDelivery = await deliverTo(a, <Map<String, Object?>>[
        firstRecord,
      ]);
      final firstAck = firstDelivery.response as ReceiverAcknowledgement;

      // A holds a position_reused finding as authored (recorded in A's
      // own transaction), naming chainDb at origin position 2. Held as
      // authored, its chain database resolves without lineage, so this
      // isolates the threshold clause of changesOtherMarks from the fork
      // and lineage clauses: it names no aggregate directly, and its kind
      // is not fork_unrecorded.
      await a.runTransaction((txn, collector) async {
        await recordFindingInTxnForTest(
          a,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.positionReused,
          evidence: const <String, Object?>{
            'database_id': chainDb,
            'origin_sequence_number': 2,
          },
          aggregates: const <String>[],
        );
      });

      final beforeGap = await a.reader.readViewRowsByKeys(_kViewSpec.viewName, {
        'agg-x',
      });
      expect(beforeGap.state, ViewConvergenceState.current);
      expect(
        (beforeGap.rows['agg-x']! as SettledRow).data[r'$integrity'],
        <String, Object?>{'security_findings': <String>[]},
      );

      // Build B delivers chainDb's next event, at the finding's threshold
      // position, of an entry type the view's interest does not match. B
      // does not register the view, so this event never folds into it.
      // It is neither a finding nor a succession event and shares no
      // predecessor hash with a fork, so the currency scan's
      // changesOtherMarks must recognize it through the threshold clause
      // alone.
      final gapEvent = resealed(
        chained(chainDb, 2, previous: firstRecord, aggregateId: 'agg-x'),
        <String, Object?>{'entry_type': _kOtherType},
      );
      await deliverTo(
        b,
        <Map<String, Object?>>[gapEvent],
        channel: testChannel(chainDb),
        number: firstAck.record.deliveryNumber + 1,
        link: firstAck.record.deliveryHash,
      );

      // A's next local append matches the view's interest. The gap event
      // changed agg-x's marks (it meets the finding's threshold), so a
      // correct copy treats it as one it must fold and reads as
      // converging until catch-up folds it.
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
}
