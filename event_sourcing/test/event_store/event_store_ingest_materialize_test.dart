// Verifies: EVS-PRD-ingest/E — the ingest path projects ingested events into
//   materialized views identically to local-appended events. The projection
//   interpreter on the ingest path is symmetric with the interpreter on the
//   append path (same gates, same atomicity, same throw-rolls-back semantics).
//   Closes Phase 4.9 design spec §398.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

// ---------------------------------------------------------------------------
// Toy ProjectionSpec — AggregateProjectionSpec that folds any 'demo_note'
// event into 'toy_view', storing latest event_id, answers, is_complete,
// is_deleted.
// ---------------------------------------------------------------------------

const _kToyViewSpec = AggregateProjectionSpec(
  viewName: 'toy_view',
  interest: SubscriptionFilter(entryTypes: <String>{'demo_note'}),
  tombstoneEventTypes: <String>{'tombstone'},
);

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

var _dbCounter = 0;

class _Fixture {
  _Fixture({required this.datastore, required this.backend});
  final AppendOnlyDatastore datastore;
  final SembastBackend backend;
  Future<void> close() => backend.close();
}

const EntryTypeDefinition _demoNoteDef = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: 1,
  name: 'Demo Note',
);

Future<_Fixture> _openDatastore({
  String hopId = 'mobile-device',
  String identifier = 'device-1',
  String softwareVersion = 'clinical_diary@1.0.0',
  ProjectionRegistry? projections,
}) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-mat-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final effectiveProjections =
      projections ?? (ProjectionRegistry()..register(_kToyViewSpec));
  final datastore = await bootstrapAppendOnlyDatastore(
    backend: backend,
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: softwareVersion,
    ),
    entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
    destinations: const <Destination>[],
    projections: effectiveProjections,
  );
  return _Fixture(datastore: datastore, backend: backend);
}

BatchEnvelope _buildEnvelope(
  List<StoredEvent> events, {
  required String senderHop,
  required String senderIdentifier,
  required String senderSoftwareVersion,
}) {
  return BatchEnvelope(
    batchFormatVersion: '1',
    batchId: const Uuid().v4(),
    senderHop: senderHop,
    senderIdentifier: senderIdentifier,
    senderSoftwareVersion: senderSoftwareVersion,
    sentAt: DateTime.now().toUtc(),
    events: events.map((e) => Map<String, Object?>.from(e.toMap())).toList(),
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('EventStore ingest path projection interpreter', () {
    //   projection interpreter per-event with the same gates as local-append.
    test('ingestEvent populates toy_view '
        'from a freshly-ingested user event', () async {
      final orig = await _openDatastore(
        hopId: 'mobile-device',
        identifier: 'device-1',
      );
      final dest = await _openDatastore(
        hopId: 'portal-server',
        identifier: 'portal-1',
        softwareVersion: 'portal@0.1.0',
      );

      try {
        // Originate an event on the sender; it materializes locally.
        final original = await orig.datastore.eventStore.append(
          entryType: 'demo_note',
          aggregateId: 'agg-ingest-1',
          aggregateType: 'SampleAggregate',
          eventType: 'finalized',
          data: const <String, Object?>{
            'answers': <String, Object?>{'title': 'hello', 'body': 'world'},
          },
          initiator: const UserInitiator('u-orig'),
        );
        expect(original, isNotNull);

        // Pre-ingest: receiver has no toy_view rows for this aggregate.
        final preRows = await dest.backend.findViewRows('toy_view');
        final preUser = preRows
            .where((r) => r['latestEventId'] == original!.eventId)
            .toList();
        expect(preUser, isEmpty);

        // Ingest the originator's event at the receiver.
        final outcome = await dest.datastore.eventStore.ingestEvent(original!);
        expect(outcome.outcome, equals(IngestOutcome.ingested));

        // Post-ingest: receiver has one toy_view row for this aggregate.
        final postRows = await dest.backend.findViewRows('toy_view');
        final postUser = postRows
            .where(
              (r) =>
                  r['aggregateId'] == 'agg-ingest-1' ||
                  r['latestEventId'] == original.eventId,
            )
            .toList();
        expect(postUser, hasLength(1));
        final row = postUser.first;
        // AggregateProjectionSpec stores the answers map inline.
        final answers = row['answers'] as Map<String, Object?>;
        expect(answers['title'], equals('hello'));
        expect(answers['body'], equals('world'));
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    //   into the toy_view view atomically with the event log write.
    test('ingestBatch projects each event in batch into '
        'toy_view', () async {
      final orig = await _openDatastore(
        hopId: 'mobile-device',
        identifier: 'device-1',
      );
      final dest = await _openDatastore(
        hopId: 'portal-server',
        identifier: 'portal-1',
        softwareVersion: 'portal@0.1.0',
      );

      try {
        final e1 = await orig.datastore.eventStore.append(
          entryType: 'demo_note',
          aggregateId: 'agg-batch-A',
          aggregateType: 'SampleAggregate',
          eventType: 'finalized',
          data: const <String, Object?>{
            'answers': <String, Object?>{'idx': 'a'},
          },
          initiator: const UserInitiator('u'),
        );
        final e2 = await orig.datastore.eventStore.append(
          entryType: 'demo_note',
          aggregateId: 'agg-batch-B',
          aggregateType: 'SampleAggregate',
          eventType: 'finalized',
          data: const <String, Object?>{
            'answers': <String, Object?>{'idx': 'b'},
          },
          initiator: const UserInitiator('u'),
        );
        final e3 = await orig.datastore.eventStore.append(
          entryType: 'demo_note',
          aggregateId: 'agg-batch-C',
          aggregateType: 'SampleAggregate',
          eventType: 'finalized',
          data: const <String, Object?>{
            'answers': <String, Object?>{'idx': 'c'},
          },
          initiator: const UserInitiator('u'),
        );
        expect(e1, isNotNull);
        expect(e2, isNotNull);
        expect(e3, isNotNull);

        final envelope = _buildEnvelope(
          <StoredEvent>[e1!, e2!, e3!],
          senderHop: 'mobile-device',
          senderIdentifier: 'device-1',
          senderSoftwareVersion: 'clinical_diary@1.0.0',
        );

        final result = await dest.datastore.eventStore.ingestBatch(
          envelope.encode(),
          wireFormat: BatchEnvelope.wireFormat,
        );
        expect(result.events, hasLength(3));
        for (final outcome in result.events) {
          expect(outcome.outcome, equals(IngestOutcome.ingested));
        }

        final rows = await dest.backend.findViewRows('toy_view');
        // Only user-event rows (exclude any system-aggregate rows).
        final userRows = rows
            .where(
              (r) =>
                  r['aggregateId'] == 'agg-batch-A' ||
                  r['aggregateId'] == 'agg-batch-B' ||
                  r['aggregateId'] == 'agg-batch-C',
            )
            .toList();
        expect(userRows, hasLength(3));
        final byId = <String, Map<String, Object?>>{
          for (final r in userRows) r['aggregateId'] as String: r,
        };
        final aAnswers =
            byId['agg-batch-A']!['answers'] as Map<String, Object?>;
        final bAnswers =
            byId['agg-batch-B']!['answers'] as Map<String, Object?>;
        final cAnswers =
            byId['agg-batch-C']!['answers'] as Map<String, Object?>;
        expect(aAnswers['idx'], equals('a'));
        expect(bAnswers['idx'], equals('b'));
        expect(cAnswers['idx'], equals('c'));
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    //   the projection interpreter because the toy_view spec's
    //   SubscriptionFilter.entryTypes excludes system entry types.
    test('ingested system events do NOT populate toy_view '
        '(excluded by SubscriptionFilter)', () async {
      final dest = await _openDatastore(
        hopId: 'portal-server',
        identifier: 'portal-1',
        softwareVersion: 'portal@0.1.0',
      );

      final sender = await _openDatastore(
        hopId: 'mobile-device',
        identifier: 'sender-id-1',
      );
      final senderEvents = await sender.backend.findAllEvents();
      final senderSystemEvent = senderEvents.firstWhere(
        (e) => e.entryType == kEntryTypeRegistryInitializedEntryType,
      );
      expect(
        kReservedSystemEntryTypeIds.contains(senderSystemEvent.entryType),
        isTrue,
        reason: 'precondition: sender system event must be a reserved id',
      );

      // Snapshot toy_view row count before ingest.
      final preRows = await dest.backend.findViewRows('toy_view');
      final preCount = preRows.length;

      try {
        final envelope = _buildEnvelope(
          <StoredEvent>[senderSystemEvent],
          senderHop: 'mobile-device',
          senderIdentifier: 'sender-id-1',
          senderSoftwareVersion: 'clinical_diary@1.0.0',
        );

        final result = await dest.datastore.eventStore.ingestBatch(
          envelope.encode(),
          wireFormat: BatchEnvelope.wireFormat,
        );
        expect(result.events, hasLength(1));
        expect(
          result.events.first.outcome,
          anyOf(
            equals(IngestOutcome.ingested),
            equals(IngestOutcome.duplicate),
          ),
        );

        // toy_view row count unchanged — system event excluded by filter.
        final postRows = await dest.backend.findViewRows('toy_view');
        expect(
          postRows.length,
          equals(preCount),
          reason:
              'system entry type is not in toy_view SubscriptionFilter; '
              'no new rows should appear.',
        );
      } finally {
        await sender.close();
        await dest.close();
      }
    });
  });
}
