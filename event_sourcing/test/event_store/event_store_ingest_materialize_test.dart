// Verifies: REQ-d00121-K, REQ-d00145-N, REQ-d00154-D — receivers project
//   ingested events into materialized views identically to local-appended
//   events. The materializer loop on the ingest path is symmetric with
//   the loop on the append path (same gates, same atomicity, same
//   throw-rolls-back semantics). Closes Phase 4.9 design spec §398.
//
// Rewritten in Task 21 (CUR-1317) to use a toy in-test materializer
// (_ToyMaterializer / _RecordingMaterializer) instead of the deleted
// DiaryEntriesMaterializer. The substrate behavior under test is identical;
// only the fixture changed.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

// ---------------------------------------------------------------------------
// Toy materializer — folds any event into 'toy_view' keyed on aggregate_id,
// storing the latest event_id and the promoted data's 'answers' map.
// ---------------------------------------------------------------------------

class _ToyMaterializer extends Materializer {
  const _ToyMaterializer();

  @override
  String get viewName => 'toy_view';

  @override
  bool appliesTo(StoredEvent event) => true;

  @override
  EntryPromoter get promoter => identityPromoter;

  @override
  Future<void> applyInTxn(
    Txn txn,
    StorageBackend backend, {
    required StoredEvent event,
    required Map<String, Object?> promotedData,
    required EntryTypeDefinition def,
    required List<StoredEvent> aggregateHistory,
  }) async {
    final answers = promotedData['answers'] as Map<String, Object?>? ?? {};
    await backend
        .upsertViewRowInTxn(txn, viewName, event.aggregateId, <String, Object?>{
          'aggregate_id': event.aggregateId,
          'latest_event_id': event.eventId,
          'answers': answers,
          'is_complete': event.eventType == 'finalized',
          'is_deleted': event.eventType == 'tombstone',
        });
  }
}

/// Recording materializer that captures every `applyInTxn` invocation.
/// Used to assert the materializer is invoked (or not invoked) on the
/// ingest path.
class _RecordingMaterializer implements Materializer {
  _RecordingMaterializer();
  final List<StoredEvent> applied = <StoredEvent>[];

  @override
  String get viewName => 'recording_view';

  @override
  bool appliesTo(StoredEvent event) => true;

  @override
  EntryPromoter get promoter => identityPromoter;

  @override
  Future<int> targetVersionFor(
    Txn txn,
    StorageBackend backend,
    String entryType,
  ) async => 1;

  @override
  Future<void> applyInTxn(
    Txn txn,
    StorageBackend backend, {
    required StoredEvent event,
    required Map<String, Object?> promotedData,
    required EntryTypeDefinition def,
    required List<StoredEvent> aggregateHistory,
  }) async {
    applied.add(event);
  }
}

/// Materializer that throws on the Nth invocation. Used to verify that a
/// materializer throw on the ingest path rolls back the entire batch.
class _ThrowingTestMaterializer implements Materializer {
  _ThrowingTestMaterializer({required this.throwOnCall});
  final int throwOnCall; // 1-indexed call number that triggers the throw
  int callCount = 0;
  final List<StoredEvent> applied = <StoredEvent>[];

  @override
  String get viewName => 'throwing_view';

  @override
  bool appliesTo(StoredEvent event) => true;

  @override
  EntryPromoter get promoter => identityPromoter;

  @override
  Future<int> targetVersionFor(
    Txn txn,
    StorageBackend backend,
    String entryType,
  ) async => 1;

  @override
  Future<void> applyInTxn(
    Txn txn,
    StorageBackend backend, {
    required StoredEvent event,
    required Map<String, Object?> promotedData,
    required EntryTypeDefinition def,
    required List<StoredEvent> aggregateHistory,
  }) async {
    callCount += 1;
    if (callCount == throwOnCall) {
      throw StateError(
        '_ThrowingTestMaterializer: explosion on call $callCount',
      );
    }
    applied.add(event);
  }
}

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
  widgetId: 'w',
  widgetConfig: <String, Object?>{},
);

Future<_Fixture> _openDatastore({
  String hopId = 'mobile-device',
  String identifier = 'device-1',
  String softwareVersion = 'clinical_diary@1.0.0',
  List<Materializer> materializers = const <Materializer>[_ToyMaterializer()],
  Map<String, Map<String, int>> initialViewTargetVersions =
      const <String, Map<String, int>>{
        'toy_view': <String, int>{'demo_note': 1},
      },
}) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-mat-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final datastore = await bootstrapAppendOnlyDatastore(
    backend: backend,
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: softwareVersion,
    ),
    entryTypes: const <EntryTypeDefinition>[_demoNoteDef],
    destinations: const <Destination>[],
    materializers: materializers,
    initialViewTargetVersions: initialViewTargetVersions,
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
  group(
    'EventStore ingest path materializer loop (REQ-d00121-K, REQ-d00145-N)',
    () {
      // Verifies: REQ-d00121-K, REQ-d00145-N — ingestEvent fires materializers
      //   per-event with the same gates as local-append.
      test('REQ-d00121-K + REQ-d00145-N: ingestEvent populates toy_view '
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
            entryTypeVersion: 1,
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
              .where((r) => r['aggregate_id'] == 'agg-ingest-1')
              .toList();
          expect(preUser, isEmpty);

          // Ingest the originator's event at the receiver.
          final outcome = await dest.datastore.eventStore.ingestEvent(
            original!,
          );
          expect(outcome.outcome, equals(IngestOutcome.ingested));

          // Post-ingest: receiver has one toy_view row reflecting the
          // ingested event's answers.
          final postRows = await dest.backend.findViewRows('toy_view');
          final postUser = postRows
              .where((r) => r['aggregate_id'] == 'agg-ingest-1')
              .toList();
          expect(postUser, hasLength(1));
          final row = postUser.first;
          expect(row['latest_event_id'], equals(original.eventId));
          expect(row['is_complete'], isTrue);
          expect(row['is_deleted'], isFalse);
          final answers = row['answers'] as Map<String, Object?>;
          expect(answers['title'], equals('hello'));
          expect(answers['body'], equals('world'));
        } finally {
          await orig.close();
          await dest.close();
        }
      });

      // Verifies: REQ-d00121-K — ingestBatch projects each event in the batch
      //   into the toy_view view atomically with the event log write.
      test('REQ-d00121-K: ingestBatch projects each event in batch into '
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
          // Originate three distinct demo_note finalized events.
          final e1 = await orig.datastore.eventStore.append(
            entryType: 'demo_note',
            entryTypeVersion: 1,
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
            entryTypeVersion: 1,
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
            entryTypeVersion: 1,
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
          // Only user-event rows (exclude any system-aggregate rows by
          // filtering on known aggregate ids).
          final userRows = rows
              .where(
                (r) =>
                    r['aggregate_id'] == 'agg-batch-A' ||
                    r['aggregate_id'] == 'agg-batch-B' ||
                    r['aggregate_id'] == 'agg-batch-C',
              )
              .toList();
          expect(userRows, hasLength(3));
          final byId = <String, Map<String, Object?>>{
            for (final r in userRows) r['aggregate_id'] as String: r,
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

      // Verifies: REQ-d00145-A + REQ-d00121-K — a materializer throw rolls
      //   back the entire batch (event log AND view writes).
      test('REQ-d00145-A + REQ-d00121-K: materializer throw rolls back entire '
          'ingestBatch (no events landed, no view rows)', () async {
        final orig = await _openDatastore(
          hopId: 'mobile-device',
          identifier: 'device-1',
        );
        final throwing = _ThrowingTestMaterializer(throwOnCall: 2);
        final dest = await _openDatastore(
          hopId: 'portal-server',
          identifier: 'portal-1',
          softwareVersion: 'portal@0.1.0',
          materializers: <Materializer>[throwing],
          initialViewTargetVersions: const <String, Map<String, int>>{
            'throwing_view': <String, int>{'demo_note': 1},
          },
        );

        try {
          // Originate three events on a non-throwing sender.
          final e1 = await orig.datastore.eventStore.append(
            entryType: 'demo_note',
            entryTypeVersion: 1,
            aggregateId: 'agg-rb-1',
            aggregateType: 'SampleAggregate',
            eventType: 'finalized',
            data: const <String, Object?>{'answers': <String, Object?>{}},
            initiator: const UserInitiator('u'),
          );
          final e2 = await orig.datastore.eventStore.append(
            entryType: 'demo_note',
            entryTypeVersion: 1,
            aggregateId: 'agg-rb-2',
            aggregateType: 'SampleAggregate',
            eventType: 'finalized',
            data: const <String, Object?>{'answers': <String, Object?>{}},
            initiator: const UserInitiator('u'),
          );
          final e3 = await orig.datastore.eventStore.append(
            entryType: 'demo_note',
            entryTypeVersion: 1,
            aggregateId: 'agg-rb-3',
            aggregateType: 'SampleAggregate',
            eventType: 'finalized',
            data: const <String, Object?>{'answers': <String, Object?>{}},
            initiator: const UserInitiator('u'),
          );

          final envelope = _buildEnvelope(
            <StoredEvent>[e1!, e2!, e3!],
            senderHop: 'mobile-device',
            senderIdentifier: 'device-1',
            senderSoftwareVersion: 'clinical_diary@1.0.0',
          );

          // Snapshot the receiver's user-event log before the failed batch.
          final preEvents = (await dest.backend.findAllEvents())
              .where((e) => !kReservedSystemEntryTypeIds.contains(e.entryType))
              .toList();
          expect(preEvents, isEmpty);

          // Ingest the batch; the throwing materializer fires on the
          // second event and rolls back the whole transaction.
          await expectLater(
            dest.datastore.eventStore.ingestBatch(
              envelope.encode(),
              wireFormat: BatchEnvelope.wireFormat,
            ),
            throwsA(isA<StateError>()),
          );

          // No user events landed.
          final postEvents = (await dest.backend.findAllEvents())
              .where((e) => !kReservedSystemEntryTypeIds.contains(e.entryType))
              .toList();
          expect(postEvents, isEmpty);

          // No view rows for the rolled-back aggregates.
          final rows = await dest.backend.findViewRows('throwing_view');
          final userRows = rows
              .where(
                (r) =>
                    r['aggregate_id'] == 'agg-rb-1' ||
                    r['aggregate_id'] == 'agg-rb-2' ||
                    r['aggregate_id'] == 'agg-rb-3',
              )
              .toList();
          expect(userRows, isEmpty);
        } finally {
          await orig.close();
          await dest.close();
        }
      });

      // Verifies: REQ-d00154-D — ingested system events do NOT fire
      //   materializers because their EntryTypeDefinitions ship
      //   `materialize: false`. The outer gate (`def.materialize`)
      //   short-circuits before the inner `appliesTo` check is reached.
      test('REQ-d00154-D: ingested system events do NOT fire materializers '
          '(def.materialize:false short-circuits the outer gate)', () async {
        // Recording materializer on the receiver. We never expect it to
        // fire because the synthetic batch carries only a system event
        // whose EntryTypeDefinition has `materialize: false`.
        final recording = _RecordingMaterializer();
        final dest = await _openDatastore(
          hopId: 'portal-server',
          identifier: 'portal-1',
          softwareVersion: 'portal@0.1.0',
          materializers: <Materializer>[recording],
          initialViewTargetVersions: const <String, Map<String, int>>{
            'recording_view': <String, int>{'demo_note': 1},
          },
        );

        // Bootstrap a sender pane — its bootstrap step already emits a
        // `system.entry_type_registry_initialized` event under the
        // sender's source.identifier aggregate (REQ-d00134-E,
        // REQ-d00154-D). Read that event off the sender's log and ship
        // it to the receiver via ingestBatch.
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

        final initialApplied = recording.applied.length;

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
          // The system event was admitted into the event log on the
          // receiver — the lib's gate is materializer-only, not ingest-
          // wide. (See REQ-d00154-E receiver-stays-passive — write-side
          // ingest is independent of registry mutation.)
          expect(result.events, hasLength(1));
          expect(
            result.events.first.outcome,
            anyOf(
              equals(IngestOutcome.ingested),
              equals(IngestOutcome.duplicate),
            ),
          );

          // Materializer was NOT fired for the system event.
          expect(
            recording.applied.length,
            equals(initialApplied),
            reason:
                'system entry type ships materialize:false; outer gate '
                'must short-circuit the materializer loop on ingest.',
          );
        } finally {
          await sender.close();
          await dest.close();
        }
      });
    },
  );
}
