// Verifies: REQ-d00140-D — rebuildView per-view, idempotent; materializer-
//   parameterized replay; strict-superset target-version map.
//
// Rewritten in Task 21 (CUR-1317) to use a toy in-test materializer
// (_ToyMaterializer) in place of the deleted DiaryEntriesMaterializer.
// rebuildMaterializedView() tests are removed (that function was deleted as
// diary-specific extraction debt). The substrate property under test
// (rebuildView generic contract) is unchanged.

import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/materialization/entry_promoter.dart';
import 'package:event_sourcing/src/materialization/entry_type_definition_lookup.dart';
import 'package:event_sourcing/src/materialization/materializer.dart';
import 'package:event_sourcing/src/materialization/rebuild.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/map_entry_type_definition_lookup.dart';

// ---------------------------------------------------------------------------
// Toy materializer — folds any event into 'toy_view' keyed on aggregate_id.
// Stores the latest event_id and answers map so tests can assert on view
// contents after a rebuildView run.
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
    // Read prior row so we can merge answers (simulates a real materializer
    // accumulating state across multiple events on the same aggregate).
    final prior = await backend.readViewRowInTxn(
      txn,
      viewName,
      event.aggregateId,
    );
    final priorAnswers =
        (prior?['answers'] as Map?)?.cast<String, Object?>() ?? {};
    final newAnswers = Map<String, Object?>.from(priorAnswers);
    final eventAnswers =
        (promotedData['answers'] as Map?)?.cast<String, Object?>() ?? {};
    newAnswers.addAll(eventAnswers);

    await backend
        .upsertViewRowInTxn(txn, viewName, event.aggregateId, <String, Object?>{
          'aggregate_id': event.aggregateId,
          'latest_event_id': event.eventId,
          'answers': newAnswers,
          'is_complete': event.eventType == 'finalized',
          'is_deleted': event.eventType == 'tombstone',
        });
  }
}

void main() {
  group('rebuildView (parameterized) — REQ-d00140-D', () {
    late SembastBackend backend;
    var counter = 0;

    setUp(() async {
      counter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'rebuild-param-$counter.db',
      );
      backend = SembastBackend(database: db);
    });

    tearDown(() async {
      await backend.close();
    });

    EntryTypeDefinition defFor(String id) => EntryTypeDefinition(
      id: id,
      registeredVersion: 1,
      name: id,
      widgetId: 'w',
      widgetConfig: const <String, Object?>{},
    );

    MapEntryTypeDefinitionLookup lookupFor(List<EntryTypeDefinition> defs) =>
        MapEntryTypeDefinitionLookup.fromDefinitions(defs);

    Future<void> appendEvent({
      required String eventId,
      required String aggregateId,
      required String entryType,
      required String eventType,
      required Map<String, dynamic> data,
      required DateTime clientTimestamp,
    }) async {
      await backend.transaction<void>((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          StoredEvent(
            key: 0,
            eventId: eventId,
            aggregateId: aggregateId,
            aggregateType: 'SampleAggregate',
            entryType: entryType,
            entryTypeVersion: 1,
            libFormatVersion: 1,
            eventType: eventType,
            sequenceNumber: seq,
            data: data,
            metadata: const <String, dynamic>{},
            initiator: const UserInitiator('u1'),
            clientTimestamp: clientTimestamp,
            eventHash: 'hash-$eventId',
          ),
        );
      });
    }

    // Verifies: REQ-d00140-D — strict-superset failure raises ArgumentError
    // when a previously-registered entry type is omitted from the supplied
    // map, and no destructive write happens.
    test(
      'REQ-d00140-D: strict-superset failure on missing existing entry type',
      () async {
        // Seed an existing target-version entry.
        await backend.transaction((txn) async {
          await backend.writeViewTargetVersionInTxn(
            txn,
            'toy_view',
            'sample_event',
            1,
          );
          await backend.writeViewTargetVersionInTxn(
            txn,
            'toy_view',
            'other_event',
            1,
          );
        });
        const m = _ToyMaterializer();
        await expectLater(
          rebuildView(
            m,
            backend,
            lookupFor([defFor('sample_event'), defFor('other_event')]),
            // 'other_event' missing — strict-superset violation.
            targetVersionByEntryType: const <String, int>{'sample_event': 1},
          ),
          throwsArgumentError,
        );
        // Existing entries remain.
        final stored = await backend.transaction<Map<String, int>>(
          (txn) async =>
              backend.readAllViewTargetVersionsInTxn(txn, 'toy_view'),
        );
        expect(stored.containsKey('other_event'), isTrue);
      },
    );

    // Verifies: REQ-d00140-D — adding a new entry type during rebuild is
    // allowed (strict superset).
    test('REQ-d00140-D: superset accept — new entry type added', () async {
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(
          txn,
          'toy_view',
          'sample_event',
          1,
        );
      });
      await appendEvent(
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{
          'answers': <String, Object?>{'intensity': 'mild'},
        },
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );
      const m = _ToyMaterializer();
      final processed = await rebuildView(
        m,
        backend,
        lookupFor([defFor('sample_event'), defFor('newcomer_type')]),
        targetVersionByEntryType: const <String, int>{
          'sample_event': 1,
          'newcomer_type': 2, // brand new — allowed
        },
      );
      expect(processed, 1);
      final stored = await backend.transaction<Map<String, int>>(
        (txn) async => backend.readAllViewTargetVersionsInTxn(txn, 'toy_view'),
      );
      expect(stored, <String, int>{'sample_event': 1, 'newcomer_type': 2});
    });

    // Verifies: REQ-d00140-D — running rebuild twice with the same map
    // produces the same view rows (idempotent).
    test('REQ-d00140-D: idempotent rebuild', () async {
      await appendEvent(
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{
          'answers': <String, Object?>{'intensity': 'mild'},
        },
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );
      const m = _ToyMaterializer();
      final lookup = lookupFor([defFor('sample_event')]);
      const map = <String, int>{'sample_event': 1};
      final first = await rebuildView(
        m,
        backend,
        lookup,
        targetVersionByEntryType: map,
      );
      final firstRows = await backend.findViewRows('toy_view');
      final second = await rebuildView(
        m,
        backend,
        lookup,
        targetVersionByEntryType: map,
      );
      final secondRows = await backend.findViewRows('toy_view');
      expect(first, second);
      expect(firstRows.length, secondRows.length);
      // Both runs produce the same row contents.
      final firstRow = firstRows.single;
      final secondRow = secondRows.single;
      expect(firstRow['aggregate_id'], equals(secondRow['aggregate_id']));
      expect(firstRow['latest_event_id'], equals(secondRow['latest_event_id']));
    });

    // Verifies: REQ-d00140-D — an event in the log whose entry_type is not
    // in the supplied map raises ArgumentError. Every materialized event
    // needs a target version to promote toward.
    test(
      'REQ-d00140-D: unmapped event entry-type raises ArgumentError',
      () async {
        await appendEvent(
          eventId: 'e1',
          aggregateId: 'agg-1',
          entryType: 'sample_event',
          eventType: 'finalized',
          data: const <String, dynamic>{
            'answers': <String, Object?>{'intensity': 'mild'},
          },
          clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
        );
        const m = _ToyMaterializer();
        await expectLater(
          rebuildView(
            m,
            backend,
            lookupFor([defFor('sample_event')]),
            // Map is non-empty, but does not cover the event's entry_type.
            // Strict-superset check passes (no existing rows), but the
            // per-event lookup fails.
            targetVersionByEntryType: const <String, int>{'unrelated': 1},
          ),
          throwsArgumentError,
        );
      },
    );

    // Verifies: REQ-d00140-D — rebuildView clears the view and rewrites
    // view_target_versions atomically; view rows absent from the rebuilt
    // event log do not survive.
    test('REQ-d00140-D: rebuild removes prior view rows not derivable from '
        'the event log', () async {
      // Seed toy_view with a garbage row not backed by any event.
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(
          txn,
          'toy_view',
          'garbage-agg',
          <String, Object?>{'aggregate_id': 'garbage-agg', 'garbage': true},
        );
      });
      // One legitimate event on agg-1.
      await appendEvent(
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{
          'answers': <String, Object?>{'intensity': 'mild'},
        },
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );

      const m = _ToyMaterializer();
      final processed = await rebuildView(
        m,
        backend,
        lookupFor([defFor('sample_event')]),
        targetVersionByEntryType: const <String, int>{'sample_event': 1},
      );

      expect(processed, 1);
      final rows = await backend.findViewRows('toy_view');
      expect(rows, hasLength(1));
      expect(rows.single['aggregate_id'], equals('agg-1'));
      expect(
        rows.map((r) => r['aggregate_id']),
        isNot(contains('garbage-agg')),
      );
    });

    // Verifies: REQ-d00140-D — rebuildView processes all events correctly
    // when the log spans multiple streaming chunks (per-aggregate state
    // accumulates across chunk boundaries).
    test('REQ-d00140-D: large event log spanning multiple chunks rebuilds '
        'correctly — no events dropped at chunk boundaries', () async {
      // 1,250 events on two aggregates — comfortably larger than the
      // 500-event chunk. The toy materializer merges answers; the final
      // row for each aggregate reflects the last event's answers map.
      const totalEvents = 1250;
      for (var i = 0; i < totalEvents; i++) {
        final aggregateId = i.isEven ? 'agg-even' : 'agg-odd';
        final isLast = i == totalEvents - 1 || i == totalEvents - 2;
        await appendEvent(
          eventId: 'ev-$i',
          aggregateId: aggregateId,
          entryType: 'sample_event',
          eventType: isLast ? 'finalized' : 'checkpoint',
          data: <String, dynamic>{
            'answers': <String, Object?>{'index': i},
          },
          clientTimestamp: DateTime.utc(
            2026,
            4,
            22,
            10,
          ).add(Duration(seconds: i)),
        );
      }

      const m = _ToyMaterializer();
      final processed = await rebuildView(
        m,
        backend,
        lookupFor([defFor('sample_event')]),
        targetVersionByEntryType: const <String, int>{'sample_event': 1},
      );
      expect(processed, totalEvents);

      final rows = await backend.findViewRows('toy_view');
      expect(rows, hasLength(2));
      final byId = <String, Map<String, Object?>>{
        for (final r in rows) r['aggregate_id'] as String: r,
      };
      // Each aggregate's last event wrote {'index': N}. With merge
      // semantics the final row reflects the last event's answers.
      final oddAnswers = byId['agg-odd']!['answers'] as Map<String, Object?>;
      final evenAnswers = byId['agg-even']!['answers'] as Map<String, Object?>;
      expect(oddAnswers['index'], totalEvents - 1); // 1249 (odd)
      expect(evenAnswers['index'], totalEvents - 2); // 1248 (even)
      expect(byId['agg-odd']!['is_complete'], isTrue);
      expect(byId['agg-even']!['is_complete'], isTrue);
    });
  });
}
