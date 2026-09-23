// Verifies: EVS-PRD-materializer/A
// rebuildView replays the event log
//   through a registered ProjectionSpec to reconstruct a view from scratch;
//   it is a library-supplied materializer helper.
// Verifies: EVS-PRD-materializer/B
// rebuild is deterministic and idempotent;
//   tests confirm identical rows across two consecutive rebuilds on the same
//   log, as well as cross-chunk correctness for large logs.
// Verifies: EVS-PRD-destinations/K
// the view rebuild writes only target
//   versions derived from the entry-type registry: a target that differs
//   from its entry type's registered version, or names an unregistered entry
//   type, is refused before any write and the view is left untouched.
//
// ProjectionSpec replay; strict-superset target-version map.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var _dbCounter = 0;

const _kEntryType = 'sample_event';

const _kAggSpec = AggregateProjectionSpec(
  viewName: 'toy_view',
  interest: SubscriptionFilter(entryTypes: <String>{_kEntryType}),
  tombstoneEventTypes: <String>{'tombstone'},
);

Future<EventStore> _openStore() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'rebuild-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final proj = ProjectionRegistry()..register(_kAggSpec);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kEntryType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kEntryType,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'other_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'other_event',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'newcomer_type',
        registeredVersion: EntryTypeVersion(2, 0),
        name: 'newcomer_type',
      ),
    );
  return EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-device',
      softwareVersion: 't',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: proj,
  );
}

Future<void> _appendEvent(
  EventStore store, {
  required String eventId,
  required String aggregateId,
  required String entryType,
  required String eventType,
  required Map<String, dynamic> data,
  required DateTime clientTimestamp,
}) async {
  await store.backend.transaction<void>((txn) async {
    final seq = await store.backend.nextSequenceNumber(txn);
    await store.backend.appendEvent(
      txn,
      StoredEvent(
        key: 0,
        eventId: eventId,
        aggregateId: aggregateId,
        aggregateType: 'SampleAggregate',
        entryType: entryType,
        entryTypeVersion: const EntryTypeVersion(1, 0),
        libFormatVersion: const DataFormatVersion(2, 0),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('rebuildView (ProjectionSpec-based)`', () {
    // when a previously-registered entry type is omitted from the supplied
    // map, and no destructive write happens.
    test('strict-superset failure on missing existing entry type', () async {
      final store = await _openStore();
      // Seed an existing target-version entry for two entry types.
      await store.backend.transaction((txn) async {
        await store.backend.writeViewTargetVersionInTxn(
          txn,
          'toy_view',
          'sample_event',
          const EntryTypeVersion(1, 0),
        );
        await store.backend.writeViewTargetVersionInTxn(
          txn,
          'toy_view',
          'other_event',
          const EntryTypeVersion(1, 0),
        );
      });
      await expectLater(
        rebuildView(
          store: store,
          viewName: 'toy_view',
          // 'other_event' missing — strict-superset violation.
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            'sample_event': EntryTypeVersion(1, 0),
          },
        ),
        throwsArgumentError,
      );
      // Existing entries remain.
      final stored = await store.backend
          .transaction<Map<String, EntryTypeVersion>>(
            (txn) async =>
                store.backend.readAllViewTargetVersionsInTxn(txn, 'toy_view'),
          );
      expect(stored.containsKey('other_event'), isTrue);
      await store.backend.close();
    });

    for (final (label, targets) in <(String, Map<String, EntryTypeVersion>)>[
      (
        'a target below the registered version',
        <String, EntryTypeVersion>{
          _kEntryType: const EntryTypeVersion(1, 0),
          'newcomer_type': const EntryTypeVersion(1, 0),
        },
      ),
      (
        'a target above the registered version',
        <String, EntryTypeVersion>{_kEntryType: const EntryTypeVersion(2, 0)},
      ),
      (
        'an unregistered entry type',
        <String, EntryTypeVersion>{
          _kEntryType: const EntryTypeVersion(1, 0),
          'bogus': const EntryTypeVersion(7, 0),
        },
      ),
    ]) {
      test('$label is refused before any write', () async {
        final store = await _openStore();
        await _appendEvent(
          store,
          eventId: 'e1',
          aggregateId: 'agg-1',
          entryType: _kEntryType,
          eventType: 'finalized',
          data: const <String, dynamic>{'intensity': 'mild'},
          clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
        );
        await rebuildView(
          store: store,
          viewName: 'toy_view',
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kEntryType: EntryTypeVersion(1, 0),
          },
        );
        final rowsBefore = await store.backend.findViewRows('toy_view');
        Future<Map<String, EntryTypeVersion>> storedTargets() =>
            store.backend.transaction<Map<String, EntryTypeVersion>>(
              (txn) =>
                  store.backend.readAllViewTargetVersionsInTxn(txn, 'toy_view'),
            );
        final targetsBefore = await storedTargets();

        await expectLater(
          rebuildView(
            store: store,
            viewName: 'toy_view',
            targetVersionByEntryType: targets,
          ),
          throwsArgumentError,
        );

        expect(await store.backend.findViewRows('toy_view'), rowsBefore);
        expect(await storedTargets(), targetsBefore);
        await store.backend.close();
      });
    }

    // raises StateError.
    test('missing ProjectionSpec raises StateError', () async {
      _dbCounter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'rebuild-no-spec-$_dbCounter.db',
      );
      final backend = SembastBackend(database: db);
      // EventStore with empty ProjectionRegistry — no 'toy_view' spec.
      final store = await EventStore.openForTest(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: const Source(
          hopId: 'test',
          identifier: 'test-device',
          softwareVersion: 't',
        ),
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      await expectLater(
        rebuildView(
          store: store,
          viewName: 'toy_view',
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            'sample_event': EntryTypeVersion(1, 0),
          },
        ),
        throwsStateError,
      );
      await store.backend.close();
    });

    // allowed (strict superset).
    test('superset accept — new entry type added', () async {
      final store = await _openStore();
      await store.backend.transaction((txn) async {
        await store.backend.writeViewTargetVersionInTxn(
          txn,
          'toy_view',
          'sample_event',
          const EntryTypeVersion(1, 0),
        );
      });
      await _appendEvent(
        store,
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{'intensity': 'mild'},
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );
      final processed = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          'sample_event': EntryTypeVersion(1, 0),
          'newcomer_type': EntryTypeVersion(2, 0), // brand new — allowed
        },
      );
      expect(processed, 1);
      final stored = await store.backend
          .transaction<Map<String, EntryTypeVersion>>(
            (txn) async =>
                store.backend.readAllViewTargetVersionsInTxn(txn, 'toy_view'),
          );
      expect(stored, <String, EntryTypeVersion>{
        'sample_event': const EntryTypeVersion(1, 0),
        'newcomer_type': const EntryTypeVersion(2, 0),
      });
      await store.backend.close();
    });

    // produces the same view rows (idempotent).
    test('idempotent rebuild', () async {
      final store = await _openStore();
      await _appendEvent(
        store,
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{'intensity': 'mild'},
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );
      const map = <String, EntryTypeVersion>{
        'sample_event': EntryTypeVersion(1, 0),
      };
      final first = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: map,
      );
      final firstRows = await store.backend.findViewRows('toy_view');
      final second = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: map,
      );
      final secondRows = await store.backend.findViewRows('toy_view');
      expect(first, second);
      expect(firstRows.length, secondRows.length);
      final firstRow = firstRows.single;
      final secondRow = secondRows.single;
      expect(firstRow['latestEventId'], equals(secondRow['latestEventId']));
      await store.backend.close();
    });

    // view_target_versions atomically; view rows absent from the rebuilt
    // event log do not survive.
    test('rebuild removes prior view rows not derivable from '
        'the event log', () async {
      final store = await _openStore();
      // Seed toy_view with a garbage row not backed by any event.
      await store.backend.transaction((txn) async {
        await store.backend.upsertViewRowInTxn(
          txn,
          'toy_view',
          'garbage-agg',
          <String, Object?>{'aggregate_id': 'garbage-agg', 'garbage': true},
        );
      });
      // One legitimate event on agg-1.
      await _appendEvent(
        store,
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{'intensity': 'mild'},
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );

      final processed = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          'sample_event': EntryTypeVersion(1, 0),
        },
      );

      expect(processed, 1);
      final rows = await store.backend.findViewRows('toy_view');
      expect(rows, hasLength(1));
      expect(rows.single['latestEventId'], equals('e1'));
      expect(
        rows.map((r) => r['latestEventId']),
        isNot(contains('garbage-agg')),
      );
      await store.backend.close();
    });

    // when the log spans multiple streaming chunks.
    test('large event log spanning multiple chunks rebuilds '
        'correctly — no events dropped at chunk boundaries', () async {
      final store = await _openStore();
      const totalEvents = 1250;
      for (var i = 0; i < totalEvents; i++) {
        final aggregateId = i.isEven ? 'agg-even' : 'agg-odd';
        await _appendEvent(
          store,
          eventId: 'ev-$i',
          aggregateId: aggregateId,
          entryType: 'sample_event',
          eventType: 'finalized',
          data: <String, dynamic>{'index': i},
          clientTimestamp: DateTime.utc(
            2026,
            4,
            22,
            10,
          ).add(Duration(seconds: i)),
        );
      }

      final processed = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          'sample_event': EntryTypeVersion(1, 0),
        },
      );
      expect(processed, totalEvents);

      final rows = await store.backend.findViewRows('toy_view');
      expect(rows, hasLength(2));
      // AggregateFold stamps 'aggregateId' into every view row so we can
      // look up directly without parsing the event-id string.
      final byId = <String, Map<String, Object?>>{
        for (final r in rows) r['aggregateId'] as String: r,
      };
      // Each aggregate's last event wrote {'index': N}.
      // With AggregateProjectionSpec deep-merge: the final row reflects
      // the last event's data merged over all prior events.
      expect(byId['agg-odd']!['index'], totalEvents - 1); // 1249 (odd)
      expect(byId['agg-even']!['index'], totalEvents - 2); // 1248 (even)
      await store.backend.close();
    });

    // PromoterRegistry with no registered steps (identity) produces the
    // original payload unchanged.
    test('identity promoter (empty registry) passes payload through', () async {
      final store = await _openStore();
      await _appendEvent(
        store,
        eventId: 'e1',
        aggregateId: 'agg-1',
        entryType: 'sample_event',
        eventType: 'finalized',
        data: const <String, dynamic>{'answer': 42},
        clientTimestamp: DateTime.parse('2026-04-22T10:00:00Z'),
      );
      final processed = await rebuildView(
        store: store,
        viewName: 'toy_view',
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          'sample_event': EntryTypeVersion(1, 0),
        },
      );
      expect(processed, 1);
      final rows = await store.backend.findViewRows('toy_view');
      expect(rows.single['answer'], 42);
      await store.backend.close();
    });
  });
}
