// Verifies: EVS-PRD-event-log/A — append-only log: dedupeByContent=true
//   suppresses spurious duplicate writes so each logical fact appears at most
//   once; EVS-PRD-ingest/F — idempotent delivery: the per-entry_type scoping
//   of dedupe ensures re-presentation of the same event within a shared
//   aggregate does not produce a second record.
//
// Verifies the refined `dedupeByContent` semantic on `EventStore.append`:
// dedupe matches against the most-recent prior event of MATCHING entry_type
// within the aggregate, not the unconditional last event of any type. This
// is a pre-condition for system-event aggregate consolidation (Phase 4.22
// Task 4 / -D), where multiple system entry types share the
// install-scoped `source.identifier` aggregate and must dedupe per stream.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _source = Source(
  hopId: 'mobile-device',
  identifier: 'install-aaaa',
  softwareVersion: 'dedupe-by-entry-type-test@1.0.0',
);

Future<EventStore> _bootstrap() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'dedupe-by-entry-type-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final ds = await bootstrapEventStore(
    backend: backend,
    source: _source,
    entryTypes: const <EntryTypeDefinition>[],
    destinations: const <Destination>[],
  );
  return ds.eventStore;
}

void main() {
  /// within aggregate).
  group('dedupeByContent matches against prior event of '
      'same entry_type within aggregate', () {
    //   content => dedupe-skip (returns null).
    test('same entry_type same content same aggregate is no-op', () async {
      final es = await _bootstrap();
      const initiator = AutomationInitiator(service: 'test');
      // A system aggregate-style id distinct from the bootstrap
      // registry-init audit's aggregate so the test owns the history.
      const aggId = 'test-aggregate-1';
      const data = <String, Object?>{'k': 'v'};

      final first = await es.append(
        aggregateId: aggId,
        aggregateType: 'system_thing',
        entryType: kEntryTypeRegistryInitializedEntryType,
        eventType: 'finalized',
        data: data,
        initiator: initiator,
        dedupeByContent: true,
      );
      expect(first, isNotNull, reason: 'first emission appends');

      final second = await es.append(
        aggregateId: aggId,
        aggregateType: 'system_thing',
        entryType: kEntryTypeRegistryInitializedEntryType,
        eventType: 'finalized',
        data: data,
        initiator: initiator,
        dedupeByContent: true,
      );
      expect(second, isNull, reason: 'identical re-emission dedupe-skipped');
    });

    //   content does NOT dedupe-match; first emission of the new
    //   entry_type appends, then a re-emission of THAT entry_type with
    //   matching content dedupe-skips.
    test('different entry_type same content same aggregate '
        'is NOT a dedupe match', () async {
      final es = await _bootstrap();
      const initiator = AutomationInitiator(service: 'test');
      const aggId = 'test-aggregate-2';
      const data = <String, Object?>{'k': 'v'};

      // First: an event of entry_type A. dedupeByContent: false so
      // the test does not depend on dedupe behavior for the seed.
      final first = await es.append(
        aggregateId: aggId,
        aggregateType: 'system_thing',
        entryType: kDestinationRegisteredEntryType,
        eventType: 'finalized',
        data: data,
        initiator: initiator,
        dedupeByContent: false,
      );
      expect(first, isNotNull);

      // Second: a different entry_type with identical content fields.
      // Refined dedupe scopes the prior-lookup to matching entry_type;
      // there is no prior `entry_type_registry_initialized` event in
      // this aggregate, so the first emission of this type fires.
      final second = await es.append(
        aggregateId: aggId,
        aggregateType: 'system_thing',
        entryType: kEntryTypeRegistryInitializedEntryType,
        eventType: 'finalized',
        data: data,
        initiator: initiator,
        dedupeByContent: true,
      );
      expect(
        second,
        isNotNull,
        reason:
            'first emission of this entry_type in shared aggregate '
            'appends despite identical content on a different prior '
            'entry_type',
      );

      // Third: identical re-emission of the second's entry_type. The
      // refined dedupe finds the prior `entry_type_registry_initialized`
      // emission with matching content and skips.
      final third = await es.append(
        aggregateId: aggId,
        aggregateType: 'system_thing',
        entryType: kEntryTypeRegistryInitializedEntryType,
        eventType: 'finalized',
        data: data,
        initiator: initiator,
        dedupeByContent: true,
      );
      expect(
        third,
        isNull,
        reason:
            'second emission of same entry_type with same content '
            'is dedupe-skipped against the matching-entry_type prior',
      );
    });
  });
}
