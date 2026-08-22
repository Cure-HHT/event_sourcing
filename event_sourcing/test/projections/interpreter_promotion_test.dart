// Verifies: EVS-DEV-ingest-promotes-before-fold/A
// ProjectionInterpreter
//   applies the per-view promoter chain when entryTypeVersion < registeredVersion.
// Verifies: EVS-DEV-ingest-promotes-before-fold/B
// the original StoredEvent
//   is not modified; fold receives an in-memory promoted copy.
// Verifies: EVS-DEV-ingest-promotes-before-fold/C
// two specs matching the
//   same entry type can register different chains and produce different fold
//   inputs from the same source event (per-spec independence test).
// Verifies: EVS-DEV-ingest-promotes-before-fold/D
// when entryTypeVersion
//   equals registeredVersion, the event is folded raw (no promotion path).

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kNoteEntryType = 'note';

Future<SembastBackend> _openBackend() async {
  final db = await databaseFactoryMemory.openDatabase(
    'test_${_dbCounter++}.db',
  );
  return SembastBackend(database: db);
}

StoredEvent _event({
  required int seq,
  required Map<String, Object?> data,
  int entryTypeVersion = 1,
}) {
  return StoredEvent(
    key: seq,
    eventId: 'e$seq',
    aggregateId: 'agg-1',
    aggregateType: 'note',
    entryType: _kNoteEntryType,
    entryTypeVersion: entryTypeVersion,
    libFormatVersion: 1,
    eventType: 'finalized',
    sequenceNumber: seq,
    data: data,
    metadata: <String, dynamic>{'provenance': <Map<String, Object?>>[]},
    initiator: const UserInitiator('test-user'),
    clientTimestamp: DateTime.utc(2026, 1, 1),
    eventHash: 'h$seq',
    flowToken: null,
    previousEventHash: null,
  );
}

void main() {
  group('ProjectionInterpreter promotion', () {
    test('event at registered version is folded raw (no promotion)', () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      entryTypes.register(
        const EntryTypeDefinition(
          id: _kNoteEntryType,
          registeredVersion: 1,
          name: 'Note',
        ),
      );

      final projections = ProjectionRegistry()
        ..register(
          const AggregateProjectionSpec(
            viewName: 'notes',
            interest: SubscriptionFilter(entryTypes: <String>{_kNoteEntryType}),
            tombstoneEventTypes: <String>{},
          ),
        );
      final promoters = PromoterRegistry();
      final interpreter = ProjectionInterpreter(
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
      );

      await backend.transaction((txn) async {
        await interpreter.applyEvent(
          txn: txn,
          backend: backend,
          event: _event(seq: 1, data: const {'body': 'hello'}),
        );
      });

      await backend.transaction((txn) async {
        final row = await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
        expect(row!['body'], 'hello');
      });
    });

    test(
      'event below registered version is folded after per-view promotion',
      () async {
        final backend = await _openBackend();
        final entryTypes = EntryTypeRegistry();
        entryTypes.register(
          const EntryTypeDefinition(
            id: _kNoteEntryType,
            registeredVersion: 2,
            name: 'Note',
          ),
        );

        final projections = ProjectionRegistry()
          ..register(
            const AggregateProjectionSpec(
              viewName: 'notes',
              interest: SubscriptionFilter(
                entryTypes: <String>{_kNoteEntryType},
              ),
              tombstoneEventTypes: <String>{},
            ),
          );
        final promoters = PromoterRegistry()
          ..register(
            const PromoterSpec(
              viewName: 'notes',
              entryType: _kNoteEntryType,
              fromVersion: 1,
              toVersion: 2,
              transforms: <TransformPrimitive>[
                RenameField(sourceField: 'body', targetField: 'note_body'),
              ],
            ),
          );
        final interpreter = ProjectionInterpreter(
          projections: projections,
          promoters: promoters,
          entryTypes: entryTypes,
        );

        await backend.transaction((txn) async {
          await interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: _event(
              seq: 1,
              data: const {'body': 'hello'},
              entryTypeVersion: 1,
            ),
          );
        });

        await backend.transaction((txn) async {
          final row = await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
          expect(
            row!['note_body'],
            'hello',
            reason:
                'the v1 event was promoted to v2 (body -> note_body) for '
                'the notes view before folding.',
          );
          expect(
            row.containsKey('body'),
            isFalse,
            reason: 'the old name was renamed away by the promoter chain.',
          );
        });
      },
    );

    test('promotion is per-spec; two specs can apply different chains '
        'to the same event', () async {
      // viewA renames body -> body_a; viewB drops body. Both match the
      // same entry type. After folding the same v1 event, viewA's row has
      // body_a; viewB's row has neither body nor body_a.
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      entryTypes.register(
        const EntryTypeDefinition(
          id: _kNoteEntryType,
          registeredVersion: 2,
          name: 'Note',
        ),
      );

      final projections = ProjectionRegistry()
        ..register(
          const AggregateProjectionSpec(
            viewName: 'view_a',
            interest: SubscriptionFilter(entryTypes: <String>{_kNoteEntryType}),
            tombstoneEventTypes: <String>{},
          ),
        )
        ..register(
          const AggregateProjectionSpec(
            viewName: 'view_b',
            interest: SubscriptionFilter(entryTypes: <String>{_kNoteEntryType}),
            tombstoneEventTypes: <String>{},
          ),
        );
      final promoters = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'view_a',
            entryType: _kNoteEntryType,
            fromVersion: 1,
            toVersion: 2,
            transforms: <TransformPrimitive>[
              RenameField(sourceField: 'body', targetField: 'body_a'),
            ],
          ),
        )
        ..register(
          const PromoterSpec(
            viewName: 'view_b',
            entryType: _kNoteEntryType,
            fromVersion: 1,
            toVersion: 2,
            transforms: <TransformPrimitive>[DropField(fieldName: 'body')],
          ),
        );
      final interpreter = ProjectionInterpreter(
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
      );

      await backend.transaction((txn) async {
        await interpreter.applyEvent(
          txn: txn,
          backend: backend,
          event: _event(
            seq: 1,
            data: const {'body': 'hello'},
            entryTypeVersion: 1,
          ),
        );
      });

      await backend.transaction((txn) async {
        final rowA = await backend.readViewRowInTxn(txn, 'view_a', 'agg-1');
        expect(rowA!['body_a'], 'hello');
        expect(rowA.containsKey('body'), isFalse);

        final rowB = await backend.readViewRowInTxn(txn, 'view_b', 'agg-1');
        expect(rowB!.containsKey('body'), isFalse);
        expect(rowB.containsKey('body_a'), isFalse);
      });
    });
  });
}
