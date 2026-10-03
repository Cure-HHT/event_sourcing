// A backend-agnostic check that a copy a catch-up transaction folds through
// its buffer -- reading the step's rows ahead, serving the step's own
// earlier writes from memory, and writing the changed rows in batches --
// equals a replay of the same log folded event by event, straight through
// the backend, by the fold step appends use.
//
// The log mixes an aggregate view's edits (nested deltas, a present null
// clearing a field), tombstones and re-creations after them, a table view's
// inserts whose keys move between producing aggregates, removes of present
// and absent keys, an entry type the converging build registers at a newer
// minor (so every event of it is promoted before its fold), and a security
// finding naming aggregates of both views (so the outstanding-finding
// refresh runs through the buffer, the table view's by its producer index).
// It spans several of the catch-up's read pages.

import 'dart:math' show Random;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show PublishCollector, recordFindingInTxnForTest;
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:test/test.dart';

const _kNote = 'equivalence_note';
const _kItem = 'equivalence_item';
const _kNoteView = 'equivalence_notes';
const _kItemView = 'equivalence_items';
const _kEvents = 700;

const _kNoteSpec = AggregateProjectionSpec(
  viewName: _kNoteView,
  interest: SubscriptionFilter(entryTypes: <String>{_kNote}),
  tombstoneEventTypes: <String>{'deleted'},
);

const _kItemSpec = TableProjectionSpec(
  viewName: _kItemView,
  interest: SubscriptionFilter(entryTypes: <String>{_kItem}),
  insertEventTypes: <String>{'added'},
  removeEventTypes: <String>{'removed'},
  rowKey: CompositeKey(<String>['data.k']),
  rowData: WholePayload(),
);

/// Opens an event store over the backend under test with [entryTypes],
/// [projections] and [promoters].
typedef EquivalenceStoreOpener =
    Future<EventStore> Function({
      required EntryTypeRegistry entryTypes,
      required ProjectionRegistry projections,
      required PromoterRegistry promoters,
    });

EntryTypeRegistry _entryTypes(EntryTypeVersion noteVersion) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      EntryTypeDefinition(
        id: _kNote,
        registeredVersion: noteVersion,
        name: _kNote,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kItem,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kItem,
      ),
    );
}

Future<void> _append(
  EventStore store,
  Transaction txn,
  PublishCollector collector, {
  required String entryType,
  required String aggregateId,
  required String eventType,
  required Map<String, Object?> data,
}) => store.appendInTxn(
  txn,
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: entryType,
  eventType: eventType,
  data: data,
  initiator: const UserInitiator('equivalence-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// Seeds the log through [store], which registers no view.
Future<void> _seed(EventStore store) async {
  final random = Random(20260930);
  const perTransaction = 50;
  for (var start = 0; start < _kEvents; start += perTransaction) {
    await store.runTransaction((txn, collector) async {
      for (var i = start; i < start + perTransaction; i++) {
        if (i == _kEvents ~/ 2) {
          await recordFindingInTxnForTest(
            store,
            txn,
            collector,
            role: FindingRole.walk,
            kind: FindingKind.hashMismatch,
            evidence: const <String, Object?>{
              'event_id': 'equivalence-tampered-event',
              'carried_hash': 'equivalence-carried-hash',
              'recomputed_hash': 'equivalence-recomputed-hash',
            },
            aggregates: const <String>['note-1', 'note-2', 'item-3', 'item-4'],
          );
        }
        final roll = random.nextInt(100);
        if (roll < 50) {
          await _append(
            store,
            txn,
            collector,
            entryType: _kNote,
            aggregateId: 'note-${random.nextInt(60)}',
            eventType: 'edited',
            data: <String, Object?>{
              'title': 't$i',
              'n': i,
              'meta': <String, Object?>{'x': i, 'y': random.nextBool()},
              if (random.nextInt(4) == 0) 'extra': null else 'extra': 'e$i',
            },
          );
        } else if (roll < 58) {
          await _append(
            store,
            txn,
            collector,
            entryType: _kNote,
            aggregateId: 'note-${random.nextInt(60)}',
            eventType: 'deleted',
            data: const <String, Object?>{},
          );
        } else if (roll < 88) {
          await _append(
            store,
            txn,
            collector,
            entryType: _kItem,
            aggregateId: 'item-${random.nextInt(40)}',
            eventType: 'added',
            data: <String, Object?>{'k': 'k${random.nextInt(50)}', 'v': i},
          );
        } else {
          await _append(
            store,
            txn,
            collector,
            entryType: _kItem,
            aggregateId: 'item-${random.nextInt(40)}',
            eventType: 'removed',
            data: <String, Object?>{'k': 'k${random.nextInt(70)}'},
          );
        }
      }
    });
  }
}

/// Whether [row] carries an outstanding-finding mark.
bool _marked(Map<String, dynamic> row) {
  final integrity = row[r'$integrity'];
  if (integrity is! Map) return false;
  final ids = integrity['security_findings'];
  return ids is List && ids.isNotEmpty;
}

Map<String, Map<String, dynamic>> _byKey(List<Map<String, dynamic>> rows) => {
  for (final row in rows) row['aggregateId']! as String: row,
};

/// Registers the equivalence test: [openBackend] supplies a fresh, empty
/// backend, [opener] opens a store over it, and [settle] yields while the
/// catch-up runs.
void runCatchUpEquivalenceConformance({
  required Future<StorageBackend> Function() openBackend,
  required EquivalenceStoreOpener Function(StorageBackend backend) opener,
  required Future<void> Function() settle,
}) {
  // Verifies: EVS-DEV-view-convergence/K
  // a catch-up transaction folds each event through the fold step an append
  //   uses, under the instance's registered version: the copy it leaves equals
  //   an event-by-event replay under that version, rows and producer index.
  test(
    'a copy caught up through the buffered catch-up equals an '
    'event-by-event replay of the log: aggregate rows, tombstones, promoted events, '
    'table rows with their producers, and outstanding-finding marks',
    () async {
      final backend = await openBackend();
      final open = opener(backend);

      final seeder = await open(
        entryTypes: _entryTypes(const EntryTypeVersion(1, 0)),
        projections: ProjectionRegistry(),
        promoters: PromoterRegistry(),
      );
      await _seed(seeder);
      await seeder.close();

      final entryTypes = _entryTypes(const EntryTypeVersion(1, 1));
      final promoters = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: _kNoteView,
            entryType: _kNote,
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: <TransformPrimitive>[
              DefaultField(fieldName: 'b', defaultValue: 0),
            ],
          ),
        );
      final projections = ProjectionRegistry()
        ..register(_kNoteSpec)
        ..register(_kItemSpec);
      final store = await open(
        entryTypes: entryTypes,
        projections: projections,
        promoters: promoters,
      );
      final head = await backend.readSequenceCounter();
      final copyIds = <String, String>{
        for (final view in const [_kNoteView, _kItemView])
          view: store.copyIdOf(view),
      };

      final deadline = DateTime.now().add(const Duration(seconds: 60));
      while (true) {
        final copies = await backend.transaction(backend.readViewCopiesInTxn);
        final behind = copyIds.values.where(
          (id) => copies.singleWhere((c) => c.copyId == id).watermark < head,
        );
        if (behind.isEmpty) break;
        if (DateTime.now().isAfter(deadline)) {
          fail('the copies did not catch up to $head in time');
        }
        await settle();
      }
      await store.close();

      // The replay: every event of the log folded event by event, straight
      // through the backend, by the fold step appends use, under the
      // converging build's registered versions, into a scratch copy of each
      // view.
      await backend.transaction((txn) async {
        for (final spec in <ProjectionSpec>[_kNoteSpec, _kItemSpec]) {
          final scratch = await backend.createViewCopyInTxn(
            txn,
            spec.viewName,
            'equivalence-replay-${spec.viewName}',
            0,
          );
          var after = 0;
          while (true) {
            final chunk = await backend.findAllEventsInTxn(
              txn,
              afterSequence: after,
              limit: 100,
            );
            if (chunk.isEmpty) break;
            for (final event in chunk) {
              await ProjectionInterpreter.foldStep(
                txn: txn,
                backend: backend,
                spec: spec,
                promoters: promoters,
                event: event,
                registeredVersion:
                    entryTypes.byId(event.entryType)?.registeredVersion ??
                    event.entryTypeVersion,
                copyId: scratch,
              );
            }
            after = chunk.last.sequenceNumber;
          }

          final caughtUp = _byKey(
            await backend.findViewRowsInTxn(txn, copyIds[spec.viewName]!),
          );
          final replayed = _byKey(
            await backend.findViewRowsInTxn(txn, scratch),
          );
          expect(
            caughtUp,
            replayed,
            reason: 'view "${spec.viewName}": caught-up rows equal the replay',
          );
          expect(replayed, isNotEmpty);

          if (spec is AggregateProjectionSpec) {
            expect(
              replayed.values.every((row) => row['b'] == 0),
              isTrue,
              reason: 'every note event was promoted before its fold',
            );
          }
          expect(
            replayed.values.any(_marked),
            isTrue,
            reason: 'the finding marks a row of view "${spec.viewName}"',
          );
          if (spec is TableProjectionSpec) {
            for (var i = 0; i < 40; i++) {
              final source = 'item-$i';
              final caughtUpRows = _byKey(
                await backend.findTableRowsBySourceAggregateInTxn(
                  txn,
                  copyIds[spec.viewName]!,
                  source,
                ),
              );
              final replayedRows = _byKey(
                await backend.findTableRowsBySourceAggregateInTxn(
                  txn,
                  scratch,
                  source,
                ),
              );
              expect(
                caughtUpRows,
                replayedRows,
                reason: 'the rows $source produced match the replay',
              );
            }
          }

          await backend.markViewCopyForDeletionInTxn(txn, scratch);
          await backend.deleteViewCopyRowsInTxn(txn, scratch, limit: 100000);
          await backend.deleteViewCopyRecordInTxn(txn, scratch);
        }
      });
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );
}
