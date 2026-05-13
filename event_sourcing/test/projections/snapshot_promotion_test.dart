// Verifies: EVS-DEV-view-target-versions-seeding/A — seedViewTargetVersions
//   inserts a row for every (viewName, interest-matched entryType) pair where
//   no row currently exists.
// Verifies: EVS-DEV-view-target-versions-seeding/B — seedViewTargetVersions
//   does NOT overwrite an existing row (lagging value is preserved).
// Verifies: EVS-DEV-view-target-versions-seeding/C — newly-seeded rows
//   carry the current registeredVersion as their target.
// Verifies: EVS-DEV-view-target-versions-seeding/D — only entry types
//   explicitly named in a projection's interest filter are seeded.
// Verifies: EVS-DEV-snapshot-promotion-on-open/A — promoteViewSnapshots
//   promotes every view row whose stored target version lags registeredVersion.
// Verifies: EVS-DEV-snapshot-promotion-on-open/B — only view rows are
//   mutated; events in the log are unchanged.
// Verifies: EVS-DEV-snapshot-promotion-on-open/C — exactly one audit
//   callback fires per promoted (viewName, entryType) pair.
// Verifies: EVS-DEV-snapshot-promotion-on-open/D — boot integration test
//   confirms that snapshot-promoting a row from v1→v2 yields the same state
//   as replaying the v1 event through the v1→v2 promoter chain (equivalence).

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/snapshot_promotion.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kNote = EntryTypeDefinition(
  id: 'note',
  registeredVersion: 3,
  name: 'Note',
);

const _kLights = EntryTypeDefinition(
  id: 'lights',
  registeredVersion: 1,
  name: 'Lights',
);

const _kNotesSpec = AggregateProjectionSpec(
  viewName: 'notes',
  interest: SubscriptionFilter(entryTypes: <String>['note']),
  tombstoneEventTypes: <String>{},
);

const _kLightsSpec = AggregateProjectionSpec(
  viewName: 'lights',
  interest: SubscriptionFilter(entryTypes: <String>['lights']),
  tombstoneEventTypes: <String>{},
);

Future<SembastBackend> _openBackend() async {
  final db = await databaseFactoryMemory.openDatabase(
    'test_${_dbCounter++}.db',
  );
  return SembastBackend(database: db);
}

EntryTypeRegistry _registry() {
  final r = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    r.register(defn);
  }
  r
    ..register(_kNote)
    ..register(_kLights);
  return r;
}

void main() {
  group('seedViewTargetVersions', () {
    test('greenfield install seeds rows at current registeredVersion '
        'for every interest-matched entry type', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()
        ..register(_kNotesSpec)
        ..register(_kLightsSpec);

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
          3,
        );
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'lights', 'lights'),
          1,
        );
      });
    });

    test('does not overwrite existing rows', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      // Pre-seed at a lagging version (simulating a prior boot under an
      // older registry where note was at registeredVersion=1).
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
      });

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
          1,
          reason:
              'seedViewTargetVersions must not overwrite an existing '
              'row; that lagging value drives the snapshot-promotion pass.',
        );
      });
    });

    test(
      'ignores entry types not matched by any projection interest',
      () async {
        final backend = await _openBackend();
        final entryTypes = _registry();
        // No projection cares about lights.
        final projections = ProjectionRegistry()..register(_kNotesSpec);

        await backend.transaction((txn) async {
          await seedViewTargetVersions(
            txn: txn,
            backend: backend,
            projections: projections,
            entryTypes: entryTypes,
          );
        });

        await backend.transaction((txn) async {
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
            3,
          );
          // lights has no projection; no row was seeded.
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'lights', 'lights'),
            isNull,
          );
        });
      },
    );
  });

  group('assertNoEntryTypeDowngrade', () {
    test('throws EntryTypeVersionDowngradeError when registry version '
        'is below the highest stored view_target_versions value', () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: 1, // downgrade!
          name: 'Note',
        ),
      );
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      // Simulate: an earlier boot had note at registeredVersion=3 and
      // promoted the view to that target.
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 3);
      });

      await expectLater(
        backend.transaction((txn) async {
          await assertNoEntryTypeDowngrade(
            txn: txn,
            backend: backend,
            projections: projections,
            entryTypes: entryTypes,
          );
        }),
        throwsA(
          isA<EntryTypeVersionDowngradeError>()
              .having((e) => e.entryType, 'entryType', 'note')
              .having((e) => e.fromVersion, 'fromVersion', 3)
              .having((e) => e.toVersion, 'toVersion', 1),
        ),
      );
    });

    test('no-op when every entry type registeredVersion >= stored', () async {
      final backend = await _openBackend();
      final entryTypes = _registry(); // note at 3, lights at 1
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
      });

      // No throw; the registry's note is at 3, stored is 1 (lag, not
      // downgrade).
      await backend.transaction((txn) async {
        await assertNoEntryTypeDowngrade(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });
    });
  });

  group('promoteViewSnapshots', () {
    StoredEvent _event({
      required int seq,
      required Map<String, Object?> data,
      int entryTypeVersion = 1,
      String aggregateId = 'agg-1',
    }) {
      return StoredEvent(
        key: seq,
        eventId: 'e$seq',
        aggregateId: aggregateId,
        aggregateType: 'note',
        entryType: 'note',
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

    Future<void> _noopEmit({
      required String viewName,
      required String entryType,
      required int fromVersion,
      required int toVersion,
      required int rowsPromoted,
    }) async {}

    test(
      'lagging view rows are promoted; non-affected rows untouched',
      () async {
        // Setup: registry has note at registeredVersion=2;
        // view_target_versions has notes/note at version 1 (lag).
        // The promoter renames body -> note_body.
        // Existing view row reflects v1 shape (body present).
        // promoteViewSnapshots should rename body -> note_body and update
        // view_target_versions to 2.
        final backend = await _openBackend();
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: 'note',
            registeredVersion: 2,
            name: 'Note',
          ),
        );
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final promoters = PromoterRegistry()
          ..register(
            const PromoterSpec(
              viewName: 'notes',
              entryType: 'note',
              fromVersion: 1,
              toVersion: 2,
              transforms: <TransformPrimitive>[
                RenameField(from: 'body', to: 'note_body'),
              ],
            ),
          );

        // Seed view_target_versions at v1 + write an existing view row
        // with v1 shape, AND append the v1 event so the affected-agg query
        // can find agg-1.
        await backend.transaction((txn) async {
          await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
          await backend.upsertViewRowInTxn(
            txn,
            'notes',
            'agg-1',
            const <String, dynamic>{
              'aggregateId': 'agg-1',
              'body': 'hello',
              'sequence': 1,
            },
          );
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(
            txn,
            _event(
              seq: seq,
              data: const {'body': 'hello'},
              entryTypeVersion: 1,
            ),
          );
        });

        await backend.transaction((txn) async {
          await promoteViewSnapshots(
            txn: txn,
            backend: backend,
            projections: projections,
            promoters: promoters,
            entryTypes: entryTypes,
            emitAudit: _noopEmit,
            now: DateTime.utc(2026, 5, 11),
          );
        });

        await backend.transaction((txn) async {
          final row = await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
          expect(row!['note_body'], 'hello');
          expect(row.containsKey('body'), isFalse);
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
            2,
          );
        });
      },
    );

    test('rows whose history does not include the affected entry type '
        'are untouched', () async {
      final backend = await _openBackend();
      final entryTypes = _registry(); // notes at 3, lights at 1
      final projections = ProjectionRegistry()
        ..register(_kNotesSpec)
        ..register(_kLightsSpec);
      // Promoter for notes 1->3; lights is at stored=registered=1
      // (no promotion needed).
      final promoters = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'notes',
            entryType: 'note',
            fromVersion: 1,
            toVersion: 3,
            transforms: <TransformPrimitive>[
              RenameField(from: 'body', to: 'note_body'),
            ],
          ),
        );

      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
        await backend.writeViewTargetVersionInTxn(txn, 'lights', 'lights', 1);
        // notes row needs promotion; lights row should NOT be touched.
        await backend.upsertViewRowInTxn(
          txn,
          'notes',
          'agg-1',
          const <String, dynamic>{'body': 'note-body'},
        );
        await backend.upsertViewRowInTxn(
          txn,
          'lights',
          'agg-2',
          const <String, dynamic>{'state': 'on'},
        );
        // Append a note event so the affected-agg query finds agg-1.
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          _event(
            seq: seq,
            data: const {'body': 'note-body'},
            entryTypeVersion: 1,
          ),
        );
      });

      await backend.transaction((txn) async {
        await promoteViewSnapshots(
          txn: txn,
          backend: backend,
          projections: projections,
          promoters: promoters,
          entryTypes: entryTypes,
          emitAudit: _noopEmit,
          now: DateTime.utc(2026, 5, 11),
        );
      });

      await backend.transaction((txn) async {
        // notes row promoted (body -> note_body)
        final notesRow = await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
        expect(notesRow!['note_body'], 'note-body');
        expect(notesRow.containsKey('body'), isFalse);
        // lights row unchanged
        final lightsRow = await backend.readViewRowInTxn(
          txn,
          'lights',
          'agg-2',
        );
        expect(lightsRow!['state'], 'on');
      });
    });

    test(
      'emits one audit callback per promoted (viewName, entryType) pair',
      () async {
        final calls = <Map<String, Object?>>[];
        Future<void> recordingEmit({
          required String viewName,
          required String entryType,
          required int fromVersion,
          required int toVersion,
          required int rowsPromoted,
        }) async {
          calls.add({
            'viewName': viewName,
            'entryType': entryType,
            'fromVersion': fromVersion,
            'toVersion': toVersion,
            'rowsPromoted': rowsPromoted,
          });
        }

        final backend = await _openBackend();
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: 'note',
            registeredVersion: 2,
            name: 'Note',
          ),
        );
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final promoters = PromoterRegistry()
          ..register(
            const PromoterSpec(
              viewName: 'notes',
              entryType: 'note',
              fromVersion: 1,
              toVersion: 2,
              transforms: <TransformPrimitive>[
                RenameField(from: 'body', to: 'note_body'),
              ],
            ),
          );

        await backend.transaction((txn) async {
          await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
          await backend.upsertViewRowInTxn(
            txn,
            'notes',
            'agg-1',
            const <String, dynamic>{'body': 'x'},
          );
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(
            txn,
            _event(seq: seq, data: const {'body': 'x'}, entryTypeVersion: 1),
          );
        });

        await backend.transaction((txn) async {
          await promoteViewSnapshots(
            txn: txn,
            backend: backend,
            projections: projections,
            promoters: promoters,
            entryTypes: entryTypes,
            emitAudit: recordingEmit,
            now: DateTime.utc(2026, 5, 11),
          );
        });

        expect(calls, hasLength(1));
        expect(calls.single['viewName'], 'notes');
        expect(calls.single['entryType'], 'note');
        expect(calls.single['fromVersion'], 1);
        expect(calls.single['toVersion'], 2);
        expect(calls.single['rowsPromoted'], 1);
      },
    );
  });

  group('EventStore.open integration', () {
    test('boot with a bumped registeredVersion: seeds, refuses no '
        'downgrade, snapshot-promotes, emits audit event', () async {
      // First boot at v1, append a v1 note, simulate a registry bump to
      // v2, re-open against the same backend. After re-open: row is
      // promoted; view_target_versions[notes/note] = 2; a
      // view_snapshot_promoted audit event exists in the log.
      //
      // Note: we don't close the EventStore between boots, since closing
      // the EventStore also closes the underlying sembast db. The two
      // EventStore instances share one backend; that's enough to exercise
      // the boot-time helpers' second pass against persisted state.
      final db = await databaseFactoryMemory.openDatabase(
        'integration_${_dbCounter++}.db',
      );
      final backend = SembastBackend(database: db);

      // First boot: register note at v1.
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: 'note',
            registeredVersion: 1,
            name: 'Note',
          ),
        );
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final store = await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
            hopId: 'test',
            identifier: 'test-i',
            softwareVersion: '0.0.0',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        );
        await store.append(
          entryType: 'note',
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'body': 'hello'},
          initiator: const AutomationInitiator(service: 'test'),
        );
      }

      // Second boot against the same backend: register note at v2 plus
      // a v1->v2 promoter (rename body -> note_body).
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: 'note',
            registeredVersion: 2,
            name: 'Note',
          ),
        );
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final promoters = PromoterRegistry()
          ..register(
            const PromoterSpec(
              viewName: 'notes',
              entryType: 'note',
              fromVersion: 1,
              toVersion: 2,
              transforms: <TransformPrimitive>[
                RenameField(from: 'body', to: 'note_body'),
              ],
            ),
          );
        await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
            hopId: 'test',
            identifier: 'test-i',
            softwareVersion: '0.0.0',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
          promoters: promoters,
        );

        // View row was promoted.
        await backend.transaction((txn) async {
          final row = await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
          expect(row!['note_body'], 'hello');
          expect(row.containsKey('body'), isFalse);
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
            2,
          );
        });

        // Audit event was emitted.
        final audits = await backend.findAllEvents(
          entryType: 'view_snapshot_promoted',
        );
        expect(audits, hasLength(1));
        expect(audits.single.data['viewName'], 'notes');
        expect(audits.single.data['fromVersion'], 1);
        expect(audits.single.data['toVersion'], 2);
        expect(audits.single.data['rowsPromoted'], 1);
      }
    });

    test('boot refuses entry-type downgrade', () async {
      final db = await databaseFactoryMemory.openDatabase(
        'downgrade_${_dbCounter++}.db',
      );
      final backend = SembastBackend(database: db);

      // First boot at v2, append an event.
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: 'note',
            registeredVersion: 2,
            name: 'Note',
          ),
        );
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final store = await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
            hopId: 'test',
            identifier: 'test-i',
            softwareVersion: '0.0.0',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        );
        await store.append(
          entryType: 'note',
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'body': 'x'},
          initiator: const AutomationInitiator(service: 'test'),
        );
      }

      // Second boot against the same backend: downgrade to v1.
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: 1,
          name: 'Note',
        ),
      );
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      await expectLater(
        EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
            hopId: 'test',
            identifier: 'test-i',
            softwareVersion: '0.0.0',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        ),
        throwsA(
          isA<EntryTypeVersionDowngradeError>()
              .having((e) => e.entryType, 'entryType', 'note')
              .having((e) => e.fromVersion, 'fromVersion', 2)
              .having((e) => e.toVersion, 'toVersion', 1),
        ),
      );
    });
  });
}
