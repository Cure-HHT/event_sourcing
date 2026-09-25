// Backend-agnostic scenarios for entry-type and data-format versions: two
// builds registering different minors of one major over one database,
// downgrade refusal by major, and the ingest version table. Run on Sembast
// by test/version_compatibility_test.dart and on Postgres by
// test/storage/postgres/postgres_versions_test.dart.
//
// Traceability lives on the individual tests below.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kViewSnapshotPromotedEntryType;
import 'package:flutter_test/flutter_test.dart';
import 'test_backends.dart';

/// One database the scenarios open several backends over, as several
/// builds of the library would.
abstract class VersionTestDatabase {
  /// Opens a new backend over this database.
  Future<StorageBackend> openBackend();

  /// The security-context store an event store over [backend] uses.
  MutableSecurityContextStore securityFor(StorageBackend backend);

  /// Stops the instance [store] belongs to, as a stop-then-start deployment
  /// stops the old revision before the new one opens: on Postgres, where
  /// each instance holds its own connections and generation locks, it
  /// closes the store; on Sembast, where the scenarios share one database
  /// handle and a registration holds nothing, it does nothing.
  Future<void> stop(EventStore store);

  /// Closes every backend this database opened.
  Future<void> close();
}

const _kType = 'versioned_note';
const _kView = 'versioned_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{'deleted'},
);

/// A table view keyed by two payload fields, holding the whole payload.
const _kItemsView = 'versioned_note_items';
const _kItemsSpec = TableProjectionSpec(
  viewName: _kItemsView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{'deleted'},
  rowKey: CompositeKey(<String>['data.list', 'data.item']),
  rowData: WholePayload(),
);

/// A table view keyed by aggregate, holding only the payload field `a`.
const _kTitlesView = 'versioned_note_titles';
const _kTitlesSpec = TableProjectionSpec(
  viewName: _kTitlesView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{'deleted'},
  rowKey: AggregateIdKey(),
  rowData: SelectedFields(<String>['a']),
);

const _kTableSpecs = <ProjectionSpec>[_kSpec, _kItemsSpec, _kTitlesSpec];

const _kSource = Source(
  hopId: 'versions-hop',
  identifier: 'versions-install',
  softwareVersion: 'versions-test',
);

/// Opens an event store over a new backend of [db] with [_kType] registered
/// at [registered], the [projections] registered, and [promoters].
Future<EventStore> _openStore(
  VersionTestDatabase db, {
  required EntryTypeVersion registered,
  List<PromoterSpec> promoters = const <PromoterSpec>[],
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) async {
  final backend = await db.openBackend();
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes.register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: registered,
      name: _kType,
    ),
  );
  final promoterRegistry = PromoterRegistry();
  for (final spec in promoters) {
    promoterRegistry.register(spec);
  }
  final projectionRegistry = ProjectionRegistry();
  for (final spec in projections) {
    projectionRegistry.register(spec);
  }
  final store = await EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: entryTypes,
    source: _kSource,
    projections: projectionRegistry,
    promoters: promoterRegistry,
  );
  trackTestBackend(store, backend);
  return store;
}

/// The `1.0 -> 1.1` step adding `b` with the default `0` to [viewName].
PromoterSpec _defaultB(String viewName) => PromoterSpec(
  viewName: viewName,
  entryType: _kType,
  fromVersion: const EntryTypeVersion(1, 0),
  toVersion: const EntryTypeVersion(1, 1),
  transforms: const <TransformPrimitive>[
    DefaultField(fieldName: 'b', defaultValue: 0),
  ],
);

/// The newer build: registers `1.1`, whose minor step adds `b` with the
/// default `0` to every view in [projections].
Future<EventStore> _openNewer(
  VersionTestDatabase db, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) => _openStore(
  db,
  registered: const EntryTypeVersion(1, 1),
  projections: projections,
  promoters: <PromoterSpec>[
    for (final spec in projections) _defaultB(spec.viewName),
  ],
);

/// The older build: registers `1.0`.
Future<EventStore> _openOlder(
  VersionTestDatabase db, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) => _openStore(
  db,
  registered: const EntryTypeVersion(1, 0),
  projections: projections,
);

Future<void> _appendNote(
  EventStore store,
  String aggregateId,
  Map<String, Object?> data, {
  String eventType = 'finalized',
}) async {
  await store.append(
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: eventType,
    data: data,
    initiator: const UserInitiator('versions-user'),
  );
}

List<Map<String, Object?>> _sortedRows(List<Map<String, dynamic>> rows) =>
    <Map<String, Object?>>[
      for (final row in rows) Map<String, Object?>.from(row),
    ]..sort(
      (x, y) =>
          (x['aggregateId'] as String).compareTo(y['aggregateId'] as String),
    );

/// Asserts that [rebuildView] of each of [views] under [target] yields the
/// rows the store holds now, and returns those rows by view.
Future<Map<String, List<Map<String, Object?>>>> _expectRebuildMatches(
  EventStore store,
  List<String> views,
  EntryTypeVersion target,
) async {
  final byView = <String, List<Map<String, Object?>>>{};
  for (final view in views) {
    final held = _sortedRows(await store.reader.findViewRows(view));
    await rebuildView(
      store: store,
      viewName: view,
      targetVersionByEntryType: <String, EntryTypeVersion>{_kType: target},
    );
    final rebuilt = _sortedRows(await store.reader.findViewRows(view));
    expect(rebuilt, held, reason: 'view $view: rebuildView differs');
    byView[view] = held;
  }
  return byView;
}

Map<String, Object?>? _rowOf(List<Map<String, Object?>> rows, String id) {
  for (final row in rows) {
    if (row['aggregateId'] == id) return row;
  }
  return null;
}

/// Asserts [row] exists, holds `a` equal to [a], and holds no `b`.
void _expectRowWithoutDefault(Map<String, Object?>? row, Object? a) {
  expect(row, isNotNull);
  expect(row!['a'], a);
  expect(row, isNot(contains('b')));
}

Future<EntryTypeVersion?> _storedTarget(EventStore store) =>
    store.reader.transaction(
      (txn) => store.reader.readViewTargetVersionInTxn(txn, _kView, _kType),
    );

Future<Map<String, Object?>?> _row(EventStore store, String aggregateId) =>
    store.reader.transaction(
      (txn) => store.reader.readViewRowInTxn(txn, _kView, aggregateId),
    );

Future<int> _promotionAudits(EventStore store) async {
  final events = await store.reader.findAllEvents(
    entryType: kViewSnapshotPromotedEntryType,
  );
  return events.where((e) => e.data['viewName'] == _kView).length;
}

/// Registers the version-compatibility scenarios against databases produced
/// by [openDatabase]. A null database skips the test.
void runVersionCompatibilityScenarios(
  Future<VersionTestDatabase?> Function() openDatabase, {
  required String backendLabel,
}) {
  group('version compatibility ($backendLabel)', () {
    VersionTestDatabase? db;

    setUp(() async {
      db = await openDatabase();
      if (db == null) markTestSkipped('no database for $backendLabel');
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    group('re-promotion after an older minor folds', () {
      // Verifies: EVS-DEV-version-compatibility/E
      // Verifies: EVS-DEV-snapshot-promotion-on-open/A
      test('an older minor lowers the stored target; the next newer open '
          're-promotes the rows it folded', () async {
        if (db == null) return;
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-n', <String, Object?>{'a': 1, 'b': 5});
        expect(await _storedTarget(newer), const EntryTypeVersion(1, 1));
        final auditsBefore = await _promotionAudits(newer);

        final older = await _openOlder(db!);
        // The older build opens a database whose target is a higher minor
        // of its major, and leaves the target as it is until it folds.
        expect(await _storedTarget(older), const EntryTypeVersion(1, 1));
        await _appendNote(older, 'agg-o', <String, Object?>{'a': 2});
        _expectRowWithoutDefault(await _row(older, 'agg-o'), 2);
        expect(await _storedTarget(older), const EntryTypeVersion(1, 0));

        final reopened = await _openNewer(db!);
        expect((await _row(reopened, 'agg-o'))!['b'], 0);
        expect((await _row(reopened, 'agg-o'))!['a'], 2);
        expect((await _row(reopened, 'agg-n'))!['b'], 5);
        expect(await _storedTarget(reopened), const EntryTypeVersion(1, 1));
        expect(await _promotionAudits(reopened), auditsBefore + 1);
      });

      // Verifies: EVS-DEV-version-compatibility/E
      // Verifies: EVS-DEV-snapshot-promotion-on-open/A
      test('canary order: the newer build promotes while the older serves; '
          "the older build's later fold is re-promoted", () async {
        if (db == null) return;
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-1', <String, Object?>{'a': 1});
        expect(await _storedTarget(older), const EntryTypeVersion(1, 0));

        final newer = await _openNewer(db!);
        expect((await _row(newer, 'agg-1'))!['b'], 0);
        expect(await _storedTarget(newer), const EntryTypeVersion(1, 1));

        // The older build is still serving and folds after the promotion.
        await _appendNote(older, 'agg-2', <String, Object?>{'a': 2});
        _expectRowWithoutDefault(await _row(older, 'agg-2'), 2);
        expect(await _storedTarget(older), const EntryTypeVersion(1, 0));

        final auditsBefore = await _promotionAudits(older);
        final reopened = await _openNewer(db!);
        expect((await _row(reopened, 'agg-2'))!['b'], 0);
        expect((await _row(reopened, 'agg-1'))!['b'], 0);
        expect(await _storedTarget(reopened), const EntryTypeVersion(1, 1));
        expect(await _promotionAudits(reopened), auditsBefore + 1);
      });

      // Verifies: EVS-DEV-snapshot-promotion-on-open/D
      test('after re-promotion, rebuildView under the newer build yields the '
          'same rows', () async {
        if (db == null) return;
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-n', <String, Object?>{'a': 1, 'b': 5});
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-o', <String, Object?>{'a': 2});
        await _appendNote(older, 'agg-n', <String, Object?>{'a': 3});
        final reopened = await _openNewer(db!);

        final promoted = await reopened.reader.findViewRows(_kView);
        await rebuildView(
          store: reopened,
          viewName: _kView,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 1),
          },
        );
        final rebuilt = await reopened.reader.findViewRows(_kView);
        expect(rebuilt, promoted);
        expect(await _storedTarget(reopened), const EntryTypeVersion(1, 1));
      });

      // Verifies: EVS-DEV-version-compatibility/E
      test('a fold at the stored target, or under a higher minor than it, '
          'writes no target', () async {
        if (db == null) return;
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-1', <String, Object?>{'a': 1});
        expect(await _storedTarget(newer), const EntryTypeVersion(1, 1));

        // Stage a stored target below the registered version, as a
        // concurrent older build's fold leaves it.
        final newerBackend = testBackendOf(newer);
        await newerBackend.transaction(
          (txn) => newerBackend.writeViewTargetVersionInTxn(
            txn,
            _kView,
            _kType,
            const EntryTypeVersion(1, 0),
          ),
        );
        await _appendNote(newer, 'agg-2', <String, Object?>{'a': 2});
        expect(
          await _storedTarget(newer),
          const EntryTypeVersion(1, 0),
          reason: 'a fold never raises the stored target; the next open does',
        );
      });

      // Verifies: EVS-DEV-version-compatibility/E
      test('a fold whose transaction fails leaves the stored target as it '
          'was', () async {
        if (db == null) return;
        await _openNewer(db!);
        final older = await _openOlder(db!);
        final eventsBefore = await older.reader.findAllEvents();
        EntryTypeVersion? targetInFold;
        Map<String, Object?>? rowInFold;
        await expectLater(
          older.runTransaction<void>((txn, collector) async {
            await older.appendInTxn(
              txn,
              collector: collector,
              flowToken: null,
              metadata: null,
              security: null,
              checkpointReason: null,
              changeReason: null,
              dedupeByContent: false,
              entryType: _kType,
              aggregateId: 'agg-1',
              aggregateType: 'note',
              eventType: 'finalized',
              data: const <String, Object?>{'a': 1},
              initiator: const UserInitiator('versions-user'),
            );
            // The fold lowers the target inside its own transaction.
            targetInFold = await older.reader.readViewTargetVersionInTxn(
              txn,
              _kView,
              _kType,
            );
            rowInFold = await older.reader.readViewRowInTxn(
              txn,
              _kView,
              'agg-1',
            );
            throw StateError('injected failure after the fold');
          }),
          throwsStateError,
        );
        expect(targetInFold, const EntryTypeVersion(1, 0));
        expect(rowInFold, isNotNull);
        expect(rowInFold!['a'], 1);
        expect(await _storedTarget(older), const EntryTypeVersion(1, 1));
        expect(await _row(older, 'agg-1'), isNull);
        expect(
          (await older.reader.findAllEvents()).length,
          eventsBefore.length,
        );
      });
    });

    group('boot promotion yields the rows replay yields', () {
      // Verifies: EVS-DEV-snapshot-promotion-on-open/B+D
      test('an aggregate whose only events are of the newer minor and never '
          'set the defaulted field keeps no default', () async {
        if (db == null) return;
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-n', <String, Object?>{'a': 1});
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-o', <String, Object?>{'a': 2});
        expect(await _storedTarget(older), const EntryTypeVersion(1, 0));

        final reopened = await _openNewer(db!);
        final rows = await _expectRebuildMatches(reopened, <String>[
          _kView,
        ], const EntryTypeVersion(1, 1));
        _expectRowWithoutDefault(_rowOf(rows[_kView]!, 'agg-n'), 1);
        expect(_rowOf(rows[_kView]!, 'agg-o')!['b'], 0);
      });

      // Verifies: EVS-DEV-snapshot-promotion-on-open/B+D
      test('an older-minor event then a newer-minor event of one aggregate, '
          'folded by different builds', () async {
        if (db == null) return;
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-m', <String, Object?>{'a': 1});
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-m', <String, Object?>{'a': 2});
        await _appendNote(older, 'agg-x', <String, Object?>{'a': 9});

        final reopened = await _openNewer(db!);
        final rows = await _expectRebuildMatches(reopened, <String>[
          _kView,
        ], const EntryTypeVersion(1, 1));
        expect(_rowOf(rows[_kView]!, 'agg-m')!['a'], 2);
        expect(_rowOf(rows[_kView]!, 'agg-m')!['b'], 0);
      });

      // Verifies: EVS-DEV-snapshot-promotion-on-open/B+D
      test('a newer-minor event then an older-minor event of one aggregate, '
          'folded by different builds', () async {
        if (db == null) return;
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-m', <String, Object?>{'a': 1});
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-m', <String, Object?>{'a': 2});
        _expectRowWithoutDefault(await _row(older, 'agg-m'), 2);

        final reopened = await _openNewer(db!);
        final rows = await _expectRebuildMatches(reopened, <String>[
          _kView,
        ], const EntryTypeVersion(1, 1));
        expect(_rowOf(rows[_kView]!, 'agg-m')!['a'], 2);
        expect(_rowOf(rows[_kView]!, 'agg-m')!['b'], 0);
      });

      // Verifies: EVS-DEV-snapshot-promotion-on-open/B+D
      test('an aggregate tombstoned and recreated by newer-minor events '
          'keeps no default from before the tombstone', () async {
        if (db == null) return;
        final older = await _openOlder(db!);
        await _appendNote(older, 'agg-t', <String, Object?>{'a': 1});
        await _appendNote(
          older,
          'agg-t',
          const <String, Object?>{},
          eventType: 'deleted',
        );
        final newer = await _openNewer(db!);
        await _appendNote(newer, 'agg-t', <String, Object?>{'a': 2});
        await _appendNote(older, 'agg-z', <String, Object?>{'a': 0});

        final reopened = await _openNewer(db!);
        final rows = await _expectRebuildMatches(reopened, <String>[
          _kView,
        ], const EntryTypeVersion(1, 1));
        _expectRowWithoutDefault(_rowOf(rows[_kView]!, 'agg-t'), 2);
      });

      // Verifies: EVS-DEV-snapshot-promotion-on-open/A+B+D
      // Verifies: EVS-DEV-version-compatibility/E
      test('table views, keyed by composite key or by aggregate, are '
          're-derived as rebuildView derives them', () async {
        if (db == null) return;
        final older = await _openOlder(db!, projections: _kTableSpecs);
        await _appendNote(older, 'agg-1', <String, Object?>{
          'list': 'l1',
          'item': 'i1',
          'a': 1,
        });
        final newer = await _openNewer(db!, projections: _kTableSpecs);
        await _appendNote(newer, 'agg-2', <String, Object?>{
          'list': 'l1',
          'item': 'i2',
          'a': 2,
        });
        await _appendNote(older, 'agg-3', <String, Object?>{
          'list': 'l2',
          'item': 'i1',
          'a': 3,
        });
        await _appendNote(older, 'agg-4', <String, Object?>{
          'list': 'l2',
          'item': 'i2',
          'a': 4,
        });
        await _appendNote(older, 'agg-4', const <String, Object?>{
          'list': 'l2',
          'item': 'i2',
        }, eventType: 'deleted');

        final reopened = await _openNewer(db!, projections: _kTableSpecs);
        final rows = await _expectRebuildMatches(reopened, <String>[
          _kView,
          _kItemsView,
          _kTitlesView,
        ], const EntryTypeVersion(1, 1));
        final items = rows[_kItemsView]!;
        expect(_rowOf(items, 'l1|i1')!['b'], 0);
        _expectRowWithoutDefault(_rowOf(items, 'l1|i2'), 2);
        expect(_rowOf(items, 'l2|i1')!['b'], 0);
        expect(_rowOf(items, 'l2|i2'), isNull);
        final titles = rows[_kTitlesView]!;
        expect(titles.map((r) => r['aggregateId']).toSet(), <String>{
          'agg-1',
          'agg-2',
          'agg-3',
        });
        for (final row in titles) {
          expect(row, isNot(contains('b')));
        }
        for (final view in <String>[_kView, _kItemsView, _kTitlesView]) {
          final target = await reopened.reader.transaction(
            (txn) =>
                reopened.reader.readViewTargetVersionInTxn(txn, view, _kType),
          );
          expect(target, const EntryTypeVersion(1, 1), reason: view);
        }
      });
    });

    group('a chain across a major decides each default under its final '
        'name', () {
      // `1.0 -> 1.1` adds `x`; `1.1 -> 2.0` renames `x` to `y`.
      const steps = <PromoterSpec>[
        PromoterSpec(
          viewName: _kView,
          entryType: _kType,
          fromVersion: EntryTypeVersion(1, 0),
          toVersion: EntryTypeVersion(1, 1),
          transforms: <TransformPrimitive>[
            DefaultField(fieldName: 'x', defaultValue: 0),
          ],
        ),
        PromoterSpec(
          viewName: _kView,
          entryType: _kType,
          fromVersion: EntryTypeVersion(1, 1),
          toVersion: EntryTypeVersion(2, 0),
          transforms: <TransformPrimitive>[
            RenameField(sourceField: 'x', targetField: 'y'),
          ],
        ),
      ];

      // Verifies: EVS-DEV-ingest-promotes-before-fold/A
      // Verifies: EVS-DEV-snapshot-promotion-on-open/D
      // Verifies: EVS-DEV-version-compatibility/D
      test('promotion at the major bump and lower-major ingest keep a value '
          'the row holds under the renamed field', () async {
        if (db == null) return;
        final v11 = await _openStore(
          db!,
          registered: const EntryTypeVersion(1, 1),
          promoters: <PromoterSpec>[steps.first],
        );
        await _appendNote(v11, 'agg-r', <String, Object?>{'x': 5});
        final v10 = await _openStore(
          db!,
          registered: const EntryTypeVersion(1, 0),
        );
        await _appendNote(v10, 'agg-r', <String, Object?>{'a': 3});
        await _appendNote(v10, 'agg-s', <String, Object?>{'a': 4});
        // The major bump is deployed stop-then-start.
        await db!.stop(v11);
        await db!.stop(v10);

        final v20 = await _openStore(
          db!,
          registered: const EntryTypeVersion(2, 0),
          promoters: steps,
        );
        var rows = await _expectRebuildMatches(v20, <String>[
          _kView,
        ], const EntryTypeVersion(2, 0));
        expect(_rowOf(rows[_kView]!, 'agg-r')!['y'], 5);
        expect(_rowOf(rows[_kView]!, 'agg-r')!['a'], 3);
        expect(_rowOf(rows[_kView]!, 'agg-r'), isNot(contains('x')));
        expect(_rowOf(rows[_kView]!, 'agg-s')!['y'], 0);

        // A lagging peer's events of the lower major, through both ingest
        // entry points, into the aggregate that holds `y`.
        await v20.ingestEvent(
          _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 0),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'a': 7},
            aggregateId: 'agg-r',
          ),
        );
        expect((await _row(v20, 'agg-r'))!['y'], 5);
        await v20.ingestBatch(
          _batchOf(
            _peerEvent(
              entryTypeVersion: const EntryTypeVersion(1, 1),
              dataFormat: LibVersion.dataFormat,
              data: const <String, Object?>{'a': 8},
              aggregateId: 'agg-r',
            ),
          ),
          wireFormat: BatchEnvelope.wireFormat,
        );
        await v20.ingestEvent(
          _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 0),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'a': 9},
            aggregateId: 'agg-new',
          ),
        );
        rows = await _expectRebuildMatches(v20, <String>[
          _kView,
        ], const EntryTypeVersion(2, 0));
        expect(_rowOf(rows[_kView]!, 'agg-r')!['y'], 5);
        expect(_rowOf(rows[_kView]!, 'agg-r')!['a'], 8);
        expect(_rowOf(rows[_kView]!, 'agg-new')!['y'], 0);
      });
    });

    group('views catch up with the log', () {
      const otherType = 'versioned_label';
      const newView = 'versioned_note_titles_new';
      const newViewSpec = AggregateProjectionSpec(
        viewName: newView,
        interest: SubscriptionFilter(entryTypes: <String>{_kType}),
        tombstoneEventTypes: <String>{},
      );
      const widenedSpec = AggregateProjectionSpec(
        viewName: _kView,
        interest: SubscriptionFilter(entryTypes: <String>{_kType, otherType}),
        tombstoneEventTypes: <String>{'deleted'},
      );

      /// Opens a build registering [_kType] at `1.0`, [otherType] when
      /// [withOther], and [projections].
      Future<EventStore> openBuild(
        List<ProjectionSpec> projections, {
        bool withOther = false,
      }) async {
        final backend = await db!.openBackend();
        final entryTypes = EntryTypeRegistry();
        for (final definition in kSystemEntryTypes) {
          entryTypes.register(definition);
        }
        entryTypes.register(
          const EntryTypeDefinition(
            id: _kType,
            registeredVersion: EntryTypeVersion(1, 0),
            name: _kType,
          ),
        );
        if (withOther) {
          entryTypes.register(
            const EntryTypeDefinition(
              id: otherType,
              registeredVersion: EntryTypeVersion(1, 0),
              name: otherType,
            ),
          );
        }
        final registry = ProjectionRegistry();
        for (final spec in projections) {
          registry.register(spec);
        }
        return EventStore.open(
          storage: ApplicationSuppliedStorage(
            backend,
            db!.securityFor(backend),
          ),
          entryTypes: entryTypes,
          source: _kSource,
          projections: registry,
        );
      }

      Future<bool> behind(EventStore store, String view, String entryType) =>
          store.reader.transaction(
            (txn) =>
                store.reader.readViewTargetBehindInTxn(txn, view, entryType),
          );

      Future<List<Map<String, Object?>>> rowsAfterRebuild(
        EventStore store,
        String view,
        Map<String, EntryTypeVersion> targets,
      ) async {
        final held = _sortedRows(await store.reader.findViewRows(view));
        await rebuildView(
          store: store,
          viewName: view,
          targetVersionByEntryType: targets,
        );
        expect(
          _sortedRows(await store.reader.findViewRows(view)),
          held,
          reason: 'view $view: rebuildView differs',
        );
        return held;
      }

      // Verifies: EVS-DEV-version-compatibility/L
      test('a new view over an existing entry type is derived at its first '
          'open, marked behind by the build that does not register it, and '
          're-derived at the next open', () async {
        if (db == null) return;
        final older = await openBuild(<ProjectionSpec>[_kSpec]);
        await _appendNote(older, 'agg-1', <String, Object?>{'a': 1});
        await _appendNote(older, 'agg-2', <String, Object?>{'a': 2});

        final newer = await openBuild(<ProjectionSpec>[_kSpec, newViewSpec]);
        expect(await newer.reader.findViewRows(newView), hasLength(2));

        // The older build keeps serving: its appends are not folded into the
        // new view, and mark it.
        await _appendNote(older, 'agg-3', <String, Object?>{'a': 3});
        expect(await behind(older, newView, _kType), isTrue);
        expect(await behind(older, _kView, _kType), isFalse);
        expect(await newer.reader.findViewRows(newView), hasLength(2));

        final reopened = await openBuild(<ProjectionSpec>[_kSpec, newViewSpec]);
        expect(await behind(reopened, newView, _kType), isFalse);
        final rows = await rowsAfterRebuild(reopened, newView, {
          _kType: const EntryTypeVersion(1, 0),
        });
        expect(rows.map((r) => r['aggregateId']), <String>[
          'agg-1',
          'agg-2',
          'agg-3',
        ]);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test("an entry type added to a view's interest, ingested by the older "
          'build, marks the view, and the next open of the newer build '
          're-derives it', () async {
        if (db == null) return;
        final newer = await openBuild(<ProjectionSpec>[
          widenedSpec,
        ], withOther: true);
        await _appendNote(newer, 'agg-1', <String, Object?>{'a': 1});

        final older = await openBuild(<ProjectionSpec>[_kSpec]);
        await older.ingestEvent(
          _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 0),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'label': 'peer'},
            aggregateId: 'agg-1',
            entryType: otherType,
          ),
        );
        expect(await behind(older, _kView, otherType), isTrue);
        expect(await behind(older, _kView, _kType), isFalse);
        final before = await older.reader.transaction(
          (txn) => older.reader.readViewRowInTxn(txn, _kView, 'agg-1'),
        );
        expect(before!.containsKey('label'), isFalse);

        final reopened = await openBuild(<ProjectionSpec>[
          widenedSpec,
        ], withOther: true);
        expect(await behind(reopened, _kView, otherType), isFalse);
        final rows = await rowsAfterRebuild(reopened, _kView, {
          _kType: const EntryTypeVersion(1, 0),
          otherType: const EntryTypeVersion(1, 0),
        });
        expect(_rowOf(rows, 'agg-1')!['label'], 'peer');
        expect(_rowOf(rows, 'agg-1')!['a'], 1);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test('a new table view over an existing entry type is derived whole at '
          'its first open, marked by the build that does not register it, and '
          're-derived whole at the next open', () async {
        if (db == null) return;
        const tableView = 'versioned_note_values_new';
        const tableSpec = TableProjectionSpec(
          viewName: tableView,
          interest: SubscriptionFilter(entryTypes: <String>{_kType}),
          insertEventTypes: <String>{'finalized'},
          removeEventTypes: <String>{'deleted'},
          rowKey: CompositeKey(<String>['data.list', 'data.item']),
          rowData: WholePayload(),
        );
        Future<List<Map<String, Object?>>> tableRows(EventStore store) async {
          final rows = <Map<String, Object?>>[
            for (final row in await store.reader.findViewRows(tableView))
              Map<String, Object?>.from(row),
          ];
          String key(Map<String, Object?> row) =>
              '${row['list']}/${row['item']}';
          return rows..sort((x, y) => key(x).compareTo(key(y)));
        }

        final older = await openBuild(<ProjectionSpec>[_kSpec]);
        await _appendNote(older, 'agg-1', <String, Object?>{
          'list': 'l1',
          'item': 'i1',
          'a': 1,
        });
        await _appendNote(older, 'agg-2', <String, Object?>{
          'list': 'l1',
          'item': 'i2',
          'a': 2,
        });

        final newer = await openBuild(<ProjectionSpec>[_kSpec, tableSpec]);
        expect(await tableRows(newer), hasLength(2));

        // The older build appends a new row and removes one: neither folds
        // into the table view, and the view is marked.
        await _appendNote(older, 'agg-3', <String, Object?>{
          'list': 'l2',
          'item': 'i3',
          'a': 3,
        });
        await _appendNote(older, 'agg-1', <String, Object?>{
          'list': 'l1',
          'item': 'i1',
        }, eventType: 'deleted');
        expect(await behind(older, tableView, _kType), isTrue);
        expect(await tableRows(newer), hasLength(2));

        final reopened = await openBuild(<ProjectionSpec>[_kSpec, tableSpec]);
        expect(await behind(reopened, tableView, _kType), isFalse);
        final held = await tableRows(reopened);
        await rebuildView(
          store: reopened,
          viewName: tableView,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 0),
          },
        );
        expect(await tableRows(reopened), held, reason: 'rebuildView differs');
        expect(held.map((row) => row['item']), <String>['i2', 'i3']);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test('a view whose interest names no entry types is not caught up: a '
          'new one registered over existing events stays empty until '
          'rebuildView', () async {
        if (db == null) return;
        const byAggregateView = 'versioned_notes_by_aggregate_type';
        const byAggregateSpec = AggregateProjectionSpec(
          viewName: byAggregateView,
          interest: SubscriptionFilter(aggregateTypes: <String>{'note'}),
          tombstoneEventTypes: <String>{},
        );
        final older = await openBuild(<ProjectionSpec>[_kSpec]);
        await _appendNote(older, 'agg-1', <String, Object?>{'a': 1});
        await _appendNote(older, 'agg-2', <String, Object?>{'a': 2});

        final newer = await openBuild(<ProjectionSpec>[
          _kSpec,
          byAggregateSpec,
        ]);
        expect(await newer.reader.findViewRows(byAggregateView), isEmpty);
        expect(
          await newer.reader.transaction(
            (txn) => newer.reader.readViewTargetsForEntryTypeInTxn(txn, _kType),
          ),
          isNot(contains(byAggregateView)),
          reason: 'no target is stored for an interest without entry types',
        );

        await rebuildView(
          store: newer,
          viewName: byAggregateView,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 0),
          },
        );
        final rows = _sortedRows(
          await newer.reader.findViewRows(byAggregateView),
        );
        expect(rows.map((r) => r['aggregateId']), <String>['agg-1', 'agg-2']);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test('builds whose interests for one view differ only in aggregate '
          'types mark nothing: the event only the wider interest matches '
          'stays out of the view until rebuildView', () async {
        if (db == null) return;
        const narrowSpec = AggregateProjectionSpec(
          viewName: _kView,
          interest: SubscriptionFilter(
            entryTypes: <String>{_kType},
            aggregateTypes: <String>{'note'},
          ),
          tombstoneEventTypes: <String>{'deleted'},
        );
        const wideSpec = AggregateProjectionSpec(
          viewName: _kView,
          interest: SubscriptionFilter(
            entryTypes: <String>{_kType},
            aggregateTypes: <String>{'note', 'memo'},
          ),
          tombstoneEventTypes: <String>{'deleted'},
        );
        await openBuild(<ProjectionSpec>[wideSpec]);
        final older = await openBuild(<ProjectionSpec>[narrowSpec]);
        await older.append(
          entryType: _kType,
          aggregateId: 'memo-1',
          aggregateType: 'memo',
          eventType: 'finalized',
          data: const <String, Object?>{'a': 1},
          initiator: const UserInitiator('versions-user'),
        );
        expect(await behind(older, _kView, _kType), isFalse);

        final reopened = await openBuild(<ProjectionSpec>[wideSpec]);
        expect(
          await reopened.reader.transaction(
            (txn) => reopened.reader.readViewRowInTxn(txn, _kView, 'memo-1'),
          ),
          isNull,
        );
        await rebuildView(
          store: reopened,
          viewName: _kView,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 0),
          },
        );
        final row = await reopened.reader.transaction(
          (txn) => reopened.reader.readViewRowInTxn(txn, _kView, 'memo-1'),
        );
        expect(row!['a'], 1);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test('a build that registers the view and names the entry type marks '
          'nothing, whether or not its interest matches the event', () async {
        if (db == null) return;
        final store = await openBuild(<ProjectionSpec>[_kSpec, newViewSpec]);
        await _appendNote(store, 'agg-1', <String, Object?>{'a': 1});
        await _appendNote(
          store,
          'agg-1',
          <String, Object?>{},
          eventType: 'deleted',
        );
        expect(await behind(store, _kView, _kType), isFalse);
        expect(await behind(store, newView, _kType), isFalse);
      });

      // Verifies: EVS-DEV-version-compatibility/L
      test('a mark whose append does not commit is not kept', () async {
        if (db == null) return;
        await openBuild(<ProjectionSpec>[_kSpec, newViewSpec]);
        final older = await openBuild(<ProjectionSpec>[_kSpec]);
        await expectLater(
          older.runTransaction<void>((txn, collector) async {
            await older.appendInTxn(
              txn,
              entryType: _kType,
              aggregateId: 'agg-1',
              aggregateType: 'note',
              eventType: 'finalized',
              data: const <String, Object?>{'a': 1},
              initiator: const UserInitiator('versions-user'),
              flowToken: null,
              metadata: null,
              security: null,
              checkpointReason: null,
              changeReason: null,
              dedupeByContent: false,
              collector: collector,
            );
            expect(
              await older.reader.readViewTargetBehindInTxn(
                txn,
                newView,
                _kType,
              ),
              isTrue,
            );
            throw StateError('roll back');
          }),
          throwsStateError,
        );
        expect(await behind(older, newView, _kType), isFalse);
      });
    });

    group('downgrade refusal compares majors', () {
      // Verifies: EVS-DEV-entry-type-downgrade-refusal/A+C
      test('a stored 2.0 target refuses a build registering 1.5, naming both '
          'versions, and changes nothing', () async {
        if (db == null) return;
        final current = await _openStore(
          db!,
          registered: const EntryTypeVersion(2, 0),
        );
        await _appendNote(current, 'agg-1', <String, Object?>{'a': 1});
        final eventsBefore = await current.reader.findAllEvents();
        final counterBefore = await current.reader.readSequenceCounter();
        await db!.stop(current);
        final reader = await db!.openBackend();

        await expectLater(
          _openStore(db!, registered: const EntryTypeVersion(1, 5)),
          throwsA(
            isA<EntryTypeVersionDowngradeError>()
                .having((e) => e.entryType, 'entryType', _kType)
                .having(
                  (e) => e.fromVersion,
                  'fromVersion',
                  const EntryTypeVersion(2, 0),
                )
                .having(
                  (e) => e.toVersion,
                  'toVersion',
                  const EntryTypeVersion(1, 5),
                )
                .having(
                  (e) => e.toString(),
                  'message',
                  allOf(contains('2.0'), contains('1.5'), contains('major')),
                ),
          ),
        );
        expect(
          await reader.transaction(
            (txn) => reader.readViewTargetVersionInTxn(txn, _kView, _kType),
          ),
          const EntryTypeVersion(2, 0),
        );
        expect((await reader.findAllEvents()).length, eventsBefore.length);
        expect(await reader.readSequenceCounter(), counterBefore);
      });

      // Verifies: EVS-DEV-entry-type-downgrade-refusal/A
      test('a stored 1.3 target opens under a build registering 1.1', () async {
        if (db == null) return;
        await _openStore(db!, registered: const EntryTypeVersion(1, 3));
        final older = await _openStore(
          db!,
          registered: const EntryTypeVersion(1, 1),
        );
        expect(await _storedTarget(older), const EntryTypeVersion(1, 3));
      });
    });

    group('ingest compares majors', () {
      Future<EventStore> openReceiver() => _openStore(
        db!,
        registered: const EntryTypeVersion(1, 4),
        promoters: const <PromoterSpec>[
          PromoterSpec(
            viewName: _kView,
            entryType: _kType,
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: <TransformPrimitive>[
              DefaultField(fieldName: 'added', defaultValue: 'default'),
            ],
          ),
        ],
      );

      final paths = <String, Future<void> Function(EventStore, StoredEvent)>{
        'ingestBatch': (store, event) async {
          await store.ingestBatch(
            _batchOf(event),
            wireFormat: BatchEnvelope.wireFormat,
          );
        },
        'ingestEvent': (store, event) async {
          await store.ingestEvent(event);
        },
      };

      final refusals = <String, (EntryTypeVersion, DataFormatVersion, Matcher)>{
        'data format 1.0': (
          const EntryTypeVersion(1, 4),
          const DataFormatVersion(1, 0),
          isA<IngestDataFormatIncompatible>()
              .having(
                (e) => e.wireFormat,
                'wireFormat',
                const DataFormatVersion(1, 0),
              )
              .having(
                (e) => e.receiverFormat,
                'receiverFormat',
                LibVersion.dataFormat,
              ),
        ),
        'data format 3.0': (
          const EntryTypeVersion(1, 4),
          const DataFormatVersion(3, 0),
          isA<IngestDataFormatIncompatible>(),
        ),
        'entry type 2.0 under 1.4': (
          const EntryTypeVersion(2, 0),
          LibVersion.dataFormat,
          isA<IngestEntryTypeVersionAhead>()
              .having(
                (e) => e.wireVersion,
                'wireVersion',
                const EntryTypeVersion(2, 0),
              )
              .having(
                (e) => e.receiverVersion,
                'receiverVersion',
                const EntryTypeVersion(1, 4),
              ),
        ),
      };

      for (final path in paths.entries) {
        for (final refusal in refusals.entries) {
          // Verifies: EVS-DEV-version-compatibility/D
          test('${path.key} refuses ${refusal.key} before any write', () async {
            if (db == null) return;
            final receiver = await openReceiver();
            final (entryVersion, dataFormat, matcher) = refusal.value;
            final eventsBefore = await receiver.reader.findAllEvents();
            final counterBefore = await receiver.reader.readSequenceCounter();
            await expectLater(
              path.value(
                receiver,
                _peerEvent(
                  entryTypeVersion: entryVersion,
                  dataFormat: dataFormat,
                  data: const <String, Object?>{'title': 'peer'},
                ),
              ),
              throwsA(matcher),
            );
            expect(
              (await receiver.reader.findAllEvents()).length,
              eventsBefore.length,
            );
            expect(await receiver.reader.readSequenceCounter(), counterBefore);
            expect(await receiver.reader.findViewRows(_kView), isEmpty);
          });
        }

        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} accepts data format 2.7', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 4),
            dataFormat: const DataFormatVersion(2, 7),
            data: const <String, Object?>{'title': 'peer'},
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.libFormatVersion, const DataFormatVersion(2, 7));
        });

        // Verifies: EVS-DEV-version-compatibility/D
        // Verifies: EVS-DEV-ingest-promotes-before-fold/D
        test('${path.key} accepts entry type 1.9 under 1.4, folds it unchanged '
            'and reads it back as 1.9', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 9),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'newer'},
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(1, 9));
          final row = await _row(receiver, event.aggregateId);
          expect(row!['title'], 'newer');
          expect(row, isNot(contains('added')));
        });

        // Verifies: EVS-DEV-version-compatibility/D
        // Verifies: EVS-DEV-ingest-promotes-before-fold/A
        test('${path.key} promotes entry type 1.0 under 1.4', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 0),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'older'},
          );
          await path.value(receiver, event);
          final row = await _row(receiver, event.aggregateId);
          expect(row!['title'], 'older');
          expect(row['added'], 'default');
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(1, 0));
        });
      }

      // A batch whose later event is refused commits nothing, not even the
      // compatible event staged before it.
      final laterRefusals = <String, (EntryTypeVersion, DataFormatVersion)>{
        'data format 3.0': (
          const EntryTypeVersion(1, 4),
          const DataFormatVersion(3, 0),
        ),
        'entry type 2.0 under 1.4': (
          const EntryTypeVersion(2, 0),
          LibVersion.dataFormat,
        ),
      };
      for (final refusal in laterRefusals.entries) {
        // Verifies: EVS-DEV-version-compatibility/D
        test('ingestBatch of [a compatible event, ${refusal.key}] writes '
            'nothing', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          await _appendNote(receiver, 'agg-held', <String, Object?>{'a': 1});
          final eventsBefore = (await receiver.reader.findAllEvents()).length;
          final counterBefore = await receiver.reader.readSequenceCounter();
          final rowsBefore = await receiver.reader.findViewRows(_kView);
          final targetBefore = await _storedTarget(receiver);
          final (entryVersion, dataFormat) = refusal.value;
          final bytes = _batchOfMaps(<Map<String, Object?>>[
            _peerEvent(
              entryTypeVersion: const EntryTypeVersion(1, 0),
              dataFormat: LibVersion.dataFormat,
              data: const <String, Object?>{'title': 'staged'},
            ).toMap(),
            _peerEvent(
              entryTypeVersion: entryVersion,
              dataFormat: dataFormat,
              data: const <String, Object?>{'title': 'refused'},
            ).toMap(),
          ]);
          await expectLater(
            receiver.ingestBatch(bytes, wireFormat: BatchEnvelope.wireFormat),
            throwsA(
              anyOf(
                isA<IngestDataFormatIncompatible>(),
                isA<IngestEntryTypeVersionAhead>(),
              ),
            ),
          );
          expect((await receiver.reader.findAllEvents()).length, eventsBefore);
          expect(await receiver.reader.readSequenceCounter(), counterBefore);
          expect(await receiver.reader.findViewRows(_kView), rowsBefore);
          expect(await _storedTarget(receiver), targetBefore);
        });
      }

      for (final path in paths.entries) {
        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} refuses a lower major no registered step promotes, '
            'by name and before any write', () async {
          if (db == null) return;
          final receiver = await _openStore(
            db!,
            registered: const EntryTypeVersion(2, 0),
          );
          final eventsBefore = (await receiver.reader.findAllEvents()).length;
          final counterBefore = await receiver.reader.readSequenceCounter();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 3),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'unpromotable'},
          );
          await expectLater(
            path.value(receiver, event),
            throwsA(
              isA<IngestEntryTypeVersionUnpromotable>()
                  .having((e) => e.eventId, 'eventId', event.eventId)
                  .having((e) => e.entryType, 'entryType', _kType)
                  .having((e) => e.viewName, 'viewName', _kView)
                  .having(
                    (e) => e.wireVersion,
                    'wireVersion',
                    const EntryTypeVersion(1, 3),
                  )
                  .having(
                    (e) => e.receiverVersion,
                    'receiverVersion',
                    const EntryTypeVersion(2, 0),
                  ),
            ),
          );
          expect((await receiver.reader.findAllEvents()).length, eventsBefore);
          expect(await receiver.reader.readSequenceCounter(), counterBefore);
          expect(await receiver.reader.findViewRows(_kView), isEmpty);
        });

        // Verifies: EVS-DEV-version-compatibility/D
        test('${path.key} accepts an entry type the receiver does not '
            'register, at any version, and stores it unchanged', () async {
          if (db == null) return;
          final receiver = await openReceiver();
          final event = _peerEvent(
            entryTypeVersion: const EntryTypeVersion(9, 3),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'relayed'},
            entryType: 'unregistered_type',
          );
          await path.value(receiver, event);
          final stored = await receiver.reader.findEventById(event.eventId);
          expect(stored!.entryTypeVersion, const EntryTypeVersion(9, 3));
          expect(stored.data, event.data);
          expect(await receiver.reader.findViewRows(_kView), isEmpty);
        });
      }

      // Verifies: EVS-DEV-version-compatibility/D
      test('ingestBatch refuses an event with a malformed version as a decode '
          'failure naming the field, before any write', () async {
        if (db == null) return;
        final receiver = await openReceiver();
        final eventsBefore = (await receiver.reader.findAllEvents()).length;
        final counterBefore = await receiver.reader.readSequenceCounter();
        final malformed = <String, Map<String, Object?>>{
          'entry_type_version': <String, Object?>{'major': 0, 'minor': 0},
          'lib_format_version': <String, Object?>{'major': 2},
        };
        for (final field in malformed.entries) {
          final map = Map<String, Object?>.from(
            _peerEvent(
              entryTypeVersion: const EntryTypeVersion(1, 4),
              dataFormat: LibVersion.dataFormat,
              data: const <String, Object?>{'title': 'malformed'},
            ).toMap(),
          )..[field.key] = field.value;
          await expectLater(
            receiver.ingestBatch(
              _batchOfMaps(<Map<String, Object?>>[map]),
              wireFormat: BatchEnvelope.wireFormat,
            ),
            throwsA(
              isA<IngestDecodeFailure>().having(
                (e) => e.message,
                'message',
                contains(field.key),
              ),
            ),
          );
        }
        expect((await receiver.reader.findAllEvents()).length, eventsBefore);
        expect(await receiver.reader.readSequenceCounter(), counterBefore);
      });

      // Verifies: EVS-DEV-version-compatibility/D
      test('a batch in the earlier envelope format is refused by name before '
          'any write', () async {
        if (db == null) return;
        final receiver = await openReceiver();
        final eventsBefore = await receiver.reader.findAllEvents();
        final bytes = _batchOf(
          _peerEvent(
            entryTypeVersion: const EntryTypeVersion(1, 4),
            dataFormat: LibVersion.dataFormat,
            data: const <String, Object?>{'title': 'peer'},
          ),
          batchFormatVersion: '1',
        );
        await expectLater(
          receiver.ingestBatch(bytes, wireFormat: 'esd/batch@1'),
          throwsA(
            isA<IngestDecodeFailure>().having(
              (e) => e.message,
              'message',
              contains('esd/batch@1'),
            ),
          ),
        );
        await expectLater(
          receiver.ingestBatch(bytes, wireFormat: BatchEnvelope.wireFormat),
          throwsA(
            isA<IngestDecodeFailure>().having(
              (e) => e.message,
              'message',
              contains('batch_format_version'),
            ),
          ),
        );
        expect(
          (await receiver.reader.findAllEvents()).length,
          eventsBefore.length,
        );
      });
    });
  });
}

var _peerCounter = 0;

/// An event as a peer at [entryTypeVersion] and [dataFormat] sends it: one
/// origin provenance entry and the canonical hash of its record.
StoredEvent _peerEvent({
  required EntryTypeVersion entryTypeVersion,
  required DataFormatVersion dataFormat,
  required Map<String, Object?> data,
  String? aggregateId,
  String entryType = _kType,
}) {
  _peerCounter += 1;
  final now = DateTime.utc(2026, 9, 1, 12, 0, _peerCounter);
  final record = <String, Object?>{
    'event_id': 'peer-event-$_peerCounter-${now.microsecondsSinceEpoch}',
    'aggregate_id': aggregateId ?? 'peer-aggregate-$_peerCounter',
    'aggregate_type': 'note',
    'entry_type': entryType,
    'entry_type_version': entryTypeVersion.toJson(),
    'lib_format_version': dataFormat.toJson(),
    'event_type': 'finalized',
    'sequence_number': 1000 + _peerCounter,
    'data': data,
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[
        ProvenanceEntry(
          hop: 'peer-hop',
          receivedAt: now,
          identifier: 'peer-install',
          softwareVersion: 'peer@1',
        ).toJson(),
      ],
    },
    'initiator': const UserInitiator('peer-user').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'previous_event_hash': null,
  };
  record['event_hash'] = canonicalEventHash(record);
  return StoredEvent.fromMap(record, 0);
}

Uint8List _batchOf(StoredEvent event, {String? batchFormatVersion}) =>
    _batchOfMaps(<Map<String, Object?>>[
      Map<String, Object?>.from(event.toMap()),
    ], batchFormatVersion: batchFormatVersion);

Uint8List _batchOfMaps(
  List<Map<String, Object?>> events, {
  String? batchFormatVersion,
}) {
  final now = DateTime.utc(2026, 9, 1, 12);
  return BatchEnvelope(
    batchFormatVersion:
        batchFormatVersion ?? BatchEnvelope.currentBatchFormatVersion,
    batchId: 'versions-batch-${events.first['event_id']}',
    senderHop: 'peer-hop',
    senderIdentifier: 'peer-install',
    senderSoftwareVersion: 'peer@1',
    sentAt: now,
    events: events,
  ).encode();
}
