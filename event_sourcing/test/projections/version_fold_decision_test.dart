// The fold decides by major and minor: a lower version is promoted, a
// same-major event at an equal or higher minor folds unchanged, and a
// higher major is refused, both in the projection interpreter and in
// rebuildView.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _kType = 'note';
const _kView = 'notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// `1.0 -> 1.1` adds `x`; `1.1 -> 1.2` adds `y`.
List<PromoterSpec> _minorSteps() => const <PromoterSpec>[
  PromoterSpec(
    viewName: _kView,
    entryType: _kType,
    fromVersion: EntryTypeVersion(1, 0),
    toVersion: EntryTypeVersion(1, 1),
    transforms: <TransformPrimitive>[
      DefaultField(fieldName: 'x', defaultValue: 'dx'),
    ],
  ),
  PromoterSpec(
    viewName: _kView,
    entryType: _kType,
    fromVersion: EntryTypeVersion(1, 1),
    toVersion: EntryTypeVersion(1, 2),
    transforms: <TransformPrimitive>[
      DefaultField(fieldName: 'y', defaultValue: 'dy'),
    ],
  ),
];

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'version-fold-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

EntryTypeRegistry _registry(EntryTypeVersion registered) {
  final r = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    r.register(definition);
  }
  r.register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: registered,
      name: _kType,
    ),
  );
  return r;
}

PromoterRegistry _promoters() {
  final r = PromoterRegistry();
  for (final spec in _minorSteps()) {
    r.register(spec);
  }
  return r;
}

StoredEvent _event(int seq, EntryTypeVersion version) => StoredEvent(
  key: seq,
  eventId: 'e$seq',
  aggregateId: 'agg-$seq',
  aggregateType: 'note',
  entryType: _kType,
  entryTypeVersion: version,
  libFormatVersion: const DataFormatVersion(2, 0),
  eventType: 'finalized',
  sequenceNumber: seq,
  data: const <String, dynamic>{'title': 't'},
  metadata: const <String, dynamic>{'provenance': <Map<String, Object?>>[]},
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026),
  eventHash: 'h$seq',
);

Future<Map<String, Object?>?> _fold(
  EntryTypeVersion registered,
  StoredEvent event,
) async {
  final backend = await _openBackend();
  final interpreter = ProjectionInterpreter(
    projections: ProjectionRegistry()..register(_kSpec),
    promoters: _promoters(),
    entryTypes: _registry(registered),
  );
  await backend.transaction(
    (txn) => interpreter.applyEvent(txn: txn, backend: backend, event: event),
  );
  return backend.transaction(
    (txn) => backend.readViewRowInTxn(txn, _kView, event.aggregateId),
  );
}

void main() {
  group('ProjectionInterpreter', () {
    // Verifies: EVS-DEV-version-compatibility/D
    // Verifies: EVS-DEV-ingest-promotes-before-fold/D
    test('an event at 1.2 under registered 1.1 folds unchanged', () async {
      final row = await _fold(
        const EntryTypeVersion(1, 1),
        _event(1, const EntryTypeVersion(1, 2)),
      );
      expect(row!['title'], 't');
      expect(row, isNot(contains('x')));
      expect(row, isNot(contains('y')));
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/D
    test('an event at the registered version folds unchanged', () async {
      final row = await _fold(
        const EntryTypeVersion(1, 2),
        _event(1, const EntryTypeVersion(1, 2)),
      );
      expect(row, isNotNull, reason: 'the event folds into a row');
      expect(row!['title'], 't');
      expect(row, isNot(contains('x')));
      expect(row, isNot(contains('y')));
    });

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('an event at 1.0 under 1.2 is promoted through the DefaultField '
        'steps', () async {
      final row = await _fold(
        const EntryTypeVersion(1, 2),
        _event(1, const EntryTypeVersion(1, 0)),
      );
      expect(row!['x'], 'dx');
      expect(row['y'], 'dy');
    });

    // Verifies: EVS-DEV-version-compatibility/D
    test('an event at 2.0 under a 1.x registration is refused and writes '
        'nothing', () async {
      final backend = await _openBackend();
      final interpreter = ProjectionInterpreter(
        projections: ProjectionRegistry()..register(_kSpec),
        promoters: _promoters(),
        entryTypes: _registry(const EntryTypeVersion(1, 2)),
      );
      await expectLater(
        backend.transaction(
          (txn) => interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: _event(1, const EntryTypeVersion(2, 0)),
          ),
        ),
        throwsStateError,
      );
      expect(await backend.findViewRows(_kView), isEmpty);
    });
  });

  group('promoted defaults against the row', () {
    StoredEvent delta(
      int seq,
      String aggregateId,
      EntryTypeVersion version,
      Map<String, Object?> data,
    ) => StoredEvent(
      key: seq,
      eventId: 'd$seq',
      aggregateId: aggregateId,
      aggregateType: 'note',
      entryType: _kType,
      entryTypeVersion: version,
      libFormatVersion: const DataFormatVersion(2, 0),
      eventType: 'finalized',
      sequenceNumber: seq,
      data: data,
      metadata: const <String, dynamic>{'provenance': <Map<String, Object?>>[]},
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026),
      eventHash: 'hd$seq',
    );

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test(
      'a promoted default does not override a value the row holds',
      () async {
        final backend = await _openBackend();
        final interpreter = ProjectionInterpreter(
          projections: ProjectionRegistry()..register(_kSpec),
          promoters: _promoters(),
          entryTypes: _registry(const EntryTypeVersion(1, 2)),
        );
        await backend.transaction((txn) async {
          await interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: delta(1, 'agg-r', const EntryTypeVersion(1, 2), {
              'a': 1,
              'x': 'set-x',
              'y': 'set-y',
            }),
          );
          await interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: delta(2, 'agg-r', const EntryTypeVersion(1, 0), {'a': 3}),
          );
        });
        final row = await backend.transaction(
          (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-r'),
        );
        expect(row!['a'], 3);
        expect(row['x'], 'set-x');
        expect(row['y'], 'set-y');
      },
    );

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test(
      "a promoted default fills a field the aggregate's row lacks",
      () async {
        final backend = await _openBackend();
        final interpreter = ProjectionInterpreter(
          projections: ProjectionRegistry()..register(_kSpec),
          promoters: _promoters(),
          entryTypes: _registry(const EntryTypeVersion(1, 2)),
        );
        await backend.transaction((txn) async {
          await interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: delta(1, 'agg-r', const EntryTypeVersion(1, 2), {
              'a': 1,
              'x': 'set-x',
            }),
          );
          await interpreter.applyEvent(
            txn: txn,
            backend: backend,
            event: delta(2, 'agg-r', const EntryTypeVersion(1, 0), {'a': 3}),
          );
        });
        final row = await backend.transaction(
          (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-r'),
        );
        expect(row!['x'], 'set-x');
        expect(row['y'], 'dy');
      },
    );

    // Verifies: EVS-DEV-ingest-promotes-before-fold/A
    test('rebuildView decides defaults against the row it rebuilds', () async {
      final backend = await _openBackend();
      final store = await EventStore.openForTest(
        storage: backend,
        entryTypes: _registry(const EntryTypeVersion(1, 2)),
        source: const Source(
          hopId: 'rebuild-hop',
          identifier: 'rebuild-install',
          softwareVersion: 't',
        ),
        securityContexts: SembastSecurityContextStore(backend: backend),
        projections: ProjectionRegistry()..register(_kSpec),
        promoters: _promoters(),
      );
      await backend.transaction((txn) async {
        for (final e in <StoredEvent>[
          delta(1, 'agg-r', const EntryTypeVersion(1, 2), {
            'a': 1,
            'x': 'set-x',
            'y': 'set-y',
          }),
          delta(2, 'agg-r', const EntryTypeVersion(1, 0), {'a': 3}),
          delta(3, 'agg-s', const EntryTypeVersion(1, 0), {'a': 4}),
        ]) {
          final seq = await backend.nextSequenceNumber(txn);
          await backend.appendEvent(
            txn,
            StoredEvent.fromMap(<String, Object?>{
              ...e.toMap(),
              'sequence_number': seq,
            }, seq),
          );
        }
      });
      await rebuildView(
        store: store,
        viewName: _kView,
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          _kType: EntryTypeVersion(1, 2),
        },
      );
      final kept = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-r'),
      );
      final filled = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-s'),
      );
      expect(kept!['a'], 3);
      expect(kept['x'], 'set-x');
      expect(kept['y'], 'set-y');
      expect(filled!['x'], 'dx');
      expect(filled['y'], 'dy');
    });
  });

  group('rebuildView', () {
    Future<EventStore> openStore(
      SembastBackend backend,
      EntryTypeVersion registered,
    ) => EventStore.openForTest(
      storage: backend,
      entryTypes: _registry(registered),
      source: const Source(
        hopId: 'rebuild-hop',
        identifier: 'rebuild-install',
        softwareVersion: 't',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: ProjectionRegistry()..register(_kSpec),
      promoters: _promoters(),
    );

    Future<void> seed(SembastBackend backend, List<StoredEvent> events) =>
        backend.transaction((txn) async {
          for (final e in events) {
            final seq = await backend.nextSequenceNumber(txn);
            await backend.appendEvent(
              txn,
              StoredEvent.fromMap(<String, Object?>{
                ...e.toMap(),
                'sequence_number': seq,
              }, seq),
            );
          }
        });

    // Verifies: EVS-DEV-version-compatibility/D
    // Verifies: EVS-DEV-ingest-promotes-before-fold/A+D
    test('promotes a lower minor and folds a higher minor unchanged', () async {
      final backend = await _openBackend();
      await seed(backend, <StoredEvent>[
        _event(1, const EntryTypeVersion(1, 0)),
        _event(2, const EntryTypeVersion(1, 3)),
      ]);
      final store = await openStore(backend, const EntryTypeVersion(1, 2));
      await rebuildView(
        store: store,
        viewName: _kView,
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          _kType: EntryTypeVersion(1, 2),
        },
      );
      final promoted = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-1'),
      );
      final unchanged = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _kView, 'agg-2'),
      );
      expect(promoted!['x'], 'dx');
      expect(promoted['y'], 'dy');
      expect(unchanged, isNotNull, reason: 'the 1.3 event folds into a row');
      expect(unchanged!['title'], 't');
      expect(unchanged, isNot(contains('x')));
      expect(unchanged, isNot(contains('y')));
    });

    // Verifies: EVS-DEV-version-compatibility/D
    test('refuses a log holding a higher major and leaves the view as it '
        'was', () async {
      final backend = await _openBackend();
      final store = await openStore(backend, const EntryTypeVersion(1, 2));
      await store.append(
        entryType: _kType,
        aggregateId: 'agg-kept',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'title': 'kept'},
        initiator: const UserInitiator('u'),
      );
      await seed(backend, <StoredEvent>[
        _event(7, const EntryTypeVersion(2, 0)),
      ]);
      final before = await backend.findViewRows(_kView);
      await expectLater(
        rebuildView(
          store: store,
          viewName: _kView,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 2),
          },
        ),
        throwsStateError,
      );
      expect(await backend.findViewRows(_kView), before);
    });
  });
}
