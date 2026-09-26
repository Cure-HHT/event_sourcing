// Backend-agnostic scenarios for EventStore.open's boot: the database
// identity, the local-event rule, the data-format decision, the boot record,
// refusals that write nothing, and databases an earlier build wrote. Run on
// Sembast by test/event_store/data_format_compatibility_test.dart and on
// Postgres by test/storage/postgres/postgres_data_format_compatibility_test.dart.
//
// Traceability lives on the individual tests below.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kViewSnapshotPromotedEntryType;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'lib_version_seed.dart';
import 'test_backends.dart';
import 'wedges_view_invariant.dart' show expectReservedShapes;

/// One database the boot scenarios open several backends over, as several
/// builds or instances of the library would.
abstract class BootTestDatabase {
  /// Opens a new backend over this database.
  Future<StorageBackend> openBackend();

  /// The security-context store an event store over [backend] uses.
  MutableSecurityContextStore securityFor(StorageBackend backend);

  /// Writes [value] as the stored database identity, or deletes it when
  /// null, bypassing the library (a durability failure or tampering).
  Future<void> rewriteStoredDatabaseId(String? value);

  /// Turns this empty database into one an earlier data format wrote, whose
  /// stored version shapes are single integers. Called before any backend
  /// is opened.
  Future<void> writeEarlierFormatShape();

  /// Rewrites every stored event, bypassing the library, as a build of data
  /// format 2.0 stored it: its `lib_format_version` is 2.0, its provenance
  /// entries carry no `database_id` and no `library_version`, and it
  /// carries no `causal` object. With [keepFields], only its
  /// `lib_format_version` is rewritten.
  Future<void> rewriteEventsAsDataFormat2({bool keepFields = false});

  /// Stops the instance [store] belongs to, as a stop-then-start deployment
  /// stops the old revision before the new one opens: on Postgres, where
  /// each instance holds its own connections and generation locks, it
  /// closes the store; on Sembast, where the scenarios share one database
  /// handle and a registration holds nothing, it does nothing.
  Future<void> stop(EventStore store);

  /// Closes every backend this database opened.
  Future<void> close();
}

const _kType = 'boot_note';
const _kView = 'boot_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// The newer build of the same data-format major every scenario plays.
final _kNewer = (version: '0.6.0', dataFormat: LibVersion.dataFormat.nextMinor);

/// The data format of the next major, which no build of this one opens.
final _kNextMajor = DataFormatVersion(LibVersion.dataFormat.major + 1, 0);

/// The compiled build.
const _kCompiled = (
  version: LibVersion.version,
  dataFormat: LibVersion.dataFormat,
);

Source _source(String identifier) => Source(
  hopId: 'boot-hop',
  identifier: identifier,
  softwareVersion: 'boot-test',
);

EntryTypeRegistry _registry(EntryTypeVersion noteVersion) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: noteVersion,
      name: _kType,
    ),
  );
  return registry;
}

/// Opens an event store over [backend] with `open` (or `openForTest` when
/// [forTest]) as the build [build], registering [_kType] at [noteVersion]
/// (at `1.1` with the `DefaultField` step adding `b`). The test seams are
/// installed around the open, [build] included.
Future<EventStore> _open(
  BootTestDatabase db,
  StorageBackend backend, {
  ({String version, DataFormatVersion dataFormat}) build = _kCompiled,
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
  String identifier = 'boot-install',
  bool forTest = false,
  bool Function()? afterBootVersionEvent,
  void Function()? onBootBodyRun,
}) {
  final promoters = PromoterRegistry();
  if (noteVersion == const EntryTypeVersion(1, 1)) {
    promoters.register(
      const PromoterSpec(
        viewName: _kView,
        entryType: _kType,
        fromVersion: EntryTypeVersion(1, 0),
        toVersion: EntryTypeVersion(1, 1),
        transforms: <TransformPrimitive>[
          DefaultField(fieldName: 'b', defaultValue: 0),
        ],
      ),
    );
  }
  final hooks = DeliveryTestHooks(
    buildDeclaration: build == _kCompiled ? null : build,
    afterBootVersionEvent: afterBootVersionEvent,
    onBootBodyRun: onBootBodyRun,
  );
  return runWithDeliveryTestHooks(hooks, () async {
    final EventStore store;
    if (forTest) {
      store = await EventStore.openForTest(
        storage: backend,
        entryTypes: _registry(noteVersion),
        source: _source(identifier),
        securityContexts: db.securityFor(backend),
        projections: ProjectionRegistry()..register(_kSpec),
        promoters: promoters,
      );
    } else {
      store = await EventStore.open(
        storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
        entryTypes: _registry(noteVersion),
        source: _source(identifier),
        projections: ProjectionRegistry()..register(_kSpec),
        promoters: promoters,
      );
    }
    trackTestBackend(store, backend);
    return store;
  });
}

Future<void> _appendNote(EventStore store, String aggregateId) async {
  await store.append(
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'title': aggregateId},
    initiator: const UserInitiator('boot-user'),
  );
}

/// The library-version events in [backend]'s log, oldest first.
Future<List<StoredEvent>> _libVersionEvents(StorageBackend backend) async {
  final events = await backend.findAllEvents();
  return <StoredEvent>[
    for (final e in events)
      if (e.eventType == LibVersionEvents.initialized ||
          e.eventType == LibVersionEvents.changed)
        e,
  ];
}

/// Everything a refused boot must leave as it was.
class _Snapshot {
  _Snapshot._(
    this.counter,
    this.eventIds,
    this.databaseId,
    this.bootCheck,
    this.targets,
    this.rows,
  );

  static Future<_Snapshot> of(StorageBackend backend) async {
    final (databaseId, bootCheck, targets) = await backend.transaction(
      (txn) async => (
        await backend.readDatabaseIdTxn(txn),
        await backend.readBootCheckTxn(txn),
        await backend.readAllViewTargetVersionsInTxn(txn, _kView),
      ),
    );
    return _Snapshot._(
      await backend.readSequenceCounter(),
      [for (final e in await backend.findAllEvents()) e.eventId],
      databaseId,
      bootCheck,
      targets,
      await backend.findViewRows(_kView),
    );
  }

  final int counter;
  final List<String> eventIds;
  final String? databaseId;
  final BootCheck? bootCheck;
  final Map<String, EntryTypeVersion> targets;
  final List<Map<String, dynamic>> rows;

  void expectUnchangedIn(_Snapshot after) {
    expect(after.counter, counter, reason: 'sequence counter');
    expect(after.eventIds, eventIds, reason: 'events');
    expect(after.databaseId, databaseId, reason: 'database identity');
    expect(after.bootCheck, bootCheck, reason: 'boot record');
    expect(after.targets, targets, reason: 'view target versions');
    expect(after.rows, rows, reason: 'view rows');
  }
}

/// A peer's database: an in-memory Sembast database whatever backend the
/// scenarios run on, since only the events it appends are used.
class _MemoryPeerDatabase implements BootTestDatabase {
  Database? _db;

  @override
  Future<StorageBackend> openBackend() async {
    _db ??= await newDatabaseFactoryMemory().openDatabase(
      'boot-peer-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    return SembastBackend(database: _db!);
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> rewriteStoredDatabaseId(String? value) =>
      throw UnimplementedError();

  @override
  Future<void> writeEarlierFormatShape() => throw UnimplementedError();

  @override
  Future<void> rewriteEventsAsDataFormat2({bool keepFields = false}) =>
      throw UnimplementedError();

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() async => _db?.close();
}

Future<BootCheck?> _bootCheck(StorageBackend backend) =>
    backend.transaction(backend.readBootCheckTxn);

/// Registers the boot scenarios against databases produced by
/// [openDatabase]. A null database skips the test.
void runBootScenarios(
  Future<BootTestDatabase?> Function() openDatabase, {
  required String backendLabel,
}) {
  group('EventStore.open boot ($backendLabel)', () {
    BootTestDatabase? db;

    setUp(() async {
      db = await openDatabase();
      if (db == null) markTestSkipped('no database for $backendLabel');
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    group('database identity', () {
      // Verifies: EVS-DEV-event-store-open/F
      // Verifies: EVS-DEV-event-store-open/B
      test('two stores of different sources share one identity, which the '
          'initialization records and a reopen keeps', () async {
        if (db == null) return;
        final a = await _open(db!, await db!.openBackend(), identifier: 'a');
        final b = await _open(db!, await db!.openBackend(), identifier: 'b');
        expect(a.databaseId, isNotEmpty);
        expect(b.databaseId, a.databaseId);
        final reopened = await _open(db!, await db!.openBackend());
        expect(reopened.databaseId, a.databaseId);
        final events = await _libVersionEvents(testBackendOf(a));
        expect(events, hasLength(1));
        expect(events.single.eventType, LibVersionEvents.initialized);
        expect(events.single.data['database_id'], a.databaseId);
        expect(events.single.data['version'], LibVersion.version);
        expect(
          events.single.data['data_format'],
          LibVersion.dataFormat.toJson(),
        );
      });

      for (final rewrite in <String, String?>{
        'deleted': null,
        'rewritten': 'another-database',
      }.entries) {
        // Verifies: EVS-DEV-event-store-open/F
        test('a stored identity ${rewrite.key} refuses the next open and '
            'writes nothing', () async {
          if (db == null) return;
          final first = await _open(db!, await db!.openBackend());
          await _appendNote(first, 'n1');
          await db!.rewriteStoredDatabaseId(rewrite.value);
          final backend = await db!.openBackend();
          final before = await _Snapshot.of(backend);
          await expectLater(
            _open(db!, backend, build: _kNewer),
            throwsA(
              isA<DatabaseIdentityMismatchError>()
                  .having(
                    (e) => e.recordedDatabaseId,
                    'recordedDatabaseId',
                    first.databaseId,
                  )
                  .having(
                    (e) => e.storedDatabaseId,
                    'storedDatabaseId',
                    rewrite.value,
                  ),
            ),
          );
          await expectLater(
            _open(db!, backend, forTest: true),
            throwsA(isA<DatabaseIdentityMismatchError>()),
          );
          before.expectUnchangedIn(await _Snapshot.of(backend));
        });
      }

      // Verifies: EVS-DEV-event-store-open/F
      test('a seeded initialization naming another identity than the stored '
          'one is refused as an identity failure, before the data format is '
          'compared', () async {
        if (db == null) return;
        final backend = await db!.openBackend();
        await backend.transaction(backend.readOrCreateDatabaseIdTxn);
        await seedLibVersionEventForTest(
          backend,
          version: '9.0.0',
          dataFormat: _kNextMajor,
          databaseId: 'not-the-stored-one',
        );
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(db!, backend),
          throwsA(isA<DatabaseIdentityMismatchError>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));
      });

      // Verifies: EVS-DEV-event-store-open/F
      // Verifies: EVS-DEV-event-store-open/B
      test('openForTest mints the identity without a log record; the next '
          'open adopts it in the one initialization it appends', () async {
        if (db == null) return;
        final forTest = await _open(
          db!,
          await db!.openBackend(),
          forTest: true,
        );
        expect(await _libVersionEvents(testBackendOf(forTest)), isEmpty);
        final opened = await _open(db!, await db!.openBackend());
        expect(opened.databaseId, forTest.databaseId);
        final events = await _libVersionEvents(testBackendOf(opened));
        expect(events, hasLength(1));
        expect(events.single.data['database_id'], forTest.databaseId);
        final again = await _open(db!, await db!.openBackend());
        expect(again.databaseId, forTest.databaseId);
        expect(await _libVersionEvents(testBackendOf(again)), hasLength(1));
      });
    });

    group('only locally appended library-version events count', () {
      // Verifies: EVS-DEV-event-store-open/B
      // Verifies: EVS-DEV-event-store-open/C
      // Verifies: EVS-DEV-event-store-open/F
      test("a newer build's forwarded initialization, ingested with an "
          'earlier timestamp, is neither the version nor the identity of the '
          'receiver', () async {
        if (db == null) return;
        // M: another database, opened by a newer build of the same major.
        final peerDb = _MemoryPeerDatabase();
        addTearDown(peerDb.close);
        final m = await _open(
          peerDb,
          await peerDb.openBackend(),
          build: _kNewer,
          identifier: 'm',
        );
        final forwarded = (await _libVersionEvents(testBackendOf(m))).single;
        expect(forwarded.data['version'], _kNewer.version);

        // H: the compiled build, opened after M, ingests M's initialization.
        final h = await _open(db!, await db!.openBackend(), identifier: 'h');
        await h.ingestEvent(forwarded);
        final hEvents = await _libVersionEvents(testBackendOf(h));
        expect(hEvents, hasLength(2));
        expect(
          hEvents.map((e) => e.data['version']),
          containsAll(<String>[LibVersion.version, _kNewer.version]),
        );

        final reopened = await _open(db!, await db!.openBackend());
        expect(reopened.databaseId, h.databaseId);
        expect(reopened.databaseId, isNot(m.databaseId));
        final after = await _libVersionEvents(testBackendOf(reopened));
        expect(after, hasLength(2), reason: 'no lib_version_changed');
        expect(
          after.where((e) => e.eventType == LibVersionEvents.changed),
          isEmpty,
        );
      });
    });

    group('only locally appended initializations name the database', () {
      // Verifies: EVS-DEV-event-store-open/F
      // Verifies: EVS-DEV-event-store-open/B
      test("a peer's ingested initialization, the only one in the log, is "
          "not the database's: the first open adopts the stored identity "
          'and records its own initialization', () async {
        if (db == null) return;
        final peerDb = _MemoryPeerDatabase();
        addTearDown(peerDb.close);
        final m = await _open(
          peerDb,
          await peerDb.openBackend(),
          identifier: 'm',
        );
        final forwarded = (await _libVersionEvents(testBackendOf(m))).single;

        // H: minted its identity through openForTest, so its log holds no
        // initialization of its own when it ingests M's.
        final h = await _open(
          db!,
          await db!.openBackend(),
          identifier: 'h',
          forTest: true,
        );
        await h.ingestEvent(forwarded);
        expect(await _libVersionEvents(testBackendOf(h)), hasLength(1));

        final opened = await _open(db!, await db!.openBackend());
        expect(opened.databaseId, h.databaseId);
        expect(opened.databaseId, isNot(m.databaseId));
        final initializations = (await _libVersionEvents(
          testBackendOf(opened),
        )).where((e) => e.data['database_id'] == h.databaseId).toList();
        expect(initializations, hasLength(1));
        expect(initializations.single.eventType, LibVersionEvents.initialized);
      });
    });

    group('boot record', () {
      // Verifies: EVS-DEV-event-store-open/E
      test('an accepted reopen with nothing due writes the boot record and '
          'appends no event', () async {
        if (db == null) return;
        final first = await _open(db!, await db!.openBackend());
        final firstCheck = await _bootCheck(testBackendOf(first));
        expect(firstCheck, isNotNull);
        expect(firstCheck!.packageVersion, LibVersion.version);
        expect(firstCheck.dataFormat, LibVersion.dataFormat);
        final counter = await first.reader.readSequenceCounter();
        await Future<void>.delayed(const Duration(milliseconds: 5));
        final second = await _open(db!, await db!.openBackend());
        expect(await second.reader.readSequenceCounter(), counter);
        final secondCheck = await _bootCheck(testBackendOf(second));
        expect(secondCheck, isNotNull);
        expect(secondCheck!.at.isAfter(firstCheck.at), isTrue);
      });

      // Verifies: EVS-DEV-event-store-open/E
      test('openForTest writes the boot record too', () async {
        if (db == null) return;
        final store = await _open(db!, await db!.openBackend(), forTest: true);
        expect(await _bootCheck(testBackendOf(store)), isNotNull);
      });

      // Verifies: EVS-DEV-event-store-open/E
      test('a refused open leaves the boot record as it was', () async {
        if (db == null) return;
        final first = await _open(db!, await db!.openBackend());
        final before = await _bootCheck(testBackendOf(first));
        await seedLibVersionEventForTest(
          testBackendOf(first),
          eventType: LibVersionEvents.changed,
          version: '9.0.0',
          dataFormat: _kNextMajor,
        );
        final backend = await db!.openBackend();
        await expectLater(
          _open(db!, backend),
          throwsA(isA<DataFormatIncompatibleError>()),
        );
        expect(await _bootCheck(backend), before);
      });

      // Verifies: EVS-DEV-event-store-open/E
      // With no concurrent writer the backend makes exactly one run of the
      // boot transaction, so the body runs exactly once. A backend that
      // re-runs the transaction is covered by the rerunning-backend test of
      // the Sembast suite.
      test('an uncontended boot runs its body exactly once', () async {
        if (db == null) return;
        var runs = 0;
        await _open(db!, await db!.openBackend(), onBootBodyRun: () => runs++);
        expect(runs, 1);
      });
    });

    group('databases an earlier build wrote', () {
      for (final shape in <String, ({bool id, bool format})>{
        'no database identity': (id: false, format: true),
        'no data format': (id: true, format: false),
      }.entries) {
        // Verifies: EVS-DEV-event-store-open/F
        test('an initialization recording ${shape.key} is refused with the '
            'reset error and writes nothing', () async {
          if (db == null) return;
          final backend = await db!.openBackend();
          await seedLibVersionEventForTest(
            backend,
            version: '0.4.0',
            dataFormat: LibVersion.dataFormat,
            recordDatabaseId: shape.value.id,
            recordDataFormat: shape.value.format,
          );
          final before = await _Snapshot.of(backend);
          await expectLater(
            _open(db!, backend),
            throwsA(isA<DatabaseResetRequiredError>()),
          );
          await expectLater(
            _open(db!, backend, forTest: true),
            throwsA(isA<DatabaseResetRequiredError>()),
          );
          before.expectUnchangedIn(await _Snapshot.of(backend));
        });
      }

      // Verifies: EVS-DEV-event-store-open/F
      test('a change recording no data format is refused with the reset '
          'error', () async {
        if (db == null) return;
        final backend = await db!.openBackend();
        await seedLibVersionEventForTest(
          backend,
          version: '0.4.0',
          dataFormat: LibVersion.dataFormat,
        );
        await seedLibVersionEventForTest(
          backend,
          eventType: LibVersionEvents.changed,
          version: '0.4.1',
          dataFormat: LibVersion.dataFormat,
          recordDataFormat: false,
        );
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(db!, backend),
          throwsA(isA<DatabaseResetRequiredError>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));
      });

      // Verifies: EVS-DEV-event-store-open/F
      test('a log recording a library-version change but no initialization '
          'is refused with the reset error and writes nothing', () async {
        if (db == null) return;
        final backend = await db!.openBackend();
        await seedLibVersionEventForTest(
          backend,
          eventType: LibVersionEvents.changed,
          version: '0.4.1',
          dataFormat: LibVersion.dataFormat,
        );
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(db!, backend),
          throwsA(
            isA<DatabaseResetRequiredError>().having(
              (e) => e.reason,
              'reason',
              contains('no initialization'),
            ),
          ),
        );
        await expectLater(
          _open(db!, backend, forTest: true),
          throwsA(isA<DatabaseResetRequiredError>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));
      });

      // Verifies: EVS-DEV-event-store-open/F
      test('stored shapes of an earlier data format are refused by name, '
          'before any write', () async {
        if (db == null) return;
        await db!.writeEarlierFormatShape();
        final backend = await db!.openBackend();
        final counter = await backend.readSequenceCounter();
        await expectLater(
          _open(db!, backend),
          throwsA(
            isA<DatabaseResetRequiredError>().having(
              (e) => e.message,
              'message',
              contains('must be reset'),
            ),
          ),
        );
        expect(await backend.readSequenceCounter(), counter);
        final (id, check) = await backend.transaction(
          (txn) async => (
            await backend.readDatabaseIdTxn(txn),
            await backend.readBootCheckTxn(txn),
          ),
        );
        expect(id, isNull);
        expect(check, isNull);
      });
    });

    group('a database a build of data format 2 wrote', () {
      for (final generation in <String, (bool, bool)>{
        'its latest event': (false, false),
        'its latest event and its generation record': (true, false),
        'its latest event, carrying every field this data format reads,': (
          false,
          true,
        ),
      }.entries) {
        // Verifies: EVS-DEV-version-compatibility/O
        test('${generation.key} in data format 2 refuses the open with '
            'DatabaseResetRequiredError, before any write', () async {
          if (db == null) return;
          final first = await _open(db!, await db!.openBackend());
          await _appendNote(first, 'n1');
          final (writeGeneration, keepFields) = generation.value;
          if (writeGeneration) {
            final backend = testBackendOf(first);
            await backend.transaction(
              (txn) => backend.writeDataGenerationTxn(
                txn,
                GenerationRecord(
                  dataFormatMajor: 2,
                  entryTypeMajors: const <String, int>{_kType: 1},
                ),
              ),
            );
          }
          await db!.stop(first);
          await db!.rewriteEventsAsDataFormat2(keepFields: keepFields);
          final backend = await db!.openBackend();
          final counter = await backend.readSequenceCounter();
          await expectLater(
            _open(db!, backend),
            throwsA(
              isA<DatabaseResetRequiredError>().having(
                (e) => e.message,
                'message',
                contains('data format'),
              ),
            ),
          );
          expect(await backend.readSequenceCounter(), counter);
        });
      }
    });

    group('data-format compatibility', () {
      // Verifies: EVS-DEV-event-store-open/C
      // Verifies: EVS-DEV-event-store-open/D
      test(
        'an older build of the same major opens a database a newer one '
        'opened, records exactly one change, and appends and reads',
        () async {
          if (db == null) return;
          final newer = await _open(
            db!,
            await db!.openBackend(),
            build: _kNewer,
          );
          await _appendNote(newer, 'n1');
          final older = await _open(db!, await db!.openBackend());
          await _appendNote(older, 'o1');
          final events = await _libVersionEvents(testBackendOf(older));
          expect(events.map((e) => e.eventType), <String>[
            LibVersionEvents.initialized,
            LibVersionEvents.changed,
          ]);
          final change = events.last.data;
          expect(change['fromVersion'], _kNewer.version);
          expect(change['toVersion'], LibVersion.version);
          expect(change['fromDataFormat'], _kNewer.dataFormat.toJson());
          expect(change['toDataFormat'], LibVersion.dataFormat.toJson());
          expect(await older.reader.findViewRows(_kView), hasLength(2));
          final again = await _open(db!, await db!.openBackend());
          expect(await _libVersionEvents(testBackendOf(again)), hasLength(2));
        },
      );

      // Verifies: EVS-DEV-event-store-open/C
      test('a newer build of the same major records the change from the '
          'compiled build', () async {
        if (db == null) return;
        await _open(db!, await db!.openBackend());
        final newer = await _open(db!, await db!.openBackend(), build: _kNewer);
        final events = await _libVersionEvents(testBackendOf(newer));
        expect(events, hasLength(2));
        expect(events.last.data['toVersion'], _kNewer.version);
        expect(events.last.data['toDataFormat'], _kNewer.dataFormat.toJson());
      });

      for (final recorded in <DataFormatVersion>[
        _kNextMajor,
        DataFormatVersion(LibVersion.dataFormat.major - 1, 4),
      ]) {
        // Verifies: EVS-DEV-event-store-open/D
        test('a latest recorded data format $recorded is refused with both '
            'versions and formats, and nothing is written', () async {
          if (db == null) return;
          final first = await _open(db!, await db!.openBackend());
          await _appendNote(first, 'n1');
          await seedLibVersionEventForTest(
            testBackendOf(first),
            eventType: LibVersionEvents.changed,
            version: '7.0.0',
            dataFormat: recorded,
          );
          final backend = await db!.openBackend();
          final before = await _Snapshot.of(backend);
          await expectLater(
            _open(db!, backend),
            throwsA(
              isA<DataFormatIncompatibleError>()
                  .having(
                    (e) => e.recordedPackageVersion,
                    'recordedPackageVersion',
                    '7.0.0',
                  )
                  .having(
                    (e) => e.recordedDataFormat,
                    'recordedDataFormat',
                    recorded,
                  )
                  .having(
                    (e) => e.packageVersion,
                    'packageVersion',
                    LibVersion.version,
                  )
                  .having(
                    (e) => e.dataFormat,
                    'dataFormat',
                    LibVersion.dataFormat,
                  ),
            ),
          );
          before.expectUnchangedIn(await _Snapshot.of(backend));
        });
      }

      // Verifies: EVS-DEV-event-store-open/D
      test('openForTest refuses another major and writes nothing; over a '
          'compatible database it opens and appends no library-version '
          'event', () async {
        if (db == null) return;
        final first = await _open(db!, await db!.openBackend());
        final backend = await db!.openBackend();
        final compatible = await _open(
          db!,
          backend,
          build: _kNewer,
          forTest: true,
        );
        expect(compatible.databaseId, first.databaseId);
        expect(await _libVersionEvents(backend), hasLength(1));
        await seedLibVersionEventForTest(
          backend,
          eventType: LibVersionEvents.changed,
          version: '7.0.0',
          dataFormat: _kNextMajor,
        );
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(db!, backend, forTest: true),
          throwsA(isA<DataFormatIncompatibleError>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));
      });

      // Verifies: EVS-DEV-entry-type-downgrade-refusal/B
      // Verifies: EVS-DEV-event-store-open/E
      test('an entry-type refusal leaves no library-version event', () async {
        if (db == null) return;
        final newer = await _open(
          db!,
          await db!.openBackend(),
          build: _kNewer,
          noteVersion: const EntryTypeVersion(2, 0),
        );
        await _appendNote(newer, 'n1');
        // A build of another entry-type major is deployed stop-then-start.
        await db!.stop(newer);
        final backend = await db!.openBackend();
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(db!, backend),
          throwsA(isA<EntryTypeVersionDowngradeError>()),
        );
        await expectLater(
          _open(db!, backend, forTest: true),
          throwsA(isA<EntryTypeVersionDowngradeError>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));
        expect(
          (await _libVersionEvents(
            backend,
          )).where((e) => e.eventType == LibVersionEvents.changed),
          isEmpty,
        );
      });

      // Verifies: EVS-DEV-event-store-open/E
      // Verifies: EVS-DEV-destination-drain/L
      // the library-version change and the snapshot-promotion audit the boot
      //   appends carry the aggregate type and an event type the library
      //   declares for their entry types.
      test('a boot that fails after its library-version event writes '
          'nothing; a clean reopen appends exactly one change and '
          'promotes', () async {
        if (db == null) return;
        final older = await _open(db!, await db!.openBackend());
        await _appendNote(older, 'n1');
        final backend = await db!.openBackend();
        final before = await _Snapshot.of(backend);
        await expectLater(
          _open(
            db!,
            backend,
            build: _kNewer,
            noteVersion: const EntryTypeVersion(1, 1),
            afterBootVersionEvent: () => true,
          ),
          throwsA(isA<InjectedFailure>()),
        );
        before.expectUnchangedIn(await _Snapshot.of(backend));

        final newer = await _open(
          db!,
          backend,
          build: _kNewer,
          noteVersion: const EntryTypeVersion(1, 1),
        );
        final changes = (await _libVersionEvents(
          testBackendOf(newer),
        )).where((e) => e.eventType == LibVersionEvents.changed);
        expect(changes, hasLength(1));
        final row = (await newer.reader.findViewRows(_kView)).single;
        expect(row['b'], 0);
        final audits = await newer.reader.findAllEvents(
          entryType: kViewSnapshotPromotedEntryType,
        );
        expect(audits, hasLength(1));
        final all = await newer.reader.findAllEvents();
        final changeIndex = all.indexWhere(
          (e) => e.eventId == changes.single.eventId,
        );
        final auditIndex = all.indexWhere(
          (e) => e.eventId == audits.single.eventId,
        );
        expect(changeIndex, greaterThanOrEqualTo(0));
        expect(auditIndex, greaterThanOrEqualTo(0));
        expect(
          changeIndex,
          lessThan(auditIndex),
          reason: 'the version change precedes the promotion it causes',
        );
        await expectReservedShapes(newer);
      });
    });
  });
}

/// The events of [backend] that the entry-type registry audit and the
/// library record, by entry type; exposed for the Postgres-only scenarios.
Future<List<StoredEvent>> libVersionEventsForTest(StorageBackend backend) =>
    _libVersionEvents(backend);

/// Opens an event store as the compiled build or as the newer build of the
/// same data-format major; exposed for the Postgres-only scenarios.
Future<EventStore> openBootStoreForTest(
  BootTestDatabase db,
  StorageBackend backend, {
  bool newer = false,
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
  String identifier = 'boot-install',
}) => _open(
  db,
  backend,
  build: newer ? _kNewer : _kCompiled,
  noteVersion: noteVersion,
  identifier: identifier,
);

/// Appends one note to [store]; exposed for the Postgres-only scenarios.
Future<void> appendBootNoteForTest(EventStore store, String aggregateId) =>
    _appendNote(store, aggregateId);

/// The newer build's package version.
const String newerBuildVersionForTest = '0.6.0';
