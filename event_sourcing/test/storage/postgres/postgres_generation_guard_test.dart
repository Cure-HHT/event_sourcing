// The incompatible-generation guard on Postgres: every instance holds shared
// advisory locks naming its data generation on a dedicated lock session, an
// open inspects them under an exclusive boot lock and refuses a conflicting
// live generation before any write, the durable generation record refuses
// the offline cases, and every transaction is fenced by that record and the
// stored schema pair. Each instance is a PostgresBackend plus an event store:
// advisory locks are per session, so two backends in one isolate are two
// instances. Gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_lock_session.dart'
    show PostgresScope, postgresAdvisoryKey;
import 'package:event_sourcing/src/storage/postgres/postgres_migration.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresMigrations;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/lib_version_seed.dart';
import '../../test_support/tool_subprocess.dart';
import 'test_postgres_url.dart';

const _kX = 'guard_x';
const _kY = 'guard_y';
const _kZ = 'guard_z';
const _kView = 'guard_x_notes';

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

Future<void> _resetSchema(String url) async {
  final c = await _connect(url);
  await c.execute('DROP SCHEMA public CASCADE');
  await c.execute('CREATE SCHEMA public');
  await c.close();
}

/// A timer the test fires by hand.
class _ManualTimer implements Timer {
  _ManualTimer(this._callback);
  final void Function(Timer) _callback;
  bool _active = true;
  int _ticks = 0;

  void fire() {
    if (!_active) return;
    _ticks++;
    _callback(this);
  }

  @override
  void cancel() => _active = false;

  @override
  bool get isActive => _active;

  @override
  int get tick => _ticks;
}

/// The timers the backends opened in its zone created.
class _Timers {
  final List<_ManualTimer> all = <_ManualTimer>[];

  /// Creates a timer whose callback runs in the zone that created it, as
  /// `Timer.periodic`'s does.
  Timer create(Duration _, void Function(Timer) callback) {
    final timer = _ManualTimer(Zone.current.bindUnaryCallback(callback));
    all.add(timer);
    return timer;
  }

  void fireAll() {
    for (final t in all) {
      t.fire();
    }
  }
}

/// The advisory locks every session holds on the current database, by pid.
Future<Map<int, Set<(int, String)>>> _locksByPid(String url) async {
  final c = await _connect(url);
  try {
    final r = await c.execute('''
      SELECT pid, (classid::bigint << 32) | objid::bigint, mode
      FROM pg_locks
      WHERE locktype = 'advisory' AND objsubid = 1 AND granted
        AND database = (SELECT oid FROM pg_database
                        WHERE datname = current_database())
    ''');
    final out = <int, Set<(int, String)>>{};
    for (final row in r) {
      (out[row[0]! as int] ??= <(int, String)>{}).add((
        row[1]! as int,
        row[2]! as String,
      ));
    }
    return out;
  } finally {
    await c.close();
  }
}

/// The pids that hold the advisory lock [key].
Future<Set<int>> _holdersOf(String url, int key) async => <int>{
  for (final e in (await _locksByPid(url)).entries)
    if (e.value.any((lock) => lock.$1 == key)) e.key,
};

/// The number of sessions on the current database waiting for a lock.
Future<int> _lockWaiters(String url) async {
  final c = await _connect(url);
  try {
    final r = await c.execute(
      "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
      'AND datname = current_database()',
    );
    return r.first[0]! as int;
  } finally {
    await c.close();
  }
}

/// The Postgres URL of a second server whose database and schema have the
/// names PG_TEST_URL's have, or null when the environment has none.
String? _otherServerUrl() {
  final other = Platform.environment['PG_TEST_URL_OTHER_SERVER'];
  return other == null || other.isEmpty ? null : other;
}

Future<PostgresScope> _scope(String url) async {
  final c = await _connect(url);
  try {
    return await PostgresScope.read(c);
  } finally {
    await c.close();
  }
}

/// The lock session's pid, or null while the session is lost.
Future<int?> _pidOf(PostgresBackend backend) async {
  try {
    return (await backend.lockSessionForTest()).pid;
  } on Object {
    return null;
  }
}

int _componentKey(PostgresScope scope, String component) =>
    postgresAdvisoryKey('event_sourcing.generation', scope, component);

/// Row counts and a hash of every library table's rows, ordered.
Future<Map<String, String>> _snapshot(String url) async {
  final c = await _connect(url);
  try {
    final tables = await c.execute(
      'SELECT table_name FROM information_schema.tables '
      "WHERE table_schema = 'public' ORDER BY table_name",
    );
    final out = <String, String>{};
    for (final row in tables) {
      final table = row[0]! as String;
      final r = await c.execute(
        "SELECT count(*), md5(coalesce(string_agg(t::text, '|' "
        "ORDER BY t::text), '')) FROM $table t",
      );
      out[table] = '${r.first[0]}:${r.first[1]}';
    }
    return out;
  } finally {
    await c.close();
  }
}

Future<void> _terminate(String url, int pid) async {
  final c = await _connect(url);
  try {
    await c.execute(
      Sql.named('SELECT pg_terminate_backend(@p)'),
      parameters: <String, Object?>{'p': pid},
    );
    for (var i = 0; i < 100; i++) {
      final r = await c.execute(
        Sql.named('SELECT count(*) FROM pg_stat_activity WHERE pid = @p'),
        parameters: <String, Object?>{'p': pid},
      );
      if (r.first[0] == 0) return;
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  } finally {
    await c.close();
  }
}

Future<void> _until(
  FutureOr<bool> Function() condition, {
  Duration? within,
}) async {
  final deadline = DateTime.now().add(within ?? const Duration(seconds: 10));
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition not reached');
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}

const _renameX = PromoterSpec(
  viewName: _kView,
  entryType: _kX,
  fromVersion: EntryTypeVersion(1, 0),
  toVersion: EntryTypeVersion(2, 0),
  transforms: <TransformPrimitive>[
    RenameField(sourceField: 'title', targetField: 'heading'),
  ],
);

const _defaultX = PromoterSpec(
  viewName: _kView,
  entryType: _kX,
  fromVersion: EntryTypeVersion(1, 0),
  toVersion: EntryTypeVersion(1, 1),
  transforms: <TransformPrimitive>[
    DefaultField(fieldName: 'b', defaultValue: 0),
  ],
);

Future<EventStore> _openStore(
  PostgresBackend backend, {
  Map<String, EntryTypeVersion> types = const <String, EntryTypeVersion>{
    _kX: EntryTypeVersion(1, 0),
  },
  List<PromoterSpec> promoters = const <PromoterSpec>[],
  bool withView = false,
  bool forTest = false,
}) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  types.forEach(
    (id, version) => registry.register(
      EntryTypeDefinition(id: id, registeredVersion: version, name: id),
    ),
  );
  final promoterRegistry = PromoterRegistry();
  for (final spec in promoters) {
    promoterRegistry.register(spec);
  }
  final projections = ProjectionRegistry();
  if (withView) {
    projections.register(
      const AggregateProjectionSpec(
        viewName: _kView,
        interest: SubscriptionFilter(entryTypes: <String>{_kX}),
        tombstoneEventTypes: <String>{},
      ),
    );
  }
  const source = Source(
    hopId: 'guard-hop',
    identifier: 'guard-install',
    softwareVersion: 'guard-test',
  );
  final security = PostgresSecurityContextStore(backend: backend);
  return forTest
      ? EventStore.openForTest(
          storage: backend,
          entryTypes: registry,
          source: source,
          securityContexts: security,
          projections: projections,
          promoters: promoterRegistry,
        )
      : EventStore.open(
          storage: backend,
          entryTypes: registry,
          source: source,
          securityContexts: security,
          projections: projections,
          promoters: promoterRegistry,
        );
}

Future<void> _append(
  EventStore store,
  String entryType, [
  Map<String, Object?> data = const <String, Object?>{'title': 't'},
  String aggregateId = 'agg-1',
]) => store.runTransaction(
  (txn, collector) =>
      _appendIn(store, txn, collector, entryType, data, aggregateId),
);

Future<void> _appendIn(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String entryType,
  Map<String, Object?> data,
  String aggregateId,
) => store.appendInTxn(
  txn,
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: 'note',
  eventType: 'finalized',
  data: data,
  initiator: const UserInitiator('guard-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

void main() {
  final url = testPostgresUrl();
  final backends = <PostgresBackend>[];

  Future<PostgresBackend> open({
    Duration lockHeartbeat = const Duration(seconds: 5),
    Duration lockQueryTimeout = const Duration(seconds: 5),
    String? lockUrl,
    String? atUrl,
  }) async {
    final backend = await PostgresBackend.open(
      url: atUrl ?? url!,
      lockUrl: lockUrl,
      sslMode: SslMode.disable,
      lockHeartbeat: lockHeartbeat,
      lockQueryTimeout: lockQueryTimeout,
    );
    backends.add(backend);
    return backend;
  }

  setUp(() async {
    if (url == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await _resetSchema(url);
    await PostgresBackend.provision(url, sslMode: SslMode.disable);
  });

  tearDown(() async {
    for (final backend in backends.reversed) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-DEV-postgres-backend/J
  test('an open store holds the shared lock of each of its components on '
      'the lock session, and on no other session', () async {
    if (url == null) return;
    final backend = await open();
    await _openStore(
      backend,
      types: const {_kX: EntryTypeVersion(1, 0), _kY: EntryTypeVersion(1, 0)},
    );
    final scope = await _scope(url);
    final keys = <int>[
      _componentKey(scope, 'data_format:${LibVersion.dataFormat.major}'),
      _componentKey(scope, 'entry_type:$_kX:1'),
      _componentKey(scope, 'entry_type:$_kY:1'),
    ];
    final pid = (await backend.lockSessionForTest()).pid;
    final locks = await _locksByPid(url);
    for (final key in keys) {
      expect(locks[pid], contains((key, 'ShareLock')));
      expect(await _holdersOf(url, key), {pid});
    }
  });

  group('live generations', () {
    // Verifies: EVS-DEV-version-compatibility/F
    test('a compatible canary opens beside the serving instance, and both '
        'hold the shared lock of the major they share', () async {
      if (url == null) return;
      final backendA = await open();
      final a = await _openStore(
        backendA,
        types: const {_kX: EntryTypeVersion(1, 0), _kY: EntryTypeVersion(1, 0)},
        withView: true,
      );
      final backendB = await open();
      final b = await _openStore(
        backendB,
        types: const {
          _kX: EntryTypeVersion(1, 1),
          _kY: EntryTypeVersion(1, 0),
          _kZ: EntryTypeVersion(1, 0),
        },
        promoters: const [_defaultX],
        withView: true,
      );
      await _append(a, _kX);
      await _append(b, _kZ);
      expect(await b.backend.findAllEvents(entryType: _kX), hasLength(1));
      final key = _componentKey(await _scope(url), 'entry_type:$_kX:1');
      final locks = await _locksByPid(url);
      final pidA = (await backendA.lockSessionForTest()).pid;
      final pidB = (await backendB.lockSessionForTest()).pid;
      expect(locks[pidA], contains((key, 'ShareLock')));
      expect(locks[pidB], contains((key, 'ShareLock')));
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('an incompatible canary is refused, naming the component, and the '
        'database is untouched', () async {
      if (url == null) return;
      final a = await _openStore(await open(), withView: true);
      await _append(a, _kX);
      final before = await _snapshot(url);
      final backendB = await open();
      await expectLater(
        _openStore(
          backendB,
          types: const {_kX: EntryTypeVersion(2, 0)},
          promoters: const [_renameX],
          withView: true,
        ),
        throwsA(
          isA<IncompatibleGenerationException>().having(
            (e) => e.conflictingComponents,
            'components',
            ['entry_type:$_kX:1'],
          ),
        ),
      );
      expect(await _snapshot(url), before);
      final pidB = (await backendB.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pidB], isNull);
      await _append(a, _kX);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('a build of another data-format major is refused while an instance '
        'is live', () async {
      if (url == null) return;
      await _openStore(await open());
      final before = await _snapshot(url);
      await expectLater(
        runWithDeliveryTestHooks(
          const DeliveryTestHooks(
            buildDeclaration: (
              version: '9.0.0',
              dataFormat: DataFormatVersion(3, 0),
            ),
          ),
          () async => _openStore(await open()),
        ),
        throwsA(
          isA<IncompatibleGenerationException>().having(
            (e) => e.conflictingComponents,
            'components',
            ['data_format:2'],
          ),
        ),
      );
      expect(await _snapshot(url), before);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('a second open on the same backend with a conflicting registry is '
        'refused and writes nothing', () async {
      if (url == null) return;
      final backend = await open();
      await _openStore(backend);
      final before = await _snapshot(url);
      await expectLater(
        _openStore(backend, types: const {_kX: EntryTypeVersion(2, 0)}),
        throwsA(isA<IncompatibleGenerationException>()),
      );
      expect(await _snapshot(url), before);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('openForTest runs the guard', () async {
      if (url == null) return;
      await _openStore(await open());
      await expectLater(
        _openStore(
          await open(),
          types: const {_kX: EntryTypeVersion(2, 0)},
          forTest: true,
        ),
        throwsA(isA<IncompatibleGenerationException>()),
      );
    });
  });

  group('boots serialize on the boot lock', () {
    // Verifies: EVS-DEV-version-compatibility/G
    test('an open waits while another holds the boot lock', () async {
      if (url == null) return;
      final release = Completer<void>();
      // Released on failure too, so a held boot cannot keep tearDown's
      // close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final heldA = Completer<void>();
      final backendA = await open();
      final backendB = await open();
      final openA = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            heldA.complete();
            return release.future;
          },
        ),
        () => _openStore(backendA),
      );
      await heldA.future;
      var waiting = false;
      var openedB = false;
      final openB =
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              onLog: (record) {
                if (record.message.contains('waiting for the boot lock')) {
                  waiting = true;
                }
              },
            ),
            () => _openStore(backendB),
          ).then((store) {
            openedB = true;
            return store;
          });
      await _until(() => waiting);
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(openedB, isFalse, reason: 'B has not passed the boot lock');
      release.complete();
      await openA;
      await openB;
      expect(openedB, isTrue);
    });

    // Verifies: EVS-DEV-version-compatibility/G
    test('of two conflicting opens at once exactly one opens, in each of 20 '
        'rounds', () async {
      if (url == null) return;
      for (var round = 0; round < 20; round++) {
        await _resetSchema(url);
        await PostgresBackend.provision(url, sslMode: SslMode.disable);
        final a = await open();
        final b = await open();
        final outcomes = await Future.wait(<Future<Object?>>[
          _openStore(a).then<Object?>((s) => s, onError: (Object e) => e),
          _openStore(
            b,
            types: const {_kX: EntryTypeVersion(2, 0)},
          ).then<Object?>((s) => s, onError: (Object e) => e),
        ]);
        expect(outcomes.whereType<EventStore>(), hasLength(1));
        expect(
          outcomes.whereType<IncompatibleGenerationException>(),
          hasLength(1),
          reason: 'round $round: $outcomes',
        );
        for (final backend in backends.reversed) {
          await backend.close();
        }
        backends.clear();
      }
    }, timeout: const Timeout(Duration(minutes: 3)));
  });

  group('bootLockWait bounds the waits of a boot', () {
    test('an open that waits longer than bootLockWait for the boot lock is '
        'refused, naming the holder', () async {
      if (url == null) return;
      final release = Completer<void>();
      // Released on failure too, so a held boot cannot keep tearDown's
      // close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final held = Completer<void>();
      final holder = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            held.complete();
            return release.future;
          },
        ),
        () async => _openStore(await open()),
      );
      await held.future;
      final waiter = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        bootLockWait: const Duration(milliseconds: 500),
      );
      backends.add(waiter);
      await expectLater(
        _openStore(waiter),
        throwsA(
          isA<GenerationGuardConfigurationException>().having(
            (e) => e.message,
            'message',
            contains('boot lock'),
          ),
        ),
      );
      release.complete();
      await holder;
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('a boot transaction whose table lock is held longer than '
        'bootLockWait is refused', () async {
      if (url == null) return;
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        bootLockWait: const Duration(milliseconds: 500),
      );
      backends.add(backend);
      final blocker = await _connect(url);
      addTearDown(blocker.close);
      await blocker.execute('BEGIN');
      await blocker.execute(
        "INSERT INTO backend_state (key, value) VALUES ('blocker', '1')",
      );
      try {
        await expectLater(
          _openStore(backend),
          throwsA(
            isA<GenerationGuardConfigurationException>().having(
              (e) => e.message,
              'message',
              contains('holds appends back'),
            ),
          ),
        );
      } finally {
        await blocker.execute('ROLLBACK');
      }
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNull);
      await _openStore(backend);
    });
  });

  group('stop-then-start and the generation record', () {
    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-entry-type-downgrade-refusal/A
    // Verifies: EVS-DEV-event-store-open/D
    test('a major bump runs after the old build stops; the old build is then '
        'refused although no view names the entry type', () async {
      if (url == null) return;
      final v1 = await _openStore(await open());
      await _append(v1, _kX);
      await v1.close();
      final v2 = await _openStore(
        await open(),
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
      final c = await _connect(url);
      final recorded = await c.execute(
        "SELECT value FROM backend_state WHERE key = 'data_generation'",
      );
      await c.close();
      expect(
        (recorded.first[0]! as Map)['entry_type_majors'],
        containsPair(_kX, 2),
      );
      await v2.close();
      final before = await _snapshot(url);
      await expectLater(
        _openStore(await open(), types: const {_kX: EntryTypeVersion(1, 3)}),
        throwsA(
          isA<EntryTypeVersionDowngradeError>()
              .having((e) => e.entryType, 'entryType', _kX)
              .having((e) => e.recordedByOpen, 'recordedByOpen', isTrue),
        ),
      );
      expect(await _snapshot(url), before);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-event-store-open/D
    test('after a build of another data-format major opened, the compiled '
        'build is refused', () async {
      if (url == null) return;
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(
          buildDeclaration: (
            version: '9.0.0',
            dataFormat: DataFormatVersion(3, 0),
          ),
        ),
        () async => (await _openStore(await open())).close(),
      );
      final before = await _snapshot(url);
      await expectLater(
        _openStore(await open()),
        throwsA(isA<DataFormatIncompatibleError>()),
      );
      expect(await _snapshot(url), before);
    });
  });

  group('a registration is released', () {
    /// No lock of [backend]'s lock session remains, and a conflicting open
    /// on another backend passes the guard (it may still be refused by the
    /// same durable check).
    Future<void> expectReleased(PostgresBackend backend) async {
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url!))[pid], isNull);
      final other = await open();
      try {
        await _openStore(other, types: const {_kX: EntryTypeVersion(2, 0)});
      } on IncompatibleGenerationException catch (e) {
        fail('the refused open still holds its generation: $e');
      } on Object {
        // Refused by the durable checks, as the first open was.
      }
    }

    Future<Object?> backendState() async {
      final c = await _connect(url!);
      try {
        final r = await c.execute(
          'SELECT key, value FROM backend_state ORDER BY key',
        );
        return r.map((row) => '${row[0]}=${row[1]}').toList();
      } finally {
        await c.close();
      }
    }

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the open is refused for its identity', () async {
      if (url == null) return;
      await (await _openStore(await open())).close();
      final c = await _connect(url);
      await c.execute("DELETE FROM backend_state WHERE key = 'database_id'");
      await c.close();
      final before = await backendState();
      final backend = await open();
      await expectLater(
        _openStore(backend),
        throwsA(isA<DatabaseIdentityMismatchError>()),
      );
      expect(await backendState(), before);
      await expectReleased(backend);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the open is refused as a pre-format database', () async {
      if (url == null) return;
      final seeder = await open();
      await seedLibVersionEventForTest(
        seeder,
        version: '0.1.0',
        dataFormat: LibVersion.dataFormat,
        recordDatabaseId: false,
      );
      final before = await backendState();
      final backend = await open();
      await expectLater(
        _openStore(backend),
        throwsA(isA<DatabaseResetRequiredError>()),
      );
      expect(await backendState(), before);
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNull);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the open is refused for its data format', () async {
      if (url == null) return;
      final seeder = await open();
      await seedLibVersionEventForTest(
        seeder,
        version: '7.0.0',
        dataFormat: const DataFormatVersion(3, 0),
      );
      final before = await backendState();
      final backend = await open();
      await expectLater(
        _openStore(backend),
        throwsA(isA<DataFormatIncompatibleError>()),
      );
      expect(await backendState(), before);
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNull);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the boot fails after its library-version event', () async {
      if (url == null) return;
      final before = await backendState();
      final backend = await open();
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(afterBootVersionEvent: () => true),
          () => _openStore(backend),
        ),
        throwsA(isA<InjectedFailure>()),
      );
      expect(await backendState(), before);
      await expectReleased(backend);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the store closes', () async {
      if (url == null) return;
      final backend = await open();
      final store = await _openStore(backend);
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNotEmpty);
      await store.close();
      backends.remove(backend);
      expect((await _locksByPid(url))[pid], isNull);
      final other = await open();
      await _openStore(other, types: const {_kX: EntryTypeVersion(2, 0)});
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('when the registration itself fails part way', () async {
      if (url == null) return;
      final backend = await open();
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(failGenerationRegistration: () => true),
          () => _openStore(backend),
        ),
        throwsA(isA<InjectedFailure>()),
      );
      final pid = (await backend.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNull);
      await _openStore(backend);
    });

    // Verifies: EVS-DEV-version-compatibility/F
    // Verifies: EVS-DEV-postgres-backend/J
    test('when the lock session dies while the open registers', () async {
      if (url == null) return;
      final timers = _Timers();
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      // A store already open on the backend, which the replacement session
      // registers again.
      await _openStore(backend, types: const {_kY: EntryTypeVersion(1, 0)});
      final scope = await _scope(url);
      final xKey = _componentKey(scope, 'entry_type:$_kX:1');
      final yKey = _componentKey(scope, 'entry_type:$_kY:1');
      final oldPid = (await backend.lockSessionForTest()).pid;
      expect(await _holdersOf(url, yKey), {oldPid});
      // The session ends after the open took the boot lock and inspected the
      // live components, before it took its shared component locks.
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(
            timerFactory: timers.create,
            insideBootLock: () => _terminate(url, oldPid),
          ),
          () => _openStore(backend),
        ),
        throwsA(isNot(isA<IncompatibleGenerationException>())),
      );
      expect((await _locksByPid(url))[oldPid], isNull);
      timers.fireAll();
      await _until(() async {
        if (backend.generationStatus != GenerationStatus.registered) {
          return false;
        }
        final pid = await _pidOf(backend);
        return pid != null && pid != oldPid;
      });
      final newPid = (await backend.lockSessionForTest()).pid;
      expect(await _holdersOf(url, yKey), {newPid});
      expect(
        await _holdersOf(url, xKey),
        isEmpty,
        reason: 'the failed open is not registered on the replacement',
      );
      // No boot lock of the failed open remains: an open on another backend
      // passes it at once.
      final watch = Stopwatch()..start();
      await _openStore(
        await open(),
        types: const {_kZ: EntryTypeVersion(1, 0)},
      );
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
      // A retry on the same backend opens and registers on the new session.
      final store = await _openStore(backend);
      expect(await _holdersOf(url, xKey), {newPid});
      await _append(store, _kX);
      await expectLater(
        _openStore(await open(), types: const {_kX: EntryTypeVersion(2, 0)}),
        throwsA(isA<IncompatibleGenerationException>()),
      );
    });
  });

  group('the lock session', () {
    // Verifies: EVS-DEV-postgres-backend/J
    test('carries keepalives and no idle-session timeout', () async {
      if (url == null) return;
      final session = await (await open()).lockSessionForTest();
      expect(session.settings['idle_session_timeout'], '0');
      // The test database is reached over TCP, where the settings apply.
      expect(session.settings['tcp_keepalives_idle'], anyOf('10', '10s'));
      expect(session.settings['tcp_keepalives_interval'], anyOf('5', '5s'));
      expect(session.settings['tcp_keepalives_count'], '3');
    });

    // Verifies: EVS-DEV-postgres-backend/J
    // Verifies: EVS-DEV-version-compatibility/H
    test('a lock connection that does not stay one server session is '
        'refused, and no lock is taken', () async {
      if (url == null) return;
      final before = await _locksByPid(url);
      await expectLater(
        runWithDeliveryTestHooks(
          const DeliveryTestHooks(splitLockSessionStatements: true),
          open,
        ),
        throwsA(isA<LockSessionConfigurationException>()),
      );
      expect(await _locksByPid(url), before);
    });

    // Verifies: EVS-DEV-postgres-backend/J
    test('a lock connection to another database is refused', () async {
      if (url == null) return;
      final admin = await _connect(url);
      addTearDown(() async {
        await admin.execute('DROP DATABASE IF EXISTS evs_guard_other');
        await admin.close();
      });
      await admin.execute('DROP DATABASE IF EXISTS evs_guard_other');
      await admin.execute('CREATE DATABASE evs_guard_other');
      final other = Uri.parse(url).replace(path: '/evs_guard_other').toString();
      await expectLater(
        open(lockUrl: other),
        throwsA(isA<LockSessionConfigurationException>()),
      );
    });

    // Verifies: EVS-DEV-postgres-backend/J
    // Verifies: EVS-DEV-version-compatibility/H
    test('a lock connection to another server, whose database and schema '
        'have the same names, is refused', () async {
      final other = _otherServerUrl();
      if (url == null) return;
      if (other == null) {
        markTestSkipped('PG_TEST_URL_OTHER_SERVER unset');
        return;
      }
      await expectLater(
        open(lockUrl: other),
        throwsA(
          isA<LockSessionConfigurationException>().having(
            (e) => e.toString(),
            'message',
            contains('another Postgres server'),
          ),
        ),
      );
      await expectLater(
        PostgresBackend.provision(
          url,
          lockUrl: other,
          sslMode: SslMode.disable,
        ),
        throwsA(isA<LockSessionConfigurationException>()),
      );
    });

    // Verifies: EVS-DEV-postgres-backend/J
    test('a lock role whose search path reaches another schema is '
        'refused', () async {
      if (url == null) return;
      final admin = await _connect(url);
      addTearDown(() async {
        await admin.execute('DROP SCHEMA IF EXISTS guard_other CASCADE');
        await admin.execute('DROP ROLE IF EXISTS evs_guard_lock');
        await admin.close();
      });
      await admin.execute('DROP SCHEMA IF EXISTS guard_other CASCADE');
      await admin.execute('DROP ROLE IF EXISTS evs_guard_lock');
      await admin.execute("CREATE ROLE evs_guard_lock LOGIN PASSWORD 'evs'");
      await admin.execute('CREATE SCHEMA guard_other');
      await admin.execute(
        'GRANT USAGE ON SCHEMA guard_other TO evs_guard_lock',
      );
      await admin.execute(
        'ALTER ROLE evs_guard_lock SET search_path = guard_other',
      );
      final lockUrl = Uri.parse(
        url,
      ).replace(userInfo: 'evs_guard_lock:evs').toString();
      await expectLater(
        open(lockUrl: lockUrl),
        throwsA(isA<LockSessionConfigurationException>()),
      );
    });

    // Verifies: EVS-DEV-postgres-backend/J
    // Verifies: EVS-DEV-version-compatibility/I
    test('a terminated lock session is replaced and the generation '
        'registered again', () async {
      if (url == null) return;
      final timers = _Timers();
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      final store = await _openStore(backend);
      final oldPid = (await backend.lockSessionForTest()).pid;
      final held = (await _locksByPid(url))[oldPid]!;
      await _terminate(url, oldPid);
      timers.fireAll();
      await _until(
        () async =>
            backend.generationStatus == GenerationStatus.registered &&
            (await _locksByPid(
              url,
            )).entries.any((e) => e.key != oldPid && e.value.containsAll(held)),
      );
      final newPid = (await backend.lockSessionForTest()).pid;
      expect(newPid, isNot(oldPid));
      expect((await _locksByPid(url))[newPid], containsAll(held));
      await _append(store, _kX);
    });

    // Verifies: EVS-DEV-postgres-backend/J
    test('a probe that outlasts the query timeout declares the session lost; '
        'the replacement ends the old server session', () async {
      if (url == null) return;
      final timers = _Timers();
      var stall = false;
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: timers.create,
          stallLockHeartbeatPastQueryTimeout: () => stall,
          failLostSessionClose: () => true,
        ),
        () => open(lockQueryTimeout: const Duration(seconds: 1)),
      );
      final store = await _openStore(backend);
      final oldPid = (await backend.lockSessionForTest()).pid;
      stall = true;
      timers.fireAll();
      // `_pidOf` is null while the replacement is not yet current, so the
      // wait is for a pid that exists and differs from the old one.
      await _until(() async {
        if (backend.generationStatus != GenerationStatus.registered) {
          return false;
        }
        final pid = await _pidOf(backend);
        return pid != null && pid != oldPid;
      });
      stall = false;
      await _until(() async => (await _locksByPid(url))[oldPid] == null);
      final newPid = (await backend.lockSessionForTest()).pid;
      expect(newPid, isNot(oldPid));
      expect((await _locksByPid(url))[newPid], isNotEmpty);
      final watch = Stopwatch()..start();
      await _openStore(await open());
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
      await _append(store, _kX);
    });

    // Verifies: EVS-DEV-postgres-backend/J
    test('while the old server session cannot be ended nothing is registered '
        'on the new one; once it can, the next retry registers', () async {
      if (url == null) return;
      final timers = _Timers();
      var stall = false;
      var refuse = true;
      final errors = <LibraryLogRecord>[];
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: timers.create,
          stallLockHeartbeatPastQueryTimeout: () => stall,
          failLostSessionClose: () => true,
          failOldSessionTermination: () => refuse,
          onLog: (r) {
            if (r.level == LibraryLogLevel.severe) errors.add(r);
          },
        ),
        () => open(lockQueryTimeout: const Duration(seconds: 1)),
      );
      await _openStore(backend);
      final oldPid = (await backend.lockSessionForTest()).pid;
      final held = (await _locksByPid(url))[oldPid]!;
      stall = true;
      timers.fireAll();
      await _until(() => errors.isNotEmpty);
      stall = false;
      expect(errors.first.message, contains('EVS-DEV-postgres-backend/J'));
      final locks = await _locksByPid(url);
      expect(
        locks.entries.where(
          (e) => e.key != oldPid && e.value.containsAll(held),
        ),
        isEmpty,
        reason: 'no new session holds the generation',
      );
      expect(locks[oldPid], containsAll(held));
      expect(backend.generationStatus, GenerationStatus.lost);
      refuse = false;
      timers.fireAll();
      await _until(
        () => backend.generationStatus == GenerationStatus.registered,
      );
      expect((await _locksByPid(url))[oldPid], isNull);
    });

    // Verifies: EVS-DEV-postgres-backend/J
    test('one operation at a time: no probe runs while a registration holds '
        'the session', () async {
      if (url == null) return;
      final timers = _Timers();
      var probes = 0;
      final release = Completer<void>();
      // Released on failure too, so a held boot cannot keep tearDown's
      // close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final inside = Completer<void>();
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: timers.create,
          failNextLockHeartbeat: () {
            probes++;
            return false;
          },
        ),
        open,
      );
      final opening = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            inside.complete();
            return release.future;
          },
        ),
        () => _openStore(backend),
      );
      await inside.future;
      for (var i = 0; i < 3; i++) {
        timers.fireAll();
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }
      expect(probes, 0);
      expect(backend.generationStatus, GenerationStatus.registered);
      release.complete();
      await opening;
      timers.fireAll();
      await _until(() => probes == 1);
    });
  });

  group('a lost lock session and its replacement', () {
    // Verifies: EVS-DEV-version-compatibility/F+G+I
    // Verifies: EVS-DEV-postgres-backend/J
    test('a lock session lost during a boot is replaced with that boot '
        'registered on the new session, and a conflicting open is then '
        'refused', () async {
      if (url == null) return;
      final timers = _Timers();
      final backendA = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      final scope = await _scope(url);
      final xKey = _componentKey(scope, 'entry_type:$_kX:1');
      // Holds A's boot transaction at its table lock, after A registered.
      final blocker = await _connect(url);
      addTearDown(blocker.close);
      await blocker.execute('BEGIN');
      await blocker.execute(
        "INSERT INTO backend_state (key, value) VALUES ('blocker', '1')",
      );
      final opening = runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        () => _openStore(backendA),
      );
      // The component lock is taken before the registration's other locks;
      // the boot reaches the blocker only once the registration is done.
      await _until(
        () async =>
            (await _holdersOf(url, xKey)).isNotEmpty &&
            await _lockWaiters(url) > 0,
      );
      final oldPid = (await _holdersOf(url, xKey)).single;
      await _terminate(url, oldPid);
      timers.fireAll();
      await _until(() async {
        final holders = await _holdersOf(url, xKey);
        return holders.isNotEmpty && !holders.contains(oldPid);
      });
      await blocker.execute('ROLLBACK');
      final a = await opening;
      expect(backendA.generationStatus, GenerationStatus.registered);
      final newPid = (await backendA.lockSessionForTest()).pid;
      expect(await _holdersOf(url, xKey), {newPid});
      // The boot lock the replacement took for the boot was released when
      // the boot completed: a compatible open passes it at once.
      final watch = Stopwatch()..start();
      await _openStore(await open());
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
      await expectLater(
        _openStore(await open(), types: const {_kX: EntryTypeVersion(2, 0)}),
        throwsA(isA<IncompatibleGenerationException>()),
      );
      await _append(a, _kX);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('a replacement the generation record refuses fences the backend '
        'holding no lock, so the newer generation keeps opening', () async {
      if (url == null) return;
      final timers = _Timers();
      final backendA = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      // Two stores on A: only the second registers X, so only it conflicts.
      await _openStore(backendA, types: const {_kY: EntryTypeVersion(1, 0)});
      await _openStore(
        backendA,
        types: const {_kX: EntryTypeVersion(1, 0), _kY: EntryTypeVersion(1, 0)},
      );
      final scope = await _scope(url);
      final x1 = _componentKey(scope, 'entry_type:$_kX:1');
      final y1 = _componentKey(scope, 'entry_type:$_kY:1');
      await _terminate(url, (await backendA.lockSessionForTest()).pid);
      // B raises the record to X:2 while A is not registered, then stops.
      final backendB = await open();
      final b = await _openStore(
        backendB,
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
      await b.close();
      backends.remove(backendB);
      timers.fireAll();
      await _until(() => backendA.generationStatus == GenerationStatus.fenced);
      // The fenced backend gives its locks up after it reports the fence.
      await _until(
        () async =>
            (await _holdersOf(url, x1)).isEmpty &&
            (await _holdersOf(url, y1)).isEmpty,
      );
      expect(await _holdersOf(url, x1), isEmpty);
      expect(await _holdersOf(url, y1), isEmpty);
      await _openStore(
        await open(),
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('a transaction the generation record refuses fences the backend, '
        'which then gives up every lock it held', () async {
      if (url == null) return;
      final backendA = await open();
      final a = await _openStore(backendA);
      final scope = await _scope(url);
      final x1 = _componentKey(scope, 'entry_type:$_kX:1');
      final pidA = (await backendA.lockSessionForTest()).pid;
      expect(await _holdersOf(url, x1), {pidA});
      // The record as a boot of X 2.0 elsewhere leaves it.
      final c = await _connect(url);
      await c.execute(
        Sql.named('UPDATE backend_state SET value = @v:jsonb WHERE key = @k'),
        parameters: <String, Object?>{
          'k': 'data_generation',
          'v': <String, Object?>{
            'data_format_major': LibVersion.dataFormat.major,
            'entry_type_majors': <String, Object?>{_kX: 2},
          },
        },
      );
      await c.close();
      await expectLater(
        _append(a, _kX),
        throwsA(isA<GenerationFencedException>()),
      );
      expect(backendA.generationStatus, GenerationStatus.fenced);
      await _until(() async => (await _holdersOf(url, x1)).isEmpty);
      await expectLater(
        _append(a, _kX),
        throwsA(isA<GenerationFencedException>()),
      );
    });

    test('a second open on one backend that waits longer than bootLockWait '
        'for the first boot is refused, and the backend opens later', () async {
      if (url == null) return;
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        bootLockWait: const Duration(milliseconds: 500),
      );
      backends.add(backend);
      final release = Completer<void>();
      // Released on failure too, so a held boot cannot keep tearDown's
      // close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final inside = Completer<void>();
      final first = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            inside.complete();
            return release.future;
          },
        ),
        () => _openStore(backend),
      );
      await inside.future;
      await expectLater(
        _openStore(backend, types: const {_kY: EntryTypeVersion(1, 0)}),
        throwsA(isA<GenerationGuardConfigurationException>()),
      );
      release.complete();
      await first;
      await _openStore(backend, types: const {_kY: EntryTypeVersion(1, 0)});
    });

    // Verifies: EVS-DEV-version-compatibility/G
    test(
      'two conflicting opens at once on one backend: exactly one opens',
      () async {
        if (url == null) return;
        final backend = await open();
        final outcomes = await Future.wait(<Future<Object>>[
          for (final major in <int>[1, 2])
            _openStore(
              backend,
              types: {_kX: EntryTypeVersion(major, 0)},
            ).then<Object>((store) => store, onError: (Object e) => e),
        ]);
        expect(outcomes.whereType<EventStore>(), hasLength(1));
        expect(
          outcomes.whereType<IncompatibleGenerationException>(),
          hasLength(1),
        );
      },
    );
  });

  group('the transaction fence', () {
    // Verifies: EVS-DEV-version-compatibility/I
    test('an instance whose lock session ended while a conflicting build '
        'booted commits nothing and is fenced', () async {
      if (url == null) return;
      final timers = _Timers();
      final backendA = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      final a = await _openStore(backendA);
      await _terminate(url, (await backendA.lockSessionForTest()).pid);
      final watch = Stopwatch()..start();
      await _openStore(
        await open(),
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
      expect(watch.elapsed, lessThan(const Duration(seconds: 6)));
      final count = await backendA.readSequenceCounter();
      await expectLater(
        _append(a, _kX),
        throwsA(isA<GenerationFencedException>()),
      );
      expect(await backendA.readSequenceCounter(), count);
      timers.fireAll();
      await _until(() => backendA.generationStatus == GenerationStatus.fenced);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('the idempotency store of a fenced backend refuses and changes '
        'nothing', () async {
      if (url == null) return;
      final timers = _Timers();
      final backendA = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      await _openStore(backendA);
      final idempotency = PostgresIdempotencyStore.forBackend(backendA);
      await idempotency.record(
        actionName: 'a',
        principalId: 'p',
        key: 'k',
        resultJson: const <String, Object?>{},
        emittedEventIds: const <String>[],
        expiresAt: DateTime.utc(2000),
      );
      await _terminate(url, (await backendA.lockSessionForTest()).pid);
      await _openStore(
        await open(),
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
      timers.fireAll();
      await _until(() => backendA.generationStatus == GenerationStatus.fenced);
      final before = await _snapshot(url);
      await expectLater(
        idempotency.sweepExpired(),
        throwsA(isA<GenerationFencedException>()),
      );
      await expectLater(
        idempotency.record(
          actionName: 'b',
          principalId: 'p',
          key: 'k',
          resultJson: const <String, Object?>{},
          emittedEventIds: const <String>[],
          expiresAt: DateTime.utc(2100),
        ),
        throwsA(isA<GenerationFencedException>()),
      );
      expect(await _snapshot(url), before);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test("a transaction that began before a conflicting build's boot "
        'commits before the boot, which then folds its event into the new '
        'shape', () async {
      if (url == null) return;
      // A's probe never runs, so A does not replace its lost session and
      // register again before B boots.
      final timers = _Timers();
      final backendA = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      final a = await _openStore(backendA, withView: true);
      await _append(a, _kX, const {'title': 'first'}, 'agg-0');
      await _terminate(url, (await backendA.lockSessionForTest()).pid);
      final release = Completer<void>();
      // Released on failure too, so a held transaction cannot keep
      // tearDown's close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final appended = Completer<void>();
      final txnA = a
          .runTransaction((txn, collector) async {
            await _appendIn(a, txn, collector, _kX, const {
              'title': 'late',
            }, 'agg-1');
            if (!appended.isCompleted) appended.complete();
            await release.future;
          })
          .then<Object?>((_) => null, onError: (Object e) => e);
      await appended.future;
      var openedB = false;
      final openB =
          _openStore(
            await open(),
            types: const {_kX: EntryTypeVersion(2, 0)},
            promoters: const [_renameX],
            withView: true,
          ).then((store) {
            openedB = true;
            return store;
          });
      // B's boot waits at the table lock A's transaction holds.
      await _until(() async => await _lockWaiters(url) > 0);
      expect(openedB, isFalse, reason: "B's boot waits for A's transaction");
      release.complete();
      expect(await txnA, isNull, reason: "A's transaction commits");
      final b = await openB;
      final rows = await b.backend.findViewRows(_kView);
      final late = rows.singleWhere((r) => r['aggregateId'] == 'agg-1');
      expect(late['heading'], 'late');
      expect(late.containsKey('title'), isFalse);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-postgres-backend/H
    // Verifies: EVS-DEV-postgres-backend/I
    test('the schema pair is re-checked under the boot lock, and a '
        'provisioning during a lost-session window fences the '
        'instance', () async {
      if (url == null) return;
      final steps = <PostgresMigrationStep>[
        postgresMigrations.single,
        const PostgresMigrationStep(
          toVersion: 2,
          minCompatibleVersion: 2,
          ddl: <String>['CREATE TABLE schema_upgrade_probe (id INTEGER)'],
        ),
      ];
      Future<void> provisionAhead() => runWithDeliveryTestHooks(
        DeliveryTestHooks(schemaDeclaration: steps),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );

      // Provisioned ahead between this backend's open and its store's open.
      final early = await open();
      await provisionAhead();
      final before = await _snapshot(url);
      await expectLater(
        _openStore(early),
        throwsA(isA<PostgresSchemaIncompatibleException>()),
      );
      expect(await _snapshot(url), before);
      final pid = (await early.lockSessionForTest()).pid;
      expect((await _locksByPid(url))[pid], isNull);

      // Loss window.
      await _resetSchema(url);
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      final timers = _Timers();
      final backend = await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: timers.create),
        open,
      );
      final store = await _openStore(backend);
      await _terminate(url, (await backend.lockSessionForTest()).pid);
      await provisionAhead();
      await expectLater(
        _append(store, _kX),
        throwsA(isA<GenerationFencedException>()),
      );
      timers.fireAll();
      await _until(() => backend.generationStatus == GenerationStatus.fenced);
    });
  });

  group('scope', () {
    // Verifies: EVS-DEV-version-compatibility/H
    test('conflicting generations in two schemas of one database open side '
        'by side', () async {
      if (url == null) return;
      final admin = await _connect(url);
      addTearDown(() async {
        for (final n in ['1', '2']) {
          await admin.execute('DROP SCHEMA IF EXISTS s$n CASCADE');
          await admin.execute('DROP ROLE IF EXISTS evs_s$n');
        }
        await admin.close();
      });
      final urls = <String>[];
      for (final n in ['1', '2']) {
        await admin.execute('DROP SCHEMA IF EXISTS s$n CASCADE');
        await admin.execute('DROP ROLE IF EXISTS evs_s$n');
        await admin.execute("CREATE ROLE evs_s$n LOGIN PASSWORD 'evs'");
        await admin.execute('CREATE SCHEMA s$n AUTHORIZATION evs_s$n');
        await admin.execute('ALTER ROLE evs_s$n SET search_path = s$n');
        final u = Uri.parse(url).replace(userInfo: 'evs_s$n:evs').toString();
        await PostgresBackend.provision(u, sslMode: SslMode.disable);
        urls.add(u);
      }
      await _openStore(await open(atUrl: urls[0]));
      await _openStore(
        await open(atUrl: urls[1]),
        types: const {_kX: EntryTypeVersion(2, 0)},
      );
    });
  });

  group('process exit', () {
    // Verifies: EVS-DEV-version-compatibility/F
    test('the locks of an instance whose process is killed are released by '
        'the server', () async {
      if (url == null) return;
      final process = await Process.start(sdkTool('dart'), <String>[
        'run',
        'tool/generation_guard_instance.dart',
        url,
      ]);
      addTearDown(() => process.kill(ProcessSignal.sigkill));
      final lines = process.stdout
          .transform(const SystemEncoding().decoder)
          .transform(const LineSplitter());
      final ready = await lines
          .firstWhere((line) => line.trim() == 'ready')
          .timeout(const Duration(minutes: 3), onTimeout: () => '');
      if (ready.isEmpty) {
        const reason = 'the instance process did not become ready';
        if (runningInCi) fail(reason);
        markTestSkipped(reason);
        return;
      }
      await expectLater(
        _openStore(await open(), types: const {_kX: EntryTypeVersion(2, 0)}),
        throwsA(isA<IncompatibleGenerationException>()),
      );
      process.kill(ProcessSignal.sigkill);
      await process.exitCode;
      final watch = Stopwatch()..start();
      while (true) {
        try {
          await _openStore(
            await open(),
            types: const {_kX: EntryTypeVersion(2, 0)},
          );
          break;
        } on IncompatibleGenerationException {
          if (watch.elapsed > const Duration(seconds: 10)) rethrow;
          await Future<void>.delayed(const Duration(milliseconds: 200));
        }
      }
    }, timeout: const Timeout(Duration(minutes: 5)));
  });
}
