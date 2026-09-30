// Postgres schema provisioning: PostgresBackend.provision applies the
// migration steps above the stored version in one transaction under the
// generation guard's boot lock, and PostgresBackend.open performs no DDL and
// verifies the stored schema pair. Gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:isolate';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:event_sourcing/src/storage/postgres/postgres_lock_session.dart'
    show PostgresScope, postgresAdvisoryKey;
import 'package:event_sourcing/src/storage/postgres/postgres_migration.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresMigrations;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/lib_version_seed.dart';
import 'test_postgres_url.dart';

/// This build's schema version, and the one a later build's step leads to.
const int _v = postgresSchemaVersion;
const int _next = postgresSchemaVersion + 1;

/// This build's migration steps, then a later build's step adding the probe
/// table and recording [minimum].
List<PostgresMigrationStep> _twoSteps({
  int minimum = postgresMinCompatibleSchemaVersion,
}) => <PostgresMigrationStep>[
  ...postgresMigrations,
  PostgresMigrationStep(
    toVersion: _next,
    minCompatibleVersion: minimum,
    ddl: const <String>['CREATE TABLE schema_upgrade_probe (id INTEGER)'],
  ),
];

const (int, int) _built = (_v, postgresMinCompatibleSchemaVersion);
const (int, int) _upgraded = (_next, postgresMinCompatibleSchemaVersion);

T _as<T>(List<PostgresMigrationStep> steps, T Function() body) =>
    runWithDeliveryTestHooks(DeliveryTestHooks(schemaDeclaration: steps), body);

Future<List<String>> _tables(PostgresTestDatabase db) async {
  final c = await db.connectAdmin();
  try {
    final r = await c.execute(
      Sql.named(
        'SELECT table_name FROM information_schema.tables '
        'WHERE table_schema = @s ORDER BY table_name',
      ),
      parameters: <String, Object?>{'s': db.schema},
    );
    return r.map((row) => row[0]! as String).toList();
  } finally {
    await c.close();
  }
}

/// Every column of the library's schema, as `table.column type`, in order.
Future<List<String>> _columns(PostgresTestDatabase db) async {
  final c = await db.connectAdmin();
  try {
    final r = await c.execute(
      Sql.named(
        'SELECT table_name, column_name, data_type '
        'FROM information_schema.columns '
        'WHERE table_schema = @s '
        'ORDER BY table_name, column_name',
      ),
      parameters: <String, Object?>{'s': db.schema},
    );
    return r.map((row) => '${row[0]}.${row[1]} ${row[2]}').toList();
  } finally {
    await c.close();
  }
}

/// The keys of `backend_state`, in order, each with its value for the
/// schema-version keys.
Future<List<String>> _backendState(PostgresTestDatabase db) async {
  final c = await db.connectAdmin();
  try {
    final r = await c.execute(
      "SELECT key, CASE WHEN key LIKE '%schema_version' "
      'THEN value::text ELSE NULL END '
      'FROM backend_state ORDER BY key',
    );
    return r.map((row) => '${row[0]}=${row[1]}').toList();
  } finally {
    await c.close();
  }
}

Future<(int?, int?)> _storedPair(PostgresTestDatabase db) async {
  final c = await db.connectAdmin();
  try {
    final r = await c.execute(
      'SELECT key, value::numeric::int FROM backend_state '
      "WHERE key IN ('schema_version', 'min_compatible_schema_version')",
    );
    int? version;
    int? minimum;
    for (final row in r) {
      if (row[0] == 'schema_version') version = row[1] as int?;
      if (row[0] == 'min_compatible_schema_version') minimum = row[1] as int?;
    }
    return (version, minimum);
  } finally {
    await c.close();
  }
}

const _kNote = 'provision_note';

Future<EventStore> _openStore(PostgresBackend backend) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: _kNote,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kNote,
    ),
  );
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      PostgresSecurityContextStore(backend: backend),
    ),
    entryTypes: registry,
    source: const Source(
      hopId: 'provision-hop',
      identifier: 'provision-install',
      softwareVersion: 'provision-test',
    ),
  );
}

Future<void> _append(EventStore store, int n) => store.runTransaction(
  (Transaction txn, PublishCollector collector) => store.appendInTxn(
    txn,
    entryType: _kNote,
    aggregateId: 'note-$n',
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'n': n},
    initiator: const UserInitiator('provision-user'),
    flowToken: null,
    metadata: null,
    security: null,
    checkpointReason: null,
    changeReason: null,
    dedupeByContent: false,
    collector: collector,
  ),
);

void main() {
  // Verifies: EVS-DEV-postgres-backend/G
  test("the exported schema versions are the last migration step's", () {
    expect(postgresSchemaVersion, postgresMigrations.last.toVersion);
    expect(
      postgresMinCompatibleSchemaVersion,
      postgresMigrations.last.minCompatibleVersion,
    );
  });

  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  Future<PostgresBackend> open() async {
    final backend = await db!.open();
    backends.add(backend);
    return backend;
  }

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset();
  });

  tearDown(() async {
    for (final backend in backends) {
      await backend.close();
    }
    backends.clear();
  });

  group('open verifies the stored schema pair', () {
    // Verifies: EVS-DEV-postgres-backend/H
    test('an unprovisioned schema is refused, naming provision, and nothing '
        'is created', () async {
      if (db == null) return;
      await expectLater(
        open(),
        throwsA(
          isA<PostgresSchemaIncompatibleException>()
              .having((e) => e.storedSchemaVersion, 'stored', isNull)
              .having(
                (e) => e.toString(),
                'message',
                contains('PostgresBackend.provision'),
              ),
        ),
      );
      expect(await _tables(db), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test("a schema below this build's version is refused", () async {
      if (db == null) return;
      await db.provision();
      await expectLater(
        _as(_twoSteps(), open),
        throwsA(
          isA<PostgresSchemaIncompatibleException>()
              .having((e) => e.storedSchemaVersion, 'stored', _v)
              .having((e) => e.buildSchemaVersion, 'build', _next),
        ),
      );
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test('a newer schema whose minimum this build meets opens', () async {
      if (db == null) return;
      await _as(_twoSteps(), db.provision);
      expect(await _storedPair(db), _upgraded);
      final store = await _openStore(await open());
      expect(store.databaseId, isNotEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test('a schema whose minimum is above this build is refused', () async {
      if (db == null) return;
      await _as(_twoSteps(minimum: _next), db.provision);
      await expectLater(
        open(),
        throwsA(
          isA<PostgresSchemaIncompatibleException>().having(
            (e) => e.storedMinCompatibleSchemaVersion,
            'minimum',
            _next,
          ),
        ),
      );
    });
  });

  group('provision', () {
    // Verifies: EVS-DEV-postgres-backend/G
    test('a second provisioning leaves the schema untouched', () async {
      if (db == null) return;
      await db.provision();
      final tables = await _tables(db);
      await db.provision();
      expect(await _tables(db), tables);
      expect(await _storedPair(db), _built);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('two provisionings of an empty schema at once both succeed and '
        'leave the schema one provisioning leaves', () async {
      if (db == null) return;
      // The schema a single provisioning of an empty schema leaves.
      await db.provision();
      final singleColumns = await _columns(db);
      final singleState = await _backendState(db);
      await db.reset();

      await Future.wait(<Future<void>>[db.provision(), db.provision()]);
      expect(await _storedPair(db), _built);
      expect(await _columns(db), singleColumns);
      expect(await _backendState(db), singleState);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('two provisionings from two isolates both succeed', () async {
      if (db == null) return;
      final u = db.ownerUrl;
      final schema = db.schema;
      final runtimeRoles = <String>{db.runtime};
      final lockRoles = <String>{db.runtime, db.lock};
      await Future.wait(<Future<void>>[
        Isolate.run(
          () => PostgresBackend.provision(
            u,
            schema: schema,
            runtimeRoles: runtimeRoles,
            lockRoles: lockRoles,
            sslMode: SslMode.disable,
          ),
        ),
        Isolate.run(
          () => PostgresBackend.provision(
            u,
            schema: schema,
            runtimeRoles: runtimeRoles,
            lockRoles: lockRoles,
            sslMode: SslMode.disable,
          ),
        ),
      ]);
      expect(await _storedPair(db), _built);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('an upgrade applies only the steps above the stored version, and an '
        'instance at the older version still opens', () async {
      if (db == null) return;
      await db.provision();
      expect(await _tables(db), isNot(contains('schema_upgrade_probe')));
      await _as(_twoSteps(), db.provision);
      expect(await _tables(db), contains('schema_upgrade_probe'));
      expect(await _storedPair(db), _upgraded);
      await _openStore(await open());
    });

    // Verifies: EVS-DEV-postgres-backend/I
    test('a provisioning that raises the minimum above what a live instance '
        'requires is refused and changes nothing', () async {
      if (db == null) return;
      await db.provision();
      await _openStore(await open());
      final tables = await _tables(db);
      await expectLater(
        _as(_twoSteps(minimum: _next), db.provision),
        throwsA(
          isA<IncompatibleGenerationException>()
              .having((e) => e.conflictingComponents, 'components', [
                'schema:$_v',
              ])
              .having((e) => e.descriptor, 'descriptor', isNull),
        ),
      );
      expect(await _tables(db), tables);
      expect(await _storedPair(db), _built);
    });

    // Verifies: EVS-DEV-postgres-backend/I
    test('once the live instance has stopped, the same provisioning '
        'runs', () async {
      if (db == null) return;
      await db.provision();
      final store = await _openStore(await open());
      await store.close();
      await _as(_twoSteps(minimum: _next), db.provision);
      expect(await _storedPair(db), (_next, _next));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('a provisioning that fails before it records the version leaves no '
        'library table on a fresh schema', () async {
      if (db == null) return;
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(failProvisioningBeforeVersionWrite: () => true),
          db.provision,
        ),
        throwsA(isA<InjectedFailure>()),
      );
      expect(await _tables(db), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('an upgrade that fails before it records the version leaves no '
        'probe table and the pair as it was', () async {
      if (db == null) return;
      await db.provision();
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(
            schemaDeclaration: _twoSteps(),
            failProvisioningBeforeVersionWrite: () => true,
          ),
          db.provision,
        ),
        throwsA(isA<InjectedFailure>()),
      );
      expect(await _tables(db), isNot(contains('schema_upgrade_probe')));
      expect(await _storedPair(db), _built);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('a schema holding library tables but no schema version is refused, '
        'naming a reset, and nothing is written', () async {
      if (db == null) return;
      final c = await db.connectOwner();
      for (final statement in postgresMigrations.first.ddl) {
        await c.execute(statement);
      }
      await c.close();
      final tables = await _tables(db);
      await expectLater(
        db.provision(),
        throwsA(
          isA<PostgresSchemaIncompatibleException>().having(
            (e) => e.reason,
            'reason',
            allOf(contains('records no schema version'), contains('reset')),
          ),
        ),
      );
      expect(await _tables(db), tables);
      expect(await _storedPair(db), (null, null));
    });

    // Verifies: EVS-DEV-postgres-backend/G+I
    // Verifies: EVS-DEV-version-compatibility/G
    test('a provisioning waits while a boot holds the boot lock, then refuses '
        'the minimum that boot registered below', () async {
      if (db == null) return;
      await db.provision();
      final release = Completer<void>();
      // Released on failure too, so a held boot cannot keep tearDown's
      // close waiting and time out the tests that follow.
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final inside = Completer<void>();
      final opening = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            inside.complete();
            return release.future;
          },
        ),
        () async => _openStore(await open()),
      );
      await inside.future;
      final tables = await _tables(db);
      var waiting = false;
      var settled = false;
      final provisioning =
          runWithDeliveryTestHooks(
                DeliveryTestHooks(
                  schemaDeclaration: _twoSteps(minimum: _next),
                  onLog: (record) {
                    if (record.message.contains('waiting for the boot lock')) {
                      waiting = true;
                    }
                  },
                ),
                db.provision,
              )
              .then<Object?>((_) => null, onError: (Object e) => e)
              .whenComplete(() => settled = true);
      final deadline = DateTime.now().add(const Duration(seconds: 10));
      while (!waiting) {
        if (DateTime.now().isAfter(deadline)) fail('provisioning never waited');
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(settled, isFalse, reason: 'the boot holds the boot lock');
      release.complete();
      await opening;
      expect(
        await provisioning,
        isA<IncompatibleGenerationException>().having(
          (e) => e.conflictingComponents,
          'components',
          ['schema:$_v'],
        ),
      );
      expect(await _tables(db), tables);
      expect(await _storedPair(db), _built);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('a provisioning that waits longer than bootLockWait for the boot '
        'lock is refused and creates nothing', () async {
      if (db == null) return;
      final holder = await db.connectAdmin();
      addTearDown(holder.close);
      final scope = await PostgresScope.read(holder);
      await holder.execute(
        Sql.named('SELECT pg_advisory_lock(@k)'),
        parameters: <String, Object?>{
          'k': postgresAdvisoryKey('event_sourcing.boot', scope),
        },
      );
      await expectLater(
        db.provision(bootLockWait: const Duration(milliseconds: 500)),
        throwsA(isA<GenerationGuardConfigurationException>()),
      );
      expect(await _tables(db), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G+J
    test('a provisioning whose lock connection reaches another database is '
        'refused and creates nothing', () async {
      if (db == null) return;
      final admin = await db.connectAdmin();
      addTearDown(() async {
        await admin.execute('DROP DATABASE IF EXISTS evs_provision_other');
        await admin.close();
      });
      await admin.execute('DROP DATABASE IF EXISTS evs_provision_other');
      await admin.execute('CREATE DATABASE evs_provision_other');
      await expectLater(
        PostgresBackend.provision(
          db.ownerUrl,
          schema: db.schema,
          runtimeRoles: <String>{db.runtime},
          lockRoles: <String>{db.runtime},
          lockUrl: Uri.parse(
            db.ownerUrl,
          ).replace(path: '/evs_provision_other').toString(),
          sslMode: SslMode.disable,
        ),
        throwsA(isA<LockSessionConfigurationException>()),
      );
      expect(await _tables(db), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G+H
    // Verifies: EVS-DEV-version-compatibility/I
    test('an instance at the older schema version keeps committing while an '
        'upgrade that keeps its minimum is provisioned', () async {
      if (db == null) return;
      await db.provision();
      final store = await _openStore(await open());
      var stop = false;
      var appended = 0;
      final loop = () async {
        while (!stop) {
          await _append(store, appended);
          appended++;
        }
      }();
      while (appended < 5) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      await _as(_twoSteps(), db.provision);
      final atUpgrade = appended;
      while (appended < atUpgrade + 5) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      stop = true;
      await loop;
      expect(await _storedPair(db), _upgraded);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-postgres-backend/R
    test('a schema that is not the current schema is refused, naming both '
        'schemas, and nothing is created', () async {
      if (db == null) return;
      await expectLater(
        PostgresBackend.provision(
          db.ownerUrl,
          schema: 'no_such_schema',
          runtimeRoles: <String>{db.runtime},
          lockRoles: <String>{db.runtime},
          sslMode: SslMode.disable,
        ),
        throwsA(
          isA<PostgresSchemaMismatchException>()
              .having((e) => e.describedSchema, 'described', 'no_such_schema')
              .having(
                (e) => e.toString(),
                'message',
                allOf(
                  contains('"no_such_schema"'),
                  contains(
                    'the current schema inside a library transaction is',
                  ),
                ),
              ),
        ),
      );
      expect(await _tables(db), isEmpty);
    });
  });

  // Verifies: EVS-DEV-event-store-open/E
  // Verifies: EVS-DEV-version-compatibility/I
  test('a boot that fails after its library-version event leaves the '
      'generation record as it was', () async {
    if (db == null) return;
    await db.provision();
    final backend = await open();
    await seedLibVersionEventForTest(
      backend,
      eventType: 'lib_version_initialized',
      version: '0.0.1',
      dataFormat: LibVersion.dataFormat,
    );
    Future<Object?> record() async {
      final c = await db.connectAdmin();
      try {
        final r = await c.execute(
          "SELECT value FROM backend_state WHERE key = 'data_generation'",
        );
        return r.isEmpty ? null : r.first[0];
      } finally {
        await c.close();
      }
    }

    final before = await record();
    await expectLater(
      runWithDeliveryTestHooks(
        DeliveryTestHooks(afterBootVersionEvent: () => true),
        () => _openStore(backend),
      ),
      throwsA(isA<InjectedFailure>()),
    );
    expect(await record(), before);
    await _openStore(backend);
    expect(await record(), isNotNull);
  });
}
