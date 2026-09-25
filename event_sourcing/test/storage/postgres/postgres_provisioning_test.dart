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

const _kRole = 'evs_provision_noschema';

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

/// The two-step migration list: this build's step, then a step adding the
/// probe table and recording [minimum].
List<PostgresMigrationStep> _twoSteps({int minimum = 1}) =>
    <PostgresMigrationStep>[
      postgresMigrations.single,
      PostgresMigrationStep(
        toVersion: 2,
        minCompatibleVersion: minimum,
        ddl: const <String>['CREATE TABLE schema_upgrade_probe (id INTEGER)'],
      ),
    ];

T _as<T>(List<PostgresMigrationStep> steps, T Function() body) =>
    runWithDeliveryTestHooks(DeliveryTestHooks(schemaDeclaration: steps), body);

Future<List<String>> _tables(String url) async {
  final c = await _connect(url);
  try {
    final r = await c.execute(
      'SELECT table_name FROM information_schema.tables '
      "WHERE table_schema = 'public' ORDER BY table_name",
    );
    return r.map((row) => row[0]! as String).toList();
  } finally {
    await c.close();
  }
}

/// Every column of the public schema, as `table.column type`, in order.
Future<List<String>> _columns(String url) async {
  final c = await _connect(url);
  try {
    final r = await c.execute(
      'SELECT table_name, column_name, data_type '
      'FROM information_schema.columns '
      "WHERE table_schema = 'public' "
      'ORDER BY table_name, column_name',
    );
    return r.map((row) => '${row[0]}.${row[1]} ${row[2]}').toList();
  } finally {
    await c.close();
  }
}

/// The keys of `backend_state`, in order, each with its value for the
/// schema-version keys.
Future<List<String>> _backendState(String url) async {
  final c = await _connect(url);
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

Future<(int?, int?)> _storedPair(String url) async {
  final c = await _connect(url);
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
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'provision-hop',
      identifier: 'provision-install',
      softwareVersion: 'provision-test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
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

  final url = testPostgresUrl();
  final backends = <PostgresBackend>[];

  Future<PostgresBackend> open() async {
    final backend = await PostgresBackend.open(
      url: url!,
      sslMode: SslMode.disable,
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
      if (url == null) return;
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
      expect(await _tables(url), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test("a schema below this build's version is refused", () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      await expectLater(
        _as(_twoSteps(), open),
        throwsA(
          isA<PostgresSchemaIncompatibleException>()
              .having((e) => e.storedSchemaVersion, 'stored', 1)
              .having((e) => e.buildSchemaVersion, 'build', 2),
        ),
      );
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test('a newer schema whose minimum this build meets opens', () async {
      if (url == null) return;
      await _as(
        _twoSteps(),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );
      expect(await _storedPair(url), (2, 1));
      final store = await _openStore(await open());
      expect(store.databaseId, isNotEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/H
    test('a schema whose minimum is above this build is refused', () async {
      if (url == null) return;
      await _as(
        _twoSteps(minimum: 2),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );
      await expectLater(
        open(),
        throwsA(
          isA<PostgresSchemaIncompatibleException>().having(
            (e) => e.storedMinCompatibleSchemaVersion,
            'minimum',
            2,
          ),
        ),
      );
    });
  });

  group('provision', () {
    // Verifies: EVS-DEV-postgres-backend/G
    test('a second provisioning leaves the schema untouched', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      final tables = await _tables(url);
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      expect(await _tables(url), tables);
      expect(await _storedPair(url), (1, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('two provisionings of an empty schema at once both succeed and '
        'leave the schema one provisioning leaves', () async {
      if (url == null) return;
      // The schema a single provisioning of an empty schema leaves.
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      final singleColumns = await _columns(url);
      final singleState = await _backendState(url);
      await _resetSchema(url);

      await Future.wait(<Future<void>>[
        PostgresBackend.provision(url, sslMode: SslMode.disable),
        PostgresBackend.provision(url, sslMode: SslMode.disable),
      ]);
      expect(await _storedPair(url), (1, 1));
      expect(await _columns(url), singleColumns);
      expect(await _backendState(url), singleState);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('two provisionings from two isolates both succeed', () async {
      if (url == null) return;
      final u = url;
      await Future.wait(<Future<void>>[
        Isolate.run(
          () => PostgresBackend.provision(u, sslMode: SslMode.disable),
        ),
        Isolate.run(
          () => PostgresBackend.provision(u, sslMode: SslMode.disable),
        ),
      ]);
      expect(await _storedPair(url), (1, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('an upgrade applies only the steps above the stored version, and an '
        'instance at the older version still opens', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      expect(await _tables(url), isNot(contains('schema_upgrade_probe')));
      await _as(
        _twoSteps(),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );
      expect(await _tables(url), contains('schema_upgrade_probe'));
      expect(await _storedPair(url), (2, 1));
      await _openStore(await open());
    });

    // Verifies: EVS-DEV-postgres-backend/I
    test('a provisioning that raises the minimum above what a live instance '
        'requires is refused and changes nothing', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      await _openStore(await open());
      final tables = await _tables(url);
      await expectLater(
        _as(
          _twoSteps(minimum: 2),
          () => PostgresBackend.provision(url, sslMode: SslMode.disable),
        ),
        throwsA(
          isA<IncompatibleGenerationException>()
              .having((e) => e.conflictingComponents, 'components', [
                'schema:1',
              ])
              .having((e) => e.descriptor, 'descriptor', isNull),
        ),
      );
      expect(await _tables(url), tables);
      expect(await _storedPair(url), (1, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/I
    test('once the live instance has stopped, the same provisioning '
        'runs', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      final store = await _openStore(await open());
      await store.close();
      await _as(
        _twoSteps(minimum: 2),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );
      expect(await _storedPair(url), (2, 2));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('a provisioning that fails before it records the version leaves no '
        'library table on a fresh schema', () async {
      if (url == null) return;
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(failProvisioningBeforeVersionWrite: () => true),
          () => PostgresBackend.provision(url, sslMode: SslMode.disable),
        ),
        throwsA(isA<InjectedFailure>()),
      );
      expect(await _tables(url), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('an upgrade that fails before it records the version leaves no '
        'probe table and the pair as it was', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(
            schemaDeclaration: _twoSteps(),
            failProvisioningBeforeVersionWrite: () => true,
          ),
          () => PostgresBackend.provision(url, sslMode: SslMode.disable),
        ),
        throwsA(isA<InjectedFailure>()),
      );
      expect(await _tables(url), isNot(contains('schema_upgrade_probe')));
      expect(await _storedPair(url), (1, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('a schema holding library tables but no schema version is refused, '
        'naming a reset, and nothing is written', () async {
      if (url == null) return;
      final c = await _connect(url);
      for (final statement in postgresMigrations.single.ddl) {
        await c.execute(statement);
      }
      await c.close();
      final tables = await _tables(url);
      await expectLater(
        PostgresBackend.provision(url, sslMode: SslMode.disable),
        throwsA(
          isA<PostgresSchemaIncompatibleException>().having(
            (e) => e.reason,
            'reason',
            allOf(contains('records no schema version'), contains('reset')),
          ),
        ),
      );
      expect(await _tables(url), tables);
      expect(await _storedPair(url), (null, null));
    });

    // Verifies: EVS-DEV-postgres-backend/G+I
    // Verifies: EVS-DEV-version-compatibility/G
    test('a provisioning waits while a boot holds the boot lock, then refuses '
        'the minimum that boot registered below', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
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
      final tables = await _tables(url);
      var waiting = false;
      var settled = false;
      final provisioning =
          runWithDeliveryTestHooks(
                DeliveryTestHooks(
                  schemaDeclaration: _twoSteps(minimum: 2),
                  onLog: (record) {
                    if (record.message.contains('waiting for the boot lock')) {
                      waiting = true;
                    }
                  },
                ),
                () => PostgresBackend.provision(url, sslMode: SslMode.disable),
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
          ['schema:1'],
        ),
      );
      expect(await _tables(url), tables);
      expect(await _storedPair(url), (1, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-version-compatibility/G
    test('a provisioning that waits longer than bootLockWait for the boot '
        'lock is refused and creates nothing', () async {
      if (url == null) return;
      final holder = await _connect(url);
      addTearDown(holder.close);
      final scope = await PostgresScope.read(holder);
      await holder.execute(
        Sql.named('SELECT pg_advisory_lock(@k)'),
        parameters: <String, Object?>{
          'k': postgresAdvisoryKey('event_sourcing.boot', scope),
        },
      );
      await expectLater(
        PostgresBackend.provision(
          url,
          sslMode: SslMode.disable,
          bootLockWait: const Duration(milliseconds: 500),
        ),
        throwsA(isA<GenerationGuardConfigurationException>()),
      );
      expect(await _tables(url), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G+J
    test('a provisioning whose lock connection reaches another database is '
        'refused and creates nothing', () async {
      if (url == null) return;
      final admin = await _connect(url);
      addTearDown(() async {
        await admin.execute('DROP DATABASE IF EXISTS evs_provision_other');
        await admin.close();
      });
      await admin.execute('DROP DATABASE IF EXISTS evs_provision_other');
      await admin.execute('CREATE DATABASE evs_provision_other');
      await expectLater(
        PostgresBackend.provision(
          url,
          lockUrl: Uri.parse(
            url,
          ).replace(path: '/evs_provision_other').toString(),
          sslMode: SslMode.disable,
        ),
        throwsA(isA<LockSessionConfigurationException>()),
      );
      expect(await _tables(url), isEmpty);
    });

    // Verifies: EVS-DEV-postgres-backend/G+H
    // Verifies: EVS-DEV-version-compatibility/I
    test('an instance at the older schema version keeps committing while an '
        'upgrade that keeps its minimum is provisioned', () async {
      if (url == null) return;
      await PostgresBackend.provision(url, sslMode: SslMode.disable);
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
      await _as(
        _twoSteps(),
        () => PostgresBackend.provision(url, sslMode: SslMode.disable),
      );
      final atUpgrade = appended;
      while (appended < atUpgrade + 5) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      stop = true;
      await loop;
      expect(await _storedPair(url), (2, 1));
    });

    // Verifies: EVS-DEV-postgres-backend/G
    test('a connection that reaches no schema is refused, naming the '
        'requirement, and nothing is created', () async {
      if (url == null) return;
      final admin = await _connect(url);
      addTearDown(() async {
        await admin.execute('DROP ROLE IF EXISTS $_kRole');
        await admin.close();
      });
      await admin.execute('DROP ROLE IF EXISTS $_kRole');
      await admin.execute("CREATE ROLE $_kRole LOGIN PASSWORD 'evs'");
      await admin.execute(
        'ALTER ROLE $_kRole SET search_path = no_such_schema',
      );
      final base = Uri.parse(url);
      final roleUrl = base.replace(userInfo: '$_kRole:evs').toString();
      await expectLater(
        PostgresBackend.provision(roleUrl, sslMode: SslMode.disable),
        throwsA(
          isA<PostgresSchemaIncompatibleException>().having(
            (e) => e.reason,
            'reason',
            contains('the deployment creates the schema'),
          ),
        ),
      );
      expect(await _tables(url), isEmpty);
    });
  });

  // Verifies: EVS-DEV-event-store-open/E
  // Verifies: EVS-DEV-version-compatibility/I
  test('a boot that fails after its library-version event leaves the '
      'generation record as it was', () async {
    if (url == null) return;
    await PostgresBackend.provision(url, sslMode: SslMode.disable);
    final backend = await open();
    await seedLibVersionEventForTest(
      backend,
      eventType: 'lib_version_initialized',
      version: '0.0.1',
      dataFormat: LibVersion.dataFormat,
    );
    Future<Object?> record() async {
      final c = await _connect(url);
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
