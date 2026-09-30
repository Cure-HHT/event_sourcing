// Verifies: EVS-DEV-postgres-backend/Q
// every statement the library runs on Postgres -- on its pool, on its lock
//   session and for provisioning -- runs with the search path set, for its
//   transaction only, to exactly the named schema, pg_catalog and pg_temp:
//   a schema named after the connecting role, created by another role that
//   can create schemas, shadows none of the library's tables for writes,
//   reads or provisioning; the path in every kind of library transaction
//   is exactly that one; the sessions keep the server's default path; and
//   a name that needs quoting round-trips.
// Verifies: EVS-DEV-postgres-backend/R
// open refuses a schema that is not the current schema inside a library
//   transaction, naming both schemas, before it registers a generation.
//
// Gated on PG_TEST_URL, whose role must be able to create roles; files
// that reset the schema run one at a time.

@TestOn('vm')
library;

import 'dart:io' show pid;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresLibraryTables;
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: 'search_path_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'search_path_note',
);

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final d in kSystemEntryTypes) {
    registry.register(d);
  }
  return registry..register(_noteDef);
}

Future<EventStore> _openStore(PostgresBackend backend) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: _entryTypes(),
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-00000000a5a5',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );

Future<StoredEvent> _append(EventStore store, String id) async =>
    (await store.append(
      entryType: 'search_path_note',
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'noted',
      data: <String, Object?>{'id': id},
      initiator: const UserInitiator('u'),
    ))!;

/// A role that may create schemas in the database: any role holding
/// `CREATE` on it, an application's role among them.
final String _intruder = 'evs_sp_intruder_$pid';

Future<int> _count(Connection c, String schema, String table) async =>
    (await c.execute(
          'SELECT count(*) FROM ${quoteIdent(schema)}.${quoteIdent(table)}',
        )).first[0]!
        as int;

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'sp');
  if (db == null) {
    test('skipped: PG_TEST_URL is not set', () {
      markTestSkipped('PG_TEST_URL is not set');
    });
    return;
  }

  Future<void> dropIntruder() => db.asAdmin((admin) async {
    for (final role in <String>[db.runtime, db.owner]) {
      await admin.execute('DROP SCHEMA IF EXISTS ${quoteIdent(role)} CASCADE');
    }
    final exists = await admin.execute(
      Sql.named('SELECT 1 FROM pg_roles WHERE rolname = @r'),
      parameters: <String, Object?>{'r': _intruder},
    );
    if (exists.isEmpty) return;
    await admin.execute('DROP OWNED BY ${quoteIdent(_intruder)}');
    await admin.execute('DROP ROLE ${quoteIdent(_intruder)}');
  });

  setUpAll(() async {
    await db.createRoles();
    await dropIntruder();
    await db.asAdmin((admin) async {
      await admin.execute(
        "CREATE ROLE ${quoteIdent(_intruder)} LOGIN PASSWORD 'evs'",
      );
      final database =
          (await admin.execute('SELECT current_database()')).first[0]!
              as String;
      await admin.execute(
        'GRANT CREATE ON DATABASE ${quoteIdent(database)} '
        'TO ${quoteIdent(_intruder)}',
      );
    });
  });
  tearDownAll(() async {
    await dropIntruder();
    await db.drop();
  });
  setUp(() async {
    await db.asAdmin((admin) async {
      for (final role in <String>[db.runtime, db.owner]) {
        await admin.execute(
          'DROP SCHEMA IF EXISTS ${quoteIdent(role)} CASCADE',
        );
      }
    });
  });

  /// As the intruder, creates a schema named after [role], which the
  /// server's default search path (`"$user", public`) puts first for that
  /// role, open to everyone. With [copyTables] it holds a table of each
  /// library table's name and shape, and a copy of the library's
  /// `backend_state` rows.
  Future<void> createDecoy(String role, {required bool copyTables}) async {
    final decoy = quoteIdent(role);
    if (copyTables) {
      // Copying the library tables' shape needs SELECT on them.
      await db.asAdmin((admin) async {
        await admin.execute(
          'GRANT USAGE ON SCHEMA ${quoteIdent(db.schema)} '
          'TO ${quoteIdent(_intruder)}',
        );
        await admin.execute(
          'GRANT SELECT ON ALL TABLES IN SCHEMA ${quoteIdent(db.schema)} '
          'TO ${quoteIdent(_intruder)}',
        );
      });
    }
    await db.asRole(_intruder, (c) async {
      await c.execute('CREATE SCHEMA $decoy');
      await c.execute('GRANT USAGE, CREATE ON SCHEMA $decoy TO PUBLIC');
      if (!copyTables) return;
      for (final table in postgresLibraryTables) {
        final t = quoteIdent(table);
        await c.execute(
          'CREATE TABLE $decoy.$t '
          '(LIKE ${quoteIdent(db.schema)}.$t INCLUDING ALL)',
        );
        await c.execute('GRANT ALL ON $decoy.$t TO PUBLIC');
      }
      await c.execute(
        'INSERT INTO $decoy.backend_state '
        'SELECT * FROM ${quoteIdent(db.schema)}.backend_state',
      );
    });
  }

  group('shadowing', () {
    test('a schema named after the runtime role takes no write and no '
        'read', () async {
      await db.reset(provision: true);
      await createDecoy(db.runtime, copyTables: true);

      final backend = await db.open();
      addTearDown(backend.close);
      final store = await _openStore(backend);
      addTearDown(store.close);
      final appended = await _append(store, 'shadow-1');

      await db.asAdmin((admin) async {
        final inLibrary = await admin.execute(
          Sql.named(
            'SELECT count(*) FROM ${quoteIdent(db.schema)}.events '
            'WHERE event_id = @id',
          ),
          parameters: <String, Object?>{'id': appended.eventId},
        );
        expect(
          inLibrary.first[0],
          1,
          reason: "the append lands in the library's schema",
        );
        expect(
          await _count(admin, db.runtime, 'events'),
          0,
          reason: 'the decoy events table receives nothing',
        );
        expect(
          await _count(admin, db.runtime, 'view_rows'),
          0,
          reason: 'the decoy view table receives nothing',
        );

        // A row planted in the decoy is not read.
        await admin.execute(
          'INSERT INTO ${quoteIdent(db.runtime)}.events '
          'SELECT * FROM ${quoteIdent(db.schema)}.events',
        );
        await admin.execute(
          'UPDATE ${quoteIdent(db.runtime)}.events '
          "SET event_id = 'planted-' || event_id",
        );
      });
      final read = await backend.findAllEvents();
      expect(read, isNotEmpty);
      expect(
        <String>[for (final e in read) e.eventId],
        everyElement(isNot(startsWith('planted-'))),
        reason: 'a read resolves the library schema, not the decoy',
      );
    });

    test('a schema named after the owner takes none of provisioning', () async {
      await db.reset();
      await createDecoy(db.owner, copyTables: false);

      await db.provision();

      final tables = await db.asAdmin((admin) async {
        final r = await admin.execute(
          Sql.named(
            'SELECT table_schema, table_name FROM information_schema.tables '
            'WHERE table_schema IN (@lib, @decoy)',
          ),
          parameters: <String, Object?>{'lib': db.schema, 'decoy': db.owner},
        );
        return <(String, String)>[
          for (final row in r) (row[0]! as String, row[1]! as String),
        ];
      });
      expect(
        <String>{
          for (final (schema, table) in tables)
            if (schema == db.owner) table,
        },
        isEmpty,
        reason: 'provisioning creates nothing in the decoy',
      );
      expect(
        <String>{
          for (final (schema, table) in tables)
            if (schema == db.schema) table,
        },
        containsAll(postgresLibraryTables),
        reason: "provisioning creates the tables in the library's schema",
      );
    });
  });

  test('every library transaction runs with exactly the pinned path, and the '
      "sessions keep the server's default", () async {
    await db.reset(provision: true);
    final backend = await db.open(lockUrl: db.lockUrl);
    addTearDown(backend.close);
    final store = await _openStore(backend);
    addTearDown(store.close);
    await _append(store, 'pinned-1');

    final expected = await db.asAdmin(
      (admin) async =>
          (await admin.execute(
                Sql.named("SELECT quote_ident(@s) || ', pg_catalog, pg_temp'"),
                parameters: <String, Object?>{'s': db.schema},
              )).first[0]!
              as String,
    );
    final paths = await backend.searchPathsForTest();
    expect(paths.transaction, expected, reason: 'a pool transaction');
    expect(paths.read, expected, reason: 'a read outside a transaction');
    expect(paths.lockSession, expected, reason: 'the lock session');

    final serverDefault = await db.asAdmin(
      (admin) async =>
          (await admin.execute(
                "SELECT boot_val FROM pg_settings WHERE name = 'search_path'",
              )).first[0]!
              as String,
    );
    final sessions = await backend.sessionSearchPathsForTest();
    expect(
      sessions.pool,
      <String>{serverDefault},
      reason:
          'the setting is local to each transaction: no pool session '
          'keeps it',
    );
    expect(
      sessions.lockSession,
      serverDefault,
      reason:
          'the setting is local to each transaction: the lock session '
          'does not keep it',
    );
  });

  test('open refuses a schema that is not the current schema, naming both, '
      'and registers nothing', () async {
    await db.reset(provision: true);

    Object? error;
    try {
      final backend = await PostgresBackend.open(
        url: db.runtimeUrl,
        schema: 'no_such_schema',
        sslMode: SslMode.disable,
      );
      await backend.close();
    } on Object catch (e) {
      error = e;
    }
    expect(error, isA<PostgresSchemaMismatchException>());
    final mismatch = error! as PostgresSchemaMismatchException;
    expect(mismatch.describedSchema, 'no_such_schema');
    expect(mismatch.currentSchema, isNot('no_such_schema'));
    expect(mismatch.toString(), contains('"no_such_schema"'));
    expect(mismatch.toString(), contains(mismatch.currentSchema ?? 'none'));

    await db.asAdmin((admin) async {
      final locks = await admin.execute(
        Sql.named('''
          SELECT count(*) FROM pg_locks l
          JOIN pg_stat_activity a ON a.pid = l.pid
          WHERE l.locktype = 'advisory' AND a.usename = @r
        '''),
        parameters: <String, Object?>{'r': db.runtime},
      );
      expect(locks.first[0], 0, reason: 'no lock is registered');
      final generation = await admin.execute(
        Sql.named(
          'SELECT count(*) FROM ${quoteIdent(db.schema)}.backend_state '
          'WHERE key = @k',
        ),
        parameters: <String, Object?>{'k': 'data_generation'},
      );
      expect(generation.first[0], 0, reason: 'no generation is recorded');
    });
  });

  test('a schema name that needs quoting round-trips', () async {
    final mixed = PostgresTestDatabase(
      db.adminUrl,
      tag: 'spmx',
      schema: 'Evs-Mixed Case_$pid',
    );
    addTearDown(mixed.drop);
    await mixed.reset(provision: true);
    final backend = await mixed.open(lockUrl: mixed.lockUrl);
    addTearDown(backend.close);
    final store = await _openStore(backend);
    addTearDown(store.close);
    final appended = await _append(store, 'mixed-1');

    final read = await backend.findAllEvents();
    expect(<String>[
      for (final e in read) e.eventId,
    ], contains(appended.eventId));
    final paths = await backend.searchPathsForTest();
    expect(paths.transaction, '"Evs-Mixed Case_$pid", pg_catalog, pg_temp');
    await mixed.asAdmin((admin) async {
      expect(await _count(admin, mixed.schema, 'events'), greaterThan(0));
    });
  });
}
