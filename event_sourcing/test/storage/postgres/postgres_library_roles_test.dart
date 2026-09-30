// The declared library roles and the refusals at open that rest on them:
// provisioning records the runtime and lock roles the deployment declares in
// a table only the owner writes; open refuses an undeclared pool or lock
// role, a pool or lock role that could change the schema, and a database on
// which a role outside the owner and the declared roles may write a library
// table or act as one of those roles. Each refusal names the role and the
// privilege or attribute, and comes before a generation is registered or a
// lock taken.
//
// Gated on PG_TEST_URL, whose role must be a superuser (it makes a role a
// superuser); files that reset the schema run one at a time. Roles and
// memberships are cluster-global, so every role is created afresh before
// each test and its name carries this process's id.

@TestOn('vm')
library;

import 'dart:io' show pid;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_migration.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresMigrations;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'lr');
  if (db == null) {
    test('skipped: PG_TEST_URL is not set', () {
      markTestSkipped('PG_TEST_URL is not set');
    });
    return;
  }

  /// A role outside the library: an application's, or anyone's.
  final foreign = 'evs_lr_foreign_$pid';

  /// A second runtime role, as a canary deployment declares.
  final canary = 'evs_lr_canary_$pid';

  /// A role another role reaches an attribute through.
  final middle = 'evs_lr_middle_$pid';

  final extraRoles = <String>[foreign, canary, middle];
  final s = quoteIdent(db.schema);

  Future<void> dropExtraRoles() => db.asAdmin((admin) async {
    for (final role in extraRoles) {
      final exists = await admin.execute(
        Sql.named('SELECT 1 FROM pg_roles WHERE rolname = @r'),
        parameters: <String, Object?>{'r': role},
      );
      if (exists.isEmpty) continue;
      await admin.execute('DROP OWNED BY ${quoteIdent(role)}');
      await admin.execute('DROP ROLE ${quoteIdent(role)}');
    }
  });

  Future<void> admin(String sql) => db.asAdmin((c) => c.execute(sql));

  setUp(() async {
    await dropExtraRoles();
    await db.createRoles();
    await db.asAdmin((c) async {
      for (final role in <String>[foreign, canary]) {
        await c.execute(
          "CREATE ROLE ${quoteIdent(role)} LOGIN PASSWORD 'evs' "
          'NOSUPERUSER NOCREATEDB NOCREATEROLE',
        );
      }
      await c.execute(
        'CREATE ROLE ${quoteIdent(middle)} NOLOGIN CREATEROLE NOSUPERUSER',
      );
    });
    await db.reset(provision: true);
  });
  tearDownAll(() async {
    await dropExtraRoles();
    await db.drop();
  });

  /// Asserts that no advisory lock is held by the runtime or lock role and
  /// that no generation is recorded.
  Future<void> expectNothingRegistered() => db.asAdmin((c) async {
    final locks = await c.execute(
      Sql.named('''
        SELECT count(*) FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'advisory' AND a.usename IN (@r, @l, @c)
      '''),
      parameters: <String, Object?>{'r': db.runtime, 'l': db.lock, 'c': canary},
    );
    expect(locks.first[0], 0, reason: 'no lock is held');
    final generation = await c.execute(
      Sql.named('SELECT count(*) FROM $s.backend_state WHERE key = @k'),
      parameters: <String, Object?>{'k': 'data_generation'},
    );
    expect(generation.first[0], 0, reason: 'no generation is recorded');
  });

  /// Opens as the runtime role (lock session as [lockUrl], or the runtime
  /// role) and expects a refusal naming [role] and [privilege], with
  /// nothing registered.
  Future<void> expectRefused({
    required String role,
    required String privilege,
    String? lockUrl,
    String? url,
  }) async {
    Object? error;
    try {
      final backend = await PostgresBackend.open(
        url: url ?? db.runtimeUrl,
        schema: db.schema,
        lockUrl: lockUrl,
        sslMode: SslMode.disable,
      );
      await backend.close();
    } on Object catch (e) {
      error = e;
    }
    expect(
      error,
      isA<PostgresRoleRefusedException>(),
      reason: 'open refuses, naming $role and $privilege',
    );
    final refused = error! as PostgresRoleRefusedException;
    expect(
      refused.refusals,
      contains(
        isA<PostgresRoleRefusal>()
            .having((r) => r.role, 'role', role)
            .having((r) => r.privilege, 'privilege', privilege),
      ),
    );
    expect(refused.toString(), contains(role));
    expect(refused.toString(), contains(privilege));
    await expectNothingRegistered();
  }

  Future<void> expectOpens({String? url, String? lockUrl}) async {
    final backend = await PostgresBackend.open(
      url: url ?? db.runtimeUrl,
      schema: db.schema,
      lockUrl: lockUrl,
      sslMode: SslMode.disable,
    );
    await backend.close();
  }

  test('the documented setup opens, with a separate lock role', () async {
    await expectOpens();
    await expectOpens(lockUrl: db.lockUrl);
  });

  group('declared roles', () {
    // Verifies: EVS-DEV-postgres-backend/P
    // provisioning records the declared runtime and lock roles.
    test('provisioning records the declared roles, and a second '
        'provisioning replaces them', () async {
      Future<Set<(String, String)>> recorded() => db.asAdmin((c) async {
        final rows = await c.execute(
          'SELECT role_name, kind FROM $s.library_roles',
        );
        return <(String, String)>{
          for (final r in rows) (r[0]! as String, r[1]! as String),
        };
      });
      expect(await recorded(), <(String, String)>{
        (db.runtime, 'runtime'),
        (db.runtime, 'lock'),
        (db.lock, 'lock'),
      });
      await db.provision(runtimeRoles: <String>{db.runtime, canary});
      expect(await recorded(), <(String, String)>{
        (db.runtime, 'runtime'),
        (canary, 'runtime'),
        (db.runtime, 'lock'),
        (db.lock, 'lock'),
      });
      await db.provision(lockRoles: <String>{db.lock});
      expect(await recorded(), <(String, String)>{
        (db.runtime, 'runtime'),
        (db.lock, 'lock'),
      });
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // Verifies: EVS-DEV-postgres-backend/G
    // a provisioning by a build below the stored schema version records
    //   the roles it declares and leaves the schema and its version pair
    //   untouched.
    test('a provisioning below the stored schema version records the '
        'declared roles and leaves the schema untouched', () async {
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(
          schemaDeclaration: <PostgresMigrationStep>[
            ...postgresMigrations,
            PostgresMigrationStep(
              toVersion: postgresSchemaVersion + 1,
              minCompatibleVersion: postgresMinCompatibleSchemaVersion,
              ddl: <String>['CREATE TABLE schema_upgrade_probe (id INTEGER)'],
            ),
          ],
        ),
        db.provision,
      );
      Future<List<String>> state() => db.asAdmin((c) async {
        final rows = await c.execute(
          'SELECT key, value::text FROM $s.backend_state '
          "WHERE key LIKE '%schema_version' ORDER BY key",
        );
        return <String>[for (final r in rows) '${r[0]}=${r[1]}'];
      });
      final before = await state();
      await db.provision(runtimeRoles: <String>{db.runtime, canary});
      expect(await state(), before);
      final roles = await db.asAdmin(
        (c) => c.execute(
          "SELECT role_name FROM $s.library_roles WHERE kind = 'runtime'",
        ),
      );
      expect(
        <String>{for (final r in roles) r[0]! as String},
        <String>{db.runtime, canary},
      );
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // provisioning takes the declared roles explicitly; an empty set is
    //   refused, writing nothing.
    test('provisioning refuses an empty set of declared roles', () async {
      for (final (runtime, lock) in <(Set<String>, Set<String>)>[
        (<String>{}, <String>{db.lock}),
        (<String>{db.runtime}, <String>{}),
      ]) {
        await expectLater(
          PostgresBackend.provision(
            db.ownerUrl,
            schema: db.schema,
            runtimeRoles: runtime,
            lockRoles: lock,
            sslMode: SslMode.disable,
          ),
          throwsArgumentError,
        );
      }
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // the declared roles are in a table only the owner writes: the runtime
    //   role reads it and cannot change it, and a declared role holding a
    //   write grant on it is refused at open.
    test('only the owner writes the declared roles', () async {
      final rows = await db.asRole(
        db.runtime,
        (c) => c.execute('SELECT role_name FROM $s.library_roles'),
      );
      expect(rows, isNotEmpty);
      for (final statement in <String>[
        "INSERT INTO $s.library_roles (role_name, kind) VALUES ('x', 'runtime')",
        "UPDATE $s.library_roles SET role_name = 'x'",
        'DELETE FROM $s.library_roles',
      ]) {
        await expectLater(
          db.asRole(db.runtime, (c) => c.execute(statement)),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '42501'),
          ),
          reason: statement,
        );
      }
      await admin(
        'GRANT INSERT ON $s.library_roles TO ${quoteIdent(db.runtime)}',
      );
      await expectRefused(role: db.runtime, privilege: 'INSERT');
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // an undeclared pool role is refused.
    test('an undeclared pool role is refused', () async {
      await db.provision(runtimeRoles: <String>{canary});
      await expectRefused(role: db.runtime, privilege: 'UNDECLARED');
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // an undeclared lock role is refused; the runtime role is a lock role
    //   only when declared one.
    test('an undeclared lock role is refused', () async {
      await db.provision(lockRoles: <String>{db.runtime});
      await expectRefused(
        role: db.lock,
        privilege: 'UNDECLARED',
        lockUrl: db.lockUrl,
      );
      await db.provision(lockRoles: <String>{db.lock});
      await expectRefused(role: db.runtime, privilege: 'UNDECLARED');
    });

    // Verifies: EVS-DEV-postgres-backend/P
    // Verifies: EVS-PRD-storage-barrier/F
    // instances under two declared runtime roles open side by side, each
    //   admitting the other's grants.
    test('a second declared runtime role opens beside the first', () async {
      await db.provision(runtimeRoles: <String>{db.runtime, canary});
      await db.asAdmin((c) async {
        await c.execute('GRANT USAGE ON SCHEMA $s TO ${quoteIdent(canary)}');
        for (final MapEntry(key: table, value: privileges)
            in postgresRuntimeRoleGrants.entries) {
          await c.execute(
            'GRANT ${privileges.join(', ')} ON $s.${quoteIdent(table)} '
            'TO ${quoteIdent(canary)}',
          );
        }
      });
      final serving = await PostgresBackend.open(
        url: db.runtimeUrl,
        schema: db.schema,
        sslMode: SslMode.disable,
      );
      addTearDown(serving.close);
      final canaryUrl = postgresUrlAsRole(db.adminUrl, canary);
      await expectRefused(
        role: canary,
        privilege: 'UNDECLARED',
        url: canaryUrl,
      );
      await db.provision(
        runtimeRoles: <String>{db.runtime, canary},
        lockRoles: <String>{db.runtime, db.lock, canary},
      );
      await expectOpens(url: canaryUrl);
    });
  });

  group('a pool or lock role that could change the schema', () {
    // Verifies: EVS-DEV-postgres-backend/N
    test('a superuser pool role is refused', () async {
      await admin('ALTER ROLE ${quoteIdent(db.runtime)} SUPERUSER');
      await expectRefused(role: db.runtime, privilege: 'SUPERUSER');
    });

    // Verifies: EVS-DEV-postgres-backend/N
    test('CREATEROLE through an inherited membership is refused', () async {
      await admin(
        'GRANT ${quoteIdent(middle)} TO ${quoteIdent(db.runtime)} '
        'WITH INHERIT TRUE, SET FALSE',
      );
      await expectRefused(role: db.runtime, privilege: 'CREATEROLE');
    });

    // Verifies: EVS-DEV-postgres-backend/N
    test('membership in the owner with SET only is refused', () async {
      await admin(
        'GRANT ${quoteIdent(db.owner)} TO ${quoteIdent(db.runtime)} '
        'WITH INHERIT FALSE, SET TRUE',
      );
      await expectRefused(role: db.runtime, privilege: 'MEMBER');
    });

    // Verifies: EVS-DEV-postgres-backend/N
    test('a pool role owning a table in the schema is refused', () async {
      await admin('CREATE TABLE $s.extra (x integer)');
      await admin('ALTER TABLE $s.extra OWNER TO ${quoteIdent(db.runtime)}');
      await expectRefused(role: db.runtime, privilege: 'OWNER');
    });

    // Verifies: EVS-DEV-postgres-backend/N
    test('a superuser lock role is refused', () async {
      await admin('ALTER ROLE ${quoteIdent(db.lock)} SUPERUSER');
      await expectRefused(
        role: db.lock,
        privilege: 'SUPERUSER',
        lockUrl: db.lockUrl,
      );
    });
  });

  group('a foreign write privilege', () {
    final f = quoteIdent(foreign);

    // Verifies: EVS-DEV-postgres-backend/M
    // Verifies: EVS-PRD-storage-barrier/F
    test('INSERT on a library table is refused', () async {
      await admin('GRANT INSERT ON $s.events TO $f');
      await expectRefused(role: foreign, privilege: 'INSERT');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    // Verifies: EVS-PRD-storage-barrier/F
    test('UPDATE on one column of a library table is refused', () async {
      await admin('GRANT UPDATE (row_data) ON $s.view_rows TO $f');
      await expectRefused(role: foreign, privilege: 'UPDATE');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('TRIGGER on a library table is refused', () async {
      await admin('GRANT TRIGGER ON $s.fifo_entries TO $f');
      await expectRefused(role: foreign, privilege: 'TRIGGER');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('REFERENCES on a library table is refused', () async {
      await admin('GRANT REFERENCES ON $s.events TO $f');
      await expectRefused(role: foreign, privilege: 'REFERENCES');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('USAGE on a sequence in the schema is refused', () async {
      await db.asRole(
        db.owner,
        (c) => c.execute('CREATE SEQUENCE $s.extra_seq'),
      );
      await admin('GRANT USAGE ON SEQUENCE $s.extra_seq TO $f');
      await expectRefused(role: foreign, privilege: 'USAGE');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    // Verifies: EVS-PRD-storage-barrier/F
    test('membership in pg_write_all_data is refused', () async {
      await admin('GRANT pg_write_all_data TO $f');
      await expectRefused(role: foreign, privilege: 'MEMBER');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    // Verifies: EVS-PRD-storage-barrier/F
    test('inheritable membership in a declared runtime role is '
        'refused', () async {
      await admin(
        'GRANT ${quoteIdent(db.runtime)} TO $f WITH INHERIT TRUE, SET FALSE',
      );
      await expectRefused(role: foreign, privilege: 'MEMBER');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('any privilege of PUBLIC on a library table is refused', () async {
      await admin('GRANT SELECT ON $s.backend_state TO PUBLIC');
      await expectRefused(role: 'PUBLIC', privilege: 'SELECT');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('CREATE on the schema for a foreign role is refused', () async {
      await admin('GRANT CREATE ON SCHEMA $s TO $f');
      await expectRefused(role: foreign, privilege: 'CREATE');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    test('CREATE on the schema for PUBLIC is refused', () async {
      await admin('GRANT CREATE ON SCHEMA $s TO PUBLIC');
      await expectRefused(role: 'PUBLIC', privilege: 'CREATE');
    });

    // Verifies: EVS-DEV-postgres-backend/M
    // CREATE on the schema is refused for a declared runtime role too.
    test('CREATE on the schema for the runtime role is refused', () async {
      await admin('GRANT CREATE ON SCHEMA $s TO ${quoteIdent(db.runtime)}');
      await expectRefused(role: db.runtime, privilege: 'CREATE');
    });
  });

  group('admitted', () {
    // Verifies: EVS-DEV-postgres-backend/M
    test('a foreign role holding only SELECT is admitted', () async {
      await admin('GRANT USAGE ON SCHEMA $s TO ${quoteIdent(foreign)}');
      await admin(
        'GRANT SELECT ON ALL TABLES IN SCHEMA $s TO ${quoteIdent(foreign)}',
      );
      await admin(
        'GRANT SELECT (row_data) ON $s.view_rows TO ${quoteIdent(foreign)}',
      );
      await expectOpens();
    });

    // Verifies: EVS-DEV-postgres-backend/M
    // Verifies: EVS-DEV-postgres-backend/N
    // a membership held only with the admin option grants neither
    //   inheritance nor set, and is admitted.
    test('an admin-option-only membership is admitted', () async {
      await admin(
        'GRANT ${quoteIdent(db.runtime)} TO ${quoteIdent(foreign)} '
        'WITH ADMIN TRUE, INHERIT FALSE, SET FALSE',
      );
      await admin(
        'GRANT ${quoteIdent(db.owner)} TO ${quoteIdent(foreign)} '
        'WITH ADMIN TRUE, INHERIT FALSE, SET FALSE',
      );
      await admin(
        'GRANT ${quoteIdent(middle)} TO ${quoteIdent(db.runtime)} '
        'WITH ADMIN TRUE, INHERIT FALSE, SET FALSE',
      );
      await expectOpens();
    });
  });
}
