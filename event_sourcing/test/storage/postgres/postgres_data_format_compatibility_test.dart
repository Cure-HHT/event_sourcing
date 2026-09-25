// Runs the boot scenarios of EventStore.open on Postgres, plus the rollback
// sequence of two builds opening one database in turn; gated on PG_TEST_URL.
// The shared scenarios' assertions are cited on their own tests in
// test_support/boot_conformance.dart.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresMigrations;
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/boot_conformance.dart';
import '../../test_support/test_backends.dart';
import 'test_postgres_url.dart';

/// A Postgres database the boot scenarios share: the `public` schema of
/// the test database, dropped and created again for each test.
class PostgresBootDatabase implements BootTestDatabase {
  PostgresBootDatabase._(this._db);

  /// Resets the schema and returns the database.
  static Future<PostgresBootDatabase> reset(PostgresTestDatabase db) async {
    await db.reset();
    return PostgresBootDatabase._(db);
  }

  final PostgresTestDatabase _db;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await _db.open(provision: true);
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<void> rewriteStoredDatabaseId(String? value) async {
    final conn = await _db.connectAdmin();
    try {
      if (value == null) {
        await conn.execute(
          "DELETE FROM backend_state WHERE key = 'database_id'",
        );
      } else {
        await conn.execute(
          Sql.named('''
            INSERT INTO backend_state (key, value)
            VALUES ('database_id', to_jsonb(@v::text))
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
          '''),
          parameters: {'v': value},
        );
      }
    } finally {
      await conn.close();
    }
  }

  @override
  Future<void> writeEarlierFormatShape() async {
    // The tables as an earlier data format created them: one integer column
    // for each version. The owner creates them, as provisioning would.
    final conn = await _db.connectOwner();
    try {
      await conn.execute('''
        CREATE TABLE events (
          sequence_number      BIGINT       PRIMARY KEY,
          event_id             TEXT         NOT NULL UNIQUE,
          aggregate_id         TEXT         NOT NULL,
          aggregate_type       TEXT         NOT NULL,
          entry_type           TEXT         NOT NULL,
          entry_type_version   INTEGER      NOT NULL,
          lib_format_version   INTEGER      NOT NULL,
          event_type           TEXT         NOT NULL,
          data                 JSONB        NOT NULL,
          metadata             JSONB        NOT NULL,
          initiator            JSONB        NOT NULL,
          client_timestamp     TIMESTAMPTZ  NOT NULL,
          event_hash           TEXT         NOT NULL,
          flow_token           TEXT,
          previous_event_hash  TEXT
        )
      ''');
      await conn.execute('''
        CREATE TABLE view_target_versions (
          view_name       TEXT     NOT NULL,
          entry_type      TEXT     NOT NULL,
          target_version  INTEGER  NOT NULL,
          PRIMARY KEY (view_name, entry_type)
        )
      ''');
      // The rest of the schema, recorded as provisioned: provisioning
      // refuses a schema whose library tables carry no schema version, so
      // this plays one whose version record claims the current schema while
      // its version columns keep the earlier shape, which the open refuses.
      for (final step in postgresMigrations) {
        for (final statement in step.ddl) {
          await conn.execute(statement);
        }
      }
      await conn.execute(
        Sql.named(
          'INSERT INTO backend_state (key, value) VALUES '
          "('schema_version', @v:jsonb), "
          "('min_compatible_schema_version', @m:jsonb) "
          'ON CONFLICT (key) DO NOTHING',
        ),
        parameters: <String, Object?>{
          'v': postgresSchemaVersion,
          'm': postgresMinCompatibleSchemaVersion,
        },
      );
      for (final (role, kind) in <(String, String)>[
        (_db.runtime, 'runtime'),
        (_db.runtime, 'lock'),
        (_db.lock, 'lock'),
      ]) {
        await conn.execute(
          Sql.named(
            'INSERT INTO library_roles (role_name, kind) VALUES (@r, @k)',
          ),
          parameters: <String, Object?>{'r': role, 'k': kind},
        );
      }
    } finally {
      await conn.close();
    }
    await _db.grant();
  }

  @override
  Future<void> stop(EventStore store) => store.close();

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
    _backends.clear();
  }
}

void main() {
  final pg = PostgresTestDatabase.fromEnvironment();
  if (pg != null) tearDownAll(pg.drop);
  runBootScenarios(() async {
    if (pg == null) return null;
    return PostgresBootDatabase.reset(pg);
  }, backendLabel: 'postgres');

  group('rollback between two builds of one data-format major (postgres)', () {
    PostgresBootDatabase? db;

    setUp(() async {
      if (pg == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      db = await PostgresBootDatabase.reset(pg);
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    // Verifies: EVS-DEV-event-store-open/C
    // Verifies: EVS-DEV-event-store-open/E
    test('the compiled and a newer build open in turn on separate backends; '
        'the log records each change in order; two concurrent opens of the '
        'older build append one change', () async {
      if (db == null) return;
      final d = db!;
      final o1 = await openBootStoreForTest(d, await d.openBackend());
      await appendBootNoteForTest(o1, 'o1');
      final n1 = await openBootStoreForTest(
        d,
        await d.openBackend(),
        newer: true,
      );
      await appendBootNoteForTest(n1, 'n1');
      final o2 = await openBootStoreForTest(d, await d.openBackend());
      await appendBootNoteForTest(o2, 'o2');
      final n2 = await openBootStoreForTest(
        d,
        await d.openBackend(),
        newer: true,
      );
      await appendBootNoteForTest(n2, 'n2');

      List<(String, Object?)> transitions(List<StoredEvent> events) => [
        for (final e in events)
          (
            e.eventType,
            e.eventType == LibVersionEvents.initialized
                ? e.data['version']
                : e.data['toVersion'],
          ),
      ];
      expect(transitions(await libVersionEventsForTest(testBackendOf(n2))), [
        (LibVersionEvents.initialized, LibVersion.version),
        (LibVersionEvents.changed, newerBuildVersionForTest),
        (LibVersionEvents.changed, LibVersion.version),
        (LibVersionEvents.changed, newerBuildVersionForTest),
      ]);
      for (final store in <EventStore>[o1, n1, o2, n2]) {
        expect(store.databaseId, o1.databaseId);
      }
      expect(await n2.reader.findViewRows('boot_notes'), hasLength(4));

      final concurrent = await Future.wait(<Future<EventStore>>[
        d.openBackend().then((b) => openBootStoreForTest(d, b)),
        d.openBackend().then((b) => openBootStoreForTest(d, b)),
      ]);
      final after = transitions(
        await libVersionEventsForTest(testBackendOf(concurrent.first)),
      );
      expect(after, hasLength(5));
      expect(after.last, (LibVersionEvents.changed, LibVersion.version));
      await appendBootNoteForTest(concurrent.last, 'o3');
      expect(
        await concurrent.first.reader.findViewRows('boot_notes'),
        hasLength(5),
      );
    });
  });
}
