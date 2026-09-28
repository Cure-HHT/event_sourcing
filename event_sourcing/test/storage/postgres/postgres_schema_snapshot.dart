// A point-in-time snapshot of a Postgres scenario schema's data, taken and
// restored in pure SQL through the schema's owner and administrative
// connections, emulating the effect of restoring a database from a backup
// taken earlier. This file declares no tests, so it carries no citation.

import 'package:postgres/postgres.dart';

import 'test_postgres_url.dart';

/// A copy of every base table [PostgresTestDatabase.schema] holds, taken by
/// [PostgresSchemaSnapshot.take] and applied back by [restore].
class PostgresSchemaSnapshot {
  PostgresSchemaSnapshot._(
    this._db,
    this._snapSchema,
    this._tables,
    this._sequenceValues,
  );

  final PostgresTestDatabase _db;
  final String _snapSchema;
  final List<String> _tables;
  final Map<String, int> _sequenceValues;

  /// Copies every base table of [db]'s schema into a fresh snapshot schema,
  /// owned by [db]'s owner role: `CREATE SCHEMA` (through [db]'s
  /// administrative connection, which alone is guaranteed database-level
  /// `CREATE`) and, for each table, `CREATE TABLE snap.t AS TABLE
  /// schema.t` (through the owner connection, which owns every table of
  /// [db]'s schema and, as the snapshot schema's declared authorization,
  /// owns its copies too). Also records every sequence [db]'s schema owns
  /// and its current value, for [restore] to reapply.
  static Future<PostgresSchemaSnapshot> take(PostgresTestDatabase db) async {
    final snapSchema = '${db.schema}_snap';
    final snapIdent = quoteIdent(snapSchema);
    await db.asAdmin((admin) async {
      await admin.execute('DROP SCHEMA IF EXISTS $snapIdent CASCADE');
      await admin.execute(
        'CREATE SCHEMA $snapIdent AUTHORIZATION ${quoteIdent(db.owner)}',
      );
    });
    final owner = await db.connectOwner();
    try {
      final tables = await _tableNames(owner, db.schema);
      for (final table in tables) {
        final t = quoteIdent(table);
        await owner.execute(
          'CREATE TABLE $snapIdent.$t AS TABLE ${quoteIdent(db.schema)}.$t',
        );
      }
      final sequenceValues = await _sequenceValuesOf(owner, db.schema);
      return PostgresSchemaSnapshot._(db, snapSchema, tables, sequenceValues);
    } finally {
      await owner.close();
    }
  }

  /// Restores [_db]'s schema to this snapshot: every table this snapshot
  /// holds is truncated and refilled from its snapshot copy, and every
  /// sequence [take] recorded is set back to its recorded value, all
  /// through the owner connection, in one transaction. The library's own
  /// queue-table guard triggers refuse a plain `TRUNCATE`/`INSERT` outside
  /// the shapes of its own writes (`EVS-DEV-destination-drain/S`), so each
  /// table's triggers are disabled for the copy and re-enabled before the
  /// transaction commits; a failure rolls the whole restore, triggers
  /// included, back to how it stood before.
  Future<void> restore() async {
    final owner = await _db.connectOwner();
    try {
      await owner.run((session) async {
        final schema = quoteIdent(_db.schema);
        for (final table in _tables) {
          final t = quoteIdent(table);
          await session.execute('ALTER TABLE $schema.$t DISABLE TRIGGER ALL');
          await session.execute('TRUNCATE TABLE $schema.$t');
          await session.execute(
            'INSERT INTO $schema.$t SELECT * FROM ${quoteIdent(_snapSchema)}.$t',
          );
          await session.execute('ALTER TABLE $schema.$t ENABLE TRIGGER ALL');
        }
        // The queue table's guards fire in every session, replica role
        // included; ENABLE TRIGGER ALL sets them back to origin-only.
        for (final trigger in const [
          'fifo_entries_guard',
          'fifo_entries_truncate_guard',
        ]) {
          await session.execute(
            'ALTER TABLE $schema.fifo_entries ENABLE ALWAYS TRIGGER $trigger',
          );
        }
        for (final MapEntry(key: sequence, value: value)
            in _sequenceValues.entries) {
          await session.execute(
            Sql.named('SELECT setval(@seq, @val)'),
            parameters: <String, Object?>{
              'seq': '${_db.schema}.$sequence',
              'val': value,
            },
          );
        }
      });
    } finally {
      await owner.close();
    }
  }

  /// Drops the snapshot schema. Redundant with [PostgresTestDatabase.drop],
  /// which drops every object the owner role owns, snapshot schemas
  /// included; a test calls this only to reclaim the schema between
  /// several snapshots of the same database.
  Future<void> drop() => _db.asAdmin(
    (admin) async => admin.execute(
      'DROP SCHEMA IF EXISTS ${quoteIdent(_snapSchema)} CASCADE',
    ),
  );

  static Future<List<String>> _tableNames(
    Connection conn,
    String schema,
  ) async {
    final rows = await conn.execute(
      Sql.named(
        'SELECT table_name FROM information_schema.tables '
        "WHERE table_schema = @schema AND table_type = 'BASE TABLE' "
        'ORDER BY table_name',
      ),
      parameters: <String, Object?>{'schema': schema},
    );
    return <String>[for (final row in rows) row[0]! as String];
  }

  static Future<Map<String, int>> _sequenceValuesOf(
    Connection conn,
    String schema,
  ) async {
    final rows = await conn.execute(
      Sql.named(
        'SELECT sequencename, last_value FROM pg_sequences '
        'WHERE schemaname = @schema',
      ),
      parameters: <String, Object?>{'schema': schema},
    );
    return <String, int>{
      for (final row in rows)
        if (row[1] != null) row[0]! as String: row[1]! as int,
    };
  }
}
