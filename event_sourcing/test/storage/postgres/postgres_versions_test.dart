// Runs the version-compatibility scenarios on Postgres; gated on
// PG_TEST_URL. The scenarios' assertions are cited on their own tests in
// test_support/version_compatibility_conformance.dart.

@TestOn('vm')
library;

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/version_compatibility_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runVersionCompatibilityScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    backendLabel: 'postgres',
  );

  group('stored versions out of range on postgres', () {
    setUp(() async {
      if (db == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      await db.reset();
    });

    // Verifies: EVS-DEV-version-compatibility/A+C
    test(
      'the schema refuses a major below 1 or a minor below 0 in events',
      () async {
        if (db == null) return;
        final backend = await db.open(provision: true);
        await backend.close();
        final conn = await db.connectAdmin();
        try {
          final eventColumns = <String, (int, int, int, int)>{
            'entry_type_version_major': (0, 0, 2, 0),
            'entry_type_version_minor': (1, -1, 2, 0),
            'lib_format_version_major': (1, 0, 0, 0),
            'lib_format_version_minor': (1, 0, 2, -1),
          };
          var seq = 0;
          for (final column in eventColumns.entries) {
            seq += 1;
            final (em, en, lm, ln) = column.value;
            await expectLater(
              conn.execute(
                Sql.named("""
                INSERT INTO events (sequence_number, event_id, aggregate_id,
                  aggregate_type, entry_type, entry_type_version_major,
                  entry_type_version_minor, lib_format_version_major,
                  lib_format_version_minor, entry_type_version_json,
                  lib_format_version_json, event_type, data, metadata,
                  initiator, client_timestamp, client_timestamp_text,
                  event_hash, unknown_fields)
                VALUES (@seq, @id, 'agg', 'note', 'versioned_note', @em,
                  @en, @lm, @ln, '{}'::jsonb, '{}'::jsonb, 'finalized',
                  '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, now(),
                  '2026-09-01T12:00:00.000Z', 'h', '{}'::jsonb)
              """),
                parameters: <String, Object?>{
                  'seq': seq,
                  'id': 'bad-$seq',
                  'em': em,
                  'en': en,
                  'lm': lm,
                  'ln': ln,
                },
              ),
              throwsA(
                isA<ServerException>()
                    .having((e) => e.code, 'code', '23514')
                    .having((e) => e.message, 'message', contains(column.key)),
              ),
              reason: column.key,
            );
          }
        } finally {
          await conn.close();
        }
      },
    );
  });
}
