// Runs the scenarios of chain_verification_conformance.dart on Postgres,
// changing the stored log through the admin connection; gated on
// PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/chain_verification_conformance.dart.

@TestOn('vm')
library;

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:postgres/postgres.dart';

import '../../test_support/chain_verification_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

/// A scenario database whose stored log a test changes through the admin
/// connection, keeping the chain lookup columns consistent with the record
/// written, as a write that knew the table would.
class _PostgresChainDatabase extends PostgresScenarioDatabase
    implements ChainTestDatabase {
  _PostgresChainDatabase(super.db);

  @override
  Future<void> rewriteEvent(
    int sequenceNumber,
    Map<String, Object?> record,
  ) async {
    final metadata = record['metadata']! as Map;
    final at = ChainCoordinates.fromFields(
      sequenceNumber: sequenceNumber,
      eventId: record['event_id']! as String,
      eventHash: record['event_hash']! as String,
      previousEventHash: record['previous_event_hash'] as String?,
      provenance: metadata['provenance'],
    );
    final conn = await db.connectAdmin();
    try {
      final updated = await conn.execute(
        Sql.named('''
          UPDATE events SET
            data = @data::jsonb,
            metadata = @metadata::jsonb,
            causal = @causal::jsonb,
            event_hash = @eventHash,
            previous_event_hash = @previous,
            origin_database_id = @originDb,
            sealed_hash = @sealed,
            origin_position = @position::bigint,
            held_as_authored_by = CASE
              WHEN @authoredBy::text = (
                SELECT value #>> '{}' FROM backend_state
                WHERE key = 'database_id'
              ) THEN @authoredBy::text END
          WHERE sequence_number = @seq
        '''),
        parameters: <String, Object?>{
          'data': jsonEncode(record['data']),
          'metadata': jsonEncode(metadata),
          'causal': jsonEncode(record['causal']),
          'eventHash': record['event_hash'],
          'previous': record['previous_event_hash'],
          'originDb': at.originatingDatabaseId,
          'sealed': at.sealedHash,
          'position': at.originPosition,
          'authoredBy': at.heldAsAuthoredBy,
          'seq': sequenceNumber,
        },
      );
      expect(updated.affectedRows, 1);
    } finally {
      await conn.close();
    }
  }

  @override
  Future<void> deleteEvent(int sequenceNumber) async {
    final conn = await db.connectAdmin();
    try {
      final deleted = await conn.execute(
        Sql.named('DELETE FROM events WHERE sequence_number = @seq'),
        parameters: <String, Object?>{'seq': sequenceNumber},
      );
      expect(deleted.affectedRows, 1);
    } finally {
      await conn.close();
    }
  }
}

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'chainwalk');
  if (pg != null) tearDownAll(pg.drop);
  runChainVerificationScenarios(
    openDatabase: () async {
      if (pg == null) return null;
      await pg.reset();
      return _PostgresChainDatabase(pg);
    },
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );

  group(
    'the non-blocking read on Postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () {
      late PostgresBackend backend;

      setUp(() async {
        await pg!.reset();
        backend = await pg.open(provision: true);
      });

      tearDown(() => backend.close());

      // Verifies: EVS-DEV-chain-verification/S
      test('reads in one repeatable-read, read-only transaction', () async {
        final settings = await backend.nonBlockingRead(
          (reads) async => <Object?>[
            for (final setting in <String>[
              'transaction_isolation',
              'transaction_read_only',
            ])
              (await backend.queryInTxnForTest(
                reads,
                'SHOW $setting',
              )).single.single,
          ],
        );
        expect(settings, <Object?>['repeatable read', 'on']);
      });

      // Verifies: EVS-DEV-chain-verification/S
      test('refuses a write', () async {
        await expectLater(
          backend.nonBlockingRead(
            (reads) => backend.writeSchemaVersion(reads, 99),
          ),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '25006'),
          ),
        );
      });
    },
  );
}
