// Runs the version-compatibility scenarios on Postgres; gated on
// PG_TEST_URL. The scenarios' assertions are cited on their own tests in
// test_support/version_compatibility_conformance.dart.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/version_compatibility_conformance.dart';
import 'test_postgres_url.dart';

class _PostgresVersionDatabase implements VersionTestDatabase {
  _PostgresVersionDatabase(this._url);

  final String _url;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await PostgresBackend.open(
      url: _url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<void> stop(EventStore store) => store.close();

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
  }
}

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

Future<void> _resetSchema(String url) async {
  final tmp = await _connect(url);
  await tmp.execute('DROP SCHEMA public CASCADE');
  await tmp.execute('CREATE SCHEMA public');
  await tmp.close();
}

const _kType = 'versioned_note';
const _kView = 'versioned_notes';

/// Opens an event store over [backend] registering [_kType] at
/// [registered]; at `1.1` the minor step adds `b` with the default `0`.
Future<EventStore> _openStore(
  PostgresBackend backend,
  EntryTypeVersion registered,
) async {
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes.register(
    EntryTypeDefinition(
      id: _kType,
      registeredVersion: registered,
      name: _kType,
    ),
  );
  final promoters = PromoterRegistry();
  if (registered == const EntryTypeVersion(1, 1)) {
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
  return EventStore.open(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'versions-hop',
      identifier: 'versions-install',
      softwareVersion: 'versions-test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
    projections: ProjectionRegistry()
      ..register(
        const AggregateProjectionSpec(
          viewName: _kView,
          interest: SubscriptionFilter(entryTypes: <String>{_kType}),
          tombstoneEventTypes: <String>{},
        ),
      ),
    promoters: promoters,
  );
}

Future<EntryTypeVersion?> _storedTarget(StorageBackend backend) =>
    backend.transaction(
      (txn) => backend.readViewTargetVersionInTxn(txn, _kView, _kType),
    );

void main() {
  final url = testPostgresUrl();
  runVersionCompatibilityScenarios(() async {
    if (url == null) return null;
    await _resetSchema(url);
    return _PostgresVersionDatabase(url);
  }, backendLabel: 'postgres');

  group('a canary overlap on postgres', () {
    final backends = <PostgresBackend>[];

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

    Future<PostgresBackend> openBackend() async {
      final backend = await PostgresBackend.open(
        url: url!,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
      backends.add(backend);
      return backend;
    }

    // Verifies: EVS-DEV-version-compatibility/E
    // Verifies: EVS-DEV-snapshot-promotion-on-open/A
    test("the older build's fold overlapping the newer build's boot "
        'promotion ends with a lowered target or a promoted row, and one '
        'event', () async {
      if (url == null) return;
      final older = await _openStore(
        await openBackend(),
        const EntryTypeVersion(1, 0),
      );
      await older.append(
        entryType: _kType,
        aggregateId: 'agg-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'a': 1},
        initiator: const UserInitiator('versions-user'),
      );

      // The older build's fold transaction reads the stored target, then
      // waits while the newer build boots and commits its promotion.
      final snapshotTaken = Completer<void>();
      final resume = Completer<void>();
      var runs = 0;
      final folding = older.runTransaction<void>((txn, collector) async {
        runs += 1;
        await older.backend.readViewTargetVersionInTxn(txn, _kView, _kType);
        if (!snapshotTaken.isCompleted) snapshotTaken.complete();
        await resume.future;
        await older.appendInTxn(
          txn,
          collector: collector,
          flowToken: null,
          metadata: null,
          security: null,
          checkpointReason: null,
          changeReason: null,
          dedupeByContent: false,
          entryType: _kType,
          aggregateId: 'agg-2',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'a': 2},
          initiator: const UserInitiator('versions-user'),
        );
      });
      await snapshotTaken.future;
      final newerBackend = await openBackend();
      await _openStore(newerBackend, const EntryTypeVersion(1, 1));
      expect(await _storedTarget(newerBackend), const EntryTypeVersion(1, 1));
      resume.complete();
      await folding;

      final target = await _storedTarget(newerBackend);
      final rows = await newerBackend.findViewRows(_kView);
      final unpromoted = rows.where((row) => !row.containsKey('b')).toList();
      expect(
        target == const EntryTypeVersion(1, 0) || unpromoted.isEmpty,
        isTrue,
        reason:
            'target $target with rows lacking b: $unpromoted '
            '(transaction runs: $runs)',
      );
      final agg2Events = await newerBackend.findEventsForAggregate('agg-2');
      expect(agg2Events, hasLength(1));

      // The next open of the newer build promotes whatever the older build
      // folded.
      final reopened = await openBackend();
      await _openStore(reopened, const EntryTypeVersion(1, 1));
      expect(await _storedTarget(reopened), const EntryTypeVersion(1, 1));
      for (final row in await reopened.findViewRows(_kView)) {
        expect(row['b'], 0, reason: 'row ${row['aggregateId']}');
      }
    });
  });

  group('stored versions out of range on postgres', () {
    setUp(() async {
      if (url == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      await _resetSchema(url);
    });

    // Verifies: EVS-DEV-version-compatibility/A+C
    test('the schema refuses a major below 1 or a minor below 0 in events '
        'and in view target versions', () async {
      if (url == null) return;
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
      await backend.close();
      final conn = await _connect(url);
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
        for (final target in <(int, int)>[(0, 0), (1, -1)]) {
          await expectLater(
            conn.execute(
              Sql.named("""
                INSERT INTO view_target_versions (view_name, entry_type,
                  target_major, target_minor)
                VALUES ('v', 'versioned_note', @major, @minor)
              """),
              parameters: <String, Object?>{
                'major': target.$1,
                'minor': target.$2,
              },
            ),
            throwsA(
              isA<ServerException>().having((e) => e.code, 'code', '23514'),
            ),
            reason: '$target',
          );
        }
      } finally {
        await conn.close();
      }
    });
  });
}
