// Verifies: EVS-DEV-storage-capability/H
// every transaction the application runs through the storage reader runs
//   READ ONLY at the database, and not deferrable: the server refuses a
//   write in the backend's read-only transaction with SQLSTATE 25006, and
//   inside the reader's transaction `transaction_read_only` is on.
//
// Gated on PG_TEST_URL, whose role must be able to create roles; files
// that reset the schema run one at a time.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

/// The value of the run-time setting [name] inside the transaction [txn]
/// of [backend] belongs to.
Future<String> _settingIn(
  PostgresBackend backend,
  Transaction txn,
  String name,
) async =>
    (await backend.queryInTxnForTest(
          txn,
          Sql.named('SELECT current_setting(@name)'),
          parameters: <String, Object?>{'name': name},
        )).first[0]!
        as String;

Future<EventStore> _openStore(PostgresBackend backend) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: EntryTypeRegistry(),
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-00000000c0de',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'reader_ro');
  if (db == null) {
    test('skipped: PG_TEST_URL is not set', () {
      markTestSkipped('PG_TEST_URL is not set');
    });
    return;
  }
  tearDownAll(db.drop);

  late PostgresBackend backend;

  setUp(() async {
    await db.reset();
    backend = await db.open(provision: true);
  });

  tearDown(() => backend.close());

  test("the server refuses a write in the backend's read-only transaction "
      'with SQLSTATE 25006', () async {
    await expectLater(
      backend.readOnlyTransaction(
        (txn) => backend.upsertViewRowInTxn(
          txn,
          'reader_ro_view',
          'k',
          <String, Object?>{'v': 1},
        ),
      ),
      throwsA(isA<ServerException>().having((e) => e.code, 'code', '25006')),
    );
    expect(await backend.findViewRows('reader_ro_view'), isEmpty);
  });

  test("the reader's transaction runs read-only and not deferrable; the "
      "event store's runs read-write", () async {
    final store = await _openStore(backend);
    try {
      final inReader = await store.reader.transaction(
        (txn) async => (
          readOnly: await _settingIn(backend, txn, 'transaction_read_only'),
          deferrable: await _settingIn(backend, txn, 'transaction_deferrable'),
          searchPath: await _settingIn(backend, txn, 'search_path'),
        ),
      );
      expect(inReader.readOnly, 'on');
      expect(inReader.deferrable, 'off');
      expect(inReader.searchPath, contains(db.schema));
      final inStore = await store.runTransaction(
        (txn, collector) => _settingIn(backend, txn, 'transaction_read_only'),
      );
      expect(inStore, 'off');
    } finally {
      await store.close();
    }
  });
}
