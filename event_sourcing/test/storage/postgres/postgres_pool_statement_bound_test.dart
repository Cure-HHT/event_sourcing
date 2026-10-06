// The bound on every statement of the Postgres backend's pool: a pool
// connection that goes silent after a statement's result, and a statement
// that runs past the configured statement timeout, fail within the bound
// with a transient error, and the pool serves the next statement on another
// connection. The pool connects through a forwarder that can stop relaying
// without closing the socket; the lock session connects directly. Gated on
// PG_TEST_URL.

@TestOn('vm')
@Timeout(Duration(minutes: 2))
@Tags(['timing'])
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_bounded_statements.dart'
    show BoundedPool;
import 'package:test/test.dart';

import '../../test_support/silent_connection_forwarder.dart';
import 'test_postgres_url.dart';

/// The statement timeout the tests configure.
const _queryTimeout = Duration(seconds: 1);

/// What a timer that is due, and the steps it starts, may be delayed by on a
/// loaded runner: the margin a wall-clock bound adds to the bound the library
/// configures.
const _schedulingSlack = Duration(milliseconds: 500);

void main() {
  group('the statement timeout', () {
    // Verifies: EVS-DEV-postgres-backend/U
    // a statement timeout that is not positive is refused before any
    //   connection is opened.
    for (final timeout in <Duration>[
      Duration.zero,
      const Duration(seconds: -1),
    ]) {
      test('$timeout is refused', () async {
        await expectLater(
          PostgresBackend.open(
            url: 'postgres://nobody:none@127.0.0.1:1/none',
            schema: 'none',
            queryTimeout: timeout,
          ),
          throwsA(
            isA<ArgumentError>().having((e) => e.name, 'name', 'queryTimeout'),
          ),
        );
      });
    }
  });

  final db = PostgresTestDatabase.fromEnvironment(tag: 'bound');
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  tearDownAll(db.drop);

  late SilentConnectionForwarder forwarder;
  late PostgresBackend backend;

  setUp(() async {
    await db.reset(provision: true);
    forwarder = SilentConnectionForwarder(Uri.parse(db.adminUrl));
    await forwarder.start();
    backend = await PostgresBackend.open(
      url: forwarder.route(db.runtimeUrl),
      schema: db.schema,
      lockUrl: db.runtimeUrl,
      sslMode: SslMode.disable,
      queryTimeout: _queryTimeout,
    );
  });

  tearDown(() async {
    await backend.close();
    await forwarder.close();
  });

  // Verifies: EVS-DEV-postgres-backend/X
  // a backend opened without a statement timeout bounds its pool by the
  //   postgres driver's default statement timeout of 5 minutes.
  test('the statement timeout defaults to the driver default', () async {
    final unconfigured = await PostgresBackend.open(
      url: db.runtimeUrl,
      schema: db.schema,
      sslMode: SslMode.disable,
    );
    try {
      expect(
        (unconfigured.pool as BoundedPool).queryTimeout,
        const Duration(minutes: 5),
      );
    } finally {
      await unconfigured.close();
    }
  });

  /// The server process id of the pool connection a transaction runs on.
  Future<int> poolPid() => backend.transaction(
    (txn) async =>
        (await backend.queryInTxnForTest(
              txn,
              'SELECT pg_backend_pid()',
            )).first[0]!
            as int,
  );

  // Verifies: EVS-DEV-postgres-backend/U+V+W
  // a pool connection that stops answering right after a statement's result
  //   (so the driver's close of the statement's portal meets a black hole)
  //   fails the statement within twice the statement timeout with
  //   PostgresStatementTimeoutException, which the library classifies as
  //   transient; the connection is not handed out again, and the next
  //   transaction runs on another server session.
  test('a pool connection silent after a statement result', () async {
    final bound = _queryTimeout * 2;
    late int silentPid;
    final watch = Stopwatch();
    Object? error;
    StackTrace? stack;
    try {
      await backend.transaction((txn) async {
        silentPid =
            (await backend.queryInTxnForTest(
                  txn,
                  'SELECT pg_backend_pid()',
                )).first[0]!
                as int;
        forwarder
          ..freezeOnlyAnsweringConnection = true
          ..freezeAfterStatementInTransaction = true;
        watch.start();
        await backend.queryInTxnForTest(txn, 'SELECT 1');
      });
    } on Object catch (e, st) {
      error = e;
      stack = st;
    }
    watch.stop();
    expect(
      forwarder.freezeAfterStatementInTransaction,
      isFalse,
      reason: 'the forwarder froze after the statement result',
    );
    expect(error, isA<PostgresStatementTimeoutException>());
    expect(watch.elapsed, greaterThanOrEqualTo(bound));
    expect(watch.elapsed, lessThan(bound + _schedulingSlack));
    expect(
      classifyStorageException(error!, stack!),
      isA<StorageTransientException>(),
    );
    final next = await poolPid();
    expect(next, isNot(silentPid));
  });

  // Verifies: EVS-DEV-postgres-backend/U+V
  // a statement on the pool that runs longer than the configured statement
  //   timeout fails within twice that timeout with
  //   PostgresStatementTimeoutException, a transient failure, and the pool
  //   serves the next transaction.
  test('a statement slower than the statement timeout', () async {
    final watch = Stopwatch()..start();
    Object? error;
    StackTrace? stack;
    try {
      await backend.transaction(
        (txn) => backend.queryInTxnForTest(txn, 'SELECT pg_sleep(5)'),
      );
    } on Object catch (e, st) {
      error = e;
      stack = st;
    }
    watch.stop();
    expect(error, isA<PostgresStatementTimeoutException>());
    expect(watch.elapsed, lessThan(_queryTimeout * 2 + _schedulingSlack));
    expect(
      classifyStorageException(error!, stack!),
      isA<StorageTransientException>(),
    );
    expect(await poolPid(), isA<int>());
  });
}
