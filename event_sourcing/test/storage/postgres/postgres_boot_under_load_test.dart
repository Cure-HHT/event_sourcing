// A boot is not starved by other sessions holding the row every append
// updates. Gated on PG_TEST_URL.
//
// The pause a boot causes while it holds back appends, and the bound on a
// catch-up's own pause of the serving loop, are measured in
// postgres_view_convergence_measured_test.dart.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const _kType = 'load_note';
const _kView = 'load_notes';

/// Waits until [reached], failing at once with the error [failure] reports
/// when the loop the wait depends on has failed.
Future<void> _waitUntil(
  bool Function() reached,
  Object? Function() failure,
) async {
  while (!reached()) {
    final error = failure();
    if (error != null) fail('the loop the wait depends on failed: $error');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

Future<EventStore> _open(PostgresBackend backend) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: _kType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kType,
    ),
  );
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      PostgresSecurityContextStore(backend: backend),
    ),
    entryTypes: registry,
    source: const Source(
      hopId: 'load-hop',
      identifier: 'load-install',
      softwareVersion: 'load-test',
    ),
    projections: ProjectionRegistry()
      ..register(
        const AggregateProjectionSpec(
          viewName: _kView,
          interest: SubscriptionFilter(entryTypes: <String>{_kType}),
          tombstoneEventTypes: <String>{},
        ),
      ),
  );
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'load');
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset();
  });

  tearDown(() async {
    for (final backend in backends) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-DEV-event-store-open/E
  // the boot is not starved by other sessions holding the row every append
  //   updates: each of five boots on the backend still runs its body
  //   exactly once, even while two other sessions rewrite and hold the
  //   sequence counter's row back to back.
  test('the boot is not starved by other instances holding the row every '
      'append updates', () async {
    if (db == null) return;
    final backend = await db.open(
      provision: true,
      bootLockWait: const Duration(seconds: 10),
    );
    backends.add(backend);

    // Two sessions stand in for two serving instances whose appends hold the
    // sequence counter row back to back: each rewrites the row and keeps it
    // for 10 ms before committing, in a loop. They run at READ COMMITTED so
    // they queue behind each other rather than abort each other.
    final holders = <Connection>[];
    var stop = false;
    var holds = 0;
    Object? holdError;
    Future<void> hold(Connection connection) async {
      try {
        while (!stop) {
          await connection.execute('BEGIN');
          await connection.execute(
            'UPDATE backend_state SET value = value '
            "WHERE key = 'sequence_counter'",
          );
          await connection.execute('SELECT pg_sleep(0.01)');
          await connection.execute('COMMIT');
          holds++;
        }
      } on Object catch (e) {
        holdError = e;
      }
    }

    // The row exists before the holders start.
    await _open(backend);
    for (var i = 0; i < 2; i++) {
      holders.add(await db.connectAdmin());
    }
    final loops = [for (final connection in holders) hold(connection)];
    try {
      await _waitUntil(() => holds >= 10, () => holdError);
      for (var boot = 0; boot < 5; boot++) {
        var bootRuns = 0;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
          () => _open(backend),
        );
        expect(bootRuns, 1, reason: 'boot $boot ran its body $bootRuns times');
      }
    } finally {
      stop = true;
      await Future.wait(loops);
      for (final connection in holders) {
        await connection.close();
      }
    }
    expect(holdError, isNull, reason: 'every hold of the counter row ran');
  }, timeout: const Timeout(Duration(minutes: 2)));
}
