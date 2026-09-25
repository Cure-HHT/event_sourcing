// A newer build boots, and promotes a large view, while the build serving
// the same Postgres database appends in a tight loop; and a boot is not
// starved by two sessions holding the row every append updates. Gated on
// PG_TEST_URL.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const _kType = 'load_note';
const _kView = 'load_notes';
const _kRows = 2000;

Future<void> _resetSchema(String url) async {
  final tmp = await Connection.open(
    PostgresBackend.endpointFromUrl(url),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
  await tmp.execute('DROP SCHEMA public CASCADE');
  await tmp.execute('CREATE SCHEMA public');
  await tmp.close();
}

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

Future<EventStore> _open(PostgresBackend backend, EntryTypeVersion version) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    EntryTypeDefinition(id: _kType, registeredVersion: version, name: _kType),
  );
  final promoters = PromoterRegistry();
  if (version == const EntryTypeVersion(1, 1)) {
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
    entryTypes: registry,
    source: const Source(
      hopId: 'load-hop',
      identifier: 'load-install',
      softwareVersion: 'load-test',
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

void main() {
  final url = testPostgresUrl();
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

  // Verifies: EVS-DEV-event-store-open/C
  // Verifies: EVS-DEV-event-store-open/E
  test("the serving build's appends wait for the boot instead of failing, "
      'and the boot promotes every row', () async {
    if (url == null) return;
    final backendA = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    backends.add(backendA);
    final a = await _open(backendA, const EntryTypeVersion(1, 0));

    // A view of 2,000 rows folded under 1.0, appended in chunks.
    for (var chunk = 0; chunk < _kRows; chunk += 250) {
      await a.runTransaction((txn, collector) async {
        for (var i = chunk; i < chunk + 250; i++) {
          await _appendOne(a, txn, collector, 'agg-$i', 'n$i');
        }
      });
    }
    expect(await backendA.findViewRows(_kView), hasLength(_kRows));

    // A appends in a tight loop, counting the runs of each append body and
    // recording when each append started and finished.
    final clock = Stopwatch()..start();
    final spans = <(int, int)>[];
    final maxRuns = <int>[];
    Object? loopError;
    var stop = false;
    var appended = 0;
    final loop = () async {
      try {
        while (!stop) {
          var runs = 0;
          final i = appended;
          final startedAt = clock.elapsedMicroseconds;
          await a.runTransaction((txn, collector) {
            runs++;
            return _appendOne(a, txn, collector, 'late-$i', 'late $i');
          });
          spans.add((startedAt, clock.elapsedMicroseconds));
          maxRuns.add(runs);
          appended++;
        }
      } on Object catch (e) {
        loopError = e;
      }
    }();

    // Let the loop get going, then boot the newer build beside it.
    await _waitUntil(() => appended >= 5, () => loopError);
    // The newer build opens its backend and boots while A appends.
    final backendN = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    backends.add(backendN);
    var bootRuns = 0;
    final n = await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        buildDeclaration: (
          version: '0.6.0',
          dataFormat: const DataFormatVersion(2, 1),
        ),
        onBootBodyRun: () => bootRuns++,
      ),
      () => _open(backendN, const EntryTypeVersion(1, 1)),
    );
    final bootCommittedBy = clock.elapsedMicroseconds;
    // Let A append after the boot committed, then stop the loop.
    final appendedAtBoot = appended;
    await _waitUntil(() => appended >= appendedAtBoot + 5, () => loopError);
    stop = true;
    await loop;

    expect(loopError, isNull, reason: 'every append of A succeeds');
    expect(bootRuns, 1, reason: 'the appends do not abort the boot');
    expect(
      spans.any(
        (span) => span.$1 < bootCommittedBy && span.$2 > bootCommittedBy,
      ),
      isTrue,
      reason: 'an append was in flight while the boot committed',
    );
    expect(
      maxRuns.every((runs) => runs <= 2),
      isTrue,
      reason: 'no append body ran more than twice: $maxRuns',
    );
    final changes = await backendN.findAllEvents(
      entryType: LibVersionEvents.changed,
    );
    expect(changes, hasLength(1));
    expect(changes.single.data['toVersion'], '0.6.0');

    // Every row the boot promoted carries the default.
    final rows = await backendN.findViewRows(_kView);
    for (var i = 0; i < _kRows; i++) {
      final row = rows.firstWhere((r) => r['aggregateId'] == 'agg-$i');
      expect(row['b'], 0, reason: 'agg-$i promoted');
    }
    // A folded under 1.0 after the boot, which lowered the stored target.
    final target = await backendN.transaction(
      (txn) => backendN.readViewTargetVersionInTxn(txn, _kView, _kType),
    );
    expect(target, const EntryTypeVersion(1, 0));
    expect(n.databaseId, a.databaseId);
  }, timeout: const Timeout(Duration(minutes: 5)));

  // Verifies: EVS-DEV-event-store-open/E
  test('the boot is not starved by other instances holding the row every '
      'append updates', () async {
    if (url == null) return;
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      bootLockWait: const Duration(seconds: 10),
      provisionSchema: true,
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
    await _open(backend, const EntryTypeVersion(1, 0));
    for (var i = 0; i < 2; i++) {
      holders.add(
        await Connection.open(
          PostgresBackend.endpointFromUrl(url),
          settings: const ConnectionSettings(sslMode: SslMode.disable),
        ),
      );
    }
    final loops = [for (final connection in holders) hold(connection)];
    try {
      await _waitUntil(() => holds >= 10, () => holdError);
      for (var boot = 0; boot < 5; boot++) {
        var bootRuns = 0;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
          () => _open(backend, const EntryTypeVersion(1, 0)),
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

Future<void> _appendOne(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String aggregateId,
  String title,
) async {
  await store.appendInTxn(
    txn,
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'title': title},
    initiator: const UserInitiator('load-user'),
    flowToken: null,
    metadata: null,
    security: null,
    checkpointReason: null,
    changeReason: null,
    dedupeByContent: false,
    collector: collector,
  );
}
