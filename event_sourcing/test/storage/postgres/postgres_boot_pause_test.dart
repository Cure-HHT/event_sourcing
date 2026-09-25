// A canary boots beside a serving instance on a seeded log and either
// re-derives a view it adds (view catch-up) or promotes a view to a newer
// minor (snapshot promotion), while the serving instance appends in a tight
// loop; the test measures how long the serving appends pause, and prints
// the measurement. The log holds 5,000 events by default; set
// PAUSE_TEST_AGGREGATES (ten events each) to measure a larger one. Gated on
// PG_TEST_URL.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const _kNote = 'pause_note';
const _kPing = 'pause_ping';
const _kView = 'pause_notes';
const _kAddedView = 'pause_notes_added';
final int _kAggregates =
    int.tryParse(Platform.environment['PAUSE_TEST_AGGREGATES'] ?? '') ?? 500;
const _kEventsPerAggregate = 10;

Future<void> _resetSchema(String url) async {
  final tmp = await Connection.open(
    PostgresBackend.endpointFromUrl(url),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
  await tmp.execute('DROP SCHEMA public CASCADE');
  await tmp.execute('CREATE SCHEMA public');
  await tmp.close();
}

AggregateProjectionSpec _view(String name) => AggregateProjectionSpec(
  viewName: name,
  interest: const SubscriptionFilter(entryTypes: <String>{_kNote}),
  tombstoneEventTypes: const <String>{},
);

Future<EventStore> _open(
  PostgresBackend backend, {
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
  bool addView = false,
}) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry
    ..register(
      EntryTypeDefinition(
        id: _kNote,
        registeredVersion: noteVersion,
        name: _kNote,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kPing,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kPing,
      ),
    );
  final promoters = PromoterRegistry();
  if (noteVersion == const EntryTypeVersion(1, 1)) {
    promoters.register(
      const PromoterSpec(
        viewName: _kView,
        entryType: _kNote,
        fromVersion: EntryTypeVersion(1, 0),
        toVersion: EntryTypeVersion(1, 1),
        transforms: <TransformPrimitive>[
          DefaultField(fieldName: 'b', defaultValue: 0),
        ],
      ),
    );
  }
  final projections = ProjectionRegistry()..register(_view(_kView));
  if (addView) projections.register(_view(_kAddedView));
  return EventStore.open(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'pause-hop',
      identifier: 'pause-install',
      softwareVersion: 'pause-test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
    projections: projections,
    promoters: promoters,
  );
}

Future<void> _append(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String entryType,
  String aggregateId,
  Map<String, Object?> data,
) => store.appendInTxn(
  txn,
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: 'note',
  eventType: 'finalized',
  data: data,
  initiator: const UserInitiator('pause-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// What one boot beside the appending instance measured. An append counts
/// in `appendsWithinBoot` only when it both started and committed inside
/// the boot, so an append the boot blocked for its whole duration does not.
typedef _Measure = ({
  Duration boot,
  Duration longestAppend,
  int appendsWithinBoot,
  EventStore serving,
});

void main() {
  final url = testPostgresUrl();
  final backends = <PostgresBackend>[];

  Future<PostgresBackend> openBackend() async {
    final backend = await PostgresBackend.open(
      url: url!,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    backends.add(backend);
    return backend;
  }

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

  /// Seeds the log, then boots [boot] on a second backend while the first
  /// appends [_kPing] events in a tight loop.
  Future<_Measure> measure(
    Future<EventStore> Function(PostgresBackend backend) boot,
  ) async {
    final serving = await _open(await openBackend());
    for (var a = 0; a < _kAggregates; a += 100) {
      await serving.runTransaction((txn, collector) async {
        for (var i = a; i < a + 100 && i < _kAggregates; i++) {
          for (var e = 0; e < _kEventsPerAggregate; e++) {
            await _append(serving, txn, collector, _kNote, 'agg-$i', {
              'title': 'n$i.$e',
            });
          }
        }
      });
    }
    final clock = Stopwatch()..start();
    final latencies = <(int, int)>[];
    var stop = false;
    Object? loopError;
    var appended = 0;
    final loop = () async {
      try {
        while (!stop) {
          final startedAt = clock.elapsedMicroseconds;
          final n = appended;
          await serving.runTransaction(
            (txn, collector) =>
                _append(serving, txn, collector, _kPing, 'ping-$n', {'n': n}),
          );
          latencies.add((startedAt, clock.elapsedMicroseconds));
          appended++;
        }
      } on Object catch (e) {
        loopError = e;
      }
    }();
    while (appended < 20 && loopError == null) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    expect(loopError, isNull, reason: 'the serving loop runs before the boot');
    final canaryBackend = await openBackend();
    final bootStart = clock.elapsedMicroseconds;
    await boot(canaryBackend);
    final bootEnd = clock.elapsedMicroseconds;
    final atBoot = appended;
    while (appended < atBoot + 20 && loopError == null) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    stop = true;
    await loop;
    expect(loopError, isNull, reason: 'every serving append succeeds');
    var longest = 0;
    var within = 0;
    for (final (start, end) in latencies) {
      if (end < bootStart || start > bootEnd) continue;
      if (end - start > longest) longest = end - start;
      if (start >= bootStart && end <= bootEnd) within++;
    }
    final result = (
      boot: Duration(microseconds: bootEnd - bootStart),
      longestAppend: Duration(microseconds: longest),
      appendsWithinBoot: within,
      serving: serving,
    );
    // ignore: avoid_print, the measurement is the point of this file
    print(
      'boot pause measurement: boot ${result.boot}, longest append '
      '${result.longestAppend}, appends within the boot $within',
    );
    return result;
  }

  // Verifies: EVS-DEV-version-compatibility/L
  // a canary that adds a view over events already in the log re-derives it
  //   in its boot, while the serving instance keeps appending: at the open
  //   the view holds a row per aggregate and carries no catch-up mark, every
  //   serving append commits, and serving appends start and commit while
  //   the boot runs. The serving appends are of an entry type outside the
  //   view's interest; afterwards a serving append of the view's entry type,
  //   from the instance that does not register the view, marks the view
  //   behind the log.
  test('a canary that adds a view over the log re-derives it in its boot '
      'while the serving instance appends', () async {
    if (url == null) return;
    final m = await measure((backend) => _open(backend, addView: true));
    final canary = backends.last;
    final rows = await canary.findViewRows(_kAddedView);
    expect(rows, hasLength(_kAggregates), reason: 'the added view caught up');
    expect(
      await canary.transaction(
        (txn) => canary.readViewTargetBehindInTxn(txn, _kAddedView, _kNote),
      ),
      isFalse,
    );
    expect(
      m.appendsWithinBoot,
      greaterThan(0),
      reason: 'the serving instance appends while the canary boots',
    );

    await m.serving.append(
      entryType: _kNote,
      aggregateId: 'agg-after-boot',
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{'title': 'after'},
      initiator: const UserInitiator('pause-user'),
    );
    expect(
      await canary.transaction(
        (txn) => canary.readViewTargetBehindInTxn(txn, _kAddedView, _kNote),
      ),
      isTrue,
      reason: 'a fold by an instance without the view marks it behind',
    );
  }, timeout: const Timeout(Duration(minutes: 5)));

  // A measurement only: the serving appends wait for a boot that promotes
  // a view, for as long as the promotion takes.
  test('a canary that promotes a view: the pause of the serving appends is '
      'measured', () async {
    if (url == null) return;
    await measure(
      (backend) => _open(backend, noteVersion: const EntryTypeVersion(1, 1)),
    );
    final rows = await backends.last.findViewRows(_kView);
    expect(rows, hasLength(_kAggregates));
    expect(rows.every((r) => r['b'] == 0), isTrue);
    // The serving appends may wait for the whole promotion, so none need
    // commit inside it; the pause is printed, not bounded.
  }, timeout: const Timeout(Duration(minutes: 5)));
}
