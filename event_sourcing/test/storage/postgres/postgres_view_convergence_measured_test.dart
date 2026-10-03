// The measured scenarios of EVS-DEV-view-convergence: a serving instance
// appends in a tight loop on a Postgres database whose log already holds
// 20,000 events of 2,000 aggregates, ten each, all folded into one
// aggregate view of 2,000 rows (the measured database), while a scenario's
// converging instances catch up a new copy of that view. Each scenario's
// window is the 60 seconds from the moment the first converging instance
// begins to open. Gated on PG_TEST_URL.

@TestOn('vm')
@Tags(['timing'])
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:test/test.dart';

import 'postgres_schema_snapshot.dart';
import 'test_postgres_url.dart';

const _kType = 'measured_note';
const _kPing = 'measured_ping';
const _kView = 'measured_notes';
const _kAddedView = 'measured_notes_added';
const _kAggregates = 2000;
const _kEventsPerAggregate = 10;
const _kWindow = Duration(seconds: 60);
const _kAppendBound = Duration(seconds: 1);

AggregateProjectionSpec _viewSpec(String name) => AggregateProjectionSpec(
  viewName: name,
  interest: const SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: const <String>{},
);

EntryTypeRegistry _entryTypes(EntryTypeVersion noteVersion) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      EntryTypeDefinition(
        id: _kType,
        registeredVersion: noteVersion,
        name: _kType,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kPing,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kPing,
      ),
    );
}

PromoterRegistry _promoters(EntryTypeVersion noteVersion, String viewName) {
  final promoters = PromoterRegistry();
  if (noteVersion == const EntryTypeVersion(1, 1)) {
    promoters.register(
      PromoterSpec(
        viewName: viewName,
        entryType: _kType,
        fromVersion: const EntryTypeVersion(1, 0),
        toVersion: const EntryTypeVersion(1, 1),
        transforms: const <TransformPrimitive>[
          DefaultField(fieldName: 'b', defaultValue: 0),
        ],
      ),
    );
  }
  return promoters;
}

ProjectionRegistry _projections(List<String> viewNames) {
  final registry = ProjectionRegistry();
  for (final name in viewNames) {
    registry.register(_viewSpec(name));
  }
  return registry;
}

Future<EventStore> _open(
  PostgresBackend backend, {
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
  List<String> viewNames = const <String>[_kView],
}) => EventStore.open(
  storage: ApplicationSuppliedStorage(
    backend,
    PostgresSecurityContextStore(backend: backend),
  ),
  entryTypes: _entryTypes(noteVersion),
  source: Source(
    hopId: 'measured-hop',
    identifier: 'measured-install-${identityHashCode(backend)}',
    softwareVersion: 'measured-test',
  ),
  projections: _projections(viewNames),
  promoters: _promoters(noteVersion, viewNames.first),
);

Future<void> _appendNote(
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
  aggregateType: entryType == _kPing ? 'ping' : 'note',
  eventType: 'finalized',
  data: data,
  initiator: const UserInitiator('measured-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// Seeds [store] with the measured database: [_kAggregates] aggregates of
/// [_kEventsPerAggregate] events each, all of [_kType], in transactions of
/// 1,000 events so the seed fits well inside the test's time budget. Runs
/// once, in `setUpAll`; a snapshot taken right after this returns is what
/// each scenario restores before it opens its own instances, so the three
/// scenarios measure against one identically seeded database instead of
/// each paying the seed's cost itself.
Future<void> _seed(EventStore store) async {
  final clock = Stopwatch()..start();
  const chunk = 100;
  for (var start = 0; start < _kAggregates; start += chunk) {
    await store.runTransaction((txn, collector) async {
      for (var i = start; i < start + chunk; i++) {
        for (var e = 0; e < _kEventsPerAggregate; e++) {
          await _appendNote(store, txn, collector, _kType, 'agg-$i', {
            'title': 'n$i.$e',
          });
        }
      }
    });
  }
  // ignore: avoid_print, the measurement is the point of this file
  print(
    'seeded ${_kAggregates * _kEventsPerAggregate} events in '
    '${clock.elapsed}',
  );
}

/// One append the serving loop made: when its call started and when its
/// transaction committed, in microseconds of the scenario's shared clock.
typedef _Span = (int start, int end);

/// A serving loop appending [_kPing] and [_kType] events, alternating,
/// against [store]'s aggregates, from before a scenario's window until
/// [stop] is called. [stop] returns once the loop's own future has
/// completed and reports any error the loop threw.
class _ServingLoop {
  _ServingLoop(this.store, this.clock);

  final EventStore store;
  final Stopwatch clock;
  final List<_Span> spans = <_Span>[];
  Object? error;
  bool _stop = false;
  late Future<void> _future;
  var _n = 0;

  void start() {
    _future = _run();
  }

  /// Starts the loop and waits for it to get going.
  Future<void> startAndWait() {
    start();
    return waitUntilStarted();
  }

  Future<void> _run() async {
    try {
      while (!_stop) {
        final n = _n++;
        final isPing = n.isEven;
        final entryType = isPing ? _kPing : _kType;
        final aggregateId = isPing
            ? 'serving-ping-$n'
            : 'agg-${n % _kAggregates}';
        final callStart = clock.elapsedMicroseconds;
        await store.runTransaction(
          (txn, collector) => _appendNote(
            store,
            txn,
            collector,
            entryType,
            aggregateId,
            {'n': n},
          ),
        );
        spans.add((callStart, clock.elapsedMicroseconds));
      }
    } on Object catch (e) {
      error = e;
    }
  }

  Future<void> waitUntilStarted() async {
    while (spans.length < 5 && error == null) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }

  Future<void> stop() async {
    _stop = true;
    await _future;
  }
}

/// Fails when a span the loop recorded starting inside
/// `[windowStart, windowStart + window)` did not commit within [bound] of
/// its call, or when the loop threw.
void _checkServingBound(
  _ServingLoop loop,
  int windowStart,
  Duration window,
  Duration bound,
) {
  expect(loop.error, isNull, reason: 'every serving append succeeds');
  final windowEnd = windowStart + window.inMicroseconds;
  final overLong = <_Span>[
    for (final span in loop.spans)
      if (span.$1 >= windowStart &&
          span.$1 < windowEnd &&
          span.$2 - span.$1 > bound.inMicroseconds)
        span,
  ];
  expect(
    overLong,
    isEmpty,
    reason:
        'every serving append starting inside the window commits within '
        '$bound: $overLong',
  );
  final inWindow = [
    for (final span in loop.spans)
      if (span.$1 >= windowStart && span.$1 < windowEnd) span,
  ];
  final longest = inWindow.isEmpty
      ? Duration.zero
      : inWindow
            .map((s) => Duration(microseconds: s.$2 - s.$1))
            .reduce((a, b) => a > b ? a : b);
  // ignore: avoid_print, the measurement is the point of this file
  print(
    'serving loop: ${inWindow.length} appends in the window, longest '
    '$longest',
  );
}

/// Polls the watermark of the copy [copyId] on [backend] until it reaches
/// [target], failing when [deadline] (a clock reading) passes first.
Future<void> _waitForWatermark(
  PostgresBackend backend,
  Stopwatch clock,
  String copyId,
  int target,
  int deadline,
) async {
  while (true) {
    final copies = await backend.transaction(backend.readViewCopiesInTxn);
    final copy = copies.where((c) => c.copyId == copyId).firstOrNull;
    if (copy != null && copy.watermark >= target) {
      // ignore: avoid_print, the measurement is the point of this file
      print(
        'converging copy $copyId reached $target at '
        '${Duration(microseconds: clock.elapsedMicroseconds)}',
      );
      return;
    }
    if (clock.elapsedMicroseconds > deadline) {
      fail(
        'the converging copy did not reach position $target within the '
        'window (last seen: ${copy?.watermark})',
      );
    }
    await Future<void>.delayed(const Duration(milliseconds: 100));
  }
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'measured');
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  // The measured database is seeded once for the whole file and snapshotted
  // so every scenario restores the identical starting point instead of
  // seeding it itself. Whichever scenario runs first pays for the seed (a
  // `setUpAll` would work too, but package:test fixes its timeout at 12
  // minutes with no way to raise it; a scenario's own `timeout:` is under
  // this file's control instead). Later scenarios just await the already
  // resolved future and restore.
  Future<PostgresSchemaSnapshot>? seedFuture;
  Future<PostgresSchemaSnapshot> measuredSnapshot() => seedFuture ??= () async {
    await db!.reset(provision: true);
    final backend = await db.open();
    final store = await _open(backend);
    await _seed(store);
    await store.close();
    final clock = Stopwatch()..start();
    final snapshot = await PostgresSchemaSnapshot.take(db);
    // ignore: avoid_print, the measurement is the point of this file
    print('snapshot taken in ${clock.elapsed}');
    return snapshot;
  }();

  Future<PostgresBackend> openBackend({bool provision = false}) async {
    final backend = await db!.open(provision: provision);
    backends.add(backend);
    return backend;
  }

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    final snapshot = await measuredSnapshot();
    // Restores the tables (view_copies, generation records and
    // backend_state included, since the snapshot covers every base table
    // of the schema) to the seeded database, so nothing a previous
    // scenario registered or converged leaks into this one.
    final clock = Stopwatch()..start();
    await snapshot.restore();
    // ignore: avoid_print, the measurement is the point of this file
    print('restored the measured database in ${clock.elapsed}');
  });

  tearDown(() async {
    for (final backend in backends.reversed) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-DEV-view-convergence/W
  // Verifies: EVS-DEV-view-convergence/X
  test(
    'three instances of one build add a view over the measured database: '
    'the serving loop keeps its 1 s bound and the added copy converges',
    () async {
      if (db == null) return;
      final servingBackend = await openBackend(provision: true);
      final serving = await _open(servingBackend);

      final clock = Stopwatch()..start();
      final loop = _ServingLoop(serving, clock);
      await loop.startAndWait();

      final windowStart = clock.elapsedMicroseconds;
      final headAtWindowStart = await servingBackend.readSequenceCounter();
      // ignore: avoid_print, the measurement is the point of this file
      print(
        'window start at ${Duration(microseconds: windowStart)}, head $headAtWindowStart',
      );

      final canaryBackends = await Future.wait(
        List.generate(3, (_) => openBackend()),
      );
      final canaries = await Future.wait([
        for (final backend in canaryBackends)
          _open(backend, viewNames: const <String>[_kAddedView]),
      ]);
      final copyId = canaries.first.copyIdOf(_kAddedView);
      final windowEnd = windowStart + _kWindow.inMicroseconds;
      // Polls concurrently with the window, so the print it makes on
      // convergence names the real time the copy converged, not just that
      // it had converged by the time this test next looked.
      final converged = _waitForWatermark(
        canaryBackends.first,
        clock,
        copyId,
        headAtWindowStart,
        windowEnd,
      );

      final remaining = windowEnd - clock.elapsedMicroseconds;
      if (remaining > 0) {
        await Future<void>.delayed(Duration(microseconds: remaining));
      }
      await loop.stop();

      await converged;
      _checkServingBound(loop, windowStart, _kWindow, _kAppendBound);
    },
    timeout: const Timeout(Duration(minutes: 15)),
  );

  // Verifies: EVS-DEV-view-convergence/W
  // Verifies: EVS-DEV-view-convergence/X
  test('a second instance opens with a newer minor of the measured entry '
      'type: the serving loop keeps its 1 s bound and the promoted copy '
      'converges', () async {
    if (db == null) return;
    final servingBackend = await openBackend(provision: true);
    final serving = await _open(servingBackend);

    final clock = Stopwatch()..start();
    final loop = _ServingLoop(serving, clock);
    await loop.startAndWait();

    final windowStart = clock.elapsedMicroseconds;
    final headAtWindowStart = await servingBackend.readSequenceCounter();
    // ignore: avoid_print, the measurement is the point of this file
    print(
      'window start at ${Duration(microseconds: windowStart)}, head $headAtWindowStart',
    );

    final canaryBackend = await openBackend();
    final canary = await _open(
      canaryBackend,
      noteVersion: const EntryTypeVersion(1, 1),
    );
    final copyId = canary.copyIdOf(_kView);
    final windowEnd = windowStart + _kWindow.inMicroseconds;
    final converged = _waitForWatermark(
      canaryBackend,
      clock,
      copyId,
      headAtWindowStart,
      windowEnd,
    );

    final remaining = windowEnd - clock.elapsedMicroseconds;
    if (remaining > 0) {
      await Future<void>.delayed(Duration(microseconds: remaining));
    }
    await loop.stop();

    await converged;
    _checkServingBound(loop, windowStart, _kWindow, _kAppendBound);

    final rows = await canaryBackend.findViewRows(copyId);
    expect(rows, hasLength(_kAggregates));
    expect(rows.every((r) => r['b'] == 0), isTrue);
  }, timeout: const Timeout(Duration(minutes: 15)));

  // Verifies: EVS-DEV-view-convergence/W
  // Verifies: EVS-DEV-view-convergence/X
  test(
    'the serving instance itself opens with a newer minor of the measured '
    'entry type: its own copy converges while it keeps its 1 s bound',
    () async {
      if (db == null) return;
      final backend = await openBackend(provision: true);

      final clock = Stopwatch()..start();
      final windowStart = clock.elapsedMicroseconds;
      final headAtWindowStart = await backend.readSequenceCounter();
      // ignore: avoid_print, the measurement is the point of this file
      print(
        'window start at ${Duration(microseconds: windowStart)}, head $headAtWindowStart',
      );

      final serving = await _open(
        backend,
        noteVersion: const EntryTypeVersion(1, 1),
      );
      final copyId = serving.copyIdOf(_kView);
      final windowEnd = windowStart + _kWindow.inMicroseconds;
      final converged = _waitForWatermark(
        backend,
        clock,
        copyId,
        headAtWindowStart,
        windowEnd,
      );

      final loop = _ServingLoop(serving, clock);
      await loop.startAndWait();

      final remaining = windowEnd - clock.elapsedMicroseconds;
      if (remaining > 0) {
        await Future<void>.delayed(Duration(microseconds: remaining));
      }
      await loop.stop();

      await converged;
      _checkServingBound(loop, windowStart, _kWindow, _kAppendBound);

      final rows = await backend.findViewRows(copyId);
      expect(rows, hasLength(_kAggregates));
      expect(rows.every((r) => r['b'] == 0), isTrue);
    },
    timeout: const Timeout(Duration(minutes: 15)),
  );
}
