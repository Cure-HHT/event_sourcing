// A measurement, on Sembast, alongside the Postgres measured scenarios
// (postgres_view_convergence_measured_test.dart): a single opener seeds the
// measured database -- 20,000 events of 2,000 aggregates, ten each, all
// folded into one aggregate view of 2,000 rows -- then reopens registering
// an added view over the same log, and appends beside the added view's
// catch-up. No `EVS-DEV-view-convergence/W`/`X` bound is stated for
// Sembast (the spec's measured scenarios are Postgres-only, since the
// bound depends on how Postgres orders a catch-up transaction against an
// append); this file measures, and loosely bounds, the pause a real-time
// catch-up causes a real-time appender on the backend the substrate ships
// for mobile.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' hide Transaction;

const _kType = 'pause_measured_note';
const _kPing = 'pause_measured_ping';
const _kView = 'pause_measured_notes';
const _kAddedView = 'pause_measured_notes_added';
const _kAggregates = 2000;
const _kEventsPerAggregate = 10;

AggregateProjectionSpec _viewSpec(String name) => AggregateProjectionSpec(
  viewName: name,
  interest: const SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: const <String>{},
);

Future<EventStore> _open(
  SembastBackend backend, {
  List<String> viewNames = const <String>[_kView],
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
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
  final registry = ProjectionRegistry();
  for (final name in viewNames) {
    registry.register(_viewSpec(name));
  }
  return EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'pause-measured-hop',
      identifier: 'pause-measured-install',
      softwareVersion: 'pause-measured-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

Future<void> _appendNote(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String entryType,
  String aggregateId,
) => store.appendInTxn(
  txn,
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: entryType == _kPing ? 'ping' : 'note',
  eventType: 'finalized',
  data: const <String, Object?>{'title': 't'},
  initiator: const UserInitiator('pause-measured-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// Seeds the measured database into [store]: [_kAggregates] aggregates of
/// [_kEventsPerAggregate] events each, in transactions of 1,000 events.
Future<void> _seed(EventStore store) async {
  final clock = Stopwatch()..start();
  const chunk = 100;
  for (var start = 0; start < _kAggregates; start += chunk) {
    await store.runTransaction((txn, collector) async {
      for (var i = start; i < start + chunk; i++) {
        for (var e = 0; e < _kEventsPerAggregate; e++) {
          await _appendNote(store, txn, collector, _kType, 'agg-$i');
        }
      }
    });
    // ignore: avoid_print, the measurement is the point of this file
    print('seeded through agg-${start + chunk} at ${clock.elapsed}');
  }
}

Future<ViewCopy> _copyOf(SembastBackend backend, String copyId) async {
  final all = await backend.transaction(backend.readViewCopiesInTxn);
  return all.singleWhere((c) => c.copyId == copyId);
}

void main() {
  test('appends beside a catch-up of the measured database on Sembast are '
      'held to about 1 s', () async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'view-catch-up-pause.db',
    );
    final backend = SembastBackend(database: db);
    addTearDown(backend.close);

    final first = await _open(backend);
    await _seed(first);
    await first.close();

    final headAtReopen = await backend.readSequenceCounter();
    final store = await _open(
      backend,
      viewNames: const <String>[_kView, _kAddedView],
    );
    final copyId = store.copyIdOf(_kAddedView);

    // The loop appends only [_kPing], outside the added view's interest:
    // its purpose is to measure the pause a real-time catch-up causes a
    // real-time appender, not to race the catch-up for the copy's
    // convergence target, which every append of [_kType] would move.
    final clock = Stopwatch()..start();
    final spans = <(int, int)>[];
    Object? loopError;
    var stop = false;
    var n = 0;
    final loop = () async {
      try {
        while (!stop) {
          final aggregateId = 'serving-ping-$n';
          n++;
          final callStart = clock.elapsedMicroseconds;
          await store.runTransaction(
            (txn, collector) =>
                _appendNote(store, txn, collector, _kPing, aggregateId),
          );
          spans.add((callStart, clock.elapsedMicroseconds));
        }
      } on Object catch (e) {
        loopError = e;
      }
    }();

    // Wait for the added copy to reach the log position held at reopen,
    // or give up after 60 s, the window the spec grants the measured
    // scenarios on Postgres; an in-memory backend should be no slower.
    final deadline = clock.elapsedMicroseconds + 60 * 1000 * 1000;
    while (loopError == null) {
      final copy = await _copyOf(backend, copyId);
      if (copy.watermark >= headAtReopen) break;
      if (clock.elapsedMicroseconds > deadline) {
        fail(
          'the added copy did not reach position $headAtReopen in time '
          '(last seen: ${copy.watermark})',
        );
      }
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    stop = true;
    await loop;

    expect(loopError, isNull, reason: 'every serving append succeeds');
    const bound = Duration(seconds: 1, milliseconds: 500);
    final longest = spans.isEmpty
        ? Duration.zero
        : spans
              .map((s) => Duration(microseconds: s.$2 - s.$1))
              .reduce((a, b) => a > b ? a : b);
    // ignore: avoid_print, the measurement is the point of this file
    print('serving loop: ${spans.length} appends, longest $longest');
    final overLong = <(int, int)>[
      for (final span in spans)
        if (span.$2 - span.$1 > bound.inMicroseconds) span,
    ];
    expect(
      overLong,
      isEmpty,
      reason:
          'every serving append commits within about $bound of a catch-up '
          'transaction: $overLong',
    );
  }, timeout: const Timeout(Duration(minutes: 20)));
}
