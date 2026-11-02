// The Sembast measured scenario of EVS-DEV-view-convergence: on the
// measured Sembast database, an in-memory Sembast database whose log
// already holds the events of the view's aggregates, the instance running
// the serving loop is itself one that opened registering a newer minor of
// the view's entry type, so its own copy of the view converges while it
// appends. The window is the 5 seconds from the moment that instance
// begins to open. A Sembast transaction runs alone on its database, so
// each catch-up transaction holds every serving append back for as long
// as it runs.

@TestOn('vm')
@Tags(['timing'])
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import '../test_support/declarations.dart';

const _kType = 'measured_note';
const _kPing = 'measured_ping';
const _kView = 'measured_notes';
const _kAggregates = 500;
const _kEventsPerAggregate = 10;
const _kWindow = Duration(seconds: 5);
const _kAppendBound = Duration(seconds: 1);

const _kViewSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
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
        declarations: <EventTypeDeclaration>[
          const EligibleVersion('finalized'),
        ],
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kPing,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kPing,
        declarations: <EventTypeDeclaration>[EligibleVersion('finalized')],
      ),
    );
}

PromoterRegistry _promoters(EntryTypeVersion noteVersion) {
  final promoters = PromoterRegistry();
  if (noteVersion == const EntryTypeVersion(1, 1)) {
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
  return promoters;
}

Future<EventStore> _open(
  SembastBackend backend, {
  EntryTypeVersion noteVersion = const EntryTypeVersion(1, 0),
}) => EventStore.open(
  storage: ApplicationSuppliedStorage(
    backend,
    SembastSecurityContextStore(backend: backend),
  ),
  entryTypes: _entryTypes(noteVersion),
  source: const Source(
    hopId: 'measured-hop',
    identifier: 'measured-install',
    softwareVersion: 'measured-test',
  ),
  projections: ProjectionRegistry()..register(_kViewSpec),
  promoters: _promoters(noteVersion),
);

Future<StoredEvent> _append(
  EventStore store,
  String entryType,
  String aggregateId,
  Map<String, Object?> data,
) async => (await store.append(
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: entryType == _kPing ? 'ping' : 'note',
  eventType: 'finalized',
  data: data,
  initiator: const UserInitiator('measured-user'),
))!;

/// Seeds [store] with the measured Sembast database: [_kAggregates]
/// aggregates of [_kEventsPerAggregate] events each, all of [_kType], in
/// transactions of one round over every aggregate. Returns the last event.
Future<StoredEvent> _seed(EventStore store) async {
  final clock = Stopwatch()..start();
  late StoredEvent last;
  for (var e = 0; e < _kEventsPerAggregate; e++) {
    await store.runTransaction((txn, collector) async {
      for (var i = 0; i < _kAggregates; i++) {
        last = (await store.appendInTxn(
          txn,
          entryType: _kType,
          aggregateId: 'agg-$i',
          aggregateType: 'note',
          eventType: 'finalized',
          data: {'title': 'n$i.$e'},
          initiator: const UserInitiator('measured-user'),
          flowToken: null,
          metadata: null,
          security: null,
          checkpointReason: null,
          changeReason: null,
          dedupeByContent: false,
          collector: collector,
        ))!;
      }
    });
  }
  // ignore: avoid_print, the measurement is the point of this file
  print(
    'seeded ${_kAggregates * _kEventsPerAggregate} events in '
    '${clock.elapsed}',
  );
  return last;
}

/// One append the serving loop made: when its call started and when it
/// committed, in microseconds of the scenario's clock.
typedef _Span = (int start, int end);

/// A serving loop appending [_kPing] and [_kType] events, alternating,
/// against [store]'s aggregates until [stop] is called.
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

  Future<void> _run() async {
    try {
      while (!_stop) {
        final n = _n++;
        final isPing = n.isEven;
        final callStart = clock.elapsedMicroseconds;
        await _append(
          store,
          isPing ? _kPing : _kType,
          isPing ? 'serving-ping-$n' : 'agg-${n % _kAggregates}',
          {'n': n},
        );
        spans.add((callStart, clock.elapsedMicroseconds));
        // Yields to the event loop between appends, as a serving instance
        // answering requests does, so the catch-up driver's timers run.
        await Future<void>.delayed(Duration.zero);
      }
    } on Object catch (e) {
      error = e;
    }
  }

  Future<void> stop() async {
    _stop = true;
    await _future;
  }
}

void main() {
  // Verifies: EVS-DEV-view-convergence/W
  // Verifies: EVS-DEV-view-convergence/X
  test('the serving instance opens with a newer minor over the measured '
      'Sembast database: its own copy converges over many catch-up '
      'transactions while every serving append commits within 1 s', () async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'measured-convergence.db',
    );
    final backend = SembastBackend(database: db);
    final seeder = await _open(backend);
    final head = (await _seed(seeder)).sequenceNumber;
    await seeder.close();

    final clock = Stopwatch()..start();
    final windowStart = clock.elapsedMicroseconds;
    var catchUpTransactions = 0;
    final serving = await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onCatchUpTransactionBegin: (_) => catchUpTransactions++,
      ),
      () => _open(backend, noteVersion: const EntryTypeVersion(1, 1)),
    );
    final copyId = serving.copyIdOf(_kView);
    final loop = _ServingLoop(serving, clock)..start();

    final windowEnd = windowStart + _kWindow.inMicroseconds;
    int? convergedAt;
    while (clock.elapsedMicroseconds < windowEnd) {
      final copies = await backend.transaction(backend.readViewCopiesInTxn);
      final copy = copies.where((c) => c.copyId == copyId).single;
      if (convergedAt == null && copy.watermark >= head) {
        convergedAt = clock.elapsedMicroseconds;
      }
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    await loop.stop();

    expect(loop.error, isNull, reason: 'every serving append succeeds');
    final overLong = <_Span>[
      for (final span in loop.spans)
        if (span.$1 < windowEnd &&
            span.$2 - span.$1 > _kAppendBound.inMicroseconds)
          span,
    ];
    final longest = loop.spans
        .map((s) => Duration(microseconds: s.$2 - s.$1))
        .fold(Duration.zero, (a, b) => a > b ? a : b);
    // ignore: avoid_print, the measurement is the point of this file
    print(
      'serving loop: ${loop.spans.length} appends in the window, longest '
      '$longest; $catchUpTransactions catch-up transactions; '
      'copy reached $head at '
      '${convergedAt == null ? 'never' : Duration(microseconds: convergedAt)}',
    );
    expect(
      overLong,
      isEmpty,
      reason:
          'every serving append starting inside the window commits '
          'within $_kAppendBound: $overLong',
    );
    expect(
      catchUpTransactions,
      greaterThan(5),
      reason: 'the copy converges over many catch-up transactions',
    );
    expect(
      convergedAt,
      isNotNull,
      reason:
          'the converging copy reaches the head the log held when the '
          'window began, after many catch-up transactions',
    );
    final rows = (await serving.reader.findViewRows(_kView)).rows;
    expect(rows, hasLength(_kAggregates));
    expect(rows.every((r) => r['b'] == 0), isTrue);
    await serving.close();
    await db.close();
  }, timeout: const Timeout(Duration(minutes: 2)));
}
