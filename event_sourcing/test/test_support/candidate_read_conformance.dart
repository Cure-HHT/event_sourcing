// A backend-agnostic check of the candidate read a catch-up and a currency
// scan make past a copy's watermark: the events of a position range that
// match every filter given, and every event of the entry types every copy
// folds, in position order, up to a limit; and of the log-head read beside
// it.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show PublishCollector, recordFindingInTxnForTest;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:test/test.dart';

import 'declarations.dart';

const _kA = 'candidate_a';
const _kB = 'candidate_b';

EntryTypeRegistry candidateReadEntryTypes() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: _kA,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kA,
      declarations: <EventTypeDeclaration>[
        EligibleVersion('a1'),
        EligibleVersion('a2'),
      ],
    ),
  )
  ..register(
    const EntryTypeDefinition(
      id: _kB,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kB,
      declarations: <EventTypeDeclaration>[EligibleVersion('b1')],
    ),
  );

Future<void> _append(
  EventStore store,
  Transaction txn,
  PublishCollector collector, {
  required String entryType,
  required String eventType,
  required String aggregateType,
  required int n,
}) => store.appendInTxn(
  txn,
  entryType: entryType,
  aggregateId: '$aggregateType-$n',
  aggregateType: aggregateType,
  eventType: eventType,
  data: <String, Object?>{'n': n},
  initiator: const UserInitiator('candidate-user'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// The reference the backends are held to, written out here rather than
/// taken from the library.
bool _expected(
  StoredEvent e, {
  Set<String>? entryTypes,
  Set<String>? eventTypes,
  Set<String>? aggregateTypes,
}) {
  if (e.entryType == 'system.security_finding' ||
      e.entryType == 'system.destination_sender_succeeded') {
    return true;
  }
  if (entryTypes != null && !entryTypes.contains(e.entryType)) return false;
  if (eventTypes != null && !eventTypes.contains(e.eventType)) return false;
  if (aggregateTypes != null && !aggregateTypes.contains(e.aggregateType)) {
    return false;
  }
  return true;
}

/// Registers the candidate-read conformance tests: [openBackend] supplies
/// a fresh, empty backend, and [openStore] opens a store over it that
/// registers [candidateReadEntryTypes] and no view.
void runCandidateReadConformance({
  required Future<StorageBackend> Function() openBackend,
  required Future<EventStore> Function(StorageBackend backend) openStore,
}) {
  late StorageBackend backend;
  late List<StoredEvent> log;

  setUp(() async {
    backend = await openBackend();
    final store = await openStore(backend);
    await store.runTransaction((txn, collector) async {
      for (var n = 0; n < 60; n++) {
        if (n == 30) {
          await recordFindingInTxnForTest(
            store,
            txn,
            collector,
            role: FindingRole.walk,
            kind: FindingKind.hashMismatch,
            evidence: const <String, Object?>{
              'event_id': 'candidate-tampered-event',
              'carried_hash': 'candidate-carried-hash',
              'recomputed_hash': 'candidate-recomputed-hash',
            },
            aggregates: const <String>['ta-1'],
          );
        }
        final (entryType, eventType) = switch (n % 3) {
          0 => (_kA, 'a1'),
          1 => (_kA, 'a2'),
          _ => (_kB, 'b1'),
        };
        await _append(
          store,
          txn,
          collector,
          entryType: entryType,
          eventType: eventType,
          aggregateType: n.isEven ? 'ta' : 'tb',
          n: n,
        );
      }
    });
    await store.close();
    log = await backend.transaction((txn) => backend.findAllEventsInTxn(txn));
  });

  Future<List<int>> read({
    int after = 0,
    int? through,
    int limit = 1000,
    Set<String>? entryTypes,
    Set<String>? eventTypes,
    Set<String>? aggregateTypes,
  }) async {
    final events = await backend.transaction(
      (txn) => backend.findCandidateEventsInTxn(
        txn,
        afterSequence: after,
        throughSequence: through ?? log.last.sequenceNumber,
        limit: limit,
        entryTypes: entryTypes,
        eventTypes: eventTypes,
        aggregateTypes: aggregateTypes,
      ),
    );
    return [for (final e in events) e.sequenceNumber];
  }

  List<int> expected({
    int after = 0,
    int? through,
    Set<String>? entryTypes,
    Set<String>? eventTypes,
    Set<String>? aggregateTypes,
  }) => [
    for (final e in log)
      if (e.sequenceNumber > after &&
          e.sequenceNumber <= (through ?? log.last.sequenceNumber) &&
          _expected(
            e,
            entryTypes: entryTypes,
            eventTypes: eventTypes,
            aggregateTypes: aggregateTypes,
          ))
        e.sequenceNumber,
  ];

  test('with no filter, the read returns every event of the range in '
      'position order, the lower bound exclusive and the upper inclusive', () {
    final after = log[5].sequenceNumber;
    final through = log[40].sequenceNumber;
    return expectLater(
      read(after: after, through: through),
      completion([for (final e in log.sublist(6, 41)) e.sequenceNumber]),
    );
  });

  test('each filter given is a conjunct, and the events every copy folds '
      'are returned whatever the filters', () async {
    expect(
      log.where((e) => e.entryType == 'system.security_finding'),
      hasLength(1),
      reason: 'the log holds the finding the read must return',
    );
    for (final filter
        in <({Set<String>? en, Set<String>? ev, Set<String>? ag})>[
          (en: {_kA}, ev: null, ag: null),
          (en: null, ev: {'a2', 'b1'}, ag: null),
          (en: null, ev: null, ag: {'tb'}),
          (en: {_kA}, ev: {'a1'}, ag: {'ta'}),
          (en: {_kB}, ev: {'a1'}, ag: null),
          (en: <String>{}, ev: null, ag: null),
          (en: null, ev: <String>{}, ag: <String>{}),
        ]) {
      expect(
        await read(
          entryTypes: filter.en,
          eventTypes: filter.ev,
          aggregateTypes: filter.ag,
        ),
        expected(
          entryTypes: filter.en,
          eventTypes: filter.ev,
          aggregateTypes: filter.ag,
        ),
        reason: 'filter $filter',
      );
    }
  });

  test('the read returns at most the limit, the first candidates of the '
      'range', () async {
    final all = expected(entryTypes: {_kB});
    expect(all.length, greaterThan(5));
    expect(await read(entryTypes: {_kB}, limit: 5), all.sublist(0, 5));
  });

  test('an empty range returns nothing', () async {
    final at = log[10].sequenceNumber;
    expect(await read(after: at, through: at), isEmpty);
  });

  test("the log head is the latest event's position", () async {
    expect(
      await backend.transaction(backend.readLogHeadInTxn),
      log.last.sequenceNumber,
    );
  });
}

const _kNote = 'window_note';
const _kSync = 'window_sync';
const _kWindowView = 'window_notes';

const _kWindowSpec = AggregateProjectionSpec(
  viewName: _kWindowView,
  interest: SubscriptionFilter(entryTypes: <String>{_kNote}),
  tombstoneEventTypes: <String>{},
);

EntryTypeRegistry _windowEntryTypes() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: _kNote,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kNote,
      declarations: <EventTypeDeclaration>[EligibleVersion('noted')],
    ),
  )
  ..register(
    const EntryTypeDefinition(
      id: _kSync,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kSync,
      declarations: <EventTypeDeclaration>[EligibleVersion('synced')],
    ),
  );

/// Opens a store over a backend with [entryTypes] and [projections].
typedef WindowStoreOpener =
    Future<EventStore> Function(
      StorageBackend backend, {
      required EntryTypeRegistry entryTypes,
      required List<ProjectionSpec> projections,
    });

/// Registers the window-boundary test: a catch-up whose candidates sit at
/// the last position of one candidate window and the first of the next
/// folds both, reaches the log's head and records the copy at it.
/// [openBackend] supplies a fresh, empty backend.
void runCandidateWindowBoundaryConformance({
  required Future<StorageBackend> Function() openBackend,
  required WindowStoreOpener openStore,
}) {
  // Verifies: EVS-DEV-view-convergence/J
  test('a catch-up folds candidates on both sides of a window boundary and '
      'reaches the head', () async {
    final backend = await openBackend();
    final seeder = await openStore(
      backend,
      entryTypes: _windowEntryTypes(),
      projections: const <ProjectionSpec>[],
    );
    Future<int> head() => backend.transaction(backend.readLogHeadInTxn);
    Future<StoredEvent> append(String entryType, String id) async =>
        (await seeder.append(
          entryType: entryType,
          aggregateId: id,
          aggregateType: entryType,
          eventType: entryType == _kNote ? 'noted' : 'synced',
          data: <String, Object?>{'id': id},
          initiator: const UserInitiator('window-user'),
        ))!;

    // The window is sized past the seeder's own boot events, so the first
    // window's last position is free for a note.
    final window = await head() + 3;
    var n = 0;
    while (await head() < window - 1) {
      await append(_kSync, 'site-${n++}');
    }
    final last = await append(_kNote, 'note-last');
    final first = await append(_kNote, 'note-first');
    expect(last.sequenceNumber, window);
    expect(first.sequenceNumber, window + 1);
    while (await head() < 2 * window + 5) {
      await append(_kSync, 'site-${n++}');
    }
    await seeder.close();

    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        candidateWindow: window,
        // Frozen: the step bound never ends the transaction, so the windows
        // are the ones counted from position 0.
        catchUpClock: () => DateTime.utc(2026, 10, 31),
      ),
      () async {
        final store = await openStore(
          backend,
          entryTypes: _windowEntryTypes(),
          projections: const <ProjectionSpec>[_kWindowSpec],
        );
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));
        final progress = (await store.reader.viewProgress()).singleWhere(
          (status) => status.viewName == _kWindowView,
        );
        expect(progress.state, ViewConvergenceState.current);
        expect(progress.watermark, await head());
        final rows = await store.reader.readViewRowsByKeys(_kWindowView, {
          'note-last',
          'note-first',
        });
        expect(rows.state, ViewConvergenceState.current);
        expect(rows.rows['note-last'], isA<SettledRow>());
        expect(rows.rows['note-first'], isA<SettledRow>());
        await store.close();
      },
    );
  });
}
