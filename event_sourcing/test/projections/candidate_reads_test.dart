// A catch-up and a converging view's currency scan ask the backend only for
// the events past a copy's watermark that its definition can fold, and the
// candidate filter they ask with admits every such event.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:event_sourcing/src/projections/view_catch_up.dart'
    show debugReleaseViewCopyLock, debugTryHoldViewCopyLock;
import 'package:event_sourcing/src/projections/view_read.dart'
    show candidateFilterOf, isMarkRefreshingEvent, isSecurityFindingEvent;
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart'
    show isCandidateEvent;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/declarations.dart';
import '../test_support/record_fixtures.dart';

const _kNote = 'candidate_note';
const _kSync = 'candidate_site_sync';
const _kView = 'candidate_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kNote}),
  tombstoneEventTypes: <String>{},
);

/// A [SembastBackend] that records, while [counting] is set, the position
/// of every event its event reads return.
class _CountingBackend extends SembastBackend {
  _CountingBackend({required super.database});

  bool counting = false;
  final List<int> positionsRead = <int>[];

  void _record(List<StoredEvent> events) {
    if (!counting) return;
    positionsRead.addAll([for (final e in events) e.sequenceNumber]);
  }

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final events = await super.findAllEventsInTxn(
      txn,
      afterSequence: afterSequence,
      limit: limit,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
    _record(events);
    return events;
  }

  @override
  Future<List<StoredEvent>> findCandidateEventsInTxn(
    Transaction txn, {
    required int afterSequence,
    required int throughSequence,
    required int limit,
    Set<String>? entryTypes,
    Set<String>? eventTypes,
    Set<String>? aggregateTypes,
  }) async {
    final events = await super.findCandidateEventsInTxn(
      txn,
      afterSequence: afterSequence,
      throughSequence: throughSequence,
      limit: limit,
      entryTypes: entryTypes,
      eventTypes: eventTypes,
      aggregateTypes: aggregateTypes,
    );
    _record(events);
    return events;
  }
}

var _dbCounter = 0;

Future<_CountingBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'candidate-reads-$_dbCounter.db',
  );
  return _CountingBackend(database: db);
}

Future<EventStore> _open(
  SembastBackend backend, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) {
  final registry = ProjectionRegistry();
  for (final spec in projections) {
    registry.register(spec);
  }
  return EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry()
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
      ),
    source: const Source(
      hopId: 'candidate-hop',
      identifier: 'candidate-install',
      softwareVersion: 'candidate-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

Future<StoredEvent> _note(EventStore store, int n) async => (await store.append(
  entryType: _kNote,
  aggregateId: 'note-${n % 7}',
  aggregateType: 'note',
  eventType: 'noted',
  data: <String, Object?>{'n': n},
  initiator: const UserInitiator('candidate-user'),
))!;

Future<StoredEvent> _sync(EventStore store, int n) async => (await store.append(
  entryType: _kSync,
  aggregateId: 'site-${n % 11}',
  aggregateType: 'site',
  eventType: 'synced',
  data: <String, Object?>{'n': n},
  initiator: const UserInitiator('candidate-user'),
))!;

Future<ViewCopy> _copyOf(SembastBackend backend) async {
  final all = await backend.transaction(backend.readViewCopiesInTxn);
  return all.singleWhere((c) => c.viewName == _kView && !c.markedForDeletion);
}

/// The positions of the events of the log that [_kSpec]'s definition
/// folds: its interest's, and the events every copy folds.
Future<List<int>> _foldedPositions(SembastBackend backend) async {
  final log = await backend.transaction(
    (txn) => backend.findAllEventsInTxn(txn),
  );
  return [
    for (final e in log)
      if (_kSpec.interest.matches(e) ||
          kEveryCopyFoldsEntryTypes.contains(e.entryType))
        e.sequenceNumber,
  ];
}

StoredEvent _event({
  required String entryType,
  required String eventType,
  required String aggregateType,
}) => StoredEvent(
  key: 1,
  eventId: 'ev-1',
  aggregateId: 'agg-1',
  aggregateType: aggregateType,
  entryType: entryType,
  entryTypeVersion: const EntryTypeVersion(1, 0),
  libFormatVersion: LibVersion.dataFormat,
  eventType: eventType,
  sequenceNumber: 1,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u1'),
  clientTimestamp: DateTime.utc(2026, 10, 31),
  eventHash: 'hash',
  causal: kRootVersionCausal,
);

void main() {
  // Verifies: EVS-DEV-view-convergence/J
  test('the candidate filter of an interest admits every event the '
      'interest matches and every event a copy folds for its marks', () {
    final reservedApp = kReservedSystemEntryTypeIds.first;
    const entryTypeSets = <Set<String>?>[
      null,
      <String>{},
      <String>{'app'},
      <String>{'app', kSecurityFindingEntryType},
    ];
    final eventTypeSets = <Set<String>?>[
      null,
      <String>{},
      <String>{'e1'},
      <String>{kSecurityFindingRecordedEventType},
    ];
    const aggregateTypeSets = <Set<String>?>[
      null,
      <String>{},
      <String>{'agg'},
    ];
    final entryTypes = <String>[
      'app',
      'other',
      reservedApp,
      kSecurityFindingEntryType,
      kDestinationSenderSucceededEntryType,
      'system.unknown_later',
    ];
    final eventTypes = <String>[
      'e1',
      'e2',
      kSecurityFindingRecordedEventType,
      kDestinationSenderSucceededEventType,
    ];
    var checked = 0;
    for (final en in entryTypeSets) {
      for (final ev in eventTypeSets) {
        for (final ag in aggregateTypeSets) {
          for (final system in [false, true]) {
            final interest = SubscriptionFilter(
              entryTypes: en,
              eventTypes: ev,
              aggregateTypes: ag,
              includeSystemEvents: system,
            );
            final filter = candidateFilterOf(interest);
            for (final entryType in entryTypes) {
              for (final eventType in eventTypes) {
                for (final aggregateType in ['agg', 'x']) {
                  final e = _event(
                    entryType: entryType,
                    eventType: eventType,
                    aggregateType: aggregateType,
                  );
                  final folds =
                      interest.matches(e) ||
                      isSecurityFindingEvent(e) ||
                      (entryType == kDestinationSenderSucceededEntryType &&
                          eventType == kDestinationSenderSucceededEventType);
                  if (!folds) continue;
                  checked++;
                  expect(
                    isCandidateEvent(
                      entryType: entryType,
                      eventType: eventType,
                      aggregateType: aggregateType,
                      entryTypes: filter.entryTypes,
                      eventTypes: filter.eventTypes,
                      aggregateTypes: filter.aggregateTypes,
                    ),
                    isTrue,
                    reason:
                        'interest (entryTypes $en, eventTypes $ev, '
                        'aggregateTypes $ag, includeSystemEvents $system) '
                        'folds $entryType/$eventType/$aggregateType',
                  );
                }
              }
            }
          }
        }
      }
    }
    expect(checked, greaterThan(0));
  });

  // Verifies: EVS-DEV-view-convergence/J
  test('a catch-up over a log of events its view does not fold reads none '
      'of them and reaches the head', () async {
    final backend = await _openBackend();
    final seeder = await _open(backend, projections: const <ProjectionSpec>[]);
    for (var n = 0; n < 600; n++) {
      if (n % 100 == 50) await _note(seeder, n);
      await _sync(seeder, n);
    }
    await seeder.close();

    late EventStore store;
    await runWithDeliveryTestHooks(
      // A frozen clock: the step bound never stops the catch-up, so one
      // transaction folds the whole log and no candidate is read twice.
      DeliveryTestHooks(catchUpClock: () => DateTime.utc(2026, 10, 31)),
      () async {
        store = await _open(backend);
        backend.counting = true;
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));
        backend.counting = false;
      },
    );
    final head = await backend.transaction(backend.readLogHeadInTxn);
    expect((await _copyOf(backend)).watermark, head);
    final folded = await _foldedPositions(backend);
    expect(folded, hasLength(6));
    expect(
      backend.positionsRead,
      folded,
      reason: 'the events the backend returned are exactly those folded',
    );
    await store.close();
  });

  // Verifies: EVS-DEV-view-convergence/J
  test('a catch-up stopped by the step bound leaves the watermark at the '
      'last folded candidate', () async {
    final backend = await _openBackend();
    final seeder = await _open(backend, projections: const <ProjectionSpec>[]);
    for (var n = 0; n < 10; n++) {
      await _sync(seeder, n);
    }
    final first = await _note(seeder, 1);
    for (var n = 10; n < 20; n++) {
      await _sync(seeder, n);
    }
    final second = await _note(seeder, 2);
    for (var n = 20; n < 30; n++) {
      await _sync(seeder, n);
    }
    await seeder.close();

    var now = DateTime.utc(2026, 10, 31);
    late String copyId;
    late EventStore store;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        catchUpClock: () => now,
        onCatchUpStep: (stepCopyId, eventId) {
          if (stepCopyId != copyId) return;
          // The second candidate's transaction fails before it writes,
          // holding the copy at what the first transaction committed.
          if (eventId == second.eventId) {
            throw const InjectedFailure('holds the copy for the test');
          }
          now = now.add(const Duration(milliseconds: 201));
        },
      ),
      () async {
        store = await _open(backend);
        copyId = store.copyIdOf(_kView);
        for (var i = 0; i < 500; i++) {
          if (store.catchUpProgressOf(copyId)?.lastFailure != null) break;
          await pumpEventQueue();
        }
        expect(store.catchUpProgressOf(copyId)?.lastFailure, isNotNull);
        expect(
          (await _copyOf(backend)).watermark,
          first.sequenceNumber,
          reason:
              'the bound stopped the first transaction after it folded the '
              'first candidate, with the second left in its window',
        );
        await store.close();
      },
    );
  });

  // Verifies: EVS-DEV-view-convergence/J
  test('a held security finding makes the catch-up read every event', () async {
    final backend = await _openBackend();
    final seeder = await _open(backend, projections: const <ProjectionSpec>[]);
    for (var n = 0; n < 40; n++) {
      if (n % 20 == 5) await _note(seeder, n);
      await _sync(seeder, n);
    }
    await seeder.runTransaction(
      (txn, collector) => recordFindingInTxnForTest(
        seeder,
        txn,
        collector,
        role: FindingRole.walk,
        kind: FindingKind.hashMismatch,
        evidence: const <String, Object?>{
          'event_id': 'candidate-tampered-event',
          'carried_hash': 'candidate-carried-hash',
          'recomputed_hash': 'candidate-recomputed-hash',
        },
        aggregates: const <String>['site-1'],
      ),
    );
    for (var n = 40; n < 60; n++) {
      await _sync(seeder, n);
    }
    await seeder.close();

    late EventStore store;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(catchUpClock: () => DateTime.utc(2026, 10, 31)),
      () async {
        store = await _open(backend);
        backend.counting = true;
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));
        backend.counting = false;
      },
    );
    final head = await backend.transaction(backend.readLogHeadInTxn);
    expect((await _copyOf(backend)).watermark, head);
    expect(
      backend.positionsRead.toSet(),
      containsAll(<int>[for (var p = 1; p <= head; p++) p]),
      reason: 'while a finding is held, every event is a candidate',
    );
    await store.close();
  });

  // Verifies: EVS-DEV-converging-view-reads/B
  test('scanViewCurrency over a filtered view reads only candidates', () async {
    final backend = await _openBackend();
    final seeder = await _open(backend, projections: const <ProjectionSpec>[]);
    for (var n = 0; n < 300; n++) {
      if (n % 100 == 10) await _note(seeder, n);
      await _sync(seeder, n);
    }
    await seeder.close();

    late String copyId;
    late EventStore store;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onCatchUpStep: (stepCopyId, eventId) {
          if (stepCopyId == copyId) {
            throw const InjectedFailure('holds the copy converging');
          }
        },
      ),
      () async {
        store = await _open(backend);
        copyId = store.copyIdOf(_kView);
        for (var i = 0; i < 500; i++) {
          if (store.catchUpProgressOf(copyId)?.lastFailure != null) break;
          await pumpEventQueue();
        }
        // The catch-up now backs off for a second, so the read below is
        // the only one running.
        backend.counting = true;
        final read = await store.reader.findViewRows(_kView);
        backend.counting = false;
        expect(read.state, ViewConvergenceState.converging);
        expect(read.rows, isEmpty);
        expect(
          backend.positionsRead,
          await _foldedPositions(backend),
          reason: "the scan read only the candidates past the copy's watermark",
        );
        await store.close();
      },
    );
  });

  // Verifies: EVS-PRD-materializer/G
  test('the mark-refreshing events are candidates of every filter', () async {
    final finding = _event(
      entryType: kSecurityFindingEntryType,
      eventType: kSecurityFindingRecordedEventType,
      aggregateType: kSecurityFindingAggregateType,
    );
    final filter = candidateFilterOf(
      const SubscriptionFilter(entryTypes: <String>{}, eventTypes: <String>{}),
    );
    final backend = await _openBackend();
    final refreshing = await backend.transaction(
      (txn) => isMarkRefreshingEvent(txn, backend, finding),
    );
    expect(refreshing, isTrue);
    expect(
      isCandidateEvent(
        entryType: finding.entryType,
        eventType: finding.eventType,
        aggregateType: finding.aggregateType,
        entryTypes: filter.entryTypes,
        eventTypes: filter.eventTypes,
        aggregateTypes: filter.aggregateTypes,
      ),
      isTrue,
    );
  });

  group("an append's currency check", () {
    /// A store registering [_kSpec] whose copy's watermark is set back to
    /// its first note, leaving what [seedTail] appended past it, and whose
    /// catch-up finds the copy's lock held, so only the store's own
    /// appends move the watermark.
    Future<
      ({
        _CountingBackend backend,
        EventStore store,
        String copyId,
        Object lockHandle,
        int watermark,
      })
    >
    behindCopy(Future<void> Function(EventStore store) seedTail) async {
      final backend = await _openBackend();
      final store = await _open(backend);
      final first = await _note(store, 0);
      await seedTail(store);
      await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));
      final copyId = store.copyIdOf(_kView);
      final lockHandle = backend.drainExclusionKey(store.databaseId);
      var held = false;
      for (var i = 0; i < 500 && !held; i++) {
        held = debugTryHoldViewCopyLock(lockHandle, copyId);
        if (!held) await pumpEventQueue();
      }
      expect(held, isTrue);
      await backend.transaction(
        (txn) => backend.setViewCopyWatermarkInTxn(
          txn,
          copyId,
          first.sequenceNumber,
        ),
      );
      return (
        backend: backend,
        store: store,
        copyId: copyId,
        lockHandle: lockHandle,
        watermark: first.sequenceNumber,
      );
    }

    Future<Map<String, dynamic>?> row(
      SembastBackend backend,
      String copyId,
      String key,
    ) => backend.transaction(
      (txn) => backend.readViewRowInTxn(txn, copyId, key),
    );

    // Verifies: EVS-DEV-view-convergence/E
    test('a copy whose tail past its watermark holds no candidate is current: '
        'the append folds inline, reading none of the tail', () async {
      final b = await behindCopy((store) async {
        for (var n = 0; n < 50; n++) {
          await _sync(store, n);
        }
      });
      expect(
        await b.backend.transaction(b.backend.readLogHeadInTxn),
        b.watermark + 50,
        reason: 'the copy is behind by the tail',
      );
      b.backend.counting = true;
      final appended = await _note(b.store, 1);
      b.backend.counting = false;

      expect((await _copyOf(b.backend)).watermark, appended.sequenceNumber);
      expect(await row(b.backend, b.copyId, appended.aggregateId), isNotNull);
      expect(
        b.backend.positionsRead,
        isEmpty,
        reason: 'the currency check read only candidates, and there were none',
      );
      debugReleaseViewCopyLock(b.lockHandle, b.copyId);
      await b.store.close();
    });

    // Verifies: EVS-DEV-view-convergence/F
    test('a copy with a candidate past its watermark is not current: the '
        'append leaves it unchanged', () async {
      late StoredEvent peerNote;
      final b = await behindCopy((store) async {
        for (var n = 0; n < 20; n++) {
          await _sync(store, n);
        }
        peerNote = await _note(store, 3);
        for (var n = 20; n < 40; n++) {
          await _sync(store, n);
        }
      });
      b.backend.counting = true;
      final appended = await _note(b.store, 5);
      b.backend.counting = false;

      expect((await _copyOf(b.backend)).watermark, b.watermark);
      expect(await row(b.backend, b.copyId, appended.aggregateId), isNull);
      expect(
        b.backend.positionsRead,
        [peerNote.sequenceNumber],
        reason: 'the check stopped at the first candidate it read',
      );
      debugReleaseViewCopyLock(b.lockHandle, b.copyId);
      await b.store.close();
    });
  });
}
