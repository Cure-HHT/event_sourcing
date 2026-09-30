import 'dart:async';

import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/projections/view_read.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_description.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Note {
  _Note({required this.entryId, required this.answers});
  factory _Note.fromMap(Map<String, Object?> m) => _Note(
    entryId: m['latestEventId'] as String? ?? '_',
    answers: (m['answers'] as Map?)?.cast<String, Object?>() ?? {},
  );
  final String entryId;
  final Map<String, Object?> answers;
}

Future<EventStore> _open() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'am-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Epistaxis Event',
      ),
    );
  final projections = ProjectionRegistry()
    ..register(
      const AggregateProjectionSpec(
        viewName: 'diary_entries',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        tombstoneEventTypes: {'tombstone'},
      ),
    );
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-instance',
      softwareVersion: '0.0.0-test',
    ),
    projections: projections,
    promoters: PromoterRegistry(),
  );
}

Future<StoredEvent?> _append(
  EventStore store,
  String aggId,
  String type, [
  Map<String, Object?>? data,
]) => store.append(
  entryType: 'epistaxis_event',
  aggregateId: aggId,
  aggregateType: 'note',
  eventType: type,
  data: data ?? const <String, Object?>{},
  initiator: const UserInitiator('u'),
);

/// Opens a store the same way [_open] does, but hands back its backend too,
/// so a test can rewind a view copy's watermark by hand to force it
/// converging (EVS-DEV-view-convergence), without a second build racing
/// the test's own reads.
Future<(EventStore, SembastBackend)> _openWithBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'am-converging-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Epistaxis Event',
      ),
    );
  final projections = ProjectionRegistry()
    ..register(
      const AggregateProjectionSpec(
        viewName: 'diary_entries',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        tombstoneEventTypes: {'tombstone'},
      ),
    );
  final store = await EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-instance',
      softwareVersion: '0.0.0-test',
    ),
    projections: projections,
    promoters: PromoterRegistry(),
  );
  return (store, backend);
}

Future<void> _rewindWatermark(
  SembastBackend backend,
  String copyId,
  int watermark,
) => backend.transaction(
  (txn) => backend.setViewCopyWatermarkInTxn(txn, copyId, watermark),
);

void main() {
  // Verifies: EVS-PRD-subscription/A
  // (null-value Snapshot delivered for an
  //   aggregate that does not yet exist)
  test(
    'snapshot for not-yet-existing aggregate emits null-value Snapshot',
    () async {
      final store = await _open();
      final updates = <Update<_Note?>>[];
      final sub = store
          .subscribe(
            const SubscriptionFilter(),
            AggregateMode<_Note?>(
              viewName: 'diary_entries',
              mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
              aggregates: const {'never-created'},
            ),
          )
          .listen(updates.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      // 1 Snapshot + 1 EndOfReplay marker (post-snapshot, pre-deltas).
      expect(updates.length, 2);
      expect(updates.first, isA<Snapshot<_Note?>>());
      expect((updates.first as Snapshot).value, isNull);
      expect(updates[1], isA<EndOfReplay<_Note?>>());
      await sub.cancel();
      await store.close();
    },
  );

  // Verifies: EVS-PRD-subscription/A (Snapshot carries current state for an
  //   existing aggregate; a subsequent append emits a Delta)
  // Verifies: EVS-PRD-subscription/B (Delta arrives reactively after append)
  // Verifies: EVS-PRD-subscription/D
  // (a matching append after subscribe
  //   produces a Delta)
  test(
    'snapshot for existing aggregate carries current state; subsequent appends emit Delta',
    () async {
      final store = await _open();
      await _append(store, 'e1', 'finalized', {
        'answers': {'q1': 'yes'},
      });

      final updates = <Update<_Note?>>[];
      final sub = store
          .subscribe(
            const SubscriptionFilter(),
            AggregateMode<_Note?>(
              viewName: 'diary_entries',
              mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
              aggregates: const {'e1'},
            ),
          )
          .listen(updates.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));

      // Snapshot delivered
      expect(updates.first, isA<Snapshot<_Note?>>());
      expect((updates.first as Snapshot).value, isNotNull);

      await _append(store, 'e1', 'checkpoint', {
        'answers': {'q2': 'no'},
      });
      await Future<void>.delayed(const Duration(milliseconds: 50));

      expect(updates.any((u) => u is Delta<_Note?>), isTrue);
      final delta = updates.whereType<Delta<_Note?>>().last;
      expect(delta.value!.answers, {'q1': 'yes', 'q2': 'no'});
      await sub.cancel();
      await store.close();
    },
  );

  // Verifies: EVS-PRD-subscription/C
  // (snapshot sequence reflects the max
  //   folded event sequence)
  test('snapshot sequence reflects latest folded event sequence', () async {
    final store = await _open();
    await _append(store, 'e1', 'finalized', {
      'answers': {'q1': 'yes'},
    });
    final stored2 = await store.append(
      entryType: 'epistaxis_event',
      aggregateId: 'e1',
      aggregateType: 'note',
      eventType: 'checkpoint',
      data: {
        'answers': {'q2': 'no'},
      },
      initiator: const UserInitiator('u'),
    );

    final updates = <Update<_Note?>>[];
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
            aggregates: const {'e1'},
          ),
        )
        .listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.first, isA<Snapshot<_Note?>>());
    expect((updates.first as Snapshot).sequence, stored2!.sequenceNumber);
    await sub.cancel();
    await store.close();
  });

  // Verifies: EVS-PRD-subscription/A (Tombstone delivered on deletion)
  // Verifies: EVS-PRD-subscription/B
  // (Tombstone arrives reactively after a
  //   tombstone event)
  test('tombstone produces Tombstone update for active subscribers', () async {
    final store = await _open();
    await _append(store, 'e1', 'finalized', {
      'answers': {'q1': 'yes'},
    });

    final updates = <Update<_Note?>>[];
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
            aggregates: const {'e1'},
          ),
        )
        .listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    await _append(store, 'e1', 'tombstone');
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.any((u) => u is Tombstone<_Note?>), isTrue);
    await sub.cancel();
    await store.close();
  });

  // Verifies: EVS-DEV-converging-view-reads/E
  // (a converging view's initial replay
  //   delivers the settled aggregate as a Snapshot, the unsettled one as
  //   Pending, and ends with EndOfReplay carrying the converging state)
  // Verifies: EVS-DEV-converging-view-reads/G
  // (once the copy catches up, the
  //   subscription redelivers the formerly-pending aggregate as a Snapshot
  //   before it reports the view current via a second EndOfReplay)
  // Verifies: EVS-PRD-subscription/A
  // Verifies: EVS-PRD-subscription/E
  test('a converging view yields settled Snapshot, Pending for the unsettled '
      'key, and EndOfReplay(converging); catch-up redelivers the pending row '
      'before the current marker', () async {
    var paused = true;
    Future<void> hook(String copyId, String eventId) async {
      if (paused) {
        throw const InjectedFailure('paused for aggregate_mode_test');
      }
    }

    await runWithDeliveryTestHooks(
      DeliveryTestHooks(onCatchUpStep: hook),
      () async {
        final (store, backend) = await _openWithBackend();
        final settled = (await _append(store, 'e1', 'finalized', {
          'answers': {'q1': 'yes'},
        }))!;
        await _append(store, 'e2', 'finalized', {
          'answers': {'q1': 'no'},
        });
        final copyId = store.copyIdOf('diary_entries');
        await _rewindWatermark(backend, copyId, settled.sequenceNumber);

        final updates = <Update<_Note?>>[];
        final firstMarker = Completer<EndOfReplay<_Note?>>();
        final secondMarker = Completer<EndOfReplay<_Note?>>();
        final sub = store
            .subscribe(
              const SubscriptionFilter(),
              AggregateMode<_Note?>(
                viewName: 'diary_entries',
                mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
                aggregates: const {'e1', 'e2'},
              ),
            )
            .listen((u) {
              updates.add(u);
              if (u is EndOfReplay<_Note?>) {
                if (!firstMarker.isCompleted) {
                  firstMarker.complete(u);
                } else if (!secondMarker.isCompleted) {
                  secondMarker.complete(u);
                }
              }
            });

        final marker1 = await firstMarker.future.timeout(
          const Duration(seconds: 5),
        );
        expect(marker1.state, ViewConvergenceState.converging);
        // Exactly the initial replay: one Snapshot (e1), one Pending
        // (e2), then the converging EndOfReplay.
        expect(updates.length, 3);
        final e1Snapshot = updates.whereType<Snapshot<_Note?>>().single;
        expect(e1Snapshot.value, isNotNull);
        final pending = updates.whereType<Pending<_Note?>>().single;
        expect(pending.aggregateId, 'e2');

        // Release the driver: its next catch-up transaction folds e2's
        // event and reaches the log's tip.
        paused = false;

        final marker2 = await secondMarker.future.timeout(
          const Duration(seconds: 10),
        );
        expect(marker2.state, ViewConvergenceState.current);

        // Ordering: the redelivered row for e2 (as a Snapshot, replacing
        // its earlier Pending) precedes the second EndOfReplay.
        final secondMarkerIndex = updates.indexOf(marker2);
        final redeliveredIndex = updates.indexWhere(
          (u) => u is Snapshot<_Note?> && u.value?.answers['q1'] == 'no',
        );
        expect(redeliveredIndex, greaterThanOrEqualTo(0));
        expect(redeliveredIndex, lessThan(secondMarkerIndex));

        await sub.cancel();
        await store.close();
      },
    );
  });

  // Verifies: EVS-PRD-subscription/E
  // (a catch-up transaction that folds
  //   one event and then throws rolls back entirely -- nothing it wrote is
  //   published -- so the subscription sees no redelivery and no current
  //   marker until a later, uninterrupted attempt commits)
  test('a catch-up transaction that rolls back publishes nothing: the '
      'subscription redelivers and reports current only once a later attempt '
      'commits', () async {
    var paused = true;
    var stepInTxn = 0;
    void onBegin(String copyId) => stepInTxn = 0;
    Future<void> onStep(String copyId, String eventId) async {
      stepInTxn++;
      // Lets the first event (e2) fold in-memory within the transaction,
      // then throws before the second (e3) folds: the whole transaction
      // -- e2's fold included -- rolls back, so nothing this attempt did
      // reaches storage or a publish.
      if (paused && stepInTxn >= 2) {
        throw const InjectedFailure('rollback for subscription/E test');
      }
    }

    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onCatchUpTransactionBegin: onBegin,
        onCatchUpStep: onStep,
      ),
      () async {
        final (store, backend) = await _openWithBackend();
        final settled = (await _append(store, 'e1', 'finalized', {
          'answers': {'q1': 'yes'},
        }))!;
        await _append(store, 'e2', 'finalized', {
          'answers': {'q1': 'no'},
        });
        await _append(store, 'e3', 'finalized', {
          'answers': {'q1': 'maybe'},
        });
        final copyId = store.copyIdOf('diary_entries');
        await _rewindWatermark(backend, copyId, settled.sequenceNumber);

        final updates = <Update<_Note?>>[];
        final firstMarker = Completer<EndOfReplay<_Note?>>();
        final secondMarker = Completer<EndOfReplay<_Note?>>();
        final sub = store
            .subscribe(
              const SubscriptionFilter(),
              AggregateMode<_Note?>(
                viewName: 'diary_entries',
                mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
                aggregates: const {'e1', 'e2', 'e3'},
              ),
            )
            .listen((u) {
              updates.add(u);
              if (u is EndOfReplay<_Note?>) {
                if (!firstMarker.isCompleted) {
                  firstMarker.complete(u);
                } else if (!secondMarker.isCompleted) {
                  secondMarker.complete(u);
                }
              }
            });

        await firstMarker.future.timeout(const Duration(seconds: 5));
        final afterReplayCount = updates.length;

        // Wait for the driver to have actually attempted and failed a
        // catch-up transaction on this copy (it retries while `paused`),
        // rather than a fixed delay guessing at its schedule.
        for (var i = 0; i < 500; i++) {
          if (store.catchUpProgressOf(copyId)?.lastFailure != null) break;
          await Future<void>.delayed(const Duration(milliseconds: 20));
        }
        expect(
          store.catchUpProgressOf(copyId)?.lastFailure,
          isA<InjectedFailure>(),
          reason: 'the driver must have attempted and rolled back a step',
        );

        // The rolled-back attempt published nothing: no redelivery, no
        // second EndOfReplay, and the update count is unchanged.
        expect(secondMarker.isCompleted, isFalse);
        expect(updates.length, afterReplayCount);

        // Release: the next attempt folds both events without throwing
        // and commits.
        paused = false;

        final marker2 = await secondMarker.future.timeout(
          const Duration(seconds: 10),
        );
        expect(marker2.state, ViewConvergenceState.current);

        await sub.cancel();
        await store.close();
      },
    );
  });
}
