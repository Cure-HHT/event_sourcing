// Verifies: EVS-PRD-subscription/A
// (AggregateMode emits EndOfReplay as the
//   deterministic "snapshot complete; stream is now live" boundary marker;
//   Events() mode emits no EndOfReplay — no replay phase)
// Verifies: EVS-PRD-subscription/B
// (post-replay Deltas are reactively
//   delivered; EndOfReplay precedes any live Delta in stream order)
// Verifies: EVS-PRD-subscription/C
// (EndOfReplay.sequence equals max snapshot
//   sequence, anchoring the ordering boundary; N Snapshots precede
//   EndOfReplay in emission order)
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
    'eor-${DateTime.now().microsecondsSinceEpoch}.db',
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

void main() {
  test('empty view emits exactly one EndOfReplay with sequence 0', () async {
    final store = await _open();
    final updates = <Update<_Note?>>[];
    final endOfReplay = Completer<EndOfReplay<_Note?>>();
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
          ),
        )
        .listen((u) {
          updates.add(u);
          if (u is EndOfReplay<_Note?> && !endOfReplay.isCompleted) {
            endOfReplay.complete(u);
          }
        });
    final marker = await endOfReplay.future.timeout(const Duration(seconds: 5));
    expect(updates.length, 1);
    expect(marker, isA<EndOfReplay<_Note?>>());
    expect(marker.sequence, 0);
    expect(marker.state, ViewConvergenceState.current);
    await sub.cancel();
    await store.close();
  });

  test(
    'populated view emits N Snapshots then EndOfReplay with max snapshot sequence',
    () async {
      final store = await _open();
      final s1 = await _append(store, 'e1', 'note_added', {
        'answers': {'q1': 'yes'},
      });
      final s2 = await _append(store, 'e2', 'note_added', {
        'answers': {'q1': 'no'},
      });
      final maxSeq = [
        s1!.sequenceNumber,
        s2!.sequenceNumber,
      ].reduce((a, b) => a > b ? a : b);

      final updates = <Update<_Note?>>[];
      final endOfReplay = Completer<EndOfReplay<_Note?>>();
      final sub = store
          .subscribe(
            const SubscriptionFilter(),
            AggregateMode<_Note?>(
              viewName: 'diary_entries',
              mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
            ),
          )
          .listen((u) {
            updates.add(u);
            if (u is EndOfReplay<_Note?> && !endOfReplay.isCompleted) {
              endOfReplay.complete(u);
            }
          });
      final marker = await endOfReplay.future.timeout(
        const Duration(seconds: 5),
      );

      expect(updates.length, 3);
      expect(updates[0], isA<Snapshot<_Note?>>());
      expect(updates[1], isA<Snapshot<_Note?>>());
      expect(updates[2], isA<EndOfReplay<_Note?>>());
      expect(marker.sequence, maxSeq);
      await sub.cancel();
      await store.close();
    },
  );

  test('EndOfReplay is ordered before any post-subscribe Delta', () async {
    final store = await _open();
    await _append(store, 'e1', 'note_added', {
      'answers': {'q1': 'yes'},
    });

    final updates = <Update<_Note?>>[];
    final endOfReplay = Completer<EndOfReplay<_Note?>>();
    final sawDelta = Completer<Delta<_Note?>>();
    final sub = store
        .subscribe(
          const SubscriptionFilter(),
          AggregateMode<_Note?>(
            viewName: 'diary_entries',
            mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
          ),
        )
        .listen((u) {
          updates.add(u);
          if (u is EndOfReplay<_Note?> && !endOfReplay.isCompleted) {
            endOfReplay.complete(u);
          }
          if (u is Delta<_Note?> && !sawDelta.isCompleted) {
            sawDelta.complete(u);
          }
        });

    // Wait for replay to complete first.
    await endOfReplay.future.timeout(const Duration(seconds: 5));

    // Now append something post-subscribe.
    await _append(store, 'e2', 'note_added', {
      'answers': {'q1': 'no'},
    });
    await sawDelta.future.timeout(const Duration(seconds: 5));

    // The EndOfReplay must precede any Delta in the recorded sequence.
    final eorIdx = updates.indexWhere((u) => u is EndOfReplay<_Note?>);
    final firstDeltaIdx = updates.indexWhere((u) => u is Delta<_Note?>);
    expect(eorIdx, greaterThanOrEqualTo(0));
    expect(firstDeltaIdx, greaterThan(eorIdx));
    await sub.cancel();
    await store.close();
  });

  test('Events()-mode subscription emits no EndOfReplay', () async {
    final store = await _open();
    final updates = <Update<StoredEvent>>[];
    final sawEvent = Completer<StoredEvent>();
    final sub = store
        .subscribe(const SubscriptionFilter(), const Events())
        .listen((u) {
          updates.add(u);
          if (u is Delta<StoredEvent> && !sawEvent.isCompleted) {
            sawEvent.complete(u.value);
          }
        });
    await _append(store, 'e1', 'note_added', {
      'answers': {'q1': 'yes'},
    });
    await sawEvent.future.timeout(const Duration(seconds: 5));
    expect(
      updates.any((u) => u is EndOfReplay<StoredEvent>),
      isFalse,
      reason: 'Events() mode has no replay phase; must not emit EndOfReplay',
    );
    await sub.cancel();
    await store.close();
  });

  // Verifies: EVS-DEV-converging-view-reads/E
  // (a converging view's initial replay,
  //   naming no aggregates, delivers only the settled row and ends with
  //   EndOfReplay carrying the converging state)
  // Verifies: EVS-DEV-converging-view-reads/G
  // (once the copy catches up, the
  //   subscription redelivers every row it would snapshot before it
  //   reports the view current via a second EndOfReplay)
  test('a converging view (no named aggregates) emits only the settled row and '
      'EndOfReplay(converging); catch-up redelivers every row before the '
      'current marker', () async {
    var paused = true;
    Future<void> hook(String copyId, String eventId) async {
      if (paused) {
        throw const InjectedFailure('paused for end-of-replay test');
      }
    }

    await runWithDeliveryTestHooks(
      DeliveryTestHooks(onCatchUpStep: hook),
      () async {
        final (store, backend) = await _openWithBackend();
        final settled = (await _append(store, 'e1', 'note_added', {
          'answers': {'q1': 'yes'},
        }))!;
        await _append(store, 'e2', 'note_added', {
          'answers': {'q1': 'no'},
        });
        final copyId = store.copyIdOf('diary_entries');
        await backend.transaction(
          (txn) => backend.setViewCopyWatermarkInTxn(
            txn,
            copyId,
            settled.sequenceNumber,
          ),
        );

        final updates = <Update<_Note?>>[];
        final firstMarker = Completer<EndOfReplay<_Note?>>();
        final secondMarker = Completer<EndOfReplay<_Note?>>();
        final sub = store
            .subscribe(
              const SubscriptionFilter(),
              AggregateMode<_Note?>(
                viewName: 'diary_entries',
                mapper: (m) => m.isEmpty ? null : _Note.fromMap(m),
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
        expect(updates.whereType<Snapshot<_Note?>>(), hasLength(1));
        expect(
          updates
              .whereType<Snapshot<_Note?>>()
              .single
              .value!
              .entryId
              .isNotEmpty,
          isTrue,
        );

        paused = false;

        final marker2 = await secondMarker.future.timeout(
          const Duration(seconds: 10),
        );
        expect(marker2.state, ViewConvergenceState.current);

        final secondMarkerIndex = updates.indexOf(marker2);
        final redeliveredE2Index = updates.indexWhere(
          (u) => u is Snapshot<_Note?> && u.value?.answers['q1'] == 'no',
        );
        expect(redeliveredE2Index, greaterThanOrEqualTo(0));
        expect(redeliveredE2Index, lessThan(secondMarkerIndex));

        await sub.cancel();
        await store.close();
      },
    );
  });
}

/// Opens a store the same way [_open] does, but hands back its backend too,
/// so a test can rewind a view copy's watermark by hand to force it
/// converging (EVS-DEV-view-convergence).
Future<(EventStore, SembastBackend)> _openWithBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'eor-converging-${DateTime.now().microsecondsSinceEpoch}.db',
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
