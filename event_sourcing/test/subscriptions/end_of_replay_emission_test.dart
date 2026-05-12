// Verifies: EVS-PRD-subscription.A (AggregateMode emits EndOfReplay as the
//   deterministic "snapshot complete; stream is now live" boundary marker;
//   Events() mode emits no EndOfReplay — no replay phase)
// Verifies: EVS-PRD-subscription.B (post-replay Deltas are reactively
//   delivered; EndOfReplay precedes any live Delta in stream order)
// Verifies: EVS-PRD-subscription.C (EndOfReplay.sequence equals max snapshot
//   sequence, anchoring the ordering boundary; N Snapshots precede
//   EndOfReplay in emission order)
import 'dart:async';

import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Note {
  final String entryId;
  final Map<String, Object?> answers;
  _Note({required this.entryId, required this.answers});
  static _Note fromMap(Map<String, Object?> m) => _Note(
    entryId: m['latestEventId'] as String? ?? '_',
    answers: (m['answers'] as Map?)?.cast<String, Object?>() ?? {},
  );
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
        registeredVersion: 1,
        name: 'Epistaxis Event',
      ),
    );
  final projections = ProjectionRegistry()
    ..register(
      AggregateProjectionSpec(
        viewName: 'diary_entries',
        interest: const SubscriptionFilter(aggregateTypes: {'note'}),
        tombstoneEventTypes: const {'tombstone'},
      ),
    );
  return EventStore.open(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-instance',
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
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
}
