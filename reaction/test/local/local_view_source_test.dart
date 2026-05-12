// Verifies: EVS-PRD-view-subscriber/B — LocalViewSource delegates
// to EventStore.subscribe<T> with AggregateMode<T>: emits Snapshot/
// EndOfReplay/Delta/Tombstone updates, applies the mapper, and
// respects the aggregates allow-list.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/local/local_view_source.dart';

import 'test_support/reaction_test_harness.dart';

void main() {
  group('LocalViewSource.watch', () {
    late ReactionTestHarness harness;
    late LocalViewSource source;

    setUp(() async {
      harness = await ReactionTestHarness.open();
      source = LocalViewSource(eventStore: harness.eventStore);
    });

    tearDown(() async {
      await harness.close();
    });

    test('empty view emits exactly one EndOfReplay then live deltas', () async {
      final updates = <Update<Map<String, Object?>>>[];
      final endOfReplay = Completer<EndOfReplay<Map<String, Object?>>>();
      final sub = source
          .watch<Map<String, Object?>>(
            viewName: 'notes_today',
            mapper: (m) => m,
          )
          .listen((u) {
            updates.add(u);
            if (u is EndOfReplay<Map<String, Object?>> &&
                !endOfReplay.isCompleted) {
              endOfReplay.complete(u);
            }
          });

      await endOfReplay.future.timeout(const Duration(seconds: 5));
      expect(updates.length, equals(1));
      expect(updates.single, isA<EndOfReplay<Map<String, Object?>>>());
      expect(updates.single.sequence, equals(0));

      // Now append a note and verify a Delta arrives.
      await harness.appendNote(
        aggregateId: 'n1',
        payload: const {'body': 'hello'},
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(updates.any((u) => u is Delta<Map<String, Object?>>), isTrue);

      await sub.cancel();
    });

    test('mapper is applied to each row', () async {
      await harness.appendNote(
        aggregateId: 'n1',
        payload: const {'body': 'first'},
      );

      final endOfReplay = Completer<void>();
      final rows = <String>[];
      final sub = source
          .watch<String>(
            viewName: 'notes_today',
            mapper: (m) => m['body'] as String,
          )
          .listen((u) {
            if (u is Snapshot<String>) {
              rows.add(u.value!);
            }
            if (u is EndOfReplay<String>) {
              endOfReplay.complete();
            }
          });

      await endOfReplay.future.timeout(const Duration(seconds: 5));
      expect(rows, contains('first'));

      await sub.cancel();
    });

    test(
      'aggregates allow-list is honored — other aggregates do not emit',
      () async {
        // Subscribe with aggregates narrowed to 'other-aggregate'. When
        // 'n1' is appended its Snapshot (n1 doesn't exist) and live Deltas
        // are scoped to 'other-aggregate' only, so n1's Delta never arrives.
        final updates = <Update<Map<String, Object?>>>[];
        final snapshotSeen = Completer<void>();
        final endOfReplay = Completer<void>();
        final sub = source
            .watch<Map<String, Object?>>(
              viewName: 'notes_today',
              mapper: (m) => m,
              aggregates: const {'other-aggregate'}, // excludes 'n1'
            )
            .listen((u) {
              updates.add(u);
              if (u is Snapshot<Map<String, Object?>> &&
                  !snapshotSeen.isCompleted) {
                snapshotSeen.complete();
              }
              if (u is EndOfReplay<Map<String, Object?>>) {
                endOfReplay.complete();
              }
            });

        await endOfReplay.future.timeout(const Duration(seconds: 5));

        // Snapshot for 'other-aggregate' (null value — doesn't exist) + EndOfReplay.
        expect(updates.any((u) => u is Snapshot<Map<String, Object?>>), isTrue);
        expect(
          updates.any((u) => u is EndOfReplay<Map<String, Object?>>),
          isTrue,
        );

        // Append 'n1' — outside the aggregates allow-list.
        await harness.appendNote(
          aggregateId: 'n1',
          payload: const {'body': 'not delivered'},
        );
        await Future<void>.delayed(const Duration(milliseconds: 50));

        // No Delta for n1 because aggregates filter excluded it.
        final deltas = updates.whereType<Delta<Map<String, Object?>>>();
        expect(deltas, isEmpty);

        await sub.cancel();
      },
    );
  });
}
