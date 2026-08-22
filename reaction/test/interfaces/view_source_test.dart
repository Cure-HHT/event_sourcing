// Verifies: EVS-PRD-view-subscriber/A/E
// the library defines a
// `ViewSource` interface whose `watch<T>(viewName, mapper, filter,
// aggregates)` returns `Stream<Update<T>>` (A), and the `Update<T>`
// variant set is a stable, exhaustively-switchable contract so that
// batched/cursor snapshot delivery can be added additively without
// changing existing variant shapes or breaking consumers (E).
//
// Structural interface-shape assertion. The test body is intentionally
// tautological at runtime: the assertion is that this file COMPILES,
// proving the library exposes `ViewSource` through the public barrel
// with the contracted generic method signature. A breaking change
// (renaming `watch`, changing the named-parameter list, dropping the
// generic, or changing the return type) would fail compilation here.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

class _StubViewSource implements ViewSource {
  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) => const Stream.empty();
}

void main() {
  group('ViewSource interface shape', () {
    test('reachable through the public barrel with the contracted '
        'generic watch<T>(...) signature', () {
      // Compile-time proof: the call type-checks at the documented
      // named-parameter list and the return is `Stream<Update<int>>`.
      // If the interface ever drifts (renamed param, missing
      // optional, dropped generic), this fails to compile.
      final ViewSource source = _StubViewSource();
      final Stream<Update<int>> stream = source.watch<int>(
        viewName: 'noop_view',
        mapper: (row) => 0,
        filter: null,
        aggregates: null,
      );
      expect(stream, isA<Stream<Update<int>>>());
    });

    test('omitting optional parameters still type-checks', () {
      // The `filter` and `aggregates` parameters are nullable/optional
      // per the spec; calling without them is a contracted use.
      final ViewSource source = _StubViewSource();
      final stream = source.watch<String>(
        viewName: 'noop_view',
        mapper: (row) => '',
      );
      expect(stream, isA<Stream<Update<String>>>());
    });
  });

  group('Update<T> variant set is a stable, additive-evolution contract', () {
    test('the sealed variant set is exhaustively switchable (E)', () {
      // EVS-PRD-view-subscriber/E: snapshot delivery MAY evolve
      // (chunking/paging/cursor resumption) only additively. The proof is
      // that Update<T> is a sealed union whose complete variant set is
      // Snapshot/EndOfReplay/Delta/Tombstone — this switch is compiler-
      // checked total, so a non-additive change (a new variant, or a changed
      // variant shape) would fail to compile here, and a consumer handling
      // these variants is unaffected by how snapshot rows are delivered.
      String describe(Update<int> u) => switch (u) {
        Snapshot<int>() => 'snapshot',
        EndOfReplay<int>() => 'end-of-replay',
        Delta<int>() => 'delta',
        Tombstone<int>() => 'tombstone',
      };
      expect(describe(const Snapshot(value: 1, sequence: 1)), 'snapshot');
      expect(describe(const EndOfReplay(sequence: 2)), 'end-of-replay');
      expect(
        describe(const Delta(value: 3, sequence: 3, cause: 'evt')),
        'delta',
      );
      expect(
        describe(const Tombstone(aggregateId: 'a', sequence: 4)),
        'tombstone',
      );
    });
  });
}
