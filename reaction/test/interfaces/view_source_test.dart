// Verifies: EVS-PRD-view-subscriber/A — the library defines a
// `ViewSource` interface whose `watch<T>(viewName, mapper, filter,
// aggregates)` returns `Stream<Update<T>>`.
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
}
