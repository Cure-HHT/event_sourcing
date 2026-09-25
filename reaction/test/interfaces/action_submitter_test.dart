// Verifies: EVS-PRD-action-submitter/A
// the library defines an
// `ActionSubmitter` interface whose `submit(ActionSubmission)` returns
// a `Future<DispatchResult<Object?>>`.
//
// Structural interface-shape assertion. The test body is intentionally
// tautological at runtime: the assertion is that this file COMPILES,
// proving the library exposes `ActionSubmitter` through the public
// barrel with the contracted method signature. A breaking change to
// the interface (renaming `submit`, changing its parameter type, or
// changing its return type) would fail compilation here.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

class _StubSubmitter implements ActionSubmitter {
  DispatchResult<Object?>? _next;
  set next(DispatchResult<Object?> r) => _next = r;

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    final r = _next;
    if (r == null) {
      throw StateError('test stub: no result primed');
    }
    return r;
  }
}

/// Returns [value] typed as [T]. Passing an explicit type argument makes the
/// compiler check that [value] is assignable to the contracted type.
T _asContracted<T>(T value) => value;

void main() {
  group('ActionSubmitter interface shape', () {
    test('reachable through the public barrel and has '
        'submit(ActionSubmission) -> Future<DispatchResult<Object?>>', () {
      // Compile-time proof: the tear-off type-checks against the
      // documented signature. If the interface ever drifts, this
      // assignment fails to compile.
      final ActionSubmitter submitter = _StubSubmitter();
      final submit =
          _asContracted<
            Future<DispatchResult<Object?>> Function(ActionSubmission)
          >(submitter.submit);
      expect(submit, isNotNull);
    });

    test('submit round-trips a DispatchResult through the interface', () async {
      // Round-trips a Success result to prove both ends of the contract
      // (parameter accepted, return value type matches).
      final stub = _StubSubmitter();
      const result = DispatchSuccess<Object?>(null, <String>[]);
      stub.next = result;
      final got = await stub.submit(
        const ActionSubmission(actionName: 'noop', rawInput: {}),
      );
      expect(got, same(result));
    });
  });
}
