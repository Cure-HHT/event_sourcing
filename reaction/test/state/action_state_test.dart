// Verifies: EVS-PRD-reaction-widget-contract/C — ActionState sealed
// type covers the 5 widget-side submission states (Idle/Submitting/
// Success/Denied/Failed) that ActionBuilder exposes to the caller-
// supplied builder, with exhaustive switching enforced by the sealed
// hierarchy.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/state/action_state.dart';

void main() {
  group('ActionState', () {
    test('Idle is a const value', () {
      const a = ActionState.idle();
      const b = ActionState.idle();
      expect(identical(a, b), isTrue);
      expect(a, isA<ActionState>());
      expect(a, isA<Idle>());
    });

    test('Submitting is a const value', () {
      const a = ActionState.submitting();
      const b = ActionState.submitting();
      expect(identical(a, b), isTrue);
      expect(a, isA<Submitting>());
    });

    test('Success carries the DispatchResult', () {
      // DispatchResult.success takes positional (TResult result,
      // List<String> emittedEventIds) — no named params, generic type.
      const result = DispatchResult<String>.success('inv-1', []);
      // Use concrete constructor so the static type exposes .result.
      const s = Success(result);
      expect(s, isA<ActionState>());
      expect(s, isA<Success>());
      expect((s.result as DispatchSuccess<String>).result, equals('inv-1'));
    });

    test('Denied carries the denial reason', () {
      // Use concrete constructor so the static type exposes .reason.
      const d = Denied('action not allowed for role');
      expect(d, isA<ActionState>());
      expect(d, isA<Denied>());
      expect(d.reason, contains('not allowed'));
    });

    test('Failed carries the error and stack', () {
      final err = StateError('boom');
      // Use concrete constructor so the static type exposes .error/.stackTrace.
      final f = Failed(err, StackTrace.current);
      expect(f, isA<ActionState>());
      expect(f, isA<Failed>());
      expect(f.error, equals(err));
      expect(f.stackTrace, isNotNull);
    });

    test('exhaustive switch across all 5 variants', () {
      const ActionState state = Idle();
      final tag = switch (state) {
        Idle() => 'idle',
        Submitting() => 'submitting',
        Success() => 'success',
        Denied() => 'denied',
        Failed() => 'failed',
      };
      expect(tag, equals('idle'));
    });
  });
}
