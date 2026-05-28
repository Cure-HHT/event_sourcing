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

    test(
      'Denied carries the full DispatchResult and exposes a reason getter',
      () {
        const result = DispatchResult<Object?>.authorizationDenied(
          Permission('test.permission'),
        );
        const d = Denied(result);
        expect(d, isA<ActionState>());
        expect(d, isA<Denied>());
        expect(d.result, same(result));
        expect(d.reason, contains('test.permission'));
      },
    );

    test(
      'Denied.reason maps each DispatchResult denial variant to a readable summary',
      () {
        expect(
          const Denied(DispatchResult<Object?>.unknownAction('foo')).reason,
          contains('foo'),
        );
        expect(
          const Denied(DispatchResult<Object?>.parseDenied('bad json')).reason,
          contains('parse'),
        );
        expect(
          const Denied(
            DispatchResult<Object?>.validationDenied('field empty'),
          ).reason,
          contains('valid'),
        );
        expect(
          const Denied(
            DispatchResult<Object?>.authorizationDenied(Permission('p')),
          ).reason,
          contains('permission'),
        );
        expect(
          const Denied(DispatchResult<Object?>.executionFailed('boom')).reason,
          isNotEmpty,
        );
        expect(
          const Denied(
            DispatchResult<Object?>.idempotencyMismatch(
              actionName: 'a',
              idempotencyKey: 'k',
              cachedRawInputHash: 'h1',
              submittedRawInputHash: 'h2',
            ),
          ).reason,
          contains('idempot'),
        );
      },
    );

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
