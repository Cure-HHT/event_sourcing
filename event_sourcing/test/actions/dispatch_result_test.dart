import 'package:event_sourcing/src/actions/dispatch_result.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/scope_class.dart';
import 'package:test/test.dart';

void main() {
  group('DispatchResult', () {
    test('success carries result + emitted ids', () {
      const r = DispatchResult<int>.success(42, ['evt-1', 'evt-2']);
      expect(r, isA<DispatchSuccess<int>>());
      r as DispatchSuccess<int>;
      expect(r.result, 42);
      expect(r.emittedEventIds, hasLength(2));
    });

    test('unknownAction carries requested name', () {
      const r = DispatchResult<int>.unknownAction('foo');
      expect(r, isA<DispatchUnknownAction<int>>());
      r as DispatchUnknownAction<int>;
      expect(r.requestedName, 'foo');
    });

    test('parseDenied carries the error', () {
      final err = ArgumentError('bad input');
      final r = DispatchResult<int>.parseDenied(err);
      expect(r, isA<DispatchParseDenied<int>>());
      r as DispatchParseDenied<int>;
      expect(r.error, err);
    });

    test('validationDenied carries the error', () {
      final err = StateError('invalid');
      final r = DispatchResult<int>.validationDenied(err);
      expect(r, isA<DispatchValidationDenied<int>>());
    });

    test('authorizationDenied carries the failed permission', () {
      const p = Permission('user.invite', scope: ScopeClass.global);
      const r = DispatchResult<int>.authorizationDenied(p);
      expect(r, isA<DispatchAuthorizationDenied<int>>());
      r as DispatchAuthorizationDenied<int>;
      expect(r.permission, p);
    });

    test('executionFailed carries the error', () {
      final err = StateError('boom');
      final r = DispatchResult<int>.executionFailed(err);
      expect(r, isA<DispatchExecutionFailed<int>>());
    });

    test('idempotencyHit carries cached payload', () {
      const r = DispatchResult<Map<String, dynamic>>.idempotencyHit(
        {'ok': true},
        ['evt-prior'],
      );
      expect(r, isA<DispatchIdempotencyHit<Map<String, dynamic>>>());
    });

    test('sealed: switch is exhaustive', () {
      const r = DispatchResult<int>.success(1, []);
      final desc = switch (r) {
        DispatchSuccess<int>() => 'success',
        DispatchUnknownAction<int>() => 'unknown',
        DispatchParseDenied<int>() => 'parse',
        DispatchValidationDenied<int>() => 'validation',
        DispatchAuthorizationDenied<int>() => 'authz',
        DispatchExecutionFailed<int>() => 'execfail',
        DispatchIdempotencyHit<int>() => 'hit',
      };
      expect(desc, 'success');
    });
  });
}
