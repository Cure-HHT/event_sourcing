// Verifies: EVS-PRD-cross-process-event-transport/A — round-trip codec
//   for every DispatchResult variant preserves all fields.
// Verifies: EVS-PRD-action-submitter/C — wire shape of the POST
//   /actions response body.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';

void main() {
  test('round-trips DispatchSuccess', () {
    const original = DispatchSuccess<Object?>(
      {'echo': 'hello'},
      ['evt-1', 'evt-2'],
    );
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'success');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchSuccess<Object?>;
    expect(decoded.result, {'echo': 'hello'});
    expect(decoded.emittedEventIds, ['evt-1', 'evt-2']);
  });

  test('round-trips DispatchUnknownAction', () {
    const original = DispatchUnknownAction<Object?>('noSuchAction');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'unknown_action');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchUnknownAction<Object?>;
    expect(decoded.requestedName, 'noSuchAction');
  });

  test('round-trips DispatchParseDenied (error -> string)', () {
    const original = DispatchParseDenied<Object?>('invalid input shape');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'parse_denied');
    expect(json['error'], 'invalid input shape');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchParseDenied<Object?>;
    expect(decoded.error, 'invalid input shape');
  });

  test('round-trips DispatchValidationDenied (error -> string)', () {
    const original = DispatchValidationDenied<Object?>('missing field "title"');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'validation_denied');
    expect(json['error'], 'missing field "title"');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchValidationDenied<Object?>;
    expect(decoded.error, 'missing field "title"');
  });

  test('round-trips DispatchAuthorizationDenied with scoped permission', () {
    const original = DispatchAuthorizationDenied<Object?>(
      Permission('note.edit', scopeClass: 'patient'),
    );
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'authorization_denied');
    final decoded =
        DispatchResultCodec.decode(json)
            as DispatchAuthorizationDenied<Object?>;
    expect(decoded.permission.name, 'note.edit');
    expect(decoded.permission.scopeClass, 'patient');
  });

  test('round-trips DispatchAuthorizationDenied with unscoped permission', () {
    const original = DispatchAuthorizationDenied<Object?>(
      Permission('admin.all'),
    );
    final json = DispatchResultCodec.encode(original);
    final decoded =
        DispatchResultCodec.decode(json)
            as DispatchAuthorizationDenied<Object?>;
    expect(decoded.permission.name, 'admin.all');
    expect(decoded.permission.scopeClass, isNull);
  });

  test('round-trips DispatchExecutionFailed (error -> string)', () {
    const original = DispatchExecutionFailed<Object?>('sembast write failed');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'execution_failed');
    expect(json['error'], 'sembast write failed');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchExecutionFailed<Object?>;
    expect(decoded.error, 'sembast write failed');
  });

  test('round-trips DispatchIdempotencyHit', () {
    const original = DispatchIdempotencyHit<Object?>(
      {'echo': 'cached'},
      ['evt-7'],
    );
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'idempotency_hit');
    final decoded =
        DispatchResultCodec.decode(json) as DispatchIdempotencyHit<Object?>;
    expect(decoded.cachedResult, {'echo': 'cached'});
    expect(decoded.priorEmittedEventIds, ['evt-7']);
  });

  test('decode rejects unknown type', () {
    expect(
      () => DispatchResultCodec.decode({'type': 'mystery'}),
      throwsA(isA<FormatException>()),
    );
  });
}
