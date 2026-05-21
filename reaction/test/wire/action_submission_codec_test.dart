import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';

void main() {
  test('round-trips minimal ActionSubmission', () {
    const original = ActionSubmission(
      actionName: 'sayHello',
      rawInput: {'name': 'Alice'},
    );
    final json = ActionSubmissionCodec.encode(original);
    final decoded = ActionSubmissionCodec.decode(json);
    expect(decoded.actionName, 'sayHello');
    expect(decoded.rawInput, {'name': 'Alice'});
    expect(decoded.idempotencyKey, isNull);
    expect(decoded.flowToken, isNull);
  });

  test('round-trips ActionSubmission with all fields', () {
    const original = ActionSubmission(
      actionName: 'editNote',
      rawInput: {'noteId': 'n-1', 'title': 'updated'},
      idempotencyKey: 'idem-123',
      flowToken: 'flow-abc',
    );
    final json = ActionSubmissionCodec.encode(original);
    final decoded = ActionSubmissionCodec.decode(json);
    expect(decoded.actionName, 'editNote');
    expect(decoded.rawInput, {'noteId': 'n-1', 'title': 'updated'});
    expect(decoded.idempotencyKey, 'idem-123');
    expect(decoded.flowToken, 'flow-abc');
  });
}
