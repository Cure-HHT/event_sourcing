// Verifies: EVS-PRD-action-dispatch/A
// (ActionSubmission value type carries actionName, rawInput, idempotencyKey, flowToken into dispatch)
// Verifies: EVS-DEV-flow-token/A
// flowToken is optional, can be supplied on a submission, and round-trips.

import 'package:event_sourcing/src/actions/action_submission.dart';
import 'package:test/test.dart';

void main() {
  group('ActionSubmission', () {
    test('required fields are populated', () {
      const s = ActionSubmission(
        actionName: 'submit_note',
        rawInput: {'title': 'hello'},
      );
      expect(s.actionName, 'submit_note');
      expect(s.rawInput, equals({'title': 'hello'}));
      expect(s.idempotencyKey, isNull);
      expect(s.flowToken, isNull);
    });

    test('optional fields can be supplied', () {
      const s = ActionSubmission(
        actionName: 'submit_note',
        rawInput: {'title': 'hello'},
        idempotencyKey: 'k-42',
        flowToken: 't-7',
      );
      expect(s.idempotencyKey, equals('k-42'));
      expect(s.flowToken, equals('t-7'));
    });

    test(
      'empty-rawInput const instances are canonicalized by the compiler',
      () {
        const a = ActionSubmission(
          actionName: 'a',
          rawInput: <String, Object?>{},
        );
        const b = ActionSubmission(
          actionName: 'a',
          rawInput: <String, Object?>{},
        );
        expect(identical(a, b), isTrue);
      },
    );
  });
}
