// Verifies: EVS-PRD-action-submitter/A
// ActionClient mints an idempotency
// key when the submission lacks one (so Idempotency.required actions are not
// parse-denied for programmatic callers), passes consumer-supplied keys
// through unchanged, and returns the submitter's DispatchResult.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/state/action_client.dart';
import 'package:reaction/src/state/idempotency_key_generator.dart';

ActionSubmission _sub({String? idempotencyKey}) => ActionSubmission(
  actionName: 'noop',
  rawInput: const <String, Object?>{'a': 1},
  idempotencyKey: idempotencyKey,
  flowToken: 'flow-1',
);

/// Captures the submission it receives and returns a canned result.
class _CapturingSubmitter implements ActionSubmitter {
  final List<ActionSubmission> received = <ActionSubmission>[];
  final DispatchResult<Object?> result;
  _CapturingSubmitter(this.result);

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    received.add(submission);
    return result;
  }
}

/// Deterministic generator emitting `key-1`, `key-2`, ...
class _CountingKeyGen implements IdempotencyKeyGenerator {
  int _n = 0;
  @override
  String generate() => 'key-${++_n}';
}

void main() {
  group('ActionClient', () {
    test(
      'mints a key when the submission has none and returns the result',
      () async {
        const result = DispatchResult<Object?>.success('ok', <String>[]);
        final submitter = _CapturingSubmitter(result);
        final client = ActionClient(submitter, keyGenerator: _CountingKeyGen());

        final returned = await client.submit(_sub());

        expect(returned, same(result));
        expect(submitter.received, hasLength(1));
        final sent = submitter.received.single;
        expect(sent.idempotencyKey, 'key-1');
        // Other fields preserved.
        expect(sent.actionName, 'noop');
        expect(sent.rawInput, const <String, Object?>{'a': 1});
        expect(sent.flowToken, 'flow-1');
      },
    );

    test('passes a consumer-supplied key through unchanged', () async {
      const result = DispatchResult<Object?>.success('ok', <String>[]);
      final submitter = _CapturingSubmitter(result);
      final client = ActionClient(submitter, keyGenerator: _CountingKeyGen());

      await client.submit(_sub(idempotencyKey: 'fixed-key'));

      expect(submitter.received.single.idempotencyKey, 'fixed-key');
    });

    test('two no-key submits get two distinct generated keys', () async {
      const result = DispatchResult<Object?>.success('ok', <String>[]);
      final submitter = _CapturingSubmitter(result);
      final client = ActionClient(submitter, keyGenerator: _CountingKeyGen());

      await client.submit(_sub());
      await client.submit(_sub());

      expect(submitter.received, hasLength(2));
      expect(
        submitter.received[0].idempotencyKey,
        isNot(equals(submitter.received[1].idempotencyKey)),
      );
    });
  });
}
