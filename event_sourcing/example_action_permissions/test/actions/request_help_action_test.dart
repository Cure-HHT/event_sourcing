// Verifies: EVS-PRD-action-dispatch/action-lifecycle
// Verifies: EVS-PRD-permissions-as-events/global-scope-enforcement
import 'package:action_permissions_demo/server/actions/request_help_action.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

ActionContext _ctx({Principal? principal}) => ActionContext(
  principal: principal ?? const Principal.anonymous(),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.utc(2026, 5, 8, 12),
);

void main() {
  group('RequestHelpAction', () {
    final action = RequestHelpAction();

    test('declares name, global help.ask permission, idempotency none', () {
      expect(action.name, 'RequestHelpAction');
      expect(
        action.permissions,
        contains(const Permission('help.ask', scope: ScopeClass.global)),
      );
      expect(action.idempotency, Idempotency.none);
    });

    test('parseInput accepts {message:String}', () {
      final input = action.parseInput(<String, Object?>{'message': 'help me'});
      expect(input.message, 'help me');
    });

    test('parseInput throws FormatException on missing/wrong-type message', () {
      expect(
        () => action.parseInput(<String, Object?>{'wrong_field': 1}),
        throwsA(isA<FormatException>()),
      );
      expect(
        () => action.parseInput(<String, Object?>{'message': 42}),
        throwsA(isA<FormatException>()),
      );
    });

    test('validate rejects empty/whitespace message with ArgumentError', () {
      expect(
        () => action.validate(const HelpInput(message: '')),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => action.validate(const HelpInput(message: '   ')),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('validate accepts non-empty message', () {
      // Should not throw.
      action.validate(const HelpInput(message: 'help'));
    });

    test('execute emits one help_request event with the message', () async {
      final result = await action.execute(
        const HelpInput(message: 'urgent'),
        _ctx(),
      );
      expect(result.events, hasLength(1));
      final draft = result.events.single;
      expect(draft.eventType, 'help_request');
      expect(draft.aggregateType, 'help_ticket');
      expect(draft.entryType, 'help_request');
      expect(draft.data['message'], 'urgent');
      expect(draft.aggregateId, isNotEmpty);
      expect(result.result.helpTicketId, draft.aggregateId);
    });

    test('execute generates a fresh aggregateId per call', () async {
      final r1 = await action.execute(const HelpInput(message: 'a'), _ctx());
      final r2 = await action.execute(const HelpInput(message: 'b'), _ctx());
      expect(r1.events.single.aggregateId, isNot(r2.events.single.aggregateId));
    });
  });
}
