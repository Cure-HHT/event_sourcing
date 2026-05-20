// Verifies: EVS-PRD-action-dispatch/A/B/C
// Verifies: EVS-PRD-permissions-as-events/B
// Verifies: EVS-PRD-action-dispatch/D
//                         enforcement by dispatcher tested at integration level),
import 'package:action_permissions_demo/server/actions/press_red_alarm_action.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'green-user-1',
    roles: const <String>{'GreenTeam'},
    activeRole: 'GreenTeam',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.utc(2026, 5, 8, 12),
);

void main() {
  group('PressRedAlarmAction', () {
    final action = PressRedAlarmAction();

    test('declares unscoped buttons.press.red, idempotency required', () {
      expect(action.name, 'PressRedAlarmAction');
      expect(
        action.permissions,
        contains(const Permission('buttons.press.red')),
      );
      expect(action.idempotency, Idempotency.required);
    });

    test('parseInput accepts {reason:String}', () {
      final input = action.parseInput(<String, Object?>{'reason': 'fire'});
      expect(input.reason, 'fire');
    });

    test('parseInput throws FormatException on missing/wrong-type reason', () {
      expect(
        () => action.parseInput(const <String, Object?>{}),
        throwsA(isA<FormatException>()),
      );
      expect(
        () => action.parseInput(<String, Object?>{'reason': 42}),
        throwsA(isA<FormatException>()),
      );
    });

    test('validate rejects empty/whitespace reason with ArgumentError', () {
      expect(
        () => action.validate(const RedAlarmInput(reason: '')),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => action.validate(const RedAlarmInput(reason: '   ')),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('execute emits one red_alarm_pressed event with the reason', () async {
      final result = await action.execute(
        const RedAlarmInput(reason: 'fire'),
        _ctx(),
      );
      expect(result.events, hasLength(1));
      final draft = result.events.single;
      expect(draft.eventType, 'red_alarm_pressed');
      expect(draft.aggregateType, 'red_alarm');
      expect(draft.entryType, 'red_alarm');
      expect(draft.aggregateId, isNotEmpty);
      expect(draft.data['reason'], 'fire');
      expect(result.result.alarmId, draft.aggregateId);
    });

    test('execute generates a fresh aggregateId per call', () async {
      final r1 = await action.execute(const RedAlarmInput(reason: 'a'), _ctx());
      final r2 = await action.execute(const RedAlarmInput(reason: 'b'), _ctx());
      expect(r1.events.single.aggregateId, isNot(r2.events.single.aggregateId));
    });
  });
}
