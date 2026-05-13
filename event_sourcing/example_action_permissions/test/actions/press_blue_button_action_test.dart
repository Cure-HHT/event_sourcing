// Verifies: EVS-PRD-action-dispatch/A/B/C
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/actions/press_blue_button_action.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

ActionContext _ctx() => ActionContext(
  principal: const Principal.user(
    userId: 'blue-user',
    roles: <String>{'BlueTeam'},
    activeRole: 'BlueTeam',
    activeSite: 'blue-workspace',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.utc(2026, 5, 8, 12),
);

void main() {
  group('PressBlueButtonAction', () {
    final action = PressBlueButtonAction();

    test('declares site-scoped buttons.press.blue, idempotency none', () {
      expect(action.name, 'PressBlueButtonAction');
      expect(
        action.permissions,
        contains(const Permission('buttons.press.blue', scopeClass: 'site')),
      );
      expect(action.idempotency, Idempotency.none);
    });

    test('parseInput accepts empty map', () {
      expect(
        action.parseInput(const <String, Object?>{}),
        isA<PressBlueInput>(),
      );
    });

    test('validate accepts the singleton input', () {
      action.validate(const PressBlueInput());
    });

    test('execute emits one blue_button_pressed event', () async {
      final result = await action.execute(const PressBlueInput(), _ctx());
      expect(result.events, hasLength(1));
      final draft = result.events.single;
      expect(draft.eventType, 'blue_button_pressed');
      expect(draft.aggregateType, 'blue_button_press');
      expect(draft.entryType, 'blue_button_press');
      expect(draft.aggregateId, isNotEmpty);
      expect(draft.data, isEmpty);
      expect(result.result.eventId, draft.aggregateId);
    });

    test('execute generates a fresh aggregateId per call', () async {
      final r1 = await action.execute(const PressBlueInput(), _ctx());
      final r2 = await action.execute(const PressBlueInput(), _ctx());
      expect(r1.events.single.aggregateId, isNot(r2.events.single.aggregateId));
    });
  });
}
