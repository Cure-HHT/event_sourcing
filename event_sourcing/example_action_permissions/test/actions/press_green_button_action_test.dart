// Verifies: EVS-PRD-action-dispatch/A/B/C
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/actions/press_green_button_action.dart';
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
  group('PressGreenButtonAction', () {
    final action = PressGreenButtonAction();

    test('declares site-scoped buttons.press.green, idempotency none', () {
      expect(action.name, 'PressGreenButtonAction');
      expect(
        action.permissions,
        contains(const Permission('buttons.press.green', scopeClass: 'site')),
      );
      expect(action.idempotency, Idempotency.none);
    });

    test('parseInput accepts empty map', () {
      expect(
        action.parseInput(const <String, Object?>{}),
        isA<PressGreenInput>(),
      );
    });

    test('validate accepts the singleton input', () {
      action.validate(const PressGreenInput());
    });

    test('execute emits one green_button_pressed event', () async {
      final result = await action.execute(const PressGreenInput(), _ctx());
      expect(result.events, hasLength(1));
      final draft = result.events.single;
      expect(draft.eventType, 'green_button_pressed');
      expect(draft.aggregateType, 'green_button_press');
      expect(draft.entryType, 'green_button_press');
      expect(draft.aggregateId, isNotEmpty);
      expect(draft.data, isEmpty);
      expect(result.result.eventId, draft.aggregateId);
    });

    test('execute generates a fresh aggregateId per call', () async {
      final r1 = await action.execute(const PressGreenInput(), _ctx());
      final r2 = await action.execute(const PressGreenInput(), _ctx());
      expect(r1.events.single.aggregateId, isNot(r2.events.single.aggregateId));
    });
  });
}
