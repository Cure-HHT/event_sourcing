// IMPLEMENTS REQUIREMENTS:
//   returns parseDenied(MissingIdempotencyKeyError) when the caller omits
//   the idempotencyKey for this action.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';
import 'package:uuid/uuid.dart';

@immutable
class RedAlarmInput {
  const RedAlarmInput({required this.reason});

  final String reason;
}

@immutable
class RedAlarmResult {
  const RedAlarmResult({required this.alarmId});

  final String alarmId;

  Map<String, Object?> toJson() => <String, Object?>{'alarmId': alarmId};
}

class PressRedAlarmAction extends Action<RedAlarmInput, RedAlarmResult> {
  PressRedAlarmAction({Uuid? uuid}) : _uuid = uuid ?? const Uuid();

  final Uuid _uuid;

  @override
  String get name => 'PressRedAlarmAction';

  @override
  String get description =>
      'Any authenticated user fires the red alarm. Unscoped (no '
      'scopeClass on Permission); requires idempotency key to prevent '
      'button-mash double-fires.';

  @override
  Set<Permission> get permissions => <Permission>{
    const Permission('buttons.press.red'),
  };

  @override
  Idempotency get idempotency => Idempotency.required;

  @override
  RedAlarmInput parseInput(Map<String, Object?> raw) {
    final reason = raw['reason'];
    if (reason is! String) {
      throw const FormatException(
        'PressRedAlarmAction expects "reason": String',
      );
    }
    return RedAlarmInput(reason: reason);
  }

  @override
  void validate(RedAlarmInput input) {
    if (input.reason.trim().isEmpty) {
      throw ArgumentError.value(input.reason, 'reason', 'must be non-empty');
    }
  }

  @override
  Future<ExecutionResult<RedAlarmResult>> execute(
    RedAlarmInput input,
    ActionContext ctx,
  ) async {
    final id = _uuid.v4();
    return ExecutionResult<RedAlarmResult>(
      result: RedAlarmResult(alarmId: id),
      events: <EventDraft>[
        EventDraft(
          aggregateType: 'red_alarm',
          aggregateId: id,
          entryType: 'red_alarm',
          eventType: 'red_alarm_pressed',
          data: <String, dynamic>{'reason': input.reason},
        ),
      ],
    );
  }
}
