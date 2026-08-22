// Verifies: EVS-PRD-action-dispatch/B
// (Action.scopeFor default impl)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _MyInput {
  const _MyInput(this.foo);
  final String foo;
}

class _UnscopedAction extends Action<_MyInput, void> {
  const _UnscopedAction();
  @override
  String get name => 'a.unscoped';
  @override
  String get description => '';
  @override
  Set<Permission> get permissions => {const Permission('foo')};
  @override
  Idempotency get idempotency => Idempotency.none;
  @override
  _MyInput parseInput(Map<String, Object?> raw) =>
      _MyInput(raw['foo']! as String);
  @override
  void validate(_MyInput input) {}
  @override
  Future<ExecutionResult<void>> execute(
    _MyInput input,
    ActionContext ctx,
  ) async => const ExecutionResult(result: null, events: []);
  // scopeFor not overridden -> default returns null.
}

void main() {
  test('default scopeFor returns null', () {
    const a = _UnscopedAction();
    expect(a.scopeFor(const Permission('foo'), const _MyInput('x')), isNull);
  });
}
