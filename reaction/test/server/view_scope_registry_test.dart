// Verifies: EVS-PRD-cross-process-event-transport/E — ViewScopeRegistry
//   provides the viewName -> scope-class binding the subscription
//   handler uses for per-subscription row-level narrowing.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/view_scope_registry.dart';

void main() {
  test('returns null for unregistered view', () {
    final r = ViewScopeRegistry();
    expect(r.lookup('unknown_view'), isNull);
  });

  test('returns binding for registered view', () {
    final r = ViewScopeRegistry()
      ..register(
        viewName: 'patient_files',
        scopeClass: 'patient',
        aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
      );
    final binding = r.lookup('patient_files');
    expect(binding, isNotNull);
    expect(binding!.scopeClass, 'patient');
    expect(
      binding.aggregateIdResolver(
        const BoundScope(class_: 'patient', value: 'p-42'),
      ),
      'p-42',
    );
  });

  test('rejects duplicate registration', () {
    final r = ViewScopeRegistry();
    r.register(
      viewName: 'v',
      scopeClass: 'site',
      aggregateIdResolver: (_) => null,
    );
    expect(
      () => r.register(
        viewName: 'v',
        scopeClass: 'site',
        aggregateIdResolver: (_) => null,
      ),
      throwsArgumentError,
    );
  });
}
