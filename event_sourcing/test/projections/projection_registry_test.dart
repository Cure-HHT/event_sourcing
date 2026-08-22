// Verifies: EVS-PRD-materializer/A
// ProjectionRegistry holds the active set
//   of materializer rules; tests confirm register, lookup, all(), duplicate
//   rejection, and post-seal enforcement.
// Verifies: EVS-PRD-materializer/C
// (partial) — the seal() behavior ensures
//   the rule set is fixed for the lifetime of one store instance.
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:flutter_test/flutter_test.dart';

ProjectionSpec _spec(String viewName) => TableProjectionSpec(
  viewName: viewName,
  interest: SubscriptionFilter(eventTypes: const {'evt'}),
  insertEventTypes: const {'evt'},
  removeEventTypes: const {},
  rowKey: const AggregateIdKey(),
  rowData: const WholePayload(),
);

void main() {
  group('ProjectionRegistry', () {
    test('register + lookup by viewName', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.register(_spec('b'));
      expect(reg.lookup('a')?.viewName, 'a');
      expect(reg.lookup('b')?.viewName, 'b');
      expect(reg.lookup('missing'), isNull);
    });

    test('all() returns every registered spec', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.register(_spec('b'));
      expect(reg.all().map((s) => s.viewName).toSet(), {'a', 'b'});
    });

    test('register throws on duplicate viewName', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      expect(() => reg.register(_spec('a')), throwsArgumentError);
    });

    test('register after seal() throws', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.seal();
      expect(() => reg.register(_spec('b')), throwsArgumentError);
    });
  });
}
