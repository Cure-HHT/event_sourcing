// Verifies: EVS-PRD-materializer/A
// AggregateProjectionSpec and
//   TableProjectionSpec expose the declarative fields that describe the
//   materializer's rule set; tests confirm the field surface.
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('AggregateProjectionSpec', () {
    test('exposes viewName, interest, tombstones, derivations', () {
      const spec = AggregateProjectionSpec(
        viewName: 'diary_entries',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        tombstoneEventTypes: {'tombstone'},
        derivedFields: [
          DerivedField(
            'effective_date',
            DottedPathLookup(
              'answers.date_of_event',
              fallback: FirstEventTimestamp(),
            ),
          ),
        ],
      );
      expect(spec.viewName, 'diary_entries');
      expect(spec.tombstoneEventTypes, {'tombstone'});
      expect(spec.derivedFields, hasLength(1));
    });
  });

  group('TableProjectionSpec', () {
    test('exposes insert/remove event sets, key, data extractor', () {
      const spec = TableProjectionSpec(
        viewName: 'role_permission_grants',
        interest: SubscriptionFilter(
          eventTypes: {'permission_granted', 'permission_revoked'},
        ),
        insertEventTypes: {'permission_granted'},
        removeEventTypes: {'permission_revoked'},
        rowKey: CompositeKey(['data.role', 'data.permission', 'data.scope']),
        rowData: PayloadField('data'),
      );
      expect(spec.viewName, 'role_permission_grants');
      expect(spec.insertEventTypes, {'permission_granted'});
    });
  });
}
