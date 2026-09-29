// Verifies: EVS-DEV-view-convergence/A (fingerprint term)
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/projections/view_fingerprint.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';

EntryTypeDefinition _def(String id, EntryTypeVersion version) =>
    EntryTypeDefinition(id: id, registeredVersion: version, name: id);

AggregateProjectionSpec _spec({
  Set<String>? entryTypeIds,
  bool includeSystemEvents = false,
}) => AggregateProjectionSpec(
  viewName: 'toy_view',
  interest: SubscriptionFilter(
    entryTypes: entryTypeIds,
    includeSystemEvents: includeSystemEvents,
  ),
  tombstoneEventTypes: const {'toy_deleted'},
);

void main() {
  group('viewFingerprint', () {
    test('equal for the same definition registered in another order', () {
      final spec = _spec(entryTypeIds: {'note', 'photo'});

      final entryTypesA = EntryTypeRegistry()
        ..register(_def('note', const EntryTypeVersion(1, 0)))
        ..register(_def('photo', const EntryTypeVersion(1, 0)));
      final promotersA = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'toy_view',
            entryType: 'note',
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: [
              DefaultField(fieldName: 'severity', defaultValue: 'mild'),
            ],
          ),
        );

      // Same definitions, registered in the opposite order.
      final entryTypesB = EntryTypeRegistry()
        ..register(_def('photo', const EntryTypeVersion(1, 0)))
        ..register(_def('note', const EntryTypeVersion(1, 0)));
      final promotersB = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'toy_view',
            entryType: 'note',
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: [
              DefaultField(fieldName: 'severity', defaultValue: 'mild'),
            ],
          ),
        );

      expect(
        viewFingerprint(spec, entryTypesA, promotersA),
        viewFingerprint(spec, entryTypesB, promotersB),
      );
    });

    test('differs for a new minor of a named entry type', () {
      final spec = _spec(entryTypeIds: {'note'});
      final v1 = EntryTypeRegistry()
        ..register(_def('note', const EntryTypeVersion(1, 0)));
      final v2 = EntryTypeRegistry()
        ..register(_def('note', const EntryTypeVersion(1, 1)));
      final promoters = PromoterRegistry();

      expect(
        viewFingerprint(spec, v1, promoters),
        isNot(viewFingerprint(spec, v2, promoters)),
      );
    });

    test('differs for a changed promoter chain (a DefaultField value)', () {
      final spec = _spec(entryTypeIds: {'note'});
      final entryTypes = EntryTypeRegistry()
        ..register(_def('note', const EntryTypeVersion(1, 0)));

      final promotersA = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'toy_view',
            entryType: 'note',
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: [
              DefaultField(fieldName: 'severity', defaultValue: 'mild'),
            ],
          ),
        );
      final promotersB = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: 'toy_view',
            entryType: 'note',
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: [
              DefaultField(fieldName: 'severity', defaultValue: 'severe'),
            ],
          ),
        );

      expect(
        viewFingerprint(spec, entryTypes, promotersA),
        isNot(viewFingerprint(spec, entryTypes, promotersB)),
      );
    });

    test("differs for any registered entry type's minor when the interest "
        'names none', () {
      // interest.entryTypes == null: names no entry type, so it covers
      // every registered entry type's version, whatever the interest
      // actually matches at fold time.
      final spec = _spec();
      final promoters = PromoterRegistry();
      final v1 = EntryTypeRegistry()
        ..register(_def('unrelated', const EntryTypeVersion(1, 0)));
      final v2 = EntryTypeRegistry()
        ..register(_def('unrelated', const EntryTypeVersion(1, 1)));

      expect(
        viewFingerprint(spec, v1, promoters),
        isNot(viewFingerprint(spec, v2, promoters)),
      );
    });

    test('differs for includeSystemEvents', () {
      final entryTypes = EntryTypeRegistry();
      final promoters = PromoterRegistry();
      final specA = _spec(entryTypeIds: {'note'});
      final specB = _spec(entryTypeIds: {'note'}, includeSystemEvents: true);

      expect(
        viewFingerprint(specA, entryTypes, promoters),
        isNot(viewFingerprint(specB, entryTypes, promoters)),
      );
    });

    test('covers table-projection shape (row key, row data)', () {
      final entryTypes = EntryTypeRegistry();
      final promoters = PromoterRegistry();
      const specA = TableProjectionSpec(
        viewName: 'toy_table',
        interest: SubscriptionFilter(entryTypes: {'note'}),
        insertEventTypes: {'created'},
        removeEventTypes: {'deleted'},
        rowKey: AggregateIdKey(),
        rowData: WholePayload(),
      );
      const specB = TableProjectionSpec(
        viewName: 'toy_table',
        interest: SubscriptionFilter(entryTypes: {'note'}),
        insertEventTypes: {'created'},
        removeEventTypes: {'deleted'},
        rowKey: CompositeKey(['data.id']),
        rowData: WholePayload(),
      );

      expect(
        viewFingerprint(specA, entryTypes, promoters),
        isNot(viewFingerprint(specB, entryTypes, promoters)),
      );
    });
  });
}
