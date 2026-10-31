import 'package:event_sourcing/src/causal_record.dart';
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
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';

EntryTypeDefinition _def(String id, EntryTypeVersion version) =>
    EntryTypeDefinition(id: id, registeredVersion: version, name: id);

EntryTypeDefinition _declaring(
  String id,
  EntryTypeVersion version,
  List<String> eventTypes, {
  int patch = 0,
}) => EntryTypeDefinition(
  id: id,
  registeredVersion: version,
  patch: patch,
  name: id,
  declarations: [
    for (final e in eventTypes)
      EventTypeDeclaration(
        eventType: e,
        kind: CausalKind.version,
        eligible: true,
      ),
  ],
);

EntryTypeDefinition _systemDef(String id, EntryTypeVersion version) =>
    EntryTypeDefinition(id: id, registeredVersion: version, name: id);

AggregateProjectionSpec _eventSpec(Set<String>? eventTypes) =>
    AggregateProjectionSpec(
      viewName: 'toy_view',
      interest: SubscriptionFilter(eventTypes: eventTypes),
      tombstoneEventTypes: const {'toy_deleted'},
    );

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

    // Verifies: EVS-DEV-view-convergence/A
    test("registering an entry type that declares none of a view's event "
        'types leaves its fingerprint unchanged', () {
      final spec = _eventSpec({'note_created'});
      final promoters = PromoterRegistry();
      final before = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 0), ['note_created']),
        );
      final after = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 0), ['note_created']),
        )
        ..register(
          _declaring('photo', const EntryTypeVersion(1, 0), ['photo_taken']),
        );

      expect(
        viewFingerprint(spec, after, promoters),
        viewFingerprint(spec, before, promoters),
      );
    });

    // Verifies: EVS-DEV-view-convergence/A
    test('adding an event type to an entry type (a patch) changes no '
        'fingerprint', () {
      final promoters = PromoterRegistry();
      final before = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 0), ['note_created']),
        );
      final after = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 0), [
            'note_created',
            'note_edited',
          ], patch: 1),
        );
      for (final spec in [
        _eventSpec({'note_created'}),
        _eventSpec(null),
        _spec(entryTypeIds: {'note'}),
      ]) {
        expect(
          viewFingerprint(spec, after, promoters),
          viewFingerprint(spec, before, promoters),
        );
      }
    });

    // Verifies: EVS-DEV-view-convergence/A
    test("a minor of an entry type declaring one of the view's event types "
        'changes its fingerprint', () {
      final spec = _eventSpec({'note_created'});
      final promoters = PromoterRegistry();
      final v1 = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 0), ['note_created']),
        );
      final v2 = EntryTypeRegistry()
        ..register(
          _declaring('note', const EntryTypeVersion(1, 1), ['note_created']),
        );

      expect(
        viewFingerprint(spec, v1, promoters),
        isNot(viewFingerprint(spec, v2, promoters)),
      );
    });

    // Verifies: EVS-DEV-view-convergence/A
    test('a view filtering only by aggregate type covers every registered '
        'entry type', () {
      const spec = AggregateProjectionSpec(
        viewName: 'toy_view',
        interest: SubscriptionFilter(aggregateTypes: {'diary'}),
        tombstoneEventTypes: {'toy_deleted'},
      );
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

    // Verifies: EVS-DEV-view-convergence/A
    test("a minor of the security-finding entry type changes every view's "
        'fingerprint', () {
      final promoters = PromoterRegistry();
      for (final spec in [
        _eventSpec({'note_created'}),
        _spec(entryTypeIds: {'note'}),
        _spec(),
      ]) {
        final v1 = EntryTypeRegistry()
          ..register(_def('note', const EntryTypeVersion(1, 0)))
          ..register(
            _systemDef(kSecurityFindingEntryType, const EntryTypeVersion(1, 0)),
          );
        final v2 = EntryTypeRegistry()
          ..register(_def('note', const EntryTypeVersion(1, 0)))
          ..register(
            _systemDef(kSecurityFindingEntryType, const EntryTypeVersion(1, 1)),
          );

        expect(
          viewFingerprint(spec, v1, promoters),
          isNot(viewFingerprint(spec, v2, promoters)),
        );
      }
    });

    // Verifies: EVS-DEV-view-convergence/A
    test('a view whose event types no entry type declares still covers the '
        'reserved entry types', () {
      final spec = _eventSpec({'nobody_declares_this'});
      final named = _spec(entryTypeIds: {'note'});
      final promoters = PromoterRegistry();
      final v1 = EntryTypeRegistry()
        ..register(
          _systemDef(
            kDestinationSenderSucceededEntryType,
            const EntryTypeVersion(1, 0),
          ),
        );
      final v2 = EntryTypeRegistry()
        ..register(
          _systemDef(
            kDestinationSenderSucceededEntryType,
            const EntryTypeVersion(1, 1),
          ),
        );

      expect(
        viewFingerprint(spec, v1, promoters),
        isNot(viewFingerprint(spec, v2, promoters)),
      );
      expect(
        viewFingerprint(named, v1, promoters),
        isNot(viewFingerprint(named, v2, promoters)),
      );
    });

    // Verifies: EVS-DEV-view-convergence/A
    test('a view including system events covers the reserved entry types it '
        'can admit', () {
      final promoters = PromoterRegistry();
      EntryTypeRegistry registry(int minor) => EntryTypeRegistry()
        ..register(
          _declaring(kDestinationWedgedEntryType, EntryTypeVersion(1, minor), [
            'wedged',
          ]),
        );
      const systemOnly = AggregateProjectionSpec(
        viewName: 'toy_view',
        interest: SubscriptionFilter(entryTypes: {}, includeSystemEvents: true),
        tombstoneEventTypes: {'toy_deleted'},
      );
      const otherEvents = AggregateProjectionSpec(
        viewName: 'toy_view',
        interest: SubscriptionFilter(
          entryTypes: {},
          eventTypes: {'something_else'},
          includeSystemEvents: true,
        ),
        tombstoneEventTypes: {'toy_deleted'},
      );

      expect(
        viewFingerprint(systemOnly, registry(0), promoters),
        isNot(viewFingerprint(systemOnly, registry(1), promoters)),
      );
      expect(
        viewFingerprint(otherEvents, registry(0), promoters),
        viewFingerprint(otherEvents, registry(1), promoters),
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
