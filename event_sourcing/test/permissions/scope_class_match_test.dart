// Verifies: EVS-DEV-scoped-permissions-match-algorithm
// the shared class
//   gate used by both the write-path policy and the read-path row-narrowing.
//   Includes the regression test for the over-grant bug: a value-wildcard
//   scope on an unrelated class must NOT apply to the target.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('matchScopeClass', () {
    // Two-class containment chain: patient containedIn site. So 'site' is an
    // ancestor of 'patient'; 'patient' is NOT an ancestor of 'site'; and the
    // two are unrelated in the descendant->ancestor direction we don't walk.
    final registry = ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'site'),
        ScopeClassSpec(
          name: 'patient',
          containedIn: ContainmentReference(
            parentClass: 'site',
            projection: 'patient_site_index',
            keyColumn: 'patient_id',
            parentColumn: 'site_id',
          ),
        ),
      ],
      projectionLookup: (name) => name == 'patient_site_index'
          ? const _FakeProjection(columns: {'patient_id', 'site_id'})
          : null,
    );

    test('TotalWildcardScope applies exactly for any target class', () {
      expect(
        matchScopeClass(const TotalWildcardScope(), 'patient', registry),
        ScopeClassMatch.appliesExact,
      );
      expect(
        matchScopeClass(const TotalWildcardScope(), 'site', registry),
        ScopeClassMatch.appliesExact,
      );
    });

    group('ValueWildcardScope', () {
      test('exact class → appliesExact', () {
        expect(
          matchScopeClass(
            const ValueWildcardScope(class_: 'patient'),
            'patient',
            registry,
          ),
          ScopeClassMatch.appliesExact,
        );
      });

      test('ancestor class → appliesViaAncestor', () {
        // 'site' wildcard against a 'patient' target: site IS an ancestor of
        // patient.
        expect(
          matchScopeClass(
            const ValueWildcardScope(class_: 'site'),
            'patient',
            registry,
          ),
          ScopeClassMatch.appliesViaAncestor,
        );
      });

      test('unrelated/cross class → doesNotApply (over-grant regression)', () {
        // 'patient' wildcard against a 'site' target: patient is NOT an
        // ancestor of site. Must NOT apply — this is the security regression
        // guard (a wildcard on the wrong class must not grant the whole
        // target class).
        expect(
          matchScopeClass(
            const ValueWildcardScope(class_: 'patient'),
            'site',
            registry,
          ),
          ScopeClassMatch.doesNotApply,
        );
      });
    });

    group('BoundScope', () {
      test('exact class → appliesExact', () {
        expect(
          matchScopeClass(
            const BoundScope(class_: 'patient', value: 'p1'),
            'patient',
            registry,
          ),
          ScopeClassMatch.appliesExact,
        );
      });

      test('ancestor class → appliesViaAncestor', () {
        expect(
          matchScopeClass(
            const BoundScope(class_: 'site', value: 'site-A'),
            'patient',
            registry,
          ),
          ScopeClassMatch.appliesViaAncestor,
        );
      });

      test('unrelated/cross class → doesNotApply', () {
        expect(
          matchScopeClass(
            const BoundScope(class_: 'patient', value: 'p1'),
            'site',
            registry,
          ),
          ScopeClassMatch.doesNotApply,
        );
      });
    });
  });
}

class _FakeProjection implements ScopeProjectionDescriptor {
  const _FakeProjection({required this.columns});
  @override
  final Set<String> columns;
}
