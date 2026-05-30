// Verifies: EVS-PRD-permissions-as-events (composition-time validation refuses cycles, dangling refs, missing columns)
// Verifies: EVS-PRD-scoped-permissions/B — composition refuses on duplicates,
//   dangling refs, missing columns, and cycles.
// Verifies: EVS-DEV-scope-class-registry-validation/A/B/C/D/E — duplicate-name
//   refusal, dangling parentClass, projection / column resolution, cycle
//   detection, and ancestor-chain walk.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClassRegistry', () {
    test('accepts a flat registry of top-level classes', () {
      final r = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(name: 'lab'),
        ],
        projectionLookup: (_) => const _FakeProjection(columns: {}),
      );
      expect(r.byName('site')!.name, 'site');
      expect(r.byName('lab')!.name, 'lab');
      expect(r.byName('nonexistent'), isNull);
    });

    test('accepts a hierarchy of two classes', () {
      final r = ScopeClassRegistry(
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
      expect(r.byName('patient')!.containedIn!.parentClass, 'site');
    });

    test('rejects duplicate class names', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(name: 'site'),
            ScopeClassSpec(name: 'site'),
          ],
          projectionLookup: (_) => null,
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('duplicate'),
          ),
        ),
      );
    });

    test('rejects parentClass that is not a registered class', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
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
          projectionLookup: (_) =>
              const _FakeProjection(columns: {'patient_id', 'site_id'}),
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('parentClass'),
          ),
        ),
      );
    });

    test('rejects projection that is not registered', () {
      expect(
        () => ScopeClassRegistry(
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
          projectionLookup: (_) => null,
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('projection'),
          ),
        ),
      );
    });

    test('rejects projection missing the keyColumn or parentColumn', () {
      expect(
        () => ScopeClassRegistry(
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
          projectionLookup: (_) =>
              const _FakeProjection(columns: {'patient_id'}),
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('parentColumn'),
          ),
        ),
      );
    });

    test('rejects cycles in the containment graph', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(
              name: 'a',
              containedIn: ContainmentReference(
                parentClass: 'b',
                projection: 'p',
                keyColumn: 'k',
                parentColumn: 'pc',
              ),
            ),
            ScopeClassSpec(
              name: 'b',
              containedIn: ContainmentReference(
                parentClass: 'a',
                projection: 'p',
                keyColumn: 'k',
                parentColumn: 'pc',
              ),
            ),
          ],
          projectionLookup: (_) => const _FakeProjection(columns: {'k', 'pc'}),
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('cycle'),
          ),
        ),
      );
    });

    test('ancestorChain returns chain from class to top', () {
      final r = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'region'),
          ScopeClassSpec(
            name: 'site',
            containedIn: ContainmentReference(
              parentClass: 'region',
              projection: 'site_region',
              keyColumn: 'site_id',
              parentColumn: 'region_id',
            ),
          ),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentReference(
              parentClass: 'site',
              projection: 'patient_site',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (_) => const _FakeProjection(
          columns: {'patient_id', 'site_id', 'region_id'},
        ),
      );
      expect(r.ancestorChain('patient').map((s) => s.name), [
        'patient',
        'site',
        'region',
      ]);
      expect(r.ancestorChain('region').map((s) => s.name), ['region']);
    });
  });
}

class _FakeProjection implements ScopeProjectionDescriptor {
  const _FakeProjection({required this.columns});
  @override
  final Set<String> columns;
}
