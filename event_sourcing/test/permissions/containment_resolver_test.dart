// Verifies: EVS-PRD-permissions-as-events (containment lookup via projection; fail-closed on miss)
// Verifies: EVS-PRD-scoped-permissions/G — fail-closed on missing containment
//   rows.
// Verifies: EVS-DEV-containment-resolver/A/B/C/D — identity on equal class,
//   null on non-ancestor target, per-hop projection read, and fail-closed
//   on empty row / missing parent column.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _FakeBackend {
  _FakeBackend(this.rows);
  final Map<String, List<Map<String, dynamic>>> rows;

  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final all = rows[viewName] ?? [];
    if (where == null) return all;
    return all
        .where((r) => where.entries.every((e) => r[e.key] == e.value))
        .toList();
  }
}

class _FakeTxn extends Transaction {
  const _FakeTxn();
}

void main() {
  group('ContainmentResolver', () {
    test('resolves a single-hop containment', () async {
      final reg = ScopeClassRegistry(
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
        projectionLookup: (n) =>
            const _FakeDescriptor(columns: {'patient_id', 'site_id'}),
      );
      final backend = _FakeBackend({
        'patient_site_index': [
          {'patient_id': 'P-42', 'site_id': 'A'},
        ],
      });
      final resolver = ContainmentResolver(
        registry: reg,
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await resolver.resolve(
        txn: const _FakeTxn(),
        from: const BoundScope(class_: 'patient', value: 'P-42'),
        target: 'site',
      );
      expect(result, equals(const BoundScope(class_: 'site', value: 'A')));
    });

    test('resolves a two-hop containment', () async {
      final reg = ScopeClassRegistry(
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
        projectionLookup: (n) => const _FakeDescriptor(
          columns: {'patient_id', 'site_id', 'region_id'},
        ),
      );
      final backend = _FakeBackend({
        'patient_site': [
          {'patient_id': 'P-42', 'site_id': 'A'},
        ],
        'site_region': [
          {'site_id': 'A', 'region_id': 'East'},
        ],
      });
      final resolver = ContainmentResolver(
        registry: reg,
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await resolver.resolve(
        txn: const _FakeTxn(),
        from: const BoundScope(class_: 'patient', value: 'P-42'),
        target: 'region',
      );
      expect(result, equals(const BoundScope(class_: 'region', value: 'East')));
    });

    test(
      'returns null when intermediate row is missing (fail-closed)',
      () async {
        final reg = ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(name: 'site'),
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
          projectionLookup: (n) =>
              const _FakeDescriptor(columns: {'patient_id', 'site_id'}),
        );
        final backend = _FakeBackend({'patient_site': []});
        final resolver = ContainmentResolver(
          registry: reg,
          findRowsInTxn: backend.findViewRowsInTxn,
        );
        final result = await resolver.resolve(
          txn: const _FakeTxn(),
          from: const BoundScope(class_: 'patient', value: 'P-42'),
          target: 'site',
        );
        expect(result, isNull);
      },
    );

    test('returns from itself when target equals from.class_', () async {
      final reg = ScopeClassRegistry(
        classes: const [ScopeClassSpec(name: 'site')],
        projectionLookup: (_) => null,
      );
      final resolver = ContainmentResolver(
        registry: reg,
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      final result = await resolver.resolve(
        txn: const _FakeTxn(),
        from: const BoundScope(class_: 'site', value: 'A'),
        target: 'site',
      );
      expect(result, equals(const BoundScope(class_: 'site', value: 'A')));
    });

    test('returns null when target is not in from\'s ancestor chain', () async {
      final reg = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(name: 'lab'),
        ],
        projectionLookup: (_) => null,
      );
      final resolver = ContainmentResolver(
        registry: reg,
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      final result = await resolver.resolve(
        txn: const _FakeTxn(),
        from: const BoundScope(class_: 'site', value: 'A'),
        target: 'lab',
      );
      expect(result, isNull);
    });
  });
}

class _FakeDescriptor implements ScopeProjectionDescriptor {
  const _FakeDescriptor({required this.columns});
  @override
  final Set<String> columns;
}
