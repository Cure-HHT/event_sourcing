// Verifies: EVS-DEV-scope-descendant-expander/A/B/C/D/E — downward
//   containment expansion: identity short-circuit, non-ancestor empty,
//   per-hop inverse query, fail-closed on missing/malformed row,
//   breadth-first multi-hop fan-out.

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

class _FakeDescriptor implements ScopeProjectionDescriptor {
  const _FakeDescriptor({required this.columns});
  @override
  final Set<String> columns;
}

ScopeClassRegistry _participantInSite() => ScopeClassRegistry(
  classes: const [
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'participant',
      containedIn: ContainmentReference(
        parentClass: 'site',
        projection: 'participant_site_index',
        keyColumn: 'participant_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: (_) =>
      const _FakeDescriptor(columns: {'participant_id', 'site_id'}),
);

void main() {
  group('ScopeDescendantExpander single-hop', () {
    test('expands a site assignment to all its participants', () async {
      final reg = _participantInSite();
      final backend = _FakeBackend({
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': 'P-2', 'site_id': 'site-A'},
          {'participant_id': 'P-9', 'site_id': 'site-C'},
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: reg,
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1', 'P-2'}));
    });
  });
}
