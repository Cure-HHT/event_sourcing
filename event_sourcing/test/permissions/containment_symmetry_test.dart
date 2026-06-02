// Verifies: EVS-DEV-scope-descendant-expander — the read-path expander and
//   the write-path ContainmentResolver traverse the SAME index data in
//   opposite directions and must agree: if the resolver maps a participant
//   UP to a site, the expander must include that participant when expanding
//   the site DOWN. Guards against the two directions silently drifting.

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

void main() {
  test('expander down(site) contains every participant resolver maps up to '
      'that site', () async {
    final reg = ScopeClassRegistry(
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
    final backend = _FakeBackend({
      'participant_site_index': [
        {'participant_id': 'P-1', 'site_id': 'site-A'},
        {'participant_id': 'P-2', 'site_id': 'site-A'},
        {'participant_id': 'P-9', 'site_id': 'site-C'},
      ],
    });
    final resolver = ContainmentResolver(
      registry: reg,
      findRowsInTxn: backend.findViewRowsInTxn,
    );
    final expander = ScopeDescendantExpander(
      registry: reg,
      findRowsInTxn: backend.findViewRowsInTxn,
    );

    final downFromA = await expander.expand(
      txn: const _FakeTxn(),
      assignment: const BoundScope(class_: 'site', value: 'site-A'),
      targetClass: 'participant',
    );

    // Completeness: the expander returns EXACTLY site-A's participants —
    // not a subset. Guards against an under-expansion regression that the
    // per-element soundness loop below would not catch (it only checks that
    // whatever IS returned resolves back to site-A).
    expect(downFromA, equals({'P-1', 'P-2'}));

    for (final pid in downFromA) {
      final up = await resolver.resolve(
        txn: const _FakeTxn(),
        from: BoundScope(class_: 'participant', value: pid),
        target: 'site',
      );
      expect(
        up,
        equals(const BoundScope(class_: 'site', value: 'site-A')),
        reason: 'expander included $pid for site-A but resolver disagrees',
      );
    }
    // And the converse: a participant at a different site is NOT in the set.
    expect(downFromA.contains('P-9'), isFalse);
  });
}
