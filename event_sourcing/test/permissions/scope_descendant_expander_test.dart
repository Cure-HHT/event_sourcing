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

  group('ScopeDescendantExpander edge cases', () {
    ScopeClassRegistry regionSiteParticipant() => ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'region'),
        ScopeClassSpec(
          name: 'site',
          containedIn: ContainmentReference(
            parentClass: 'region',
            projection: 'site_region_index',
            keyColumn: 'site_id',
            parentColumn: 'region_id',
          ),
        ),
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
      projectionLookup: (_) => const _FakeDescriptor(
        columns: {'participant_id', 'site_id', 'region_id'},
      ),
    );

    test('two-hop: region expands to all participants in its sites', () async {
      final backend = _FakeBackend({
        'site_region_index': [
          {'site_id': 'site-A', 'region_id': 'region-West'},
          {'site_id': 'site-B', 'region_id': 'region-West'},
          {'site_id': 'site-C', 'region_id': 'region-East'},
        ],
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': 'P-2', 'site_id': 'site-B'},
          {'participant_id': 'P-9', 'site_id': 'site-C'},
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'region', value: 'region-West'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1', 'P-2'}));
    });

    test('identity: assignment class equals target class', () async {
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'participant', value: 'P-7'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-7'}));
    });

    test('non-ancestor target returns empty set', () async {
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      // participant is NOT an ancestor of site.
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'participant', value: 'P-7'),
        targetClass: 'site',
      );
      expect(result, isEmpty);
    });

    test('empty index (fail-closed) yields empty set', () async {
      final backend = _FakeBackend({'participant_site_index': []});
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, isEmpty);
    });

    test('malformed row (missing/empty key) is skipped', () async {
      final backend = _FakeBackend({
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': '', 'site_id': 'site-A'}, // empty -> skip
          {'site_id': 'site-A'}, // missing key -> skip
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1'}));
    });
  });
}
