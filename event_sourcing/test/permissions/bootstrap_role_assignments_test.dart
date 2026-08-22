// test/permissions/bootstrap_role_assignments_test.dart
// Verifies: EVS-PRD-permissions-as-events/A
// bootstrapRoleAssignments
//   emits role_assigned events for each seed entry, so user-role-scope
//   assignments are recorded as first-class events in the same log.
// Verifies: EVS-PRD-permissions-as-events/C
// idempotent re-runs emit no
//   new events; partial-overlap runs emit only the truly-missing entries;
//   the event log alone suffices to reconstruct role-assignment state.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_support/sembast_event_store_harness.dart';

void main() {
  group('bootstrapRoleAssignments', () {
    late SembastEventStoreHarness harness;

    setUp(() async {
      harness = await SembastEventStoreHarness.create(
        projectionSpecs: [userRoleScopesSpec],
      );
    });

    tearDown(() async {
      await harness.close();
    });

    test('emits role_assigned for every entry when view is empty', () async {
      const seed = RoleAssignmentSeed(
        entries: [
          RoleAssignmentSeedEntry(
            userId: 'u1',
            role: 'admin',
            scope: TotalWildcardScope(),
          ),
          RoleAssignmentSeedEntry(
            userId: 'u2',
            role: 'investigator',
            scope: BoundScope(class_: 'study', value: 'STUDY-A'),
          ),
          RoleAssignmentSeedEntry(
            userId: 'u3',
            role: 'reviewer',
            scope: ValueWildcardScope(class_: 'site'),
          ),
        ],
      );

      final result = await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: seed,
      );

      expect(result.entriesEmitted, 3);
      expect(result.entriesAlreadyPresent, 0);
      expect(result.entriesInViewNotInSeed, isEmpty);

      final rows = await harness.findRows('user_role_scopes');
      expect(rows, hasLength(3));
    });

    test('re-running with same seed emits zero events (idempotent)', () async {
      const seed = RoleAssignmentSeed(
        entries: [
          RoleAssignmentSeedEntry(
            userId: 'u1',
            role: 'admin',
            scope: TotalWildcardScope(),
          ),
          RoleAssignmentSeedEntry(
            userId: 'u2',
            role: 'investigator',
            scope: BoundScope(class_: 'study', value: 'STUDY-A'),
          ),
        ],
      );

      final first = await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: seed,
      );
      expect(first.entriesEmitted, 2);

      final second = await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: seed,
      );
      expect(second.entriesEmitted, 0);
      expect(second.entriesAlreadyPresent, 2);
      expect(second.entriesInViewNotInSeed, isEmpty);

      // View still has exactly the two original rows.
      final rows = await harness.findRows('user_role_scopes');
      expect(rows, hasLength(2));
    });

    test('partial-overlap seed emits only truly-missing entries', () async {
      // Pre-populate the log with entry A directly.
      const entryA = RoleAssignmentSeedEntry(
        userId: 'u1',
        role: 'admin',
        scope: TotalWildcardScope(),
      );
      await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: const RoleAssignmentSeed(entries: [entryA]),
      );

      // Now bootstrap with a seed containing {A, B}; only B should emit.
      const seedAB = RoleAssignmentSeed(
        entries: [
          entryA,
          RoleAssignmentSeedEntry(
            userId: 'u2',
            role: 'investigator',
            scope: BoundScope(class_: 'study', value: 'STUDY-A'),
          ),
        ],
      );
      final result = await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: seedAB,
      );

      expect(result.entriesEmitted, 1);
      expect(result.entriesAlreadyPresent, 1);
      expect(result.entriesInViewNotInSeed, isEmpty);

      final rows = await harness.findRows('user_role_scopes');
      expect(rows, hasLength(2));
    });

    test('reports drift (view rows not in seed) without unassigning', () async {
      // Seed and apply A.
      const entryA = RoleAssignmentSeedEntry(
        userId: 'u1',
        role: 'admin',
        scope: TotalWildcardScope(),
      );
      await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: const RoleAssignmentSeed(entries: [entryA]),
      );

      // Apply a seed that no longer contains A. A should be reported as
      // drift; no role_unassigned event is emitted.
      final result = await bootstrapRoleAssignments(
        eventStore: harness.eventStore,
        seed: const RoleAssignmentSeed(entries: []),
      );
      expect(result.entriesEmitted, 0);
      expect(result.entriesAlreadyPresent, 0);
      expect(result.entriesInViewNotInSeed, hasLength(1));

      // Drift entry must reference the correct aggregate id.
      final expectedId = computeRoleAssignmentAggregateId(
        userId: 'u1',
        role: 'admin',
        scope: const TotalWildcardScope(),
      );
      expect(result.entriesInViewNotInSeed.single, expectedId);

      // Row still present in the view (no auto-unassign).
      final rows = await harness.findRows('user_role_scopes');
      expect(rows, hasLength(1));
    });
  });

  group('RoleAssignmentSeedEntry', () {
    test('rejects empty userId via assertion', () {
      expect(
        () => RoleAssignmentSeedEntry(
          userId: '',
          role: 'admin',
          scope: const TotalWildcardScope(),
        ),
        throwsA(isA<AssertionError>()),
      );
    });

    test('rejects empty role via assertion', () {
      expect(
        () => RoleAssignmentSeedEntry(
          userId: 'u1',
          role: '',
          scope: const TotalWildcardScope(),
        ),
        throwsA(isA<AssertionError>()),
      );
    });
  });
}
