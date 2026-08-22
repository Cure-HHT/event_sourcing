// lib/src/permissions/bootstrap_role_assignments.dart
// Implements: EVS-PRD-permissions-as-events/A
// emits role_assigned events
//   for any seed entries not yet in the log, ensuring user-role-scope
//   assignments are recorded as first-class events alongside other state
//   changes.
// Implements: EVS-PRD-permissions-as-events/C
// idempotent application
//   ensures the event log alone is sufficient to reconstruct role-assignment
//   state; re-running against an already-populated store emits nothing.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
class RoleAssignmentSeedResult {
  const RoleAssignmentSeedResult({
    required this.entriesEmitted,
    required this.entriesAlreadyPresent,
    required this.entriesInViewNotInSeed,
  });

  /// Number of `role_assigned` events appended on this run.
  final int entriesEmitted;

  /// Number of seed entries that were already represented in the
  /// `user_role_scopes` view at apply time (no event emitted for them).
  final int entriesAlreadyPresent;

  /// Aggregate ids present in the view but absent from the seed. Reported
  /// for observability; bootstrap does not auto-unassign drift.
  final List<String> entriesInViewNotInSeed;
}

/// Bootstraps `user_role_scopes` from a declarative seed.
///
/// Reads the current `user_role_scopes` view, computes the set of aggregate
/// ids implied by [seed], and emits one `role_assigned` event for every
/// entry not yet represented in the view. Re-running with the same seed
/// emits nothing.
///
/// Aggregate id is the canonical-JSON encoding of `(user_id, role, scope)`
/// produced by [computeRoleAssignmentAggregateId]; the same convention is used by
/// the `userRoleScopesSpec` projection's `AggregateIdKey` rowKey so the
/// emitted events land on the rows this function reconstructs.
Future<RoleAssignmentSeedResult> bootstrapRoleAssignments({
  required EventStore eventStore,
  required RoleAssignmentSeed seed,
  Initiator seedInitiator = const AutomationInitiator(
    service: 'event_sourcing_role_assignments_seed',
  ),
}) async {
  // 1. Compute the aggregate-id -> entry map implied by the seed.
  final inSeed = <String, RoleAssignmentSeedEntry>{
    for (final e in seed.entries)
      computeRoleAssignmentAggregateId(
        userId: e.userId,
        role: e.role,
        scope: e.scope,
      ): e,
  };

  // 2. Reconstruct the aggregate-id set currently materialized in the
  //    user_role_scopes view. The row payload carries user_id / role /
  //    scope; the storage key is not surfaced by findViewRows, so we
  //    rebuild the aggregate id from the row body.
  final rows = await eventStore.backend.findViewRows('user_role_scopes');
  final inView = <String>{};
  for (final r in rows) {
    final scope = ScopeValue.fromJson(
      (r['scope']! as Map).cast<String, Object?>(),
    );
    inView.add(
      computeRoleAssignmentAggregateId(
        userId: r['user_id']! as String,
        role: r['role']! as String,
        scope: scope,
      ),
    );
  }

  // 3. Diff: emit role_assigned for seed entries missing from the view,
  //    report drift for view rows missing from the seed.
  final inSeedIds = inSeed.keys.toSet();
  final missing = inSeedIds.difference(inView);
  final present = inSeedIds.intersection(inView);
  final drift = inView.difference(inSeedIds).toList()..sort();

  for (final aggId in missing) {
    final entry = inSeed[aggId]!;
    await eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: aggId,
      eventType: 'role_assigned',
      data: RoleAssignedPayload(
        userId: entry.userId,
        role: entry.role,
        scope: entry.scope,
      ).toJson(),
      initiator: seedInitiator,
    );
  }

  return RoleAssignmentSeedResult(
    entriesEmitted: missing.length,
    entriesAlreadyPresent: present.length,
    entriesInViewNotInSeed: drift,
  );
}
