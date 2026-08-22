// lib/src/permissions/permission_seed_applier.dart
// Implements: EVS-PRD-permissions-as-events/A
// emits permission_granted
//   events for any seed grants not yet in the log, ensuring all grants are
//   recorded as events alongside other state changes.
// Implements: EVS-PRD-permissions-as-events/C
// idempotent application
//   ensures the event log alone is sufficient to reconstruct permission state;
//   re-running the applier against an already-populated store emits nothing.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
class SeedApplyResult {
  const SeedApplyResult({
    required this.grantsEmitted,
    required this.grantsAlreadyPresent,
    required this.grantsInViewNotInSeed,
  });

  final int grantsEmitted;
  final int grantsAlreadyPresent;
  final List<String> grantsInViewNotInSeed; // aggregate ids
}

class PermissionSeedApplier {
  PermissionSeedApplier({
    required this.eventStore,
    required this.seedInitiator,
  });

  final EventStore eventStore;
  final Initiator seedInitiator;

  Future<SeedApplyResult> apply(
    PermissionSeed seed,
    Set<Permission> declared,
  ) async {
    final declaredByName = <String, Permission>{
      for (final p in declared) p.name: p,
    };

    // Read current grants in view. Reconstruct the '<role>:<permName>'
    // pair-id (matching the events' aggregateId) from the row payload —
    // the row itself does not carry the storage key.
    final rows = await eventStore.backend.findViewRows(
      'role_permission_grants',
    );
    final inView = <String>{
      for (final r in rows) '${r['role']}:${r['permissionName']}',
    };

    // Compute pairs implied by seed.
    final inSeed = <String>{};
    for (final entry in seed.grants.entries) {
      for (final perm in entry.value) {
        inSeed.add('${entry.key}:$perm');
      }
    }

    final missing = inSeed.difference(inView);
    final present = inSeed.intersection(inView);
    final drift = inView.difference(inSeed).toList()..sort();

    // Emit a permission_granted for each missing.
    for (final id in missing) {
      final colonIx = id.indexOf(':');
      final role = id.substring(0, colonIx);
      final permName = id.substring(colonIx + 1);
      final perm = declaredByName[permName];
      if (perm == null) {
        // Validator should have caught this earlier; defensive.
        throw StateError(
          'permission $permName not in declaredPermissions during apply',
        );
      }
      await eventStore.append(
        entryType: 'role_permission_grant',
        aggregateType: 'role_permission_grant',
        aggregateId: id,
        eventType: 'permission_granted',
        data: PermissionGrantedPayload(
          role: role,
          permissionName: permName,
        ).toJson(),
        initiator: seedInitiator,
      );
    }

    return SeedApplyResult(
      grantsEmitted: missing.length,
      grantsAlreadyPresent: present.length,
      grantsInViewNotInSeed: drift,
    );
  }
}
