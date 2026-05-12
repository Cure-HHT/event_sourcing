// lib/src/permissions/bootstrap_action_permissions.dart
// Implements: EVS-PRD-permissions-as-events/A — orchestrates the full
//   bootstrap: loads seed, validates it, then emits permission_granted
//   events into the event log for any missing grants, so that all grants
//   are recorded as first-class events.
// Implements: EVS-PRD-permissions-as-events/B — constructs a
//   TableBackedAuthorizationPolicy over MaterializedViewRoleMatrixReader,
//   ensuring the resulting policy evaluates decisions solely from the
//   event-derived projection (load seed -> validate -> emit events
//   -> construct policy).
// Implements: EVS-PRD-permissions-as-events/C — idempotent event emission
//   guarantees that the event log alone is sufficient to reproduce the
//   permission state; re-running bootstrap produces no new events when the
//   log already contains all required grants.

import 'package:event_sourcing/event_sourcing.dart';

/// Bootstraps the role-permission matrix from a YAML seed.
///
/// Provide either [yamlPath] (loads from disk) or [yamlSource] (loads from
/// string); supplying both, or neither, is a programmer error.
Future<AuthorizationPolicyBootstrap> bootstrapActionPermissions({
  required EventStore eventStore,
  required Set<Permission> declaredPermissions,
  String? yamlPath,
  String? yamlSource,
  Initiator seedInitiator = const AutomationInitiator(
    service: 'event_sourcing_permissions_seed',
  ),
}) async {
  if ((yamlPath == null) == (yamlSource == null)) {
    throw ArgumentError(
      'exactly one of yamlPath or yamlSource must be provided',
    );
  }

  // 1. Load seed.
  final loader = YamlSeedLoader();
  final seed = yamlSource != null
      ? loader.loadFromString(yamlSource)
      : loader.loadFromFile(yamlPath!);

  // 2. Validate.
  final validation = SeedValidator().validate(seed, declaredPermissions);
  if (validation is SeedInvalid) {
    return PolicyFailSafe(validation.errors);
  }

  // 3. Apply seed (emit missing grants).
  final applier = EventSeedApplier(
    eventStore: eventStore,
    seedInitiator: seedInitiator,
  );
  await applier.apply(seed, declaredPermissions);

  // 4. Wrap in TableBackedAuthorizationPolicy over MaterializedViewRoleMatrixReader.
  final reader = MaterializedViewRoleMatrixReader(eventStore.backend);
  final policy = TableBackedAuthorizationPolicy(reader);
  return PolicyReady(policy);
}
