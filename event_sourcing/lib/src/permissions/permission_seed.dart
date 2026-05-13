// lib/src/permissions/permission_seed.dart
// Implements: EVS-PRD-permissions-as-events/A — the seed is the declarative
// source of initial role-permission grants; EventSeedApplier realises these
// as permission_granted events written into the event log.

import 'package:meta/meta.dart';

@immutable
class PermissionSeed {
  const PermissionSeed({required this.roles, required this.grants});

  final Set<String> roles;
  final Map<String, Set<String>> grants;
}
