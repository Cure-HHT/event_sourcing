// lib/src/permissions/seed_validator.dart
// Implements: EVS-DEV-bootstrap-action-permissions/B
// validates the seed
// before any events are emitted (unknown permission name, scope-class
// reference not registered, etc.); an invalid seed drives the PolicyFailSafe
// path rather than emitting malformed permission grants into the event log.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
sealed class SeedValidationResult {
  const SeedValidationResult();
}

final class SeedValid extends SeedValidationResult {
  const SeedValid();
}

final class SeedInvalid extends SeedValidationResult {
  const SeedInvalid(this.errors);
  final List<String> errors;
}

class SeedValidator {
  SeedValidationResult validate(
    PermissionSeed seed,
    Set<Permission> declared, {
    ScopeClassRegistry? scopeClassRegistry,
  }) {
    final errors = <String>[];
    final declaredByName = {for (final p in declared) p.name: p};
    final declaredNames = declaredByName.keys.toSet();

    for (final role in seed.roles) {
      if (role.contains(':')) {
        errors.add("role name contains ':': $role");
      }
    }
    for (final entry in seed.grants.entries) {
      for (final perm in entry.value) {
        if (perm.contains(':')) {
          errors.add("permission name contains ':': $perm");
        }
      }
    }

    for (final role in seed.grants.keys) {
      if (!seed.roles.contains(role)) {
        errors.add('grant key "$role" not in roles list');
      }
    }

    for (final role in seed.roles) {
      if (!seed.grants.containsKey(role)) {
        errors.add('role "$role" missing from grants');
      }
    }

    for (final entry in seed.grants.entries) {
      for (final perm in entry.value) {
        if (!declaredNames.contains(perm)) {
          errors.add(
            'permission "$perm" granted to "${entry.key}" not declared by any Action',
          );
        }
      }
    }

    // Scope-class registry check: any permission granted by the seed that
    // declares a scopeClass must resolve against the supplied registry.
    // Skipped when no registry is supplied (legacy/unscoped callers).
    if (scopeClassRegistry != null) {
      // Track the (perm, scopeClass) pairs already reported so a
      // permission granted to multiple roles only produces one error.
      final reported = <String>{};
      for (final entry in seed.grants.entries) {
        for (final permName in entry.value) {
          final perm = declaredByName[permName];
          final scopeClass = perm?.scopeClass;
          if (scopeClass == null) continue;
          if (scopeClassRegistry.byName(scopeClass) != null) continue;
          final key = '$permName::$scopeClass';
          if (!reported.add(key)) continue;
          final registered = scopeClassRegistry.all
              .map((c) => c.name)
              .join(', ');
          errors.add(
            'permission "$permName" declares scopeClass '
            '"$scopeClass" which is not in ScopeClassRegistry '
            '(registered classes: $registered)',
          );
        }
      }
    }

    return errors.isEmpty ? const SeedValid() : SeedInvalid(errors);
  }
}
