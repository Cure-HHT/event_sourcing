// lib/src/permissions/seed_validator.dart
// Implements: EVS-PRD-permissions-as-events/A — validates the seed before
// any events are emitted; an invalid seed results in PolicyFailSafe rather
// than emitting malformed permission grants into the event log.

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
  SeedValidationResult validate(PermissionSeed seed, Set<Permission> declared) {
    final errors = <String>[];
    final declaredNames = declared.map((p) => p.name).toSet();

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

    return errors.isEmpty ? const SeedValid() : SeedInvalid(errors);
  }
}
