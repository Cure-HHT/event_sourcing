// Implements: EVS-PRD-permissions-as-events — raw assignment record exposed to
//   clients/UI via effectivePermissionsFor.
// Implements: EVS-DEV-effective-permissions-shape/C — sub-shape carrying
//   exactly one sealed-variant ScopeValue per assignment.

import 'package:meta/meta.dart';

import '../actions/scope_value.dart';

/// One row of the user's scope assignments under their active role,
/// surfaced to clients via [EffectiveAuthorization].
@immutable
class ScopeAssignment {
  const ScopeAssignment({required this.scope});

  final ScopeValue scope;

  @override
  bool operator ==(Object other) =>
      other is ScopeAssignment && scope == other.scope;

  @override
  int get hashCode => scope.hashCode;

  @override
  String toString() => 'ScopeAssignment($scope)';
}
