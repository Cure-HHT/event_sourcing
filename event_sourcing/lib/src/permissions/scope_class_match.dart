// lib/src/permissions/scope_class_match.dart
// The single source of truth for the scope "class gate": given an assigned
// scope and a target scope class, classify whether the assignment's class
// applies to the target — exactly, via an ancestor relationship, or not at
// all. Both the write-path policy (TableBackedAuthorizationPolicy._matches)
// and the read-path row-narrowing (reaction's _expandAssignments) call this
// so the two paths agree by construction and the class gate cannot drift.
//
// This helper classifies CLASS only. It carries no payload: the bound value,
// the resolved containment id, and the accumulation of allowed aggregates
// are each caller responsibilities. Callers already hold the ScopeValue and
// do their own value-equality / containment resolution / row accumulation.
//
// Implements: EVS-DEV-scoped-permissions-match-algorithm — the class-and-
//   ancestry gate shared by the bound, value-wildcard, and total-wildcard
//   variants per Section 2 of spec/scoped-permissions.md.

import 'package:event_sourcing/event_sourcing.dart';

/// Three-way classification of an assigned scope's class against a target
/// scope class.
///
/// Deliberately three-way (not boolean): the read path treats
/// [appliesExact] (resolve to an aggregate id) differently from
/// [appliesViaAncestor] (containment resolution, which it does not yet
/// plumb, so it conservatively defers).
enum ScopeClassMatch {
  /// The assigned scope's class neither equals nor is an ancestor of the
  /// target class. The assignment does not apply to the target.
  doesNotApply,

  /// The assigned scope's class equals the target class (or the assignment
  /// is a [TotalWildcardScope], which covers everything and carries no
  /// class).
  appliesExact,

  /// The assigned scope's class is a strict ancestor of the target class
  /// (per the [ScopeClassRegistry] containment graph).
  appliesViaAncestor,
}

/// Classify [assigned]'s scope class against [targetClass].
///
/// Rules (these MUST match both call sites exactly):
/// - [TotalWildcardScope] → [ScopeClassMatch.appliesExact] (covers
///   everything; carries no class).
/// - [ValueWildcardScope] / [BoundScope] with class `c`:
///   - `c == targetClass` → [ScopeClassMatch.appliesExact].
///   - `registry.isAncestor(c, targetClass)` → [ScopeClassMatch.appliesViaAncestor].
///   - otherwise → [ScopeClassMatch.doesNotApply].
ScopeClassMatch scopeClassMatch(
  ScopeValue assigned,
  String targetClass,
  ScopeClassRegistry registry,
) {
  switch (assigned) {
    case TotalWildcardScope():
      return ScopeClassMatch.appliesExact;
    case ValueWildcardScope(class_: final c):
    case BoundScope(class_: final c):
      if (c == targetClass) return ScopeClassMatch.appliesExact;
      if (registry.isAncestor(c, targetClass)) {
        return ScopeClassMatch.appliesViaAncestor;
      }
      return ScopeClassMatch.doesNotApply;
  }
}
