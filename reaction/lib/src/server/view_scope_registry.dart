// Implements: EVS-PRD-cross-process-event-transport/E
// supplies the
//   viewName -> scope-class binding the subscription handler consults
//   when narrowing each per-subscription request to the requesting
//   Principal's EffectiveAuthorization.

import 'package:event_sourcing/event_sourcing.dart';

/// Maps a substrate [ProjectionSpec.viewName] to its scope-class
/// binding (which scope class scopes this view, and how to resolve
/// a ScopeValue assigned to the user into the aggregate IDs the user
/// covers under that scope). Composition-time registration.
///
/// A view without a registration is unscoped at the row level
/// (admin views, public views, etc.). The reaction server's per-
/// subscription authorization treats unregistered views as "no
/// row-level narrowing required."
class ViewScopeRegistry {
  ViewScopeRegistry();

  final Map<String, ViewScopeBinding> _bindings = {};

  /// Register a view-to-scope-class binding.
  ///
  /// - [viewName]: matches a registered ProjectionSpec.viewName.
  /// - [scopeClass]: the scope class scoping this view (e.g., 'site',
  ///   'patient'). Must match a registered ScopeClassSpec.name.
  /// - [aggregateIdResolver]: given a BoundScope (or ValueWildcardScope),
  ///   returns the aggregate ID this scope value corresponds to on
  ///   the view. For 1:1 mappings (patient scope value = patient
  ///   aggregate id), this is `(sv) => sv.value` for BoundScope and
  ///   `null` (or the full row scan via containment) for wildcards.
  ///   The server's expansion logic walks the containment graph as
  ///   needed; this resolver handles the "direct" case.
  void register({
    required String viewName,
    required String scopeClass,
    required String? Function(ScopeValue) aggregateIdResolver,
  }) {
    if (_bindings.containsKey(viewName)) {
      throw ArgumentError(
        'ViewScopeRegistry: duplicate registration for "$viewName"',
      );
    }
    _bindings[viewName] = ViewScopeBinding._(
      viewName: viewName,
      scopeClass: scopeClass,
      aggregateIdResolver: aggregateIdResolver,
    );
  }

  ViewScopeBinding? lookup(String viewName) => _bindings[viewName];
}

class ViewScopeBinding {
  ViewScopeBinding._({
    required this.viewName,
    required this.scopeClass,
    required this.aggregateIdResolver,
  });

  final String viewName;
  final String scopeClass;
  final String? Function(ScopeValue) aggregateIdResolver;
}
