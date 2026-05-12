// Implements: EVS-PRD-materializer/A — ProjectionRegistry is the substrate
//   component that holds the active set of materializer rules.  It is the
//   authoritative in-memory table of which projections the substrate runs.
// Implements: EVS-PRD-materializer/C (partial) — the registry is sealed at
//   EventStore.open; the closed-registry invariant is part of how the
//   substrate enforces that the rules in effect are deterministically known
//   for the lifetime of one store instance.
import 'package:event_sourcing/src/projections/projection_spec.dart';

class ProjectionRegistry {
  final Map<String, ProjectionSpec> _byView = {};
  bool _sealed = false;

  void register(ProjectionSpec spec) {
    if (_sealed) {
      throw ArgumentError.value(
        spec.viewName,
        'spec.viewName',
        'ProjectionRegistry: cannot register after seal()',
      );
    }
    if (_byView.containsKey(spec.viewName)) {
      throw ArgumentError.value(
        spec.viewName,
        'spec.viewName',
        'ProjectionRegistry: duplicate registration for viewName',
      );
    }
    _byView[spec.viewName] = spec;
  }

  ProjectionSpec? lookup(String viewName) => _byView[viewName];

  Iterable<ProjectionSpec> all() => _byView.values;

  /// Called by EventStore.open after composition; further register() calls
  /// throw. Phase II's settings-event-driven registration is gated behind
  /// a separate substrate-level event flow that bypasses this seal.
  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
