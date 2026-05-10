import 'package:event_sourcing/src/projections/projection_spec.dart';

class ProjectionRegistry {
  final Map<String, ProjectionSpec> _byView = {};
  bool _sealed = false;

  void register(ProjectionSpec spec) {
    if (_sealed) {
      throw StateError(
        'ProjectionRegistry: cannot register "${spec.viewName}" after seal()',
      );
    }
    if (_byView.containsKey(spec.viewName)) {
      throw StateError(
        'ProjectionRegistry: duplicate registration for viewName '
        '"${spec.viewName}"',
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
