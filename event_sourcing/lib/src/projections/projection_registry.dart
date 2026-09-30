// Implements: EVS-PRD-materializer/A
// ProjectionRegistry is the substrate
//   component that holds the active set of materializer rules.  It is the
//   authoritative in-memory table of which projections the substrate runs.
// Implements: EVS-PRD-materializer/C
// (partial) — the registry is sealed at
//   EventStore.open; the closed-registry invariant is part of how the
//   substrate enforces that the rules in effect are deterministically known
//   for the lifetime of one store instance.
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';

class ProjectionRegistry {
  final Map<String, ProjectionSpec> _byView = {};
  bool _sealed = false;

  /// Registers [spec].
  ///
  /// Throws [ArgumentError] after [seal], for a second spec of one view
  /// name, and, naming it, for a key path, column or derived field name
  /// beginning with `$`, which the default views reserve for the keys they
  /// stamp on every row.
  void register(ProjectionSpec spec) {
    _refuseReservedNames(spec);
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

  // Implements: EVS-PRD-materializer/H
  // a projection whose key, column or derived field name begins with `$` is
  //   refused at registration, by name.
  static void _refuseReservedNames(ProjectionSpec spec) {
    final names = <String>[
      ...switch (spec) {
        AggregateProjectionSpec(:final derivedFields) => <String>[
          for (final field in derivedFields) field.fieldName,
        ],
        TableProjectionSpec(:final rowKey, :final rowData) => <String>[
          ...switch (rowKey) {
            AggregateIdKey() => const <String>[],
            CompositeKey(:final paths) => <String>[
              for (final path in paths) ...path.split('.'),
            ],
          },
          ...switch (rowData) {
            WholePayload() => const <String>[],
            PayloadField(:final fieldName) => <String>[fieldName],
            SelectedFields(:final fieldNames) => fieldNames,
          },
        ],
      },
    ];
    for (final name in names) {
      if (name.startsWith(r'$')) {
        throw ArgumentError.value(
          name,
          'spec',
          'view "${spec.viewName}" names "$name"; key, column and derived '
              r'field names beginning with "$" are reserved for the keys the '
              'default views stamp on every row',
        );
      }
    }
  }

  ProjectionSpec? lookup(String viewName) => _byView[viewName];

  Iterable<ProjectionSpec> all() => _byView.values;

  /// Called by EventStore.open after composition; further register() calls
  /// throw. A future settings-event-driven registration flow
  /// (`spec/roadmap/multi-source-editing.md`) would bypass this seal.
  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
