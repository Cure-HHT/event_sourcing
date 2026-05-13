// Implements: EVS-PRD-permissions-as-events (composition-time validation; refuses cycles, dangling parent refs, missing projection columns)

import 'package:event_sourcing/event_sourcing.dart';

/// Helper returned by the projection-lookup callback; lets the registry
/// verify named columns exist without depending on the concrete
/// TableProjectionSpec type (the registry doesn't otherwise need to read
/// projection internals).
abstract class ScopeProjectionDescriptor {
  Set<String> get columns;
}

/// Compose-time registry of [ScopeClassSpec]s. Validates the registry
/// against the projection registry (via [projectionLookup]) so that
/// containment references are guaranteed to resolve at evaluate time.
///
/// Throws `StateError` on validation failure with a message naming the
/// specific defect (duplicate, dangling parent, missing column, cycle).
class ScopeClassRegistry {
  ScopeClassRegistry({
    required List<ScopeClassSpec> classes,
    required ScopeProjectionDescriptor? Function(String projectionName)
    projectionLookup,
  }) : _byName = _buildAndValidate(classes, projectionLookup);

  final Map<String, ScopeClassSpec> _byName;

  ScopeClassSpec? byName(String name) => _byName[name];

  Iterable<ScopeClassSpec> get all => _byName.values;

  /// Returns [className]'s ancestor chain starting at [className] itself
  /// and ending at the top-of-graph class. Returns [className] alone if
  /// the class has no containment.
  Iterable<ScopeClassSpec> ancestorChain(String className) sync* {
    var current = _byName[className];
    while (current != null) {
      yield current;
      final ref = current.containedIn;
      if (ref == null) return;
      current = _byName[ref.parentClass];
    }
  }

  /// True iff [ancestor] is in [descendant]'s ancestor chain (or equals it).
  bool isAncestor(String ancestor, String descendant) {
    for (final c in ancestorChain(descendant)) {
      if (c.name == ancestor) return true;
    }
    return false;
  }

  static Map<String, ScopeClassSpec> _buildAndValidate(
    List<ScopeClassSpec> classes,
    ScopeProjectionDescriptor? Function(String) projectionLookup,
  ) {
    final byName = <String, ScopeClassSpec>{};
    for (final c in classes) {
      if (byName.containsKey(c.name)) {
        throw StateError(
          'ScopeClassRegistry: duplicate class name "${c.name}"',
        );
      }
      byName[c.name] = c;
    }

    for (final c in classes) {
      final ref = c.containedIn;
      if (ref == null) continue;
      if (!byName.containsKey(ref.parentClass)) {
        throw StateError(
          'ScopeClassRegistry: class "${c.name}" has parentClass '
          '"${ref.parentClass}" which is not a registered class',
        );
      }
      final p = projectionLookup(ref.projection);
      if (p == null) {
        throw StateError(
          'ScopeClassRegistry: class "${c.name}" references projection '
          '"${ref.projection}" which is not a registered projection',
        );
      }
      if (!p.columns.contains(ref.keyColumn)) {
        throw StateError(
          'ScopeClassRegistry: class "${c.name}" containment '
          'keyColumn "${ref.keyColumn}" is not a column of projection '
          '"${ref.projection}" (columns: ${p.columns})',
        );
      }
      if (!p.columns.contains(ref.parentColumn)) {
        throw StateError(
          'ScopeClassRegistry: class "${c.name}" containment '
          'parentColumn "${ref.parentColumn}" is not a column of '
          'projection "${ref.projection}" (columns: ${p.columns})',
        );
      }
    }

    // Cycle check: walk each class's containment chain, refusing repeats.
    for (final start in classes) {
      final seen = <String>{};
      var current = start;
      while (true) {
        if (!seen.add(current.name)) {
          throw StateError(
            'ScopeClassRegistry: containment cycle detected starting '
            'at class "${start.name}"',
          );
        }
        final ref = current.containedIn;
        if (ref == null) break;
        current = byName[ref.parentClass]!;
      }
    }

    return byName;
  }
}
