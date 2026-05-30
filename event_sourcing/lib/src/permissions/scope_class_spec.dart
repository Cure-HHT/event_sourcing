// Implements: EVS-PRD-permissions-as-events (scope-class registration; substrate ships the mechanism, apps declare classes)

import 'package:meta/meta.dart';

/// A named scope dimension along which permissions can be scoped.
/// Apps register these at composition time with `ScopeClassRegistry`.
///
/// Top-level scope classes (no containment) match by direct equality.
/// Classes with `containedIn` participate in hierarchy expansion: a
/// principal's assignment at the parent class covers descendants whose
/// containment lookup resolves to the assigned value.
@immutable
class ScopeClassSpec {
  const ScopeClassSpec({required this.name, this.containedIn})
    : assert(name != '', 'name must not be empty');

  final String name;
  final ContainmentReference? containedIn;

  @override
  bool operator ==(Object other) =>
      other is ScopeClassSpec &&
      name == other.name &&
      containedIn == other.containedIn;

  @override
  int get hashCode => Object.hash(name, containedIn);

  @override
  String toString() => containedIn == null
      ? 'ScopeClassSpec($name)'
      : 'ScopeClassSpec($name in ${containedIn!.parentClass})';
}

/// Points a scope class at the projection that records its containment
/// in a parent class. The substrate reads `projection` at evaluate time,
/// indexes by `keyColumn`, and reads the parent value from `parentColumn`.
///
/// Example:
///   ContainmentReference(parentClass: 'site',
///                  projection: 'patient_site_index',
///                  keyColumn: 'patient_id',
///                  parentColumn: 'site_id')
///
/// At evaluate time, to resolve "what site is P-42 at?", the substrate
/// queries `patient_site_index` for the row with `patient_id == 'P-42'`
/// and returns the value in the `site_id` column.
@immutable
class ContainmentReference {
  const ContainmentReference({
    required this.parentClass,
    required this.projection,
    required this.keyColumn,
    required this.parentColumn,
  }) : assert(parentClass != '', 'parentClass must not be empty'),
       assert(projection != '', 'projection must not be empty'),
       assert(keyColumn != '', 'keyColumn must not be empty'),
       assert(parentColumn != '', 'parentColumn must not be empty');

  final String parentClass;
  final String projection;
  final String keyColumn;
  final String parentColumn;

  @override
  bool operator ==(Object other) =>
      other is ContainmentReference &&
      parentClass == other.parentClass &&
      projection == other.projection &&
      keyColumn == other.keyColumn &&
      parentColumn == other.parentColumn;

  @override
  int get hashCode =>
      Object.hash(parentClass, projection, keyColumn, parentColumn);

  @override
  String toString() =>
      'ContainmentReference(in $parentClass via $projection'
      '[$keyColumn -> $parentColumn])';
}
