// Implements: EVS-DEV-view-convergence/A (fingerprint term)
// viewFingerprint computes the fingerprint the Terms section above
//   defines: a digest of a view's definition, covering its name, its
//   shape, the event types and derived fields the shape declares, its
//   interest, the registered version of every entry type the interest
//   names (or of every registered entry type when it names none), and the
//   promoter chain the view registers for each of those entry types.
import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';

/// The fingerprint of [spec]'s definition against [entryTypes] and
/// [promoters], as `EVS-DEV-view-convergence` Terms defines it.
///
/// Two builds that register [spec], the same entry-type versions and the
/// same promoter chains produce an equal digest whatever order they
/// registered them in: every collection here is sorted before it is
/// canonicalized, so registration order never reaches the digest. Only
/// what the library can read of a definition is covered -- a closure (a
/// `SubscriptionFilter.predicate`) is not part of it, so two definitions
/// that differ only in a predicate's behavior share a fingerprint (see
/// `EVS-DEV-view-convergence` Rationale, "one copy per definition").
// Implements: EVS-DEV-view-convergence/A (fingerprint term)
String viewFingerprint(
  ProjectionSpec spec,
  EntryTypeRegistry entryTypes,
  PromoterRegistry promoters,
) {
  final namedEntryTypes = spec.interest.entryTypes;
  final resolvedIds =
      (namedEntryTypes ?? {for (final def in entryTypes.all()) def.id}).toList()
        ..sort();

  final entryTypeVersions = <String, Object?>{};
  final promoterChains = <String, Object?>{};
  for (final id in resolvedIds) {
    final def = entryTypes.byId(id);
    if (def == null) continue;
    entryTypeVersions[id] = def.registeredVersion.toString();
    promoterChains[id] = [
      for (final step in promoters.stepsFor(
        viewName: spec.viewName,
        entryType: id,
      ))
        _encodeStep(step),
    ];
  }

  final input = <String, Object?>{
    'viewName': spec.viewName,
    'shape': _encodeShape(spec),
    'interest': {
      'entryTypes': _sortedOrNull(spec.interest.entryTypes),
      'eventTypes': _sortedOrNull(spec.interest.eventTypes),
      'aggregateTypes': _sortedOrNull(spec.interest.aggregateTypes),
      'includeSystemEvents': spec.interest.includeSystemEvents,
    },
    'entryTypeVersions': entryTypeVersions,
    'promoterChains': promoterChains,
  };
  return sha256.convert(canonicalizeBytes(input)).toString();
}

List<String>? _sortedOrNull(Set<String>? values) =>
    values == null ? null : (values.toList()..sort());

Map<String, Object?> _encodeShape(ProjectionSpec spec) => switch (spec) {
  AggregateProjectionSpec(:final tombstoneEventTypes, :final derivedFields) => {
    'kind': 'aggregate',
    'tombstoneEventTypes': tombstoneEventTypes.toList()..sort(),
    'derivedFields': derivedFields.map(_encodeDerivedField).toList()
      ..sort(
        (a, b) =>
            (a['fieldName']! as String).compareTo(b['fieldName']! as String),
      ),
  },
  TableProjectionSpec(
    :final insertEventTypes,
    :final removeEventTypes,
    :final rowKey,
    :final rowData,
  ) =>
    {
      'kind': 'table',
      'insertEventTypes': insertEventTypes.toList()..sort(),
      'removeEventTypes': removeEventTypes.toList()..sort(),
      'rowKey': _encodeRowKey(rowKey),
      'rowData': _encodeRowData(rowData),
    },
};

Map<String, Object?> _encodeDerivedField(DerivedField field) => {
  'fieldName': field.fieldName,
  'computation': _encodeComputation(field.computation),
};

Object? _encodeComputation(DerivedFieldComputation computation) =>
    switch (computation) {
      DottedPathLookup(:final path, :final fallback) => {
        'type': 'DottedPathLookup',
        'path': path,
        'fallback': _encodeFallback(fallback),
      },
    };

Object? _encodeFallback(FallbackValue fallback) => switch (fallback) {
  ConstantValue(:final value) => {'type': 'ConstantValue', 'value': value},
  FirstEventTimestamp() => {'type': 'FirstEventTimestamp'},
};

Map<String, Object?> _encodeRowKey(RowKeyExtractor rowKey) => switch (rowKey) {
  AggregateIdKey() => {'type': 'AggregateIdKey'},
  CompositeKey(:final paths) => {'type': 'CompositeKey', 'paths': paths},
};

Map<String, Object?> _encodeRowData(RowDataExtractor rowData) =>
    switch (rowData) {
      WholePayload() => {'type': 'WholePayload'},
      PayloadField(:final fieldName) => {
        'type': 'PayloadField',
        'fieldName': fieldName,
      },
      SelectedFields(:final fieldNames) => {
        'type': 'SelectedFields',
        'fieldNames': fieldNames,
      },
    };

Map<String, Object?> _encodeStep(PromoterSpec step) => {
  'from': step.fromVersion.toString(),
  'to': step.toVersion.toString(),
  'transforms': step.transforms.map(_encodeTransform).toList(),
};

Map<String, Object?> _encodeTransform(TransformPrimitive transform) =>
    switch (transform) {
      RenameField(:final sourceField, :final targetField) => {
        'type': 'RenameField',
        'sourceField': sourceField,
        'targetField': targetField,
      },
      DefaultField(:final fieldName, :final defaultValue) => {
        'type': 'DefaultField',
        'fieldName': fieldName,
        'defaultValue': defaultValue,
      },
      DropField(:final fieldName) => {
        'type': 'DropField',
        'fieldName': fieldName,
      },
    };
