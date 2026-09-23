// event_sourcing/lib/src/promoters/promoter_executor.dart
// Implements: EVS-DEV-ingest-promotes-before-fold/A+B
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/versions.dart';

class PromoterExecutor {
  /// Promotes [payload] through the registered chain from [fromVersion] to
  /// [toVersion] for ([viewName], [entryType]).
  ///
  /// [existingRow] is the view row the promoted payload is folded into, in
  /// the shape of [toVersion], or null when there is none. The library's
  /// default promotion treats a field that an event of an older version
  /// does not carry as a field the event leaves as it is, exactly as the
  /// fold treats any key an event omits. A `DefaultField` in the chain
  /// therefore supplies its value only when neither the payload nor
  /// [existingRow] carries the field under the name it has at
  /// [toVersion]: the field's name is carried forward through every
  /// `RenameField` that follows the `DefaultField` in the chain, and a
  /// default that a later `DropField` removes has no effect. A promoted
  /// default thus never overrides a value [existingRow] holds.
  /// `RenameField` and `DropField` apply to the payload unchanged. With a
  /// null [existingRow] the chain is the plain composition of its
  /// transforms.
  ///
  /// Throws [StateError] when the registry has no chain from [fromVersion]
  /// to [toVersion] (see [PromoterRegistry.chain]).
  // Implements: EVS-DEV-ingest-promotes-before-fold/A
  // a DefaultField in the chain supplies its value only for a field that
  //   neither the event nor the view's existing row carries under the
  //   field's name at the target version.
  static Map<String, Object?> promote({
    required PromoterRegistry registry,
    required String viewName,
    required String entryType,
    required EntryTypeVersion fromVersion,
    required EntryTypeVersion toVersion,
    required Map<String, Object?> payload,
    Map<String, Object?>? existingRow,
  }) {
    final transforms = <TransformPrimitive>[
      for (final spec in registry.chain(
        viewName: viewName,
        entryType: entryType,
        fromVersion: fromVersion,
        toVersion: toVersion,
      ))
        ...spec.transforms,
    ];
    var current = Map<String, Object?>.unmodifiable(payload);
    for (var i = 0; i < transforms.length; i++) {
      final transform = transforms[i];
      if (transform is DefaultField && existingRow != null) {
        final finalName = _nameAtTarget(
          transform.fieldName,
          transforms.skip(i + 1),
        );
        if (finalName == null || existingRow.containsKey(finalName)) {
          continue;
        }
      }
      current = transform.apply(current);
    }
    return current;
  }

  /// The name [fieldName] has after [later] transforms: carried through
  /// each `RenameField` of it, or null once a `DropField` removes it.
  static String? _nameAtTarget(
    String fieldName,
    Iterable<TransformPrimitive> later,
  ) {
    String? name = fieldName;
    for (final transform in later) {
      switch (transform) {
        case RenameField(:final sourceField, :final targetField)
            when sourceField == name:
          name = targetField;
        case DropField(:final fieldName) when fieldName == name:
          name = null;
        case DefaultField():
        case RenameField():
        case DropField():
          break;
      }
      if (name == null) return null;
    }
    return name;
  }
}
