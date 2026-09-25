// event_sourcing/lib/src/promoters/primitives/transform.dart
// Implements: EVS-PRD-materializer/A+B
// Implements: EVS-DEV-ingest-promotes-before-fold/A
sealed class TransformPrimitive {
  const TransformPrimitive();
  Map<String, Object?> apply(Map<String, Object?> input);
}

class RenameField extends TransformPrimitive {
  const RenameField({required this.sourceField, required this.targetField});
  final String sourceField;
  final String targetField;

  @override
  Map<String, Object?> apply(Map<String, Object?> input) {
    if (!input.containsKey(sourceField)) return input;
    if (input.containsKey(targetField)) {
      throw StateError(
        'RenameField($sourceField -> $targetField): '
        'target field "$targetField" already present',
      );
    }
    final next = Map<String, Object?>.from(input);
    next[targetField] = next.remove(sourceField);
    return Map.unmodifiable(next);
  }
}

/// Adds [fieldName] with [defaultValue] to a payload that lacks it.
///
/// As a transform, [apply] supplies the field whenever the payload lacks
/// it. In the fold, a promoter chain's `DefaultField` is also decided
/// against the view row the promoted event folds into: it supplies its
/// value only when that row does not carry the field under the name it has
/// at the registered version either (see `PromoterExecutor.promote`), so a
/// promoted default never overrides a value the row holds.
class DefaultField extends TransformPrimitive {
  const DefaultField({required this.fieldName, required this.defaultValue});
  final String fieldName;
  final Object? defaultValue;

  @override
  Map<String, Object?> apply(Map<String, Object?> input) {
    if (input.containsKey(fieldName)) return input;
    final next = Map<String, Object?>.from(input);
    next[fieldName] = defaultValue;
    return Map.unmodifiable(next);
  }
}

class DropField extends TransformPrimitive {
  const DropField({required this.fieldName});
  final String fieldName;

  @override
  Map<String, Object?> apply(Map<String, Object?> input) {
    if (!input.containsKey(fieldName)) return input;
    final next = Map<String, Object?>.from(input)..remove(fieldName);
    return Map.unmodifiable(next);
  }
}

class TransformChain {
  static Map<String, Object?> applyAll(
    List<TransformPrimitive> chain,
    Map<String, Object?> input,
  ) {
    var current = input;
    for (final t in chain) {
      current = t.apply(current);
    }
    return current;
  }
}
