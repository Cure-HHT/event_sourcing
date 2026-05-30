// event_sourcing/lib/src/promoters/primitives/transform.dart
// Implements: EVS-PRD-materializer/A/B
// Implements: EVS-DEV-snapshot-promotion-on-open/D
// Implements: EVS-DEV-ingest-promotes-before-fold/A
sealed class TransformPrimitive {
  const TransformPrimitive();
  Map<String, Object?> apply(Map<String, Object?> input);
}

class RenameField extends TransformPrimitive {
  final String sourceField;
  final String targetField;
  const RenameField({required this.sourceField, required this.targetField});

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

class DefaultField extends TransformPrimitive {
  final String fieldName;
  final Object? defaultValue;
  const DefaultField({required this.fieldName, required this.defaultValue});

  @override
  Map<String, Object?> apply(Map<String, Object?> input) {
    if (input.containsKey(fieldName)) return input;
    final next = Map<String, Object?>.from(input);
    next[fieldName] = defaultValue;
    return Map.unmodifiable(next);
  }
}

class DropField extends TransformPrimitive {
  final String fieldName;
  const DropField({required this.fieldName});

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
