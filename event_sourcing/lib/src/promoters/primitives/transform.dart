// event_sourcing/lib/src/promoters/primitives/transform.dart
sealed class TransformPrimitive {
  const TransformPrimitive();
  Map<String, Object?> apply(Map<String, Object?> input);
}

class RenameField extends TransformPrimitive {
  final String from;
  final String to;
  const RenameField({required this.from, required this.to});

  @override
  Map<String, Object?> apply(Map<String, Object?> input) {
    if (!input.containsKey(from)) return input;
    if (input.containsKey(to)) {
      throw StateError(
        'RenameField($from -> $to): target field "$to" already present',
      );
    }
    final next = Map<String, Object?>.from(input);
    next[to] = next.remove(from);
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
