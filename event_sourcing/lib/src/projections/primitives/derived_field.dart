// event_sourcing/lib/src/projections/primitives/derived_field.dart

sealed class FallbackValue {
  const FallbackValue();
  Object? resolve({required DateTime firstEventTimestamp});
}

class ConstantValue extends FallbackValue {
  final Object? value;
  const ConstantValue(this.value);
  @override
  Object? resolve({required DateTime firstEventTimestamp}) => value;
}

class FirstEventTimestamp extends FallbackValue {
  const FirstEventTimestamp();
  @override
  Object? resolve({required DateTime firstEventTimestamp}) =>
      firstEventTimestamp.toUtc().toIso8601String();
}

sealed class DerivedFieldComputation {
  const DerivedFieldComputation();
  Object? resolve({
    required Map<String, Object?> rowState,
    required DateTime firstEventTimestamp,
  });
}

class DottedPathLookup extends DerivedFieldComputation {
  final String path;
  final FallbackValue fallback;
  const DottedPathLookup(this.path, {required this.fallback});

  @override
  Object? resolve({
    required Map<String, Object?> rowState,
    required DateTime firstEventTimestamp,
  }) {
    final segments = path.split('.');
    Object? current = rowState;
    for (final seg in segments) {
      if (current is! Map) {
        return fallback.resolve(firstEventTimestamp: firstEventTimestamp);
      }
      if (!current.containsKey(seg)) {
        return fallback.resolve(firstEventTimestamp: firstEventTimestamp);
      }
      current = current[seg];
    }
    return current ??
        fallback.resolve(firstEventTimestamp: firstEventTimestamp);
  }
}

class DerivedField {
  final String fieldName;
  final DerivedFieldComputation computation;
  const DerivedField(this.fieldName, this.computation);
}
