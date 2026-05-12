// event_sourcing/lib/src/projections/primitives/merge.dart
//
// Implements: EVS-PRD-materializer/A — Merge is a library-supplied
//   materializer primitive; it is the key-wise delta-application algorithm
//   the library ships for consumers using the default Layer-2 fold.
// Implements: EVS-PRD-materializer/B — both applyDelta and applyDeepDelta
//   are pure functions (no side effects, no non-deterministic inputs); the
//   same (prior, delta) pair always produces the same result.

/// Library-supplied projection primitive: key-wise merge with
/// null-as-clear semantics.
///
/// Each key present in [delta] overwrites the corresponding key in
/// [prior], including when the delta's value is `null` (explicit clear).
/// Each key absent from [delta] preserves the prior value.
///
/// The iteration uses [delta.keys] rather than indexing into delta, so
/// "key absent" and "key present with null value" are distinguished.
class Merge {
  /// Shallow merge: each top-level key in [delta] overwrites or clears the
  /// corresponding key in [prior]. Nested maps are replaced wholesale (not
  /// recursed into).
  ///
  /// Returns an unmodifiable map.
  static Map<String, Object?> applyDelta(
    Map<String, Object?> prior,
    Map<String, Object?> delta,
  ) {
    final merged = Map<String, Object?>.from(prior);
    for (final key in delta.keys) {
      merged[key] = delta[key];
    }
    return Map<String, Object?>.unmodifiable(merged);
  }

  /// Deep merge: recursively merges [delta] into [prior] with null-as-clear
  /// semantics.
  ///
  /// When both [prior] and [delta] have a Map value for the same key, the
  /// merge recurses into that nested map (so inner keys absent from the delta
  /// are preserved). When the delta value is non-null and non-Map, it
  /// overwrites. When the delta value is null, the key is cleared (set to
  /// null) in the result. Keys absent from the delta are preserved unchanged.
  ///
  /// The two `is Map` branches handle real-world decoded-from-JSON shapes
  /// where the runtime type may be `Map<dynamic, dynamic>` rather than
  /// `Map<String, Object?>` despite the static declaration — Sembast and
  /// JSON decode can produce such values. Both branches normalize to
  /// `Map<String, Object?>` before recursing so the result type is always
  /// correct.
  ///
  /// Returns a mutable map (callers should freeze if needed).
  static Map<String, Object?> applyDeepDelta(
    Map<String, Object?> prior,
    Map<String, Object?> delta,
  ) {
    final result = Map<String, Object?>.from(prior);
    for (final key in delta.keys) {
      final deltaVal = delta[key];
      final priorVal = prior[key];
      if (deltaVal is Map<String, Object?> &&
          priorVal is Map<String, Object?>) {
        result[key] = applyDeepDelta(priorVal, deltaVal);
      } else if (deltaVal is Map && priorVal is Map) {
        // Defensive branch: handle decoded-from-JSON Maps whose runtime type
        // is Map<dynamic, dynamic>. Normalizes both sides before recursing.
        result[key] = applyDeepDelta(
          Map<String, Object?>.from(priorVal as Map),
          Map<String, Object?>.from(deltaVal as Map),
        );
      } else {
        result[key] = deltaVal;
      }
    }
    return result;
  }
}
