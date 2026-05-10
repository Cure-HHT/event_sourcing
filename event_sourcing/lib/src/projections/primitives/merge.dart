// event_sourcing/lib/src/projections/primitives/merge.dart

/// Library-supplied projection primitive: key-wise merge with
/// null-as-clear semantics.
///
/// Each key present in [delta] overwrites the corresponding key in
/// [prior], including when the delta's value is `null` (explicit clear).
/// Each key absent from [delta] preserves the prior value.
///
/// The iteration uses [delta.keys] rather than indexing into delta, so
/// "key absent" and "key present with null value" are distinguished.
///
/// Returns an unmodifiable map.
class Merge {
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
}
