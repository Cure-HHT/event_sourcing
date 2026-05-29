// Implements: EVS-PRD-action-dispatch/D (IdempotencyStore is the pluggable cache: lookup hit → same outcome; record stores the result after successful dispatch)
// Implements: EVS-PRD-action-dispatch/E (record persists rawInputCanonicalJson alongside the result; lookup returns it so the dispatcher can detect same-key, different-content collisions)

import 'package:event_sourcing/src/actions/idempotency.dart';

/// Pluggable cache for action dispatch outcomes, keyed by
/// `(actionName, principalId, key)`. Lookup hits short-circuit a
/// dispatch and return the cached result.
abstract class IdempotencyStore {
  Future<IdempotencyEntry?> lookup(
    String actionName,
    String principalId,
    String key, {
    DateTime? now,
  });

  /// Record a dispatch outcome under `(actionName, principalId, key)`.
  ///
  /// [rawInputCanonicalJson] is the RFC-8785 canonicalization of the
  /// submission's `rawInput` at the time of recording. The dispatcher
  /// stamps it so a subsequent lookup with the same key but different
  /// content can be detected (EVS-PRD-action-dispatch/E). It is
  /// nullable for callers (e.g. test harnesses) that don't need the
  /// mismatch detection; persisting null is equivalent to "this entry
  /// can't be content-compared", and the dispatcher falls back to
  /// returning a cache hit regardless of content.
  Future<void> record({
    required String actionName,
    required String principalId,
    required String key,
    required Map<String, Object?> resultJson,
    required List<String> emittedEventIds,
    required DateTime expiresAt,
    String? rawInputCanonicalJson,
  });

  Future<int> sweepExpired({DateTime? before});

  /// Enumerate all currently-cached entries, ordered by
  /// `(actionName, principalId, idempotencyKey)` ascending. Not filtered
  /// by expiry — callers can compare `IdempotencyEntry.expiresAt`
  /// against `DateTime.now()` if they want only-fresh entries.
  ///
  /// Used by inspector UIs to surface the cache contents (the demo's
  /// `collectIdempotencyEntries` path goes through here). Returns an
  /// unmodifiable list. Implementations SHOULD be efficient for cache
  /// sizes in the low thousands; the contract does not promise
  /// pagination.
  Future<List<IdempotencyEntry>> listEntries();
}

/// In-memory `IdempotencyStore` for tests and single-process deployments.
class InMemoryIdempotencyStore implements IdempotencyStore {
  InMemoryIdempotencyStore();

  final Map<String, IdempotencyEntry> _entries = {};

  String _composite(String a, String p, String k) => '$a|$p|$k';

  @override
  Future<IdempotencyEntry?> lookup(
    String actionName,
    String principalId,
    String key, {
    DateTime? now,
  }) async {
    final entry = _entries[_composite(actionName, principalId, key)];
    if (entry == null) return null;
    if (entry.isExpired(now: now ?? DateTime.now())) return null;
    return entry;
  }

  @override
  Future<void> record({
    required String actionName,
    required String principalId,
    required String key,
    required Map<String, Object?> resultJson,
    required List<String> emittedEventIds,
    required DateTime expiresAt,
    String? rawInputCanonicalJson,
  }) async {
    _entries[_composite(actionName, principalId, key)] = IdempotencyEntry(
      actionName: actionName,
      principalId: principalId,
      idempotencyKey: key,
      resultJson: Map<String, Object?>.unmodifiable(resultJson),
      emittedEventIds: List<String>.unmodifiable(emittedEventIds),
      recordedAt: DateTime.now(),
      expiresAt: expiresAt,
      rawInputCanonicalJson: rawInputCanonicalJson,
    );
  }

  @override
  Future<int> sweepExpired({DateTime? before}) async {
    final cutoff = before ?? DateTime.now();
    final keysToRemove = _entries.entries
        .where((e) => e.value.isExpired(now: cutoff))
        .map((e) => e.key)
        .toList();
    for (final k in keysToRemove) {
      _entries.remove(k);
    }
    return keysToRemove.length;
  }

  @override
  Future<List<IdempotencyEntry>> listEntries() async {
    final list = _entries.values.toList()
      ..sort((a, b) {
        final ac = a.actionName.compareTo(b.actionName);
        if (ac != 0) return ac;
        final pc = a.principalId.compareTo(b.principalId);
        if (pc != 0) return pc;
        return a.idempotencyKey.compareTo(b.idempotencyKey);
      });
    return List<IdempotencyEntry>.unmodifiable(list);
  }
}
