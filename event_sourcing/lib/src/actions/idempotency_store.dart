// Implements: EVS-PRD-action-dispatch/D (IdempotencyStore is the pluggable cache: lookup hit → same outcome; record stores the result after successful dispatch)

import 'package:event_sourcing/src/actions/idempotency.dart';

/// Pluggable cache for action dispatch outcomes, keyed by
/// `(actionName, principalId, key)`. Lookup hits short-circuit a
/// dispatch and return the cached result.
//
abstract class IdempotencyStore {
  Future<IdempotencyEntry?> lookup(
    String actionName,
    String principalId,
    String key, {
    DateTime? now,
  });

  Future<void> record({
    required String actionName,
    required String principalId,
    required String key,
    required Map<String, Object?> resultJson,
    required List<String> emittedEventIds,
    required DateTime expiresAt,
  });

  Future<int> sweepExpired({DateTime? before});
}

/// In-memory `IdempotencyStore` for tests and per-process state during
/// early development. Production uses a Postgres-backed impl from a
/// later "port to portal" ticket.
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
  }) async {
    _entries[_composite(actionName, principalId, key)] = IdempotencyEntry(
      resultJson: Map<String, Object?>.unmodifiable(resultJson),
      emittedEventIds: List<String>.unmodifiable(emittedEventIds),
      recordedAt: DateTime.now(),
      expiresAt: expiresAt,
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
}
