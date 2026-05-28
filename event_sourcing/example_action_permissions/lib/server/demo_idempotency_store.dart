// lib/server/demo_idempotency_store.dart
//
// Demo-side `IdempotencyStore` impl used by the action-permissions demo.
// Same contract as `InMemoryIdempotencyStore` from the substrate, kept
// as a distinct class so demo wiring can pin it explicitly and so the
// demo retains a place to add inspector-friendly behavior later if
// needed. Inspector enumeration goes through the interface method
// `IdempotencyStore.listEntries()` — no bespoke snapshot type is
// required.

import 'package:event_sourcing/event_sourcing.dart';

/// In-memory `IdempotencyStore` for the action-permissions demo.
/// Mirrors `InMemoryIdempotencyStore` but kept as a distinct class so
/// the demo's server-side wiring can refer to it by name. Inspector
/// enumeration uses the interface's [listEntries] contract.
class DemoIdempotencyStore implements IdempotencyStore {
  DemoIdempotencyStore();

  final Map<String, IdempotencyEntry> _entries = <String, IdempotencyEntry>{};

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
    final keys = _entries.entries
        .where((e) => e.value.isExpired(now: cutoff))
        .map((e) => e.key)
        .toList();
    for (final k in keys) {
      _entries.remove(k);
    }
    return keys.length;
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
