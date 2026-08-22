// Implements: EVS-PRD-action-dispatch/D
// (Idempotency enum declares the per-action policy; IdempotencyEntry carries the cached outcome)
// Implements: EVS-PRD-action-dispatch/E
// (IdempotencyEntry.rawInputCanonicalJson lets the dispatcher detect same-key, different-content collisions; nullable for forward-compatibility with rows that pre-date the column)

/// Per-action declaration of how the dispatcher treats `idempotencyKey`.
///
/// - [none]: caller MUST NOT pass a key; if they do, it is ignored.
/// - [optional]: caller MAY pass a key; without one, no replay protection.
/// - [required]: caller MUST pass a key; absence is a parse-stage denial.
enum Idempotency { none, optional, required }

/// A cached dispatch outcome stored in the `IdempotencyStore`.
///
/// Carries both the keying tuple — `(actionName, principalId,
/// idempotencyKey)` — and the cached outcome (`resultJson`,
/// `emittedEventIds`) along with the timestamps. The keying tuple is
/// redundant with the lookup arguments on `IdempotencyStore.lookup`,
/// but it makes `IdempotencyStore.listEntries` self-describing: each
/// returned entry identifies which cache slot it occupies without the
/// caller having to thread the key separately.
/// `emittedEventIds` is the audit-trail link to the events written by
/// the original dispatch.
class IdempotencyEntry {
  const IdempotencyEntry({
    required this.actionName,
    required this.principalId,
    required this.idempotencyKey,
    required this.resultJson,
    required this.emittedEventIds,
    required this.recordedAt,
    required this.expiresAt,
    this.rawInputCanonicalJson,
  });

  final String actionName;
  final String principalId;
  final String idempotencyKey;
  final Map<String, Object?> resultJson;
  final List<String> emittedEventIds;
  final DateTime recordedAt;
  final DateTime expiresAt;

  /// EVS-PRD-action-dispatch/E: canonical-JSON (RFC 8785) encoding of the
  /// original submission's `rawInput`. The dispatcher compares this
  /// against the current submission's canonical-JSON `rawInput` at
  /// Stage 4 — match → return [DispatchIdempotencyHit]; mismatch →
  /// emit `idempotency_mismatch` denial event and return
  /// [DispatchIdempotencyMismatch].
  ///
  /// Nullable for forward-compatibility: rows recorded before this
  /// field shipped have no canonical JSON to compare against. The
  /// dispatcher treats `null` as "no mismatch detection possible" and
  /// falls back to the cache-hit behavior — never raising a false
  /// `idempotency_mismatch` against a legacy entry.
  final String? rawInputCanonicalJson;

  bool isExpired({required DateTime now}) => !expiresAt.isAfter(now);
}

/// Default TTL for idempotency cache entries when an action does not
/// override via its `idempotencyTtl` getter.
const Duration defaultIdempotencyTtl = Duration(hours: 24);
