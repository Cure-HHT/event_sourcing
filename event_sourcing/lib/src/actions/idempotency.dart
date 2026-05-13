// Implements: EVS-PRD-action-dispatch/D (Idempotency enum declares the per-action policy; IdempotencyEntry carries the cached outcome)

/// Per-action declaration of how the dispatcher treats `idempotencyKey`.
///
/// - [none]: caller MUST NOT pass a key; if they do, it is ignored.
/// - [optional]: caller MAY pass a key; without one, no replay protection.
/// - [required]: caller MUST pass a key; absence is a parse-stage denial.
//
// each documented in  (DISPATCH) and IdempotencyStore tests.
enum Idempotency { none, optional, required }

/// A cached dispatch outcome stored in the `IdempotencyStore`.
//
// returns this verbatim. `emittedEventIds` is the audit-trail link to the
// events written by the original dispatch.
class IdempotencyEntry {
  const IdempotencyEntry({
    required this.resultJson,
    required this.emittedEventIds,
    required this.recordedAt,
    required this.expiresAt,
  });

  final Map<String, Object?> resultJson;
  final List<String> emittedEventIds;
  final DateTime recordedAt;
  final DateTime expiresAt;

  bool isExpired({required DateTime now}) => !expiresAt.isAfter(now);
}

/// Default TTL for idempotency cache entries when an action does not
/// override.
//
// otherwise via its `idempotencyTtl` getter.
const Duration defaultIdempotencyTtl = Duration(hours: 24);
