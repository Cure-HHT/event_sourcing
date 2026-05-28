// Implements: EVS-PRD-action-dispatch/B (sealed outcome type covering every pipeline stage result)
// Implements: EVS-PRD-action-dispatch/C (DispatchSuccess carries emittedEventIds; denial variants represent recorded denial outcomes)
// Implements: EVS-PRD-action-dispatch/D (DispatchIdempotencyHit is returned on cache hit: same outcome, no new event emitted)
// Implements: EVS-PRD-action-dispatch/E (DispatchIdempotencyMismatch is returned when a cached entry exists for the (action, principal, key) tuple but the submitted rawInput's canonical JSON differs from the cached value; the corresponding idempotency_mismatch denial event is appended by the dispatcher)

import 'package:event_sourcing/src/actions/permission.dart';

/// Sealed outcome of `ActionDispatcher.dispatch(...)`. Each pipeline
/// stage's success or failure maps to a variant.
//
// stage. Sealed: exhaustiveness checked at every switch site.
sealed class DispatchResult<TResult> {
  const DispatchResult();

  const factory DispatchResult.success(
    TResult result,
    List<String> emittedEventIds,
  ) = DispatchSuccess<TResult>;

  const factory DispatchResult.unknownAction(String requestedName) =
      DispatchUnknownAction<TResult>;

  const factory DispatchResult.parseDenied(Object error) =
      DispatchParseDenied<TResult>;

  const factory DispatchResult.validationDenied(Object error) =
      DispatchValidationDenied<TResult>;

  const factory DispatchResult.authorizationDenied(Permission permission) =
      DispatchAuthorizationDenied<TResult>;

  const factory DispatchResult.executionFailed(Object error) =
      DispatchExecutionFailed<TResult>;

  const factory DispatchResult.idempotencyHit(
    TResult cachedResult,
    List<String> priorEmittedEventIds,
  ) = DispatchIdempotencyHit<TResult>;

  const factory DispatchResult.idempotencyMismatch({
    required String actionName,
    required String idempotencyKey,
    required String cachedRawInputHash,
    required String submittedRawInputHash,
  }) = DispatchIdempotencyMismatch<TResult>;
}

class DispatchSuccess<TResult> extends DispatchResult<TResult> {
  const DispatchSuccess(this.result, this.emittedEventIds);
  final TResult result;
  final List<String> emittedEventIds;
}

class DispatchUnknownAction<TResult> extends DispatchResult<TResult> {
  const DispatchUnknownAction(this.requestedName);
  final String requestedName;
}

class DispatchParseDenied<TResult> extends DispatchResult<TResult> {
  const DispatchParseDenied(this.error);
  final Object error;
}

class DispatchValidationDenied<TResult> extends DispatchResult<TResult> {
  const DispatchValidationDenied(this.error);
  final Object error;
}

class DispatchAuthorizationDenied<TResult> extends DispatchResult<TResult> {
  const DispatchAuthorizationDenied(this.permission);
  final Permission permission;
}

class DispatchExecutionFailed<TResult> extends DispatchResult<TResult> {
  const DispatchExecutionFailed(this.error);
  final Object error;
}

class DispatchIdempotencyHit<TResult> extends DispatchResult<TResult> {
  const DispatchIdempotencyHit(this.cachedResult, this.priorEmittedEventIds);
  final TResult cachedResult;
  final List<String> priorEmittedEventIds;
}

/// EVS-PRD-action-dispatch/E: returned when an `IdempotencyStore` entry
/// exists for the `(actionName, principalId, idempotencyKey)` tuple but
/// the submitted `rawInput`'s canonical JSON does not match the cached
/// `rawInputCanonicalJson`. The dispatcher appends a corresponding
/// `idempotency_mismatch` denial event before returning this variant.
///
/// The cached and submitted `rawInput` values themselves are NOT carried
/// on this variant or its denial event — they may contain sensitive
/// data. The substrate carries SHA-256 hashes of the canonical-JSON
/// encodings instead so auditors can correlate the mismatch without the
/// payload contents leaving the dispatcher.
class DispatchIdempotencyMismatch<TResult> extends DispatchResult<TResult> {
  const DispatchIdempotencyMismatch({
    required this.actionName,
    required this.idempotencyKey,
    required this.cachedRawInputHash,
    required this.submittedRawInputHash,
  });

  final String actionName;
  final String idempotencyKey;

  /// SHA-256 hex digest of the cached entry's `rawInputCanonicalJson`.
  /// Empty string when the cached entry pre-dates the column being
  /// recorded (forward-compat: legacy entries fall back to the
  /// match-by-existence semantics and never produce a mismatch).
  final String cachedRawInputHash;

  /// SHA-256 hex digest of the current submission's `rawInput`,
  /// canonicalized via RFC 8785 JCS before hashing.
  final String submittedRawInputHash;
}
