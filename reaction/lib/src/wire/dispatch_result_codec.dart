// Implements: EVS-PRD-cross-process-event-transport/A
// JSON codec for
//   DispatchResult wire envelopes (one variant per "type" discriminator).
// Implements: EVS-PRD-action-submitter/C
// wire shape of the
//   POST /actions response body.

import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for the substrate's [DispatchResult]. The substrate type
/// carries either a typed result + emitted event IDs (on success /
/// idempotency hits), an opaque `Object` error (on parse / validation /
/// execution denials), or a [Permission] (on authorization denial).
///
/// Wire shapes per variant (all carry a `type` discriminator):
///   success              + result (jsonable) + emittedEventIds (List<String>)
///   unknown_action       + requestedName
///   parse_denied         + error (string; lossy — see below)
///   validation_denied    + error (string; lossy — see below)
///   authorization_denied + permission ({name, scopeClass: nullable})
///   execution_failed     + error (string; lossy — see below)
///   idempotency_hit      + cachedResult (jsonable) + priorEmittedEventIds
///                          (List<String>)
///   idempotency_mismatch + actionName + idempotencyKey
///                          + cachedRawInputHash + submittedRawInputHash
///                          (SHA-256 hex of the canonical-JSON inputs;
///                           the inputs themselves are not on the wire)
///
/// **Error encoding is lossy.** The substrate's parse/validation/
/// execution variants carry `Object error` (so an action can throw any
/// structured error type). On the wire the codec emits `error.toString()`
/// and the decoded variant carries that String as its `error` field
/// (Object accepts String). Custom error types do NOT round-trip with
/// their full structure. See `spec/reaction-remote.md` "Future work" for
/// the named-error / sealed-Error path if structured errors over the
/// wire are ever required.
///
/// `result` and `cachedResult` are typed `TResult` on the substrate; on
/// the wire we carry whatever JSON-friendly value the action produced.
/// The codec is bound to `DispatchResult<Object?>`; consumers ascribe
/// further on the client side.
class DispatchResultCodec {
  const DispatchResultCodec._();

  static Map<String, Object?> encode(DispatchResult<Object?> result) {
    return switch (result) {
      DispatchSuccess<Object?>() => {
        'type': 'success',
        'result': result.result,
        'emittedEventIds': result.emittedEventIds,
      },
      DispatchUnknownAction<Object?>() => {
        'type': 'unknown_action',
        'requestedName': result.requestedName,
      },
      DispatchParseDenied<Object?>() => {
        'type': 'parse_denied',
        'error': result.error.toString(),
      },
      DispatchValidationDenied<Object?>() => {
        'type': 'validation_denied',
        'error': result.error.toString(),
      },
      DispatchAuthorizationDenied<Object?>() => {
        'type': 'authorization_denied',
        'permission': _encodePermission(result.permission),
      },
      DispatchExecutionFailed<Object?>() => {
        'type': 'execution_failed',
        'error': result.error.toString(),
      },
      DispatchIdempotencyHit<Object?>() => {
        'type': 'idempotency_hit',
        'cachedResult': result.cachedResult,
        'priorEmittedEventIds': result.priorEmittedEventIds,
      },
      DispatchIdempotencyMismatch<Object?>() => {
        'type': 'idempotency_mismatch',
        'actionName': result.actionName,
        'idempotencyKey': result.idempotencyKey,
        'cachedRawInputHash': result.cachedRawInputHash,
        'submittedRawInputHash': result.submittedRawInputHash,
      },
    };
  }

  static DispatchResult<Object?> decode(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'success':
        return DispatchSuccess<Object?>(
          json['result'],
          (json['emittedEventIds']! as List).cast<String>(),
        );
      case 'unknown_action':
        return DispatchUnknownAction<Object?>(
          requireString(json, 'requestedName'),
        );
      case 'parse_denied':
        return DispatchParseDenied<Object?>(requireString(json, 'error'));
      case 'validation_denied':
        return DispatchValidationDenied<Object?>(requireString(json, 'error'));
      case 'authorization_denied':
        return DispatchAuthorizationDenied<Object?>(
          _decodePermission(requireMap(json, 'permission')),
        );
      case 'execution_failed':
        return DispatchExecutionFailed<Object?>(requireString(json, 'error'));
      case 'idempotency_hit':
        return DispatchIdempotencyHit<Object?>(
          json['cachedResult'],
          (json['priorEmittedEventIds']! as List).cast<String>(),
        );
      case 'idempotency_mismatch':
        return DispatchIdempotencyMismatch<Object?>(
          actionName: requireString(json, 'actionName'),
          idempotencyKey: requireString(json, 'idempotencyKey'),
          cachedRawInputHash: requireString(json, 'cachedRawInputHash'),
          submittedRawInputHash: requireString(json, 'submittedRawInputHash'),
        );
      default:
        throw FormatException('unknown DispatchResult type: $type');
    }
  }

  // --- Permission helpers (substrate Permission has no toJson) ---

  static Map<String, Object?> _encodePermission(Permission p) => {
    'name': p.name,
    'scopeClass': p.scopeClass,
  };

  static Permission _decodePermission(Map<String, Object?> j) => Permission(
    requireString(j, 'name'),
    scopeClass: readString(j, 'scopeClass'),
  );
}
