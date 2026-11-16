import 'package:event_sourcing/src/storage/send_result.dart';

/// One historical send attempt for a FifoEntry.
///
/// Attempts accumulate on the entry across the drain loop's retries; they
/// are never dropped. `outcome` is a wire-format string discriminator —
/// `"ok"`, `"transient"`, or `"permanent"` — matching the three variants of
/// SendResult. It is deliberately a string rather than an enum at this
/// layer so that a destination's judgment on response categorization can
/// evolve without ABI pressure on this persisted record.
// Implements: EVS-PRD-portability/C
// pure Dart value type; platform-
//   independent JSON serialisation.
class AttemptResult {
  const AttemptResult({
    required this.attemptedAt,
    required this.outcome,
    this.errorMessage,
    this.httpStatus,
    this.deliveryNumber,
    this.deliveryHash,
    this.scope,
  });

  /// Decode from snake_case JSON; throws [FormatException] on missing
  /// required or wrong-typed fields.
  factory AttemptResult.fromJson(Map<String, Object?> json) {
    final attemptedAtRaw = json['attempted_at'];
    if (attemptedAtRaw is! String) {
      throw const FormatException(
        'AttemptResult: missing or non-string "attempted_at"',
      );
    }
    final outcome = json['outcome'];
    if (outcome is! String) {
      throw const FormatException(
        'AttemptResult: missing or non-string "outcome"',
      );
    }
    final errorMessage = json['error_message'];
    if (errorMessage != null && errorMessage is! String) {
      throw const FormatException(
        'AttemptResult: "error_message" must be a String when present',
      );
    }
    final httpStatus = json['http_status'];
    if (httpStatus != null && httpStatus is! int) {
      throw const FormatException(
        'AttemptResult: "http_status" must be an int when present',
      );
    }
    final deliveryNumber = json['delivery_number'];
    if (deliveryNumber != null && deliveryNumber is! int) {
      throw const FormatException(
        'AttemptResult: "delivery_number" must be an int when present',
      );
    }
    final deliveryHash = json['delivery_hash'];
    if (deliveryHash != null && deliveryHash is! String) {
      throw const FormatException(
        'AttemptResult: "delivery_hash" must be a String when present',
      );
    }
    final scopeRaw = json['scope'];
    if (scopeRaw != null && scopeRaw is! String) {
      throw const FormatException(
        'AttemptResult: "scope" must be a String when present',
      );
    }
    WedgeScope? scope;
    if (scopeRaw != null) {
      scope = WedgeScope.values.asNameMap()[scopeRaw];
      if (scope == null) {
        throw FormatException('AttemptResult: unknown "scope" $scopeRaw');
      }
    }
    return AttemptResult(
      attemptedAt: DateTime.parse(attemptedAtRaw),
      outcome: outcome,
      errorMessage: errorMessage as String?,
      httpStatus: httpStatus as int?,
      deliveryNumber: deliveryNumber as int?,
      deliveryHash: deliveryHash as String?,
      scope: scope,
    );
  }

  /// UTC instant (or timezone-offset-explicit) at which the drain loop ran
  /// `destination.send()` for the head entry.
  final DateTime attemptedAt;

  /// `"ok"` | `"transient"` | `"permanent"` — matches SendResult variants.
  final String outcome;

  /// Human-readable error string from the destination; null on `outcome="ok"`.
  final String? errorMessage;

  /// HTTP status code when the destination is HTTP-based; null otherwise
  /// (e.g., network failure before a response was received).
  final int? httpStatus;

  /// The delivery number of the delivery the attempt sent, on a delivery
  /// channel; null for a destination that is no channel.
  // Implements: EVS-DEV-delivery-channel/I
  // the attempt a send produces records the delivery's number and hash.
  final int? deliveryNumber;

  /// The delivery hash of the delivery the attempt sent, on a delivery
  /// channel; null for a destination that is no channel.
  final String? deliveryHash;

  /// What the permanent failure the attempt reported concerns, for an
  /// attempt whose outcome is `"permanent"`; null for every other outcome.
  // Implements: EVS-DEV-destination-key-hold/J
  // the attempt records whether its permanent failure was scoped to the
  //   item's block key.
  final WedgeScope? scope;

  /// Encode to snake_case JSON. `error_message` and `http_status` are
  /// emitted with explicit null; `delivery_number` and `delivery_hash` are
  /// emitted only for an attempt that sent a delivery on a channel, and
  /// `scope` only for an attempt that reported a permanent failure.
  Map<String, Object?> toJson() => <String, Object?>{
    'attempted_at': attemptedAt.toIso8601String(),
    'outcome': outcome,
    'error_message': errorMessage,
    'http_status': httpStatus,
    if (deliveryNumber != null) 'delivery_number': deliveryNumber,
    if (deliveryHash != null) 'delivery_hash': deliveryHash,
    if (scope != null) 'scope': scope!.name,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is AttemptResult &&
          attemptedAt == other.attemptedAt &&
          outcome == other.outcome &&
          errorMessage == other.errorMessage &&
          httpStatus == other.httpStatus &&
          deliveryNumber == other.deliveryNumber &&
          deliveryHash == other.deliveryHash &&
          scope == other.scope;

  @override
  int get hashCode => Object.hash(
    attemptedAt,
    outcome,
    errorMessage,
    httpStatus,
    deliveryNumber,
    deliveryHash,
    scope,
  );

  @override
  String toString() =>
      'AttemptResult(attemptedAt: ${attemptedAt.toIso8601String()}, '
      'outcome: $outcome, errorMessage: $errorMessage, '
      'httpStatus: $httpStatus, deliveryNumber: $deliveryNumber, '
      'deliveryHash: $deliveryHash, scope: ${scope?.name})';
}
