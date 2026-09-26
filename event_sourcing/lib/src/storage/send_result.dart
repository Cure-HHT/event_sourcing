import 'package:event_sourcing/src/destinations/receiver_response.dart';

/// Categorized outcome of a single `destination.send()` call.
///
/// The drain loop switches on the four subclasses:
/// - [SendOk]: the payload was delivered; mark the FIFO head `sent` and
///   continue draining.
/// - [SendAnswered]: a receiver of the native batch format answered with
///   its record of the delivery's channel.
/// - [SendTransient]: retry later per SyncPolicy; `httpStatus` optional
///   because not every destination is HTTP-based.
/// - [SendPermanent]: the payload will never be accepted as-is; mark the
///   FIFO head `wedged` and halt this destination's FIFO.
///
/// The translation from a raw HTTP or IO response to a [SendResult] is a
/// per-destination judgment — default categorization is `2xx -> SendOk`,
/// `5xx/network -> SendTransient`, `4xx -> SendPermanent`, with
/// destination-level carve-outs possible (see design doc §8.1, §11.1).
// Implements: EVS-PRD-portability/C
// pure Dart sealed type; platform-
//   independent; no platform-specific imports.
sealed class SendResult {
  const SendResult();
}

/// The destination accepted the payload.
class SendOk extends SendResult {
  const SendOk();

  @override
  bool operator ==(Object other) => other is SendOk;

  @override
  int get hashCode => (SendOk).hashCode;

  @override
  String toString() => 'SendOk()';
}

/// A receiver of the native batch format answered a delivery with an
/// acknowledgement or an `out_of_sequence` refusal, carrying its record of
/// the delivery's channel ([decodeReceiverAnswer]). A destination that
/// serializes natively reports every answer it receives this way; one that
/// does not never returns it.
// Implements: EVS-DEV-delivery-channel/L
// the send outcome that carries the receiver's answer and its record.
class SendAnswered extends SendResult {
  const SendAnswered(this.response);

  /// The receiver's answer.
  final ReceiverResponse response;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SendAnswered && response == other.response;

  @override
  int get hashCode => Object.hash(SendAnswered, response);

  @override
  String toString() => 'SendAnswered($response)';
}

/// The destination is temporarily unable to accept the payload. The drain
/// loop SHALL retry after a backoff per SyncPolicy.
class SendTransient extends SendResult {
  const SendTransient({required this.error, this.httpStatus});

  /// Operator-readable error string.
  final String error;

  /// HTTP status code, when the destination is HTTP-based; null otherwise.
  final int? httpStatus;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SendTransient &&
          error == other.error &&
          httpStatus == other.httpStatus;

  @override
  int get hashCode => Object.hash(SendTransient, error, httpStatus);

  @override
  String toString() => 'SendTransient(error: $error, httpStatus: $httpStatus)';
}

/// The destination will not accept the payload, and retry would not change
/// that. The drain loop SHALL mark the FIFO head `wedged` and stop
/// draining this destination until operator action.
class SendPermanent extends SendResult {
  const SendPermanent({required this.error});

  /// Operator-readable error string.
  final String error;

  @override
  bool operator ==(Object other) =>
      identical(this, other) || other is SendPermanent && error == other.error;

  @override
  int get hashCode => Object.hash(SendPermanent, error);

  @override
  String toString() => 'SendPermanent(error: $error)';
}
