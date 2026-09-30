import 'package:event_sourcing/event_sourcing.dart';

/// In-memory bridge from one datastore's outgoing `Native` wire payload
/// to another datastore's receiver endpoint. Demo-only glue used by the
/// dual-pane example to wire the mobile pane's outgoing native stream into
/// the hub pane.
///
/// A native delivery (`esd/batch@3`) goes to the hub's
/// [EventStore.receiverEndpoint], whose acknowledgement or refusal body is
/// mapped to a [SendResult] by [decodeReceiverAnswer], as a transport
/// carrying the receiver's answer does. The demo is one process and trusts
/// its own panes, so the bridge authenticates the caller for the sender the
/// delivery's channel names; a deployment's authentication decides that
/// set from the caller's credential instead.
///
/// - a payload in any other wire format → [SendPermanent] (the receiver
///   admits events only in native deliveries);
/// - [IngestDecodeFailure] (bytes from which no channel can be read) and
///   [DeliveryAuthenticationRefused] → [SendPermanent] (won't fix on
///   retry);
/// - any other thrown exception → [SendTransient] (treat unknowns as
///   recoverable so drain retries on the next tick).
class DownstreamBridge {
  const DownstreamBridge(this._target);
  final EventStore _target;

  Future<SendResult> deliver(WirePayload payload) async {
    if (payload.contentType != DeliveryEnvelope.wireFormat) {
      return SendPermanent(
        error:
            'unsupported wire format "${payload.contentType}"; the hub '
            'admits native deliveries (${DeliveryEnvelope.wireFormat}) only',
      );
    }
    try {
      final sender = DeliveryEnvelope.decode(
        payload.bytes,
      ).channel.senderDatabaseId;
      final answer = await _target.receiverEndpoint.accept(
        payload.bytes,
        senderDatabaseIds: <String>{sender},
      );
      return decodeReceiverAnswer(answer.encode());
    } on IngestDecodeFailure catch (e) {
      return SendPermanent(error: e.toString());
    } on DeliveryAuthenticationRefused catch (e) {
      return SendPermanent(error: e.toString());
    } catch (e) {
      return SendTransient(error: e.toString());
    }
  }
}
