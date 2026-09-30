import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:flutter/foundation.dart';

/// Native demo destination — declares it speaks the library's native
/// batch format (`esd/batch@3`) so the library handles serialization
/// itself. FIFO rows for this destination
/// store envelope metadata with a null wire_payload. Used in the example
/// to demonstrate the storage-shape difference vs `DemoDestination`
/// (lossy 3rd-party).
///
/// Implements [DemoKnobs] so the FIFO panel exposes the same live-tunable
/// connection / latency / batch-size / accumulate sliders as the lossy
/// `DemoDestination`. Default knob values: `batchSize=10` (highlights
/// native multi-event batches), `sendLatency=0` (instant succeed),
/// `connection=ok`, `maxAccumulateTime=0` (no hold).
///
/// Optional [DownstreamBridge] hook: when supplied via the `bridge:`
/// constructor parameter and `connection.value == Connection.ok`,
/// `send()` delegates to the bridge after the latency delay. The bridge
/// forwards the wire bytes to the downstream event store (a native
/// `esd/batch@3` delivery to its receiver endpoint) and maps the answer
/// back to a [SendResult]. When `connection != ok`,
/// the bridge is NOT invoked — link failures are simulated upstream of
/// the bridge so the existing `broken`/`rejecting` UX is unchanged.
class NativeDemoDestination implements Destination, DemoKnobs {
  NativeDemoDestination({
    this.id = 'Native',
    this.filter = const SubscriptionFilter(),
    this.allowHardDelete = false,
    Duration initialSendLatency = Duration.zero,
    int initialBatchSize = 10,
    Duration initialAccumulate = Duration.zero,
    Connection initialConnection = Connection.ok,
    DownstreamBridge? bridge,
  }) : connection = ValueNotifier<Connection>(initialConnection),
       sendLatency = ValueNotifier<Duration>(initialSendLatency),
       batchSize = ValueNotifier<int>(initialBatchSize),
       maxAccumulateTimeN = ValueNotifier<Duration>(initialAccumulate),
       _bridge = bridge;

  final DownstreamBridge? _bridge;

  @override
  final String id;

  @override
  final SubscriptionFilter filter;

  @override
  final bool allowHardDelete;

  /// Live-tunable network simulation. Drives send() branch selection.
  @override
  final ValueNotifier<Connection> connection;

  /// Live-tunable delay applied when `connection = ok` before returning
  /// `SendOk`. Makes the drain/retry cadence observable in the UI.
  @override
  final ValueNotifier<Duration> sendLatency;

  /// Live-tunable upper bound on current-batch length. When the delivery
  /// cycle fills the queue it asks `canAddToBatch` once per candidate; when the batch reaches this
  /// length, the next candidate is rejected.
  @override
  final ValueNotifier<int> batchSize;

  /// Backing notifier for `maxAccumulateTime`. Named with an `N` suffix
  /// so the interface getter can keep the un-suffixed name.
  @override
  final ValueNotifier<Duration> maxAccumulateTimeN;

  @override
  Duration get maxAccumulateTime => maxAccumulateTimeN.value;

  @override
  bool get serializesNatively => true;

  // The demo's in-process hub keeps no deliveries to serve, so every pull
  // fails permanently.
  @override
  ChannelPull? get channelPull =>
      (request) async =>
          const PullPermanent(error: 'the demo hub serves no pull');

  @override
  String get wireFormat => DeliveryEnvelope.wireFormat;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < batchSize.value;

  // The delivery cycle serializes a native batch itself when it fills the
  // queue; the
  // contract guarantees transform is never invoked when serializesNatively
  // is true, so this throw is defense-in-depth, not a code path.
  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    throw StateError(
      'transform must not be called on a native destination '
      '(serializesNatively=true); the delivery cycle builds the envelope '
      'itself.',
    );
  }

  // Demo: routes by `connection.value`. `ok` answers after `sendLatency`;
  // `broken` returns SendTransient; `rejecting` returns SendPermanent.
  // Real native destinations would POST the delivery's bytes (built by the
  // drainer from envelope_metadata + the row's events) to a server's
  // receiver endpoint. With no bridge, the demo answers as an in-memory
  // receiver that keeps only its record of each channel: the drainer marks
  // an item sent only on a receiver record naming its delivery.
  @override
  Future<SendResult> send(WirePayload payload) async {
    switch (connection.value) {
      case Connection.ok:
        await Future<void>.delayed(sendLatency.value);
        final bridge = _bridge;
        if (bridge != null) {
          return bridge.deliver(payload);
        }
        return _answerInMemory(payload);
      case Connection.broken:
        return const SendTransient(error: 'simulated disconnect');
      case Connection.rejecting:
        return const SendPermanent(error: 'simulated rejection');
    }
  }

  /// The in-memory receiver's record of each channel.
  final Map<DeliveryChannel, DeliveryRecord> _records =
      <DeliveryChannel, DeliveryRecord>{};

  SendResult _answerInMemory(WirePayload payload) {
    final DeliveryEnvelope delivery;
    try {
      delivery = DeliveryEnvelope.decode(payload.bytes);
    } on IngestDecodeFailure catch (e) {
      return SendPermanent(error: e.toString());
    }
    final channel = delivery.channel;
    final record = _records[channel] ?? DeliveryRecord.none;
    final ReceiverResponse answer;
    if (delivery.deliveryNumber == record.deliveryNumber &&
        delivery.deliveryHash == record.deliveryHash) {
      answer = ReceiverAcknowledgement(
        channel: channel,
        receiverDatabaseId: _receiverDatabaseId,
        record: record,
        outcome: AcknowledgementOutcome.represented,
      );
    } else if (delivery.deliveryNumber == record.deliveryNumber + 1 &&
        delivery.previousDeliveryHash == record.deliveryHash) {
      final accepted = DeliveryRecord(
        deliveryNumber: delivery.deliveryNumber,
        deliveryHash: delivery.deliveryHash,
      );
      _records[channel] = accepted;
      answer = ReceiverAcknowledgement(
        channel: channel,
        receiverDatabaseId: _receiverDatabaseId,
        record: accepted,
        outcome: AcknowledgementOutcome.accepted,
      );
    } else {
      answer = ReceiverRefusal(
        channel: channel,
        receiverDatabaseId: _receiverDatabaseId,
        record: record,
        refusal: RefusalKind.outOfSequence,
      );
    }
    return decodeReceiverAnswer(answer.encode());
  }

  static const String _receiverDatabaseId = 'demo-in-memory-receiver';
}
