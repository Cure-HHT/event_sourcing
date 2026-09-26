import 'package:event_sourcing/event_sourcing.dart';

/// Test-support [Destination] that declares `serializesNatively == true`
/// and answers like a receiver of the library's native batch format.
///
/// `transform` throws if invoked: `fillBatch` builds the envelope of a
/// native destination from the library's source identity.
///
/// Each `send` pops the next [SendResult] of the script supplied at
/// construction (or pushed with [enqueueScript]). With the script empty it
/// answers as an in-step receiver: it decodes the payload as a
/// [DeliveryEnvelope], keeps its record of each channel, and answers with
/// the library's acknowledgement or refusal body passed through
/// [decodeReceiverAnswer]:
///
/// - the delivery that follows its record is accepted;
/// - the delivery its record names is acknowledged as re-presented;
/// - a delivery whose hash does not recompute is refused with
///   `delivery_hash_mismatch`;
/// - a batch that does not decode is refused as `rejected`, naming the
///   decoder's reason;
/// - every other delivery is refused `out_of_sequence`.
///
/// Its [channelPull] serves the channels and the deliveries it accepted.
class NativeDestination extends Destination {
  NativeDestination({
    this.id = 'native',
    SubscriptionFilter? filter,
    List<SendResult>? script,
    this.batchCapacity = 1,
    this.maxAccumulateTime = Duration.zero,
    this.allowHardDelete = false,
    this.receiverDatabaseId = 'native-receiver',
  }) : _script = script ?? <SendResult>[],
       _filter = filter ?? const SubscriptionFilter();

  @override
  final String id;

  @override
  String get wireFormat => 'esd/batch@2';

  @override
  bool get serializesNatively => true;

  /// Cap on events accepted into a single batch by [canAddToBatch].
  final int batchCapacity;

  @override
  final Duration maxAccumulateTime;

  @override
  final bool allowHardDelete;

  /// The database identity the in-step receiver answers with.
  final String receiverDatabaseId;

  final SubscriptionFilter _filter;
  final List<SendResult> _script;

  /// The deliveries the in-step receiver accepted, by channel, in order.
  final Map<DeliveryChannel, List<DeliveryEnvelope>> accepted =
      <DeliveryChannel, List<DeliveryEnvelope>>{};

  /// Every send() call: the payload handed in.
  final List<WirePayload> sent = <WirePayload>[];

  /// Every send() call: the SendResult that was returned, in order.
  final List<SendResult> returned = <SendResult>[];

  @override
  SubscriptionFilter get filter => _filter;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < batchCapacity;

  /// Native destinations do not own a transform: `fillBatch` builds their
  /// envelope from the library's source identity. Any call here is a
  /// contract violation.
  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    throw StateError(
      'NativeDestination($id).transform invoked: fillBatch must build '
      'envelope metadata from source identity instead',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    final result = _script.isNotEmpty
        ? _script.removeAt(0)
        : decodeReceiverAnswer(_answer(payload).encode());
    returned.add(result);
    return result;
  }

  /// Push [result] onto the tail of the script.
  void enqueueScript(SendResult result) => _script.add(result);

  /// The in-step receiver's record of [channel].
  DeliveryRecord recordOf(DeliveryChannel channel) {
    final deliveries = accepted[channel];
    if (deliveries == null || deliveries.isEmpty) return DeliveryRecord.none;
    final last = deliveries.last;
    return DeliveryRecord(
      deliveryNumber: last.deliveryNumber,
      deliveryHash: last.deliveryHash,
    );
  }

  ReceiverResponse _answer(WirePayload payload) {
    final DeliveryEnvelope delivery;
    try {
      delivery = DeliveryEnvelope.decode(payload.bytes);
    } on IngestDecodeFailure catch (e) {
      return ReceiverRefusal(
        channel: const DeliveryChannel(
          senderDatabaseId: '',
          destinationId: '',
          registrationId: '',
          generation: 1,
        ),
        receiverDatabaseId: receiverDatabaseId,
        record: DeliveryRecord.none,
        refusal: RefusalKind.rejected,
        reason: e.reason,
      );
    }
    final channel = delivery.channel;
    final record = recordOf(channel);
    ReceiverRefusal refuse(RefusalKind kind) => ReceiverRefusal(
      channel: channel,
      receiverDatabaseId: receiverDatabaseId,
      record: record,
      refusal: kind,
    );
    ReceiverAcknowledgement acknowledge(
      AcknowledgementOutcome outcome,
      DeliveryRecord record,
    ) => ReceiverAcknowledgement(
      channel: channel,
      receiverDatabaseId: receiverDatabaseId,
      record: record,
      outcome: outcome,
    );
    if (delivery.recomputedDeliveryHash != delivery.deliveryHash) {
      return refuse(RefusalKind.deliveryHashMismatch);
    }
    if (delivery.deliveryNumber == record.deliveryNumber &&
        delivery.deliveryHash == record.deliveryHash) {
      return acknowledge(AcknowledgementOutcome.represented, record);
    }
    if (delivery.deliveryNumber != record.deliveryNumber + 1 ||
        delivery.previousDeliveryHash != record.deliveryHash) {
      return refuse(RefusalKind.outOfSequence);
    }
    accepted.putIfAbsent(channel, () => <DeliveryEnvelope>[]).add(delivery);
    return acknowledge(AcknowledgementOutcome.accepted, recordOf(channel));
  }

  @override
  ChannelPull get channelPull => _pull;

  Future<PullOutcome> _pull(PullRequest request) async {
    final PullResponse response;
    switch (request) {
      case ChannelListingPull(:final senderDatabaseId):
        response = ChannelListing(
          receiverDatabaseId: receiverDatabaseId,
          senderDatabaseId: senderDatabaseId,
          channels: <ListedChannel>[
            for (final channel in accepted.keys)
              if (channel.senderDatabaseId == senderDatabaseId)
                ListedChannel(channel: channel, record: recordOf(channel)),
          ],
        );
      case DeliveryRangePull(
        :final channel,
        :final fromDeliveryNumber,
        :final toDeliveryNumber,
      ):
        final held = accepted[channel] ?? const <DeliveryEnvelope>[];
        final served = <ServedDelivery>[];
        int? unservable;
        for (var n = fromDeliveryNumber; n <= toDeliveryNumber; n++) {
          if (n > held.length) {
            unservable = n;
            break;
          }
          final d = held[n - 1];
          served.add(
            ServedDelivery(
              deliveryNumber: d.deliveryNumber,
              previousDeliveryHash: d.previousDeliveryHash,
              deliveryHash: d.deliveryHash,
              attributes: d.attributes,
              events: d.events,
            ),
          );
        }
        response = DeliveryRange(
          receiverDatabaseId: receiverDatabaseId,
          channel: channel,
          record: recordOf(channel),
          deliveries: served,
          unservableDeliveryNumber: unservable,
        );
    }
    return decodePullResponse(response.encode());
  }
}
