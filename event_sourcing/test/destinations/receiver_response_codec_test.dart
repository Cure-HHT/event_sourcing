// Verifies: EVS-DEV-delivery-receiver/L
// an acknowledgement carries exactly channel, receiver_database_id, record
//   (exactly delivery_number and delivery_hash) and outcome (accepted or
//   represented).
// Verifies: EVS-DEV-delivery-receiver/M
// a refusal carries exactly channel, receiver_database_id, record, refusal,
//   reason and refused_event_id, reason and refused_event_id null unless the
//   refusal is rejected.
// Verifies: EVS-DEV-delivery-channel/L
// the library's decoder maps an acknowledgement or refusal body to the send
//   outcome it states.
// Verifies: EVS-DEV-delivery-channel/S
// the decoder maps a delivery_hash_mismatch refusal to a transient failure.
// Verifies: EVS-DEV-delivery-channel/R
// the library's pull decoder maps a pull response to served, a transient
//   failure or a permanent failure.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/native_destination.dart';

const DeliveryChannel _channel = DeliveryChannel(
  senderDatabaseId: 'db-sender',
  destinationId: 'primary',
  registrationId: 'reg-1',
  generation: 1,
);

const DeliveryRecord _record = DeliveryRecord(
  deliveryNumber: 4,
  deliveryHash: 'h4',
);

Map<String, Object?> _json(Uint8List bytes) =>
    jsonDecode(utf8.decode(bytes)) as Map<String, Object?>;

Uint8List _bytes(Object? json) =>
    Uint8List.fromList(utf8.encode(jsonEncode(json)));

ReceiverRefusal _refusal(
  RefusalKind kind, {
  String? reason,
  String? refusedEventId,
}) => ReceiverRefusal(
  channel: _channel,
  receiverDatabaseId: 'db-receiver',
  record: _record,
  refusal: kind,
  reason: reason,
  refusedEventId: refusedEventId,
);

void main() {
  group('response bodies', () {
    test('an acknowledgement carries exactly its keys', () {
      for (final outcome in AcknowledgementOutcome.values) {
        final ack = ReceiverAcknowledgement(
          channel: _channel,
          receiverDatabaseId: 'db-receiver',
          record: _record,
          outcome: outcome,
        );
        final body = _json(ack.encode());
        expect(body.keys.toSet(), <String>{
          'channel',
          'receiver_database_id',
          'record',
          'outcome',
        });
        expect((body['record']! as Map).keys.toSet(), <String>{
          'delivery_number',
          'delivery_hash',
        });
        expect((body['channel']! as Map).keys.toSet(), <String>{
          'sender_database_id',
          'destination_id',
          'registration_id',
          'generation',
        });
        expect(body['outcome'], outcome.wire);
        expect(ReceiverResponse.decode(ack.encode()), ack);
      }
      expect(AcknowledgementOutcome.values.map((o) => o.wire).toSet(), <String>{
        'accepted',
        'represented',
      });
    });

    test('a refusal carries exactly its keys', () {
      final refusals = <ReceiverRefusal>[
        _refusal(RefusalKind.outOfSequence),
        _refusal(RefusalKind.deliveryHashMismatch),
        _refusal(
          RefusalKind.rejected,
          reason: 'batch_malformed',
          refusedEventId: null,
        ),
        _refusal(
          RefusalKind.rejected,
          reason: 'validation_failed',
          refusedEventId: 'e7',
        ),
      ];
      for (final refusal in refusals) {
        final body = _json(refusal.encode());
        expect(body.keys.toSet(), <String>{
          'channel',
          'receiver_database_id',
          'record',
          'refusal',
          'reason',
          'refused_event_id',
        });
        expect(body['refusal'], refusal.refusal.wire);
        expect(ReceiverResponse.decode(refusal.encode()), refusal);
      }
      expect(RefusalKind.values.map((k) => k.wire).toSet(), <String>{
        'out_of_sequence',
        'delivery_hash_mismatch',
        'rejected',
      });
    });

    test('only a rejected refusal carries a reason and a refused event', () {
      expect(
        () => _refusal(RefusalKind.outOfSequence, reason: 'x'),
        throwsArgumentError,
      );
      expect(
        () => _refusal(RefusalKind.deliveryHashMismatch, refusedEventId: 'e'),
        throwsArgumentError,
      );
      expect(() => _refusal(RefusalKind.rejected), throwsArgumentError);
      final body = _json(_refusal(RefusalKind.outOfSequence).encode());
      expect(body['reason'], isNull);
      expect(body['refused_event_id'], isNull);
    });

    test('a body without exactly its keys does not decode', () {
      final ack = _json(
        const ReceiverAcknowledgement(
          channel: _channel,
          receiverDatabaseId: 'db-receiver',
          record: _record,
          outcome: AcknowledgementOutcome.accepted,
        ).encode(),
      );
      expect(
        () => ReceiverResponse.decode(
          _bytes(<String, Object?>{...ack}..remove('record')),
        ),
        throwsFormatException,
      );
      expect(
        () => ReceiverResponse.decode(
          _bytes(<String, Object?>{...ack, 'extra': 1}),
        ),
        throwsFormatException,
      );
      expect(
        () => ReceiverResponse.decode(
          _bytes(<String, Object?>{...ack, 'outcome': 'maybe'}),
        ),
        throwsFormatException,
      );
    });
  });

  group('the sender decoder', () {
    test('an acceptance and a re-presentation carry the receiver response', () {
      for (final outcome in AcknowledgementOutcome.values) {
        final ack = ReceiverAcknowledgement(
          channel: _channel,
          receiverDatabaseId: 'db-receiver',
          record: _record,
          outcome: outcome,
        );
        expect(decodeReceiverAnswer(ack.encode()), SendAnswered(ack));
      }
    });

    test('an out_of_sequence refusal carries the receiver response', () {
      final refusal = _refusal(RefusalKind.outOfSequence);
      expect(decodeReceiverAnswer(refusal.encode()), SendAnswered(refusal));
    });

    test('a delivery_hash_mismatch refusal is a transient failure', () {
      expect(
        decodeReceiverAnswer(
          _refusal(RefusalKind.deliveryHashMismatch).encode(),
        ),
        isA<SendTransient>(),
      );
    });

    test('a rejected refusal is a permanent failure naming its reason', () {
      final result = decodeReceiverAnswer(
        _refusal(
          RefusalKind.rejected,
          reason: 'batch_malformed',
          refusedEventId: 'e7',
        ).encode(),
      );
      expect(
        result,
        isA<SendPermanent>().having(
          (r) => r.error,
          'error',
          allOf(contains('batch_malformed'), contains('e7')),
        ),
      );
    });

    test('a body that is not a receiver response is an acceptance carrying '
        'no record', () {
      expect(decodeReceiverAnswer(Uint8List(0)), const SendOk());
      expect(
        decodeReceiverAnswer(_bytes(<String, Object?>{'ok': true})),
        const SendOk(),
      );
    });
  });

  group('the pull decoder', () {
    test('a channel listing is served', () {
      const listing = ChannelListing(
        receiverDatabaseId: 'db-receiver',
        senderDatabaseId: 'db-sender',
        channels: <ListedChannel>[
          ListedChannel(channel: _channel, record: _record),
        ],
      );
      final body = _json(listing.encode());
      expect(body.keys.toSet(), <String>{
        'receiver_database_id',
        'sender_database_id',
        'channels',
      });
      expect(((body['channels']! as List).single as Map).keys.toSet(), <String>{
        'channel',
        'record',
      });
      expect(decodePullResponse(listing.encode()), const PullServed(listing));
    });

    test('a range of deliveries is served, with the delivery it cannot '
        'serve named', () {
      const range = DeliveryRange(
        receiverDatabaseId: 'db-receiver',
        channel: _channel,
        record: _record,
        deliveries: <ServedDelivery>[
          ServedDelivery(
            deliveryNumber: 1,
            previousDeliveryHash: null,
            deliveryHash: 'h1',
            attributes: <String, Object?>{'later': 1},
            events: <Map<String, Object?>>[
              <String, Object?>{'event_id': 'e1'},
            ],
          ),
        ],
        unservableDeliveryNumber: 2,
      );
      final body = _json(range.encode());
      expect(body.keys.toSet(), <String>{
        'receiver_database_id',
        'channel',
        'record',
        'deliveries',
        'unservable_delivery_number',
      });
      expect(
        ((body['deliveries']! as List).single as Map).keys.toSet(),
        <String>{
          'delivery_number',
          'previous_delivery_hash',
          'delivery_hash',
          'attributes',
          'events',
        },
      );
      expect(decodePullResponse(range.encode()), const PullServed(range));
    });

    test('an unavailable receiver is a transient failure', () {
      const refusal = PullRefusal(
        receiverDatabaseId: 'db-receiver',
        refusal: PullRefusalKind.unavailable,
        reason: 'busy',
      );
      expect(decodePullResponse(refusal.encode()), isA<PullTransient>());
    });

    test('a rejected pull and a body that does not decode are permanent '
        'failures', () {
      const refusal = PullRefusal(
        receiverDatabaseId: 'db-receiver',
        refusal: PullRefusalKind.rejected,
        reason: 'request_malformed',
      );
      expect(
        decodePullResponse(refusal.encode()),
        isA<PullPermanent>().having(
          (p) => p.error,
          'error',
          contains('request_malformed'),
        ),
      );
      expect(decodePullResponse(Uint8List(0)), isA<PullPermanent>());
      expect(
        decodePullResponse(_bytes(<String, Object?>{'channels': 1})),
        isA<PullPermanent>(),
      );
    });

    test('pull requests round-trip', () {
      const listing = ChannelListingPull(senderDatabaseId: 'db-sender');
      const range = DeliveryRangePull(
        channel: _channel,
        fromDeliveryNumber: 1,
        toDeliveryNumber: 4,
      );
      expect(PullRequest.fromJson(listing.toJson()), listing);
      expect(PullRequest.fromJson(range.toJson()), range);
      expect(
        () => PullRequest.fromJson(<String, Object?>{
          ...range.toJson(),
          'from_delivery_number': 0,
        }),
        throwsFormatException,
      );
    });
  });

  group('the native test receiver', () {
    DeliveryEnvelope delivery(int number, String? link, String eventHash) =>
        DeliveryEnvelope.seal(
          batchId: 'b$number',
          senderHop: 'h',
          senderIdentifier: 'i',
          senderSoftwareVersion: 'v',
          sentAt: DateTime.utc(2026),
          channel: _channel,
          deliveryNumber: number,
          previousDeliveryHash: link,
          events: <Map<String, Object?>>[
            <String, Object?>{'event_id': eventHash, 'event_hash': eventHash},
          ],
        );

    WirePayload payload(DeliveryEnvelope e) => WirePayload(
      bytes: e.encode(),
      contentType: DeliveryEnvelope.wireFormat,
      transformVersion: null,
    );

    ReceiverResponse answered(SendResult r) => (r as SendAnswered).response;

    test('accepts in step, acknowledges a re-presentation and refuses out '
        'of sequence', () async {
      final receiver = NativeDestination();
      final first = delivery(1, null, 'a');
      final ack1 = answered(await receiver.send(payload(first)));
      expect(ack1, isA<ReceiverAcknowledgement>());
      expect(
        (ack1 as ReceiverAcknowledgement).outcome,
        AcknowledgementOutcome.accepted,
      );
      expect(
        ack1.record,
        DeliveryRecord(deliveryNumber: 1, deliveryHash: first.deliveryHash),
      );

      final again = answered(await receiver.send(payload(first)));
      expect(
        (again as ReceiverAcknowledgement).outcome,
        AcknowledgementOutcome.represented,
      );

      final skipped = answered(
        await receiver.send(payload(delivery(3, 'x', 'c'))),
      );
      expect((skipped as ReceiverRefusal).refusal, RefusalKind.outOfSequence);
      expect(skipped.record, ack1.record);
      expect(receiver.recordOf(_channel), ack1.record);
    });

    test('refuses a delivery hash that does not recompute as transient, '
        'and a batch in another format as rejected', () async {
      final receiver = NativeDestination();
      final wire =
          jsonDecode(utf8.decode(delivery(1, null, 'a').encode()))
              as Map<String, Object?>;
      wire['delivery_hash'] = 'tampered';
      expect(
        await receiver.send(
          WirePayload(
            bytes: _bytes(wire),
            contentType: DeliveryEnvelope.wireFormat,
            transformVersion: null,
          ),
        ),
        isA<SendTransient>(),
      );
      final v2 = BatchEnvelope(
        batchFormatVersion: '2',
        batchId: 'b',
        senderHop: 'h',
        senderIdentifier: 'i',
        senderSoftwareVersion: 'v',
        sentAt: DateTime.utc(2026),
        events: const <Map<String, Object?>>[<String, Object?>{}],
      );
      expect(
        await receiver.send(
          WirePayload(
            bytes: v2.encode(),
            contentType: BatchEnvelope.wireFormat,
            transformVersion: null,
          ),
        ),
        isA<SendPermanent>(),
      );
      expect(receiver.recordOf(_channel), DeliveryRecord.none);
    });

    test('a scripted answer takes precedence', () async {
      final receiver = NativeDestination(
        script: <SendResult>[const SendTransient(error: 'scripted')],
      );
      expect(
        await receiver.send(payload(delivery(1, null, 'a'))),
        const SendTransient(error: 'scripted'),
      );
      expect(receiver.recordOf(_channel), DeliveryRecord.none);
    });

    test(
      'serves its channel listing and deliveries through its pull',
      () async {
        final receiver = NativeDestination();
        final first = delivery(1, null, 'a');
        await receiver.send(payload(first));
        final pull = receiver.channelPull;
        final listing = await pull(
          const ChannelListingPull(senderDatabaseId: 'db-sender'),
        );
        expect(
          ((listing as PullServed).response as ChannelListing).channels.single,
          ListedChannel(channel: _channel, record: receiver.recordOf(_channel)),
        );
        final range = await pull(
          const DeliveryRangePull(
            channel: _channel,
            fromDeliveryNumber: 1,
            toDeliveryNumber: 2,
          ),
        );
        final served = (range as PullServed).response as DeliveryRange;
        expect(served.deliveries.single.deliveryHash, first.deliveryHash);
        expect(served.unservableDeliveryNumber, 2);
      },
    );
  });
}
