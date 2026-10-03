import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/native_demo_destination.dart';
import 'package:flutter_test/flutter_test.dart';

WirePayload _payload() => WirePayload(
  bytes: Uint8List.fromList(<int>[1, 2, 3]),
  contentType: DeliveryEnvelope.wireFormat,
  transformVersion: null,
);

class _SpyBridge implements DownstreamBridge {
  _SpyBridge(this._result);
  final SendResult _result;
  int callCount = 0;

  @override
  Future<SendResult> deliver(WirePayload payload) async {
    callCount++;
    return _result;
  }
}

void main() {
  group('NativeDemoDestination.send with optional bridge', () {
    // With no bridge the demo answers as an in-memory receiver: the answer
    // carries its record of the channel, which names the delivery.
    test('connection=ok, bridge=null → the receiver record names the '
        'delivery', () async {
      final d = NativeDemoDestination();
      final delivery = DeliveryEnvelope.seal(
        batchId: 'b-1',
        senderHop: 'mobile',
        senderIdentifier: 'install',
        senderSoftwareVersion: 'demo@1',
        sentAt: DateTime.utc(2026, 9, 1),
        channel: const DeliveryChannel(
          senderDatabaseId: 'sender',
          destinationId: 'Native',
          registrationId: 'r1',
          generation: 1,
        ),
        deliveryNumber: 1,
        previousDeliveryHash: null,
        events: <Map<String, Object?>>[
          <String, Object?>{'event_id': 'e1', 'event_hash': 'h1'},
        ],
      );
      final payload = WirePayload(
        bytes: delivery.encode(),
        contentType: DeliveryEnvelope.wireFormat,
        transformVersion: null,
      );
      final result = await d.send(payload);
      expect(result, isA<SendAnswered>());
      expect(
        (result as SendAnswered).response.record,
        DeliveryRecord(deliveryNumber: 1, deliveryHash: delivery.deliveryHash),
      );
      final again = await d.send(payload);
      expect(
        ((again as SendAnswered).response as ReceiverAcknowledgement).outcome,
        AcknowledgementOutcome.represented,
      );
    });

    test('connection=ok, bridge returns SendOk → SendOk', () async {
      final spy = _SpyBridge(const SendOk());
      final d = NativeDemoDestination(bridge: spy);
      final result = await d.send(_payload());
      expect(result, isA<SendOk>());
      expect(spy.callCount, 1);
    });

    test(
      'connection=ok, bridge returns SendPermanent → SendPermanent',
      () async {
        final spy = _SpyBridge(const SendPermanent(error: 'decode bad'));
        final d = NativeDemoDestination(bridge: spy);
        final result = await d.send(_payload());
        expect(result, isA<SendPermanent>());
        expect((result as SendPermanent).error, 'decode bad');
        expect(spy.callCount, 1);
      },
    );

    test(
      'connection=broken, bridge wired → SendTransient, bridge not called',
      () async {
        final spy = _SpyBridge(const SendOk());
        final d = NativeDemoDestination(
          bridge: spy,
          initialConnection: Connection.broken,
        );
        final result = await d.send(_payload());
        expect(result, isA<SendTransient>());
        expect(spy.callCount, 0);
      },
    );

    test(
      'connection=rejecting, bridge wired → SendPermanent, bridge not called',
      () async {
        final spy = _SpyBridge(const SendOk());
        final d = NativeDemoDestination(
          bridge: spy,
          initialConnection: Connection.rejecting,
        );
        final result = await d.send(_payload());
        expect(result, isA<SendPermanent>());
        expect(spy.callCount, 0);
      },
    );

    test('sendLatency is awaited before bridge is called', () async {
      final spy = _SpyBridge(const SendOk());
      final d = NativeDemoDestination(
        bridge: spy,
        initialSendLatency: const Duration(milliseconds: 40),
      );
      final stopwatch = Stopwatch()..start();
      await d.send(_payload());
      stopwatch.stop();
      expect(stopwatch.elapsedMilliseconds, greaterThanOrEqualTo(30));
      expect(spy.callCount, 1);
    });
  });
}
