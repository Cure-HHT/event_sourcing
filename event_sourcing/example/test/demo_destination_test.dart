// Verifies: EVS-PRD-destinations/A/E
// Verifies: EVS-PRD-destinations/B
// Verifies: EVS-PRD-destinations/E
// Verifies: EVS-PRD-destinations/E
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_datastore_demo/demo_destination.dart';
import 'package:event_sourcing_datastore_demo/demo_knobs.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _mkEvent(String id) => StoredEvent.synthetic(
  eventId: 'e-$id',
  aggregateId: 'agg-$id',
  entryType: 'demo_note',
  initiator: const UserInitiator('user-1'),
  clientTimestamp: DateTime.utc(2026, 4, 23, 12),
  eventHash: 'hash-$id',
);

void main() {
  group('DemoDestination implements Destination contract', () {
    test('id is stable once constructed', () {
      final d = DemoDestination(id: 'Primary');
      expect(d.id, 'Primary');
    });
    test('wireFormat is "demo-json-v1"', () {
      final d = DemoDestination(id: 'Primary');
      expect(d.wireFormat, 'demo-json-v1');
    });
    // DemoDestination.filter has null allow-lists and no predicate,
    // so it accepts all events.
    test('filter accepts events regardless of entryType / eventType', () {
      final d = DemoDestination(id: 'Primary');
      expect(d.filter.matches(_mkEvent('1')), isTrue);
      expect(d.filter.matches(_mkEvent('2')), isTrue);
    });
  });

  group('allowHardDelete', () {
    test('default is false', () {
      final d = DemoDestination(id: 'x');
      expect(d.allowHardDelete, isFalse);
    });
    test('opt-in flips to true', () {
      final d = DemoDestination(id: 'x', allowHardDelete: true);
      expect(d.allowHardDelete, isTrue);
    });
  });

  group('canAddToBatch', () {
    // batchSize.value is the live knob; changes take effect immediately.
    test('batchSize = 1 rejects a second event', () {
      final d = DemoDestination(id: 'x');
      d.batchSize.value = 1;
      expect(
        d.canAddToBatch(<StoredEvent>[_mkEvent('1')], _mkEvent('2')),
        isFalse,
      );
    });
    test('batchSize = 5 accepts 5th candidate, rejects 6th', () {
      final d = DemoDestination(id: 'x');
      d.batchSize.value = 5;
      final four = <StoredEvent>[
        _mkEvent('1'),
        _mkEvent('2'),
        _mkEvent('3'),
        _mkEvent('4'),
      ];
      expect(d.canAddToBatch(four, _mkEvent('5')), isTrue);
      final five = <StoredEvent>[...four, _mkEvent('5')];
      expect(d.canAddToBatch(five, _mkEvent('6')), isFalse);
    });
    test('empty batch accepts the first candidate regardless of batchSize', () {
      final d = DemoDestination(id: 'x');
      d.batchSize.value = 1;
      expect(d.canAddToBatch(const <StoredEvent>[], _mkEvent('1')), isTrue);
    });
  });

  group('maxAccumulateTime', () {
    // maxAccumulateTime reads the current maxAccumulateTimeN notifier value.
    test('reads the current notifier value', () {
      final d = DemoDestination(id: 'x');
      d.maxAccumulateTimeN.value = const Duration(seconds: 3);
      expect(d.maxAccumulateTime, const Duration(seconds: 3));
      d.maxAccumulateTimeN.value = Duration.zero;
      expect(d.maxAccumulateTime, Duration.zero);
    });
  });

  group('transform', () {
    // transform covers every event in the batch and stamps contentType
    // and transformVersion on the resulting WirePayload.
    test('produces JSON bytes + contentType + transformVersion', () async {
      final d = DemoDestination(id: 'x');
      final batch = <StoredEvent>[_mkEvent('1'), _mkEvent('2')];
      final payload = await d.transform(batch);
      expect(payload.contentType, 'application/json');
      expect(payload.transformVersion, 'demo-v1');
      final decoded =
          jsonDecode(utf8.decode(payload.bytes)) as Map<String, Object?>;
      expect(decoded['batch'], isA<List<Object?>>());
      final list = decoded['batch']! as List<Object?>;
      expect(list.length, 2);
      expect((list[0]! as Map<String, Object?>)['event_id'], 'e-1');
      expect((list[1]! as Map<String, Object?>)['event_id'], 'e-2');
    });
  });

  group('send routes by Connection (REQ-p01001)', () {
    // Tests each Connection variant: Connection.ok → SendOk after
    // waiting sendLatency; Connection.broken → SendTransient with
    // "simulated disconnect"; Connection.rejecting → SendPermanent
    // with "simulated rejection".
    test('Connection.ok returns SendOk after the latency window', () async {
      final d = DemoDestination(id: 'x');
      d.sendLatency.value = const Duration(milliseconds: 40);
      final payload = await d.transform(<StoredEvent>[_mkEvent('1')]);
      final stopwatch = Stopwatch()..start();
      final result = await d.send(payload);
      stopwatch.stop();
      expect(result, isA<SendOk>());
      expect(stopwatch.elapsedMilliseconds, greaterThanOrEqualTo(30));
    });
    test('Connection.broken returns SendTransient immediately', () async {
      final d = DemoDestination(id: 'x');
      d.sendLatency.value = const Duration(seconds: 10);
      d.connection.value = Connection.broken;
      final payload = await d.transform(<StoredEvent>[_mkEvent('1')]);
      final stopwatch = Stopwatch()..start();
      final result = await d.send(payload);
      stopwatch.stop();
      expect(result, isA<SendTransient>());
      expect((result as SendTransient).error, 'simulated disconnect');
      expect(stopwatch.elapsedMilliseconds, lessThan(500));
    });
    test('Connection.rejecting returns SendPermanent immediately', () async {
      final d = DemoDestination(id: 'x');
      d.sendLatency.value = const Duration(seconds: 10);
      d.connection.value = Connection.rejecting;
      final payload = await d.transform(<StoredEvent>[_mkEvent('1')]);
      final stopwatch = Stopwatch()..start();
      final result = await d.send(payload);
      stopwatch.stop();
      expect(result, isA<SendPermanent>());
      expect((result as SendPermanent).error, 'simulated rejection');
      expect(stopwatch.elapsedMilliseconds, lessThan(500));
    });
  });

  group('initial-value wiring', () {
    test('constructor seeds notifiers with provided initial values', () {
      final d = DemoDestination(
        id: 'x',
        initialConnection: Connection.rejecting,
        initialSendLatency: const Duration(seconds: 7),
        initialBatchSize: 4,
        initialAccumulate: const Duration(seconds: 2),
      );
      expect(d.connection.value, Connection.rejecting);
      expect(d.sendLatency.value, const Duration(seconds: 7));
      expect(d.batchSize.value, 4);
      expect(d.maxAccumulateTimeN.value, const Duration(seconds: 2));
    });
  });
}
