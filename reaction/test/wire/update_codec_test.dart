// Verifies: EVS-PRD-cross-process-event-transport/A
// round-trip codec
//   for the four Update<T> variants preserves all fields.
// Verifies: EVS-PRD-cross-process-event-transport/B
// sequence +
//   subscriptionId on every encoded envelope.
// Verifies: EVS-PRD-cross-process-event-transport/G
// raw rows ship as
//   Map<String, Object?>; consumer-side mapper applies later.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/update_codec.dart';

void main() {
  test('round-trips Snapshot envelope', () {
    const original = Snapshot<Map<String, Object?>>(
      value: {'aggregateId': 'agg-1', 'title': 'hello'},
      sequence: 42,
    );
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'snapshot');
    expect(json['subscriptionId'], 'sub-1');
    expect(json['sequence'], 42);
    expect(json['value'], {'aggregateId': 'agg-1', 'title': 'hello'});

    final decoded = UpdateCodec.decode(json) as Snapshot<Map<String, Object?>>;
    expect(decoded.value, {'aggregateId': 'agg-1', 'title': 'hello'});
    expect(decoded.sequence, 42);
  });

  test('round-trips Snapshot envelope with null value', () {
    const original = Snapshot<Map<String, Object?>>(value: null, sequence: 7);
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'snapshot');
    expect(json['value'], isNull);
    expect(json.containsKey('value'), isTrue);

    final decoded = UpdateCodec.decode(json) as Snapshot<Map<String, Object?>>;
    expect(decoded.value, isNull);
    expect(decoded.sequence, 7);
  });

  test('round-trips Delta envelope', () {
    const original = Delta<Map<String, Object?>>(
      value: {'aggregateId': 'agg-1', 'title': 'world'},
      sequence: 43,
      cause: 'event-abc',
    );
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'delta');
    expect(json['sequence'], 43);
    expect(json['value'], {'aggregateId': 'agg-1', 'title': 'world'});
    expect(json['cause'], 'event-abc');

    final decoded = UpdateCodec.decode(json) as Delta<Map<String, Object?>>;
    expect(decoded.value, {'aggregateId': 'agg-1', 'title': 'world'});
    expect(decoded.sequence, 43);
    expect(decoded.cause, 'event-abc');
  });

  test('round-trips Tombstone envelope', () {
    const original = Tombstone<Map<String, Object?>>(
      aggregateId: 'agg-1',
      sequence: 44,
    );
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'tombstone');
    expect(json['aggregateId'], 'agg-1');
    expect(json['sequence'], 44);

    final decoded = UpdateCodec.decode(json) as Tombstone<Map<String, Object?>>;
    expect(decoded.aggregateId, 'agg-1');
    expect(decoded.sequence, 44);
  });

  test('round-trips EndOfReplay envelope', () {
    const original = EndOfReplay<Map<String, Object?>>(sequence: 45);
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'end_of_replay');
    expect(json['sequence'], 45);
    expect(json.containsKey('aggregateId'), isFalse);
    expect(json.containsKey('value'), isFalse);
    expect(json.containsKey('cause'), isFalse);

    final decoded =
        UpdateCodec.decode(json) as EndOfReplay<Map<String, Object?>>;
    expect(decoded.sequence, 45);
  });

  test('decode reads subscriptionId from envelope', () {
    final encoded = UpdateCodec.encode(
      const EndOfReplay<Map<String, Object?>>(sequence: 1),
      subscriptionId: 'sub-xyz',
    );
    expect(UpdateCodec.subscriptionIdOf(encoded), 'sub-xyz');
  });

  test('decode rejects unknown type', () {
    expect(
      () => UpdateCodec.decode({
        'type': 'unknown',
        'subscriptionId': 'x',
        'sequence': 1,
      }),
      throwsA(isA<FormatException>()),
    );
  });
}
