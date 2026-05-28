// Verifies: EVS-PRD-cross-process-event-transport/A — round-trip codec
//   for SubscriptionFilter (carried inside SubscribeMsg envelopes).

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/filter_codec.dart';

void main() {
  test('round-trips empty filter', () {
    const original = SubscriptionFilter();
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips filter with entryTypes', () {
    const original = SubscriptionFilter(entryTypes: {'note', 'greeting'});
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips filter with aggregateTypes + eventTypes', () {
    const original = SubscriptionFilter(
      aggregateTypes: {'note'},
      eventTypes: {'note_updated'},
    );
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips filter with includeSystemEvents=true', () {
    const original = SubscriptionFilter(includeSystemEvents: true);
    final json = FilterCodec.encode(original);
    expect(json['includeSystemEvents'], true);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('encode omits includeSystemEvents when false (default)', () {
    const original = SubscriptionFilter();
    final json = FilterCodec.encode(original);
    expect(json.containsKey('includeSystemEvents'), isFalse);
  });
}
