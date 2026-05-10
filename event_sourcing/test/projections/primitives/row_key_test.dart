// event_sourcing/test/projections/primitives/row_key_test.dart
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _event({
  String aggregateId = 'agg-1',
  Map<String, Object?>? data,
}) => StoredEvent.synthetic(
  eventId: 'e-1',
  aggregateId: aggregateId,
  aggregateType: 'X',
  entryType: 'x',
  eventType: 'permission_granted',
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026, 5, 9),
  eventHash: 'h',
  data: data ?? {},
);

void main() {
  group('AggregateIdKey', () {
    test('returns event.aggregateId', () {
      const k = AggregateIdKey();
      expect(k.extract(_event(aggregateId: 'foo')), 'foo');
    });
  });

  group('CompositeKey', () {
    test('joins extracted path values with a separator', () {
      const k = CompositeKey(['data.role', 'data.permission', 'data.scope']);
      final key = k.extract(
        _event(
          data: {
            'role': 'admin',
            'permission': 'users.invite',
            'scope': 'site',
          },
        ),
      );
      expect(key, 'admin|users.invite|site');
    });

    test('throws when a required path is missing', () {
      const k = CompositeKey(['data.missing']);
      expect(
        () => k.extract(_event(data: {'role': 'admin'})),
        throwsStateError,
      );
    });
  });
}
