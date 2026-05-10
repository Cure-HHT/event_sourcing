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

    // Dual-path behaviour: a path whose first segment is NOT a recognised root
    // ('data', 'aggregateId', 'eventType') falls through to the default case,
    // which sets root=event.data and keeps ALL segments as the navigation path.
    // This means 'answers.q1' and 'data.answers.q1' are equivalent — both
    // resolve to event.data['answers']['q1']. The following tests document and
    // pin this behaviour so readers and future maintainers are not surprised.
    test(
      'shorthand path without data. prefix resolves inside event.data',
      () async {
        // 'answers.q1' is the shorthand form; 'data.answers.q1' is the
        // explicit form. Both must reach the same value.
        const shorthand = CompositeKey(['answers.q1']);
        const explicit = CompositeKey(['data.answers.q1']);
        final event = _event(
          data: {
            'answers': {'q1': 'yes'},
          },
        );
        expect(shorthand.extract(event), 'yes');
        expect(explicit.extract(event), 'yes');
        // Confirm the two forms produce identical composite keys.
        expect(shorthand.extract(event), explicit.extract(event));
      },
    );

    test('aggregateId path returns event.aggregateId', () {
      const k = CompositeKey(['aggregateId']);
      expect(k.extract(_event(aggregateId: 'agg-xyz')), 'agg-xyz');
    });

    test('eventType path returns event.eventType', () {
      const k = CompositeKey(['eventType']);
      expect(k.extract(_event()), 'permission_granted');
    });

    test('composite key mixes aggregateId, eventType, and data paths', () {
      const k = CompositeKey(['aggregateId', 'eventType', 'data.role']);
      expect(
        k.extract(_event(aggregateId: 'a1', data: {'role': 'owner'})),
        'a1|permission_granted|owner',
      );
    });
  });
}
