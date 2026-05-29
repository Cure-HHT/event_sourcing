// Verifies: EVS-PRD-subscription/A (Update<T> and SubscriptionMode<T> sealed
//   types carry correct structural shapes for both subscription kinds)
// Verifies: EVS-PRD-subscription/C (sequence field present on every variant
//   provides the per-subscription ordering anchor)
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Update<T>', () {
    test('Snapshot carries optional value and as-of sequence', () {
      const u = Snapshot<String>(value: 'hello', sequence: 42);
      expect(u.value, 'hello');
      expect(u.sequence, 42);
    });

    test('Snapshot allows null value (for absent aggregate)', () {
      const u = Snapshot<String>(value: null, sequence: 0);
      expect(u.value, isNull);
    });

    test('Delta carries value, sequence, cause', () {
      const u = Delta<int>(value: 7, sequence: 5, cause: 'updated');
      expect(u.value, 7);
      expect(u.cause, 'updated');
    });

    test('Tombstone carries aggregateId and sequence', () {
      const u = Tombstone<int>(aggregateId: 'a1', sequence: 10);
      expect(u.aggregateId, 'a1');
      expect(u.sequence, 10);
    });

    test('EndOfReplay carries sequence and is a subtype of Update<T>', () {
      const u = EndOfReplay<String>(sequence: 42);
      expect(u, isA<Update<String>>());
      expect(u.sequence, 42);
    });

    test('Pattern matching across variants', () {
      Update<int> any = const Snapshot<int>(value: 1, sequence: 0);
      final tag = switch (any) {
        Snapshot() => 'snap',
        EndOfReplay() => 'eor',
        Delta() => 'delta',
        Tombstone() => 'tomb',
      };
      expect(tag, 'snap');
    });
  });

  group('SubscriptionMode<T>', () {
    test('Events is a const, parameterized over StoredEvent', () {
      const e = Events();
      expect(e, isA<SubscriptionMode<StoredEvent>>());
    });

    test('AggregateMode carries viewName and mapper', () {
      final a = AggregateMode<String>(
        viewName: 'diary_entries',
        mapper: (m) => m['title'] as String,
      );
      expect(a.viewName, 'diary_entries');
      expect(a.mapper({'title': 'hi'}), 'hi');
    });
  });
}
