// Verifies: EVS-PRD-event-log/A — Initiator is part of the immutable event
//   record; JSON round-trip preserves all variants.
// Verifies: EVS-PRD-portability/C — sealed enum JSON serialisation is
//   platform-independent.
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Initiator', () {
    // A sealed switch over Initiator covers all three variants at compile time
    // (analyzer flags a missing arm).
    test('sealed pattern-match is exhaustive over three variants', () {
      String describe(Initiator i) => switch (i) {
        UserInitiator() => 'user',
        AutomationInitiator() => 'automation',
        AnonymousInitiator() => 'anonymous',
      };
      expect(describe(const UserInitiator('u')), 'user');
      expect(describe(const AutomationInitiator(service: 's')), 'automation');
      expect(describe(const AnonymousInitiator(ipAddress: null)), 'anonymous');
    });

    // UserInitiator serializes with a 'type' discriminator and round-trips.
    test('UserInitiator JSON round-trips with type discriminator', () {
      const u = UserInitiator('user-123');
      expect(u.toJson(), {'type': 'user', 'user_id': 'user-123'});
      expect(Initiator.fromJson(u.toJson()), u);
    });

    // AutomationInitiator round-trips with both the required service field
    // and the optional nullable triggeringEventId.
    test('AutomationInitiator JSON round-trips with both required '
        'and optional fields', () {
      const a1 = AutomationInitiator(service: 'mobile-bg-sync');
      expect(a1.toJson(), {
        'type': 'automation',
        'service': 'mobile-bg-sync',
        'triggering_event_id': null,
      });
      expect(Initiator.fromJson(a1.toJson()), a1);

      const a2 = AutomationInitiator(
        service: 'email-service',
        triggeringEventId: 'evt-9',
      );
      expect(a2.toJson(), {
        'type': 'automation',
        'service': 'email-service',
        'triggering_event_id': 'evt-9',
      });
      expect(Initiator.fromJson(a2.toJson()), a2);
    });

    test('AnonymousInitiator accepts null ipAddress', () {
      const a = AnonymousInitiator(ipAddress: null);
      expect(a.toJson(), {'type': 'anonymous', 'ip_address': null});
      expect(Initiator.fromJson(a.toJson()), a);
    });

    test('AnonymousInitiator with ip round-trips', () {
      const a = AnonymousInitiator(ipAddress: '203.0.113.7');
      expect(a.toJson(), {'type': 'anonymous', 'ip_address': '203.0.113.7'});
      expect(Initiator.fromJson(a.toJson()), a);
    });

    test('fromJson rejects unknown type with FormatException', () {
      expect(
        () => Initiator.fromJson({'type': 'bogus'}),
        throwsFormatException,
      );
    });

    test('fromJson rejects missing user_id on user variant', () {
      expect(() => Initiator.fromJson({'type': 'user'}), throwsFormatException);
    });

    test('fromJson rejects missing service on automation variant', () {
      expect(
        () => Initiator.fromJson({'type': 'automation'}),
        throwsFormatException,
      );
    });

    test('fromJson rejects non-string triggering_event_id', () {
      expect(
        () => Initiator.fromJson({
          'type': 'automation',
          'service': 's',
          'triggering_event_id': 42,
        }),
        throwsFormatException,
      );
    });

    test('fromJson rejects non-string ip_address', () {
      expect(
        () => Initiator.fromJson({
          'type': 'anonymous',
          'ip_address': <int>[127],
        }),
        throwsFormatException,
      );
    });

    test('equality and hashCode: equal variants compare equal', () {
      expect(const UserInitiator('x'), const UserInitiator('x'));
      expect(
        const UserInitiator('x').hashCode,
        const UserInitiator('x').hashCode,
      );
      expect(
        const AutomationInitiator(service: 's'),
        const AutomationInitiator(service: 's'),
      );
      expect(
        const AnonymousInitiator(ipAddress: null),
        const AnonymousInitiator(ipAddress: null),
      );
    });

    test('equality: different variants are not equal', () {
      expect(
        const UserInitiator('u') == const AnonymousInitiator(ipAddress: 'u'),
        isFalse,
      );
    });
  });
}
