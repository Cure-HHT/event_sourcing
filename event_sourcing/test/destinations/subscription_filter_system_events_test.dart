// Verifies: EVS-PRD-destinations/B
// exercises the system-events opt-in of
// SubscriptionFilter: includeSystemEvents=false rejects all system entry
// types (default, so app destinations don't accidentally admit audit events);
// includeSystemEvents=true admits them bypassing the entryTypes allow-list.
// User entry types continue to use the entryTypes allow-list regardless.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

/// Build a synthetic [StoredEvent] for [SubscriptionFilter.matches]
/// assertions. `matches()` only inspects `entryType` and `eventType`, so
/// every other field is filled with valid placeholder data.
StoredEvent _mkEvent({
  required String entryType,
  String eventType = 'finalized',
}) => StoredEvent(
  key: 1,
  eventId: 'ev-$entryType',
  aggregateId: 'agg-1',
  aggregateType: 'note',
  entryType: entryType,
  entryTypeVersion: 1,
  libFormatVersion: 1,
  eventType: eventType,
  sequenceNumber: 1,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u1'),
  clientTimestamp: DateTime.utc(2026, 4, 26),
  eventHash: 'hash',
);

StoredEvent _systemEvent() =>
    _mkEvent(entryType: kDestinationRegisteredEntryType);

StoredEvent _userEvent(String entryType) => _mkEvent(entryType: entryType);

void main() {
  group('SubscriptionFilter.includeSystemEvents', () {
    // includeSystemEvents=false rejects system events regardless of
    // entryTypes content.
    test('includeSystemEvents=false rejects system events '
        'regardless of entryTypes', () {
      const f = SubscriptionFilter(entryTypes: {'demo_note'});
      expect(f.includeSystemEvents, isFalse);
      expect(f.matches(_systemEvent()), isFalse);
    });

    // includeSystemEvents=true admits system events even with an empty
    // entryTypes set (an empty set does not exclude them).
    test('includeSystemEvents=true admits system events even '
        'with empty entryTypes', () {
      const f = SubscriptionFilter(
        entryTypes: <String>{},
        includeSystemEvents: true,
      );
      expect(f.matches(_systemEvent()), isTrue);
    });

    // includeSystemEvents=true does not override entryTypes for user events;
    // user events still use the allow-list.
    test('includeSystemEvents=true still applies entryTypes '
        'for user events', () {
      const f = SubscriptionFilter(
        entryTypes: {'demo_note'},
        includeSystemEvents: true,
      );
      expect(f.matches(_userEvent('demo_note')), isTrue);
      expect(f.matches(_userEvent('red_button_pressed')), isFalse);
    });

    test('default includeSystemEvents is false', () {
      const f = SubscriptionFilter(entryTypes: {'demo_note'});
      expect(f.includeSystemEvents, isFalse);
    });

    // Every reserved system entry type is gated by the same flag (keyed
    // off the reserved set, not a single id).
    test('includeSystemEvents=true admits every reserved '
        'system entry type', () {
      const f = SubscriptionFilter(includeSystemEvents: true);
      for (final id in kReservedSystemEntryTypeIds) {
        expect(
          f.matches(_mkEvent(entryType: id)),
          isTrue,
          reason:
              'system entry type $id should be admitted when '
              'includeSystemEvents=true',
        );
      }
    });
  });
}
