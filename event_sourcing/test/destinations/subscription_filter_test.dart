// Verifies: EVS-PRD-destinations/B — exercises SubscriptionFilter semantics:
// allow-list matching by entry_type and event_type (null vs empty distinction),
// predicate escape-hatch, AND composition, and default match-all behavior.
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _mkEvent({
  String entryType = 'epistaxis_event',
  String eventType = 'finalized',
  String eventId = 'ev-1',
}) => StoredEvent(
  key: 1,
  eventId: eventId,
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
  clientTimestamp: DateTime.utc(2026, 4, 22),
  eventHash: 'hash',
);

void main() {
  group('SubscriptionFilter', () {
    test('null lists match everything', () {
      const f = SubscriptionFilter();
      expect(f.matches(_mkEvent()), isTrue);
      expect(
        f.matches(
          _mkEvent(entryType: 'nose_symptom_survey', eventType: 'tombstone'),
        ),
        isTrue,
      );
    });

    test('entryTypes allow-list selects by entry_type', () {
      const f = SubscriptionFilter(entryTypes: {'epistaxis_event'});
      expect(f.matches(_mkEvent(entryType: 'epistaxis_event')), isTrue);
      expect(f.matches(_mkEvent(entryType: 'nose_symptom_survey')), isFalse);
    });

    test('eventTypes allow-list selects by event_type', () {
      const f = SubscriptionFilter(eventTypes: {'finalized'});
      expect(f.matches(_mkEvent(eventType: 'finalized')), isTrue);
      expect(f.matches(_mkEvent(eventType: 'checkpoint')), isFalse);
      expect(f.matches(_mkEvent(eventType: 'tombstone')), isFalse);
    });

    // Intersection: both allow-lists must match when both are set.
    test('entryTypes AND eventTypes — both must match', () {
      const f = SubscriptionFilter(
        entryTypes: {'epistaxis_event'},
        eventTypes: {'finalized'},
      );
      expect(
        f.matches(
          _mkEvent(entryType: 'epistaxis_event', eventType: 'finalized'),
        ),
        isTrue,
      );
      expect(
        f.matches(
          _mkEvent(entryType: 'epistaxis_event', eventType: 'checkpoint'),
        ),
        isFalse,
      );
      expect(
        f.matches(
          _mkEvent(entryType: 'nose_symptom_survey', eventType: 'finalized'),
        ),
        isFalse,
      );
    });

    // (set of length 0 = match nothing). This guards the foot-gun where
    // an unintended `{}` default would accept an event by accident.
    test('empty entryTypes set matches nothing '
        '(distinct from null)', () {
      const emptyEntryTypes = SubscriptionFilter(entryTypes: {});
      expect(emptyEntryTypes.matches(_mkEvent()), isFalse);
      expect(
        emptyEntryTypes.matches(_mkEvent(entryType: 'nose_symptom_survey')),
        isFalse,
      );
    });

    test('empty eventTypes set matches nothing '
        '(distinct from null)', () {
      const emptyEventTypes = SubscriptionFilter(eventTypes: {});
      expect(emptyEventTypes.matches(_mkEvent()), isFalse);
      expect(
        emptyEventTypes.matches(_mkEvent(eventType: 'tombstone')),
        isFalse,
      );
    });

    // pass; a predicate returning false blocks the event.
    test('predicate escape-hatch filters further', () {
      final f = SubscriptionFilter(
        predicate: (event) => event.eventId == 'ev-allow',
      );
      expect(f.matches(_mkEvent(eventId: 'ev-allow')), isTrue);
      expect(f.matches(_mkEvent(eventId: 'ev-block')), isFalse);
    });

    // Short-circuit: if the allow-lists fail, the predicate MUST NOT be
    // invoked. This matters when the predicate is expensive (e.g., hits
    // a registry lookup).
    test('predicate is not invoked when allow-lists fail', () {
      var predicateCalls = 0;
      final f = SubscriptionFilter(
        entryTypes: const {'epistaxis_event'},
        predicate: (event) {
          predicateCalls += 1;
          return true;
        },
      );
      expect(f.matches(_mkEvent(entryType: 'nose_symptom_survey')), isFalse);
      expect(predicateCalls, 0);

      // Sanity: the predicate IS invoked when allow-lists pass.
      expect(f.matches(_mkEvent(entryType: 'epistaxis_event')), isTrue);
      expect(predicateCalls, 1);
    });

    // All three constraints compose: entryTypes + eventTypes + predicate.
    test('all three constraints compose (AND)', () {
      final f = SubscriptionFilter(
        entryTypes: const {'epistaxis_event'},
        eventTypes: const {'finalized'},
        predicate: (event) => event.aggregateId == 'agg-1',
      );
      final match = _mkEvent(
        entryType: 'epistaxis_event',
        eventType: 'finalized',
      );
      expect(f.matches(match), isTrue);
    });

    // Default filter — no constraints at all — is functionally "match
    // everything"; equivalent to its any-event behavior.
    test('default SubscriptionFilter (no constraints) matches everything', () {
      const f = SubscriptionFilter();
      for (final entry in [
        'epistaxis_event',
        'nose_symptom_survey',
        'random',
      ]) {
        for (final event in ['finalized', 'checkpoint', 'tombstone']) {
          expect(
            f.matches(_mkEvent(entryType: entry, eventType: event)),
            isTrue,
            reason: 'entry=$entry event=$event should match',
          );
        }
      }
    });
  });
}
