// Verifies: EVS-DEV-destination-drain/H
// a declarative filter tells destination
//   audits apart by event type: a filter that admits reserved system events
//   and allow-lists one destination-audit event type admits that kind of
//   audit and rejects every other kind, and a user event that copies the
//   event type and aggregate type is still rejected by the empty
//   entry-type allow-list.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';
import '../test_support/queue_test_support.dart';

const _automation = AutomationInitiator(service: 'test-bootstrap');
const _source = Source(
  hopId: 'mobile-device',
  identifier: 'audit-filter-test',
  softwareVersion: 'audit-filter-test@1.0.0',
);

/// Every kind of destination audit, entry type to event type.
const _eventTypeOf = <String, String>{
  kDestinationRegisteredEntryType: kDestinationRegisteredEventType,
  kDestinationStartDateSetEntryType: kDestinationStartDateSetEventType,
  kDestinationEndDateSetEntryType: kDestinationEndDateSetEventType,
  kDestinationDeletedEntryType: kDestinationDeletedEventType,
  kDestinationWedgeRecoveredEntryType: kDestinationWedgeRecoveredEventType,
  kDestinationWedgedEntryType: kDestinationWedgedEventType,
  kDestinationHaltRequestedEntryType: kDestinationHaltRequestedEventType,
  kDestinationHaltCancelledEntryType: kDestinationHaltCancelledEventType,
};

void main() {
  group('SubscriptionFilter over destination audit event types', () {
    late SembastBackend backend;
    late EventStoreBundle ds;
    var counter = 0;

    setUp(() async {
      counter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'audit-filter-$counter.db',
      );
      backend = SembastBackend(database: db);
      ds = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[],
        destinations: const <Destination>[],
      );
    });

    tearDown(() async {
      await backend.close();
    });

    /// Runs one of each destination-registry operation on destination `d`
    /// and returns the audit each appended, keyed by its entry type.
    Future<Map<String, StoredEvent>> oneAuditOfEachKind() async {
      await ds.destinations.addDestination(
        FakeDestination(id: 'd', allowHardDelete: true),
        initiator: _automation,
      );
      await ds.destinations.setStartDate(
        'd',
        DateTime.utc(2026, 1, 1),
        initiator: _automation,
      );
      await ds.destinations.setEndDate(
        'd',
        DateTime.now().add(const Duration(days: 30)),
        initiator: _automation,
      );
      final head = await enqueueSingle(
        backend,
        'd',
        eventId: 'evt-1',
        sequenceNumber: 1,
      );
      await ds.destinations.requestHalt(
        'd',
        initiator: _automation,
        purpose: HaltPurpose.pause,
      );
      await ds.destinations.cancelHalt('d', initiator: _automation);
      await wedgeHeadForTest(ds.destinations, 'd');
      await ds.destinations.tombstoneAndRefill(
        'd',
        head.entryId,
        initiator: _automation,
      );
      await ds.destinations.deleteDestination('d', initiator: _automation);
      final all = await backend.findAllEvents();
      return <String, StoredEvent>{
        for (final entryType in _eventTypeOf.keys)
          entryType: all.singleWhere((e) => e.entryType == entryType),
      };
    }

    for (final kind in _eventTypeOf.entries) {
      test('a filter on ${kind.value} admits the ${kind.key} audit and '
          'rejects every other kind of destination audit', () async {
        final audits = await oneAuditOfEachKind();
        final filter = SubscriptionFilter(
          entryTypes: const <String>{},
          includeSystemEvents: true,
          eventTypes: <String>{kind.value},
          aggregateTypes: const <String>{kDestinationAuditAggregateType},
        );
        for (final audit in audits.values) {
          expect(
            filter.matches(audit),
            audit.entryType == kind.key,
            reason: '${audit.entryType} against a filter on ${kind.value}',
          );
        }
      });
    }

    test('a filter on the deletion event type rejects a user event that '
        'carries the same event type and aggregate type', () async {
      const filter = SubscriptionFilter(
        entryTypes: <String>{},
        includeSystemEvents: true,
        eventTypes: <String>{kDestinationDeletedEventType},
        aggregateTypes: <String>{kDestinationAuditAggregateType},
      );
      final forged = StoredEvent(
        key: 99,
        eventId: 'user-evt',
        aggregateId: 'agg-1',
        aggregateType: kDestinationAuditAggregateType,
        entryType: 'demo_note',
        entryTypeVersion: const EntryTypeVersion(1, 0),
        libFormatVersion: const DataFormatVersion(2, 0),
        eventType: kDestinationDeletedEventType,
        sequenceNumber: 99,
        data: const <String, dynamic>{'id': 'd'},
        metadata: const <String, dynamic>{},
        initiator: const UserInitiator('u1'),
        clientTimestamp: DateTime.utc(2026, 4, 26),
        eventHash: 'hash',
      );
      expect(filter.matches(forged), isFalse);
      expect(
        const SubscriptionFilter(
          eventTypes: <String>{kDestinationDeletedEventType},
          aggregateTypes: <String>{kDestinationAuditAggregateType},
        ).matches(forged),
        isTrue,
        reason:
            'sanity: without the empty entry-type allow-list the forged '
            'user event passes the event-type and aggregate-type checks',
      );
    });
  });
}
