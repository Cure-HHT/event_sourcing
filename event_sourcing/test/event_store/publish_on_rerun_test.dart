// Verifies: EVS-PRD-subscription/E
// a storage backend may run a transaction
//   body more than once before one run commits (Postgres after a
//   serialization conflict, sembast_web after another tab commits first);
//   live subscribers receive only the committed run's events and view
//   changes, once each.
//
// RerunningSembastBackend models such a re-run on the VM: it runs the body
// once and rolls that run back, then runs it again and commits.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/rerunning_sembast_backend.dart';

const _viewName = 'notes_by_id';

void main() {
  test('only the committed run of a re-run body is published, once', () async {
    final backend = await RerunningSembastBackend.openInMemory('publish-rerun');
    addTearDown(backend.close);
    final registry = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'note',
        ),
      );
    final projections = ProjectionRegistry()
      ..register(
        const AggregateProjectionSpec(
          viewName: _viewName,
          interest: SubscriptionFilter(aggregateTypes: <String>{'Note'}),
          tombstoneEventTypes: <String>{},
        ),
      );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: registry,
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-000000000001',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: projections,
    );

    final delivered = <StoredEvent>[];
    final eventSub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value);
        });
    final rowUpdates = <Update<Map<String, Object?>>>[];
    final viewSub = store
        .subscribe<Map<String, Object?>>(
          const SubscriptionFilter(aggregateTypes: <String>{'Note'}),
          AggregateMode<Map<String, Object?>>(
            viewName: _viewName,
            mapper: (row) => row,
          ),
        )
        .listen(rowUpdates.add);
    await pumpEventQueue();
    final initialRowUpdates = rowUpdates.length;
    backend.bodyRuns = 0;

    final appended = await store.append(
      entryType: 'note',
      aggregateId: 'n1',
      aggregateType: 'Note',
      eventType: 'created',
      data: const <String, Object?>{'text': 'hello'},
      initiator: const UserInitiator('u1'),
    );
    await pumpEventQueue();
    await eventSub.cancel();
    await viewSub.cancel();

    expect(backend.bodyRuns, 2, reason: 'the body must have been re-run');
    final stored = await backend.findAllEvents();
    expect(stored, hasLength(1));
    expect(appended!.eventId, stored.single.eventId);
    expect(delivered.map((e) => e.eventId), [stored.single.eventId]);
    expect(delivered.single.sequenceNumber, stored.single.sequenceNumber);
    expect(
      rowUpdates
          .skip(initialRowUpdates)
          .whereType<Delta<Map<String, Object?>>>(),
      hasLength(1),
      reason: 'one view-row change for one committed append',
    );
  });
}
