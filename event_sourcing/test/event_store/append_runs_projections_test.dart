// Verifies: EVS-DEV-event-store-open/E
// appending an event with a registered
//   ProjectionRegistry causes the matching AggregateFold to run inside the
//   same transaction as the event-log write, producing a view row that is
//   readable after commit.

import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
  database: await newDatabaseFactoryMemory().openDatabase(
    'rp-${DateTime.now().microsecondsSinceEpoch}.db',
  ),
);

void main() {
  test('appended event produces projection row via interpreter', () async {
    final backend = await _backend();

    final projections = ProjectionRegistry()
      ..register(
        AggregateProjectionSpec(
          viewName: 'diary_entries',
          interest: const SubscriptionFilter(aggregateTypes: {'note'}),
          tombstoneEventTypes: const {'tombstone'},
        ),
      );

    final entryTypes = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: 1,
          name: 'Epistaxis Event',
        ),
      );

    final store = await EventStore.open(
      storage: backend,
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'test',
        identifier: 'test-instance',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: projections,
    );

    await store.append(
      entryType: 'epistaxis_event',
      aggregateId: 'e1',
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{
        'answers': <String, Object?>{'q1': 'yes'},
      },
      initiator: const UserInitiator('u'),
    );

    final row = await backend.transaction(
      (txn) => backend.readViewRowInTxn(txn, 'diary_entries', 'e1'),
    );
    expect(row, isNotNull);
    expect((row!['answers'] as Map)['q1'], 'yes');
    await store.close();
  });
}
