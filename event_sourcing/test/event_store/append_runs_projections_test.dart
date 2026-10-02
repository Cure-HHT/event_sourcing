import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_description.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
  database: await newDatabaseFactoryMemory().openDatabase(
    'rp-${DateTime.now().microsecondsSinceEpoch}.db',
  ),
);

void main() {
  // Verifies: EVS-PRD-materializer/A
  test('appended event produces projection row via interpreter', () async {
    final backend = await _backend();

    final projections = ProjectionRegistry()
      ..register(
        const AggregateProjectionSpec(
          viewName: 'diary_entries',
          interest: SubscriptionFilter(aggregateTypes: {'note'}),
          tombstoneEventTypes: {'tombstone'},
        ),
      );

    final entryTypes = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'Epistaxis Event',
        ),
      );

    final store = await EventStore.open(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'test',
        identifier: 'test-instance',
        softwareVersion: '0.0.0-test',
      ),
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

    final copyId = store.copyIdOf('diary_entries');
    final row = await backend.transaction(
      (txn) => backend.readViewRowInTxn(txn, copyId, 'e1'),
    );
    expect(row, isNotNull);
    expect((row!['answers'] as Map)['q1'], 'yes');
    await store.close();
  });

  // Verifies: EVS-PRD-ingest/G
  // (contrast) a local append's fold failure is not the always-stored
  //   case: it still fails synchronously to its caller, with nothing
  //   stored, the behavior ApplyEventMode.alwaysStored is never applied
  //   to.
  test('a local append whose fold cannot key the event still throws to its '
      'caller', () async {
    final backend = await _backend();

    final projections = ProjectionRegistry()
      ..register(
        const TableProjectionSpec(
          viewName: 'unkeyable_notes',
          interest: SubscriptionFilter(aggregateTypes: {'note'}),
          insertEventTypes: {'finalized'},
          removeEventTypes: {},
          rowKey: CompositeKey(<String>['data.k']),
          rowData: WholePayload(),
        ),
      );

    final entryTypes = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'Epistaxis Event',
        ),
      );

    final store = await EventStore.open(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'test',
        identifier: 'test-instance',
        softwareVersion: '0.0.0-test',
      ),
      projections: projections,
    );

    await expectLater(
      store.append(
        entryType: 'epistaxis_event',
        aggregateId: 'e1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': 'no key'},
        initiator: const UserInitiator('u'),
      ),
      throwsA(isA<StateError>()),
      reason:
          "a locally-dispatched action's fold failure propagates to "
          'the caller, unlike a received event',
    );
    expect(
      await store.reader.findAllEvents(entryType: 'epistaxis_event'),
      isEmpty,
      reason: 'a local append that throws stores nothing',
    );
    await store.close();
  });
}
