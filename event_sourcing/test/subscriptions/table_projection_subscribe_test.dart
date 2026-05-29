// Verifies: EVS-PRD-subscription/A/B — subscribe(AggregateMode) over a
//   TableProjectionSpec view replays existing rows as Snapshots and delivers
//   live Deltas, exactly as it does for AggregateProjectionSpec views.
//
// Regression guard for the view-row-shape asymmetry where TableFold persisted
// only the extracted rowData (omitting `aggregateId` / `sequence`). That broke
// every consumer following the documented view-row contract — most visibly the
// reaction_widgets `ViewBuilder`, whose `aggregateIdOf` extractor reads
// `row['aggregateId']`, and subscribe()'s snapshot replay, which reads
// `row['sequence']`. A user_role_scopes-style TableProjectionSpec is used here
// because that is the surface that surfaced the bug end-to-end.
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _spec = TableProjectionSpec(
  viewName: 'user_role_scopes',
  interest: SubscriptionFilter(
    eventTypes: {'role_assigned', 'role_unassigned'},
    aggregateTypes: {'user_role_scope'},
  ),
  insertEventTypes: {'role_assigned'},
  removeEventTypes: {'role_unassigned'},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

Future<EventStore> _open() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'tps-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: 1,
        name: 'User-Role-Scope Assignment',
      ),
    );
  final projections = ProjectionRegistry()..register(_spec);
  return EventStore.open(
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
}

Future<void> _assign(EventStore store, String aggId, String user) =>
    store.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: aggId,
      eventType: 'role_assigned',
      data: <String, Object?>{'user_id': user, 'role': 'editor'},
      initiator: const AutomationInitiator(service: 'seed'),
    );

void main() {
  test('replays TableProjectionSpec rows with aggregateId + sequence', () async {
    final store = await _open();
    addTearDown(store.close);

    await _assign(store, 'alice|editor', 'alice');
    await _assign(store, 'bob|editor', 'bob');

    final updates = <Update<Map<String, Object?>>>[];
    final sub = store
        .subscribe<Map<String, Object?>>(
          const SubscriptionFilter(),
          AggregateMode<Map<String, Object?>>(
            viewName: 'user_role_scopes',
            mapper: (row) => row,
          ),
        )
        .listen(updates.add);

    // Drain until EndOfReplay.
    while (!updates.any((u) => u is EndOfReplay)) {
      await Future<void>.delayed(Duration.zero);
    }

    final snapshots = updates.whereType<Snapshot<Map<String, Object?>>>().toList();
    expect(snapshots, hasLength(2), reason: 'both seeded rows replay');
    for (final snap in snapshots) {
      final row = snap.value!;
      // The field the reaction_widgets ViewBuilder aggregateIdOf extractor
      // reads — previously absent on TableProjectionSpec rows.
      expect(row['aggregateId'], isNotNull);
      expect(row['aggregateId'], isIn(<String>['alice|editor', 'bob|editor']));
      // Snapshot sequence is driven by the stamped row `sequence`, not 0.
      expect(snap.sequence, greaterThan(0));
    }

    // A live assignment after subscribe arrives as a Delta carrying the
    // stamped aggregateId (the path a multi-user grant-push exercises).
    await _assign(store, 'carol|editor', 'carol');
    while (!updates.any((u) => u is Delta)) {
      await Future<void>.delayed(Duration.zero);
    }
    final delta = updates.whereType<Delta<Map<String, Object?>>>().single;
    expect(delta.value['aggregateId'], 'carol|editor');
    expect(delta.value['user_id'], 'carol');

    await sub.cancel();
  });
}
