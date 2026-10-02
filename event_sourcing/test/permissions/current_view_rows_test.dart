// test/permissions/current_view_rows_test.dart
// currentViewRows is the one adapter feeding ContainmentResolver and
//   ScopeDescendantExpander a converging-aware read: it throws
//   ViewConvergingRefusal naming the view while the view's copy converges
//   for the instance, for both walkers built on it, and returns the rows
//   once the view is current.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

/// Fails every catch-up step so a copy this test has rewound behind the log
/// stays converging for as long as the hook is installed.
void _pauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure('paused for a current-view-rows test');

var _dbCounter = 0;

Future<sembast.Database> _openDb() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'current-view-rows-$_dbCounter.db',
  );
}

/// Declares the columns `patient_site_index` produces, for the scope-class
/// registry's composition-time column validation.
class _PatientSiteIndexDescriptor implements ScopeProjectionDescriptor {
  const _PatientSiteIndexDescriptor();
  @override
  Set<String> get columns => const {'patient_id', 'site_id'};
}

const _patientSiteIndexSpec = TableProjectionSpec(
  viewName: 'patient_site_index',
  interest: SubscriptionFilter(
    eventTypes: {'patient_site_set'},
    aggregateTypes: {'patient_site_assignment'},
  ),
  insertEventTypes: {'patient_site_set'},
  removeEventTypes: {},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry..register(
    const EntryTypeDefinition(
      id: 'patient_site_assignment',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Patient-to-site assignment',
    ),
  );
}

Future<EventStore> _openWith(SembastBackend backend) => EventStore.openForTest(
  storage: backend,
  entryTypes: _entryTypes(),
  source: const Source(
    hopId: 'test-server',
    identifier: 'test-instance-1',
    softwareVersion: 'event_sourcing_test@0.0.0',
  ),
  securityContexts: SembastSecurityContextStore(backend: backend),
  projections: ProjectionRegistry()..register(_patientSiteIndexSpec),
);

Future<EventStore> _openStore(
  SembastBackend backend, {
  DeliveryTestHooks? hooks,
}) {
  return hooks == null
      ? _openWith(backend)
      : runWithDeliveryTestHooks(hooks, () => _openWith(backend));
}

Future<void> _setPatientSite(
  EventStore store, {
  required String patientId,
  required String siteId,
}) => store.append(
  entryType: 'patient_site_assignment',
  aggregateType: 'patient_site_assignment',
  aggregateId: patientId,
  eventType: 'patient_site_set',
  data: <String, Object?>{'patient_id': patientId, 'site_id': siteId},
  initiator: const AutomationInitiator(service: 'test'),
);

Future<void> _rewindWatermark(
  EventStore store,
  SembastBackend backend,
  String viewName,
  int watermark,
) async {
  final copyId = store.copyIdOf(viewName);
  await backend.transaction(
    (txn) => backend.setViewCopyWatermarkInTxn(txn, copyId, watermark),
  );
}

final _registry = ScopeClassRegistry(
  classes: const [
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'patient',
      containedIn: ContainmentReference(
        parentClass: 'site',
        projection: 'patient_site_index',
        keyColumn: 'patient_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: (name) =>
      name == 'patient_site_index' ? const _PatientSiteIndexDescriptor() : null,
);

void main() {
  // Verifies: EVS-DEV-converging-view-reads/H
  group('currentViewRows: a converging containment view refuses transiently '
      'for every walker built on the adapter', () {
    test('ContainmentResolver.resolve throws ViewConvergingRefusal naming '
        'the view while it is converging', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');
      await _rewindWatermark(store, backend, 'patient_site_index', 0);

      final resolver = ContainmentResolver(
        registry: _registry,
        findRowsInTxn: currentViewRows(store.reader),
      );

      await store.reader.transaction((txn) async {
        await expectLater(
          () => resolver.resolve(
            txn: txn,
            from: const BoundScope(class_: 'patient', value: 'p-1'),
            target: 'site',
          ),
          throwsA(
            isA<ViewConvergingRefusal>().having(
              (e) => e.viewName,
              'viewName',
              'patient_site_index',
            ),
          ),
        );
      });
    });

    test('ScopeDescendantExpander.expand throws ViewConvergingRefusal '
        'naming the view, never an empty or narrowed set', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');
      await _rewindWatermark(store, backend, 'patient_site_index', 0);

      final expander = ScopeDescendantExpander(
        registry: _registry,
        findRowsInTxn: currentViewRows(store.reader),
      );

      await store.reader.transaction((txn) async {
        await expectLater(
          () => expander.expand(
            txn: txn,
            assignment: const BoundScope(class_: 'site', value: 'site-A'),
            targetClass: 'patient',
          ),
          throwsA(
            isA<ViewConvergingRefusal>().having(
              (e) => e.viewName,
              'viewName',
              'patient_site_index',
            ),
          ),
        );
      });
    });
  });

  group('currentViewRows: a current view returns rows to every walker '
      'built on the adapter', () {
    test(
      'ContainmentResolver.resolve returns the resolved ancestor scope',
      () async {
        final db = await _openDb();
        final backend = SembastBackend(database: db);
        final store = await _openStore(backend);
        addTearDown(store.close);

        await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');

        final resolver = ContainmentResolver(
          registry: _registry,
          findRowsInTxn: currentViewRows(store.reader),
        );

        final resolved = await store.reader.transaction(
          (txn) => resolver.resolve(
            txn: txn,
            from: const BoundScope(class_: 'patient', value: 'p-1'),
            target: 'site',
          ),
        );

        expect(resolved, const BoundScope(class_: 'site', value: 'site-A'));
      },
    );

    test('ScopeDescendantExpander.expand returns the expanded descendant '
        'set', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(backend);
      addTearDown(store.close);

      await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');

      final expander = ScopeDescendantExpander(
        registry: _registry,
        findRowsInTxn: currentViewRows(store.reader),
      );

      final expanded = await store.reader.transaction(
        (txn) => expander.expand(
          txn: txn,
          assignment: const BoundScope(class_: 'site', value: 'site-A'),
          targetClass: 'patient',
        ),
      );

      expect(expanded, {'p-1'});
    });
  });
}
