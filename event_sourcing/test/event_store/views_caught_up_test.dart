// The awaitable signal a boot waits on in place of polling viewProgress:
// every view the instance registers has had a catch-up transaction on its
// copy end at the log's head since the store opened.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

const _kType = 'caught_up_note';
const _kAggregateView = 'caught_up_notes';
const _kTableView = 'caught_up_notes_table';
const _kLogLength = 500;

const _kAggregateSpec = AggregateProjectionSpec(
  viewName: _kAggregateView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kTableSpec = TableProjectionSpec(
  viewName: _kTableView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'views-caught-up-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

Future<EventStore> _open(
  SembastBackend backend, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[
    _kAggregateSpec,
    _kTableSpec,
  ],
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry();
  for (final spec in projections) {
    registry.register(spec);
  }
  return EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'caught-up-hop',
      identifier: 'caught-up-install',
      softwareVersion: 'caught-up-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

/// A backend whose log holds [_kLogLength] events no view has folded: the
/// events are appended through a store registering no view, so the next
/// store to open with views creates their copies at watermark 0 and its
/// catch-up folds the whole log.
Future<SembastBackend> _backendWithUnfoldedLog() async {
  final backend = await _openBackend();
  final seeder = await _open(backend, projections: const <ProjectionSpec>[]);
  for (var i = 0; i < _kLogLength; i++) {
    await seeder.append(
      entryType: _kType,
      aggregateId: 'agg-$i',
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'title': 'note $i'},
      initiator: const UserInitiator('caught-up-user'),
    );
  }
  await seeder.close();
  return backend;
}

void main() {
  group('viewsCaughtUp', () {
    // Verifies: EVS-DEV-converging-view-reads/J+L
    test('viewsCaughtUp completes once both copies reach the head', () async {
      final backend = await _backendWithUnfoldedLog();
      final store = await _open(backend);

      await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));

      final progress = await store.reader.viewProgress();
      expect(
        progress.map((s) => s.viewName),
        containsAll(<String>[_kAggregateView, _kTableView]),
      );
      for (final status in progress) {
        expect(status.state, ViewConvergenceState.current);
        expect(status.watermark, status.logHead);
      }
      await store.close();
    });

    // Verifies: EVS-DEV-converging-view-reads/J
    test(
      'viewsCaughtUp returns at once when every copy is already at head',
      () async {
        final backend = await _backendWithUnfoldedLog();
        final store = await _open(backend);
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));

        final watch = Stopwatch()..start();
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));
        watch.stop();
        expect(watch.elapsed, lessThan(const Duration(milliseconds: 100)));
        await store.close();
      },
    );

    // Verifies: EVS-DEV-converging-view-reads/J+L
    test(
      'viewsCaughtUp throws ViewConvergenceTimeout naming the paused view',
      () async {
        const paused = _kTableView;
        final backend = await _backendWithUnfoldedLog();
        late final EventStore store;
        var opened = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onCatchUpStep: (copyId, eventId) {
              if (opened && copyId == store.copyIdOf(paused)) {
                throw const InjectedFailure('paused for a viewsCaughtUp test');
              }
            },
          ),
          () async {
            store = await _open(backend);
            opened = true;
            await expectLater(
              store.reader.viewsCaughtUp(timeout: const Duration(seconds: 1)),
              throwsA(
                isA<ViewConvergenceTimeout>().having(
                  (e) => e.converging.map((s) => s.viewName),
                  'views',
                  [paused],
                ),
              ),
            );
            await store.close();
          },
        );
      },
    );

    // Verifies: EVS-DEV-converging-view-reads/J
    test(
      'viewsCaughtUp throws StateError when the store closes first',
      () async {
        final backend = await _backendWithUnfoldedLog();
        late final EventStore store;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onCatchUpStep: (copyId, eventId) =>
                throw const InjectedFailure('held behind for a close test'),
          ),
          () async {
            store = await _open(backend);
            final waiting = store.reader.viewsCaughtUp(
              timeout: const Duration(seconds: 20),
            );
            final check = expectLater(waiting, throwsA(isA<StateError>()));
            await store.close();
            await check;
          },
        );
      },
    );
  });
}
