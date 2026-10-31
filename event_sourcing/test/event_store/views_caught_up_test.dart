// The awaitable signal a boot waits on in place of polling viewProgress:
// every view the instance registers has had a catch-up transaction on its
// copy end at the log's head since the store opened.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/declarations.dart';

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
        declarations: <EventTypeDeclaration>[EligibleVersion('finalized')],
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
    // Verifies: EVS-DEV-converging-view-reads/M
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

    // Verifies: EVS-DEV-converging-view-reads/M
    test(
      'viewsCaughtUp returns at once when every copy is already at head',
      () async {
        final backend = await _backendWithUnfoldedLog();
        final store = await _open(backend);
        await store.reader.viewsCaughtUp(timeout: const Duration(seconds: 20));

        // The second wait completes within the microtasks of its own
        // call, before the next event-loop task (a zero-length timer) runs.
        var returned = false;
        final second = store.reader
            .viewsCaughtUp(timeout: const Duration(seconds: 20))
            .then((_) => returned = true);
        await Future<void>.delayed(Duration.zero);
        expect(returned, isTrue);
        await second;
        await store.close();
      },
    );

    // Verifies: EVS-DEV-converging-view-reads/N+O+P
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
            // Precondition: every view but the paused one has caught up, so
            // the 1 s deadline below measures only the paused view.
            final settleBy = DateTime.now().add(const Duration(seconds: 20));
            while (true) {
              final progress = await store.reader.viewProgress();
              final others = progress.where((s) => s.viewName != paused);
              if (others.every(
                (s) => s.state == ViewConvergenceState.current,
              )) {
                break;
              }
              expect(DateTime.now().isBefore(settleBy), isTrue);
              await Future<void>.delayed(const Duration(milliseconds: 20));
            }
            await expectLater(
              store.reader.viewsCaughtUp(timeout: const Duration(seconds: 1)),
              throwsA(
                isA<ViewConvergenceTimeout>()
                    .having(
                      (e) => e.converging.map((s) => s.viewName),
                      'views',
                      [paused],
                    )
                    .having(
                      (e) => e.converging.single.logHead,
                      "the paused copy's progress: the log's head",
                      greaterThanOrEqualTo(_kLogLength),
                    )
                    .having(
                      (e) => e.converging.single.watermark,
                      "the paused copy's progress: its watermark",
                      lessThan(_kLogLength),
                    ),
              ),
            );
            await store.close();
          },
        );
      },
    );

    // Verifies: EVS-DEV-converging-view-reads/M
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
