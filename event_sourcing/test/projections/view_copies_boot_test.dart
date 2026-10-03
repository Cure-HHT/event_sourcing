import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _kType = 'boot_copy_note';
const _kView = 'boot_copy_notes';
const _kOtherView = 'boot_copy_notes_other';

const _kSpecV1 = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kOtherSpec = AggregateProjectionSpec(
  viewName: _kOtherView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

/// Installed as `onCatchUpStep` to keep the catch-up driver backed off
/// (EVS-DEV-view-convergence/Q) for the duration of a test that asserts a
/// copy's converging state.
void _throwToPauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure('paused for a converging-copy test');

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'view-copies-boot-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

/// Opens an [EventStore] over [backend], registering [_kType] at
/// [registered] and each spec in [projections].
Future<EventStore> _open(
  SembastBackend backend, {
  EntryTypeVersion registered = const EntryTypeVersion(1, 0),
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpecV1],
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      EntryTypeDefinition(
        id: _kType,
        registeredVersion: registered,
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
      hopId: 'boot-copy-hop',
      identifier: 'boot-copy-install',
      softwareVersion: 'boot-copy-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

/// The stored view copies of [backend]'s views under test, excluding the
/// library's own default destination-wedges view.
Future<List<ViewCopy>> _copies(SembastBackend backend) async {
  final all = await backend.transaction(
    (txn) => backend.readViewCopiesInTxn(txn),
  );
  return [
    for (final c in all)
      if (c.viewName.startsWith('boot_copy')) c,
  ];
}

void main() {
  group('view copies at boot', () {
    // Verifies: EVS-DEV-view-convergence/B
    test('the boot creates one copy per new fingerprint with the initial '
        'watermark', () async {
      final backend = await _openBackend();
      final store = await _open(backend);
      final copies = await _copies(backend);
      expect(copies, hasLength(1));
      expect(copies.single.viewName, _kView);
      expect(copies.single.watermark, 0);
      expect(copies.single.markedForDeletion, isFalse);
      expect(store.copyIdOf(_kView), copies.single.copyId);
      await store.close();
    });

    // Verifies: EVS-DEV-view-convergence/A+B
    test('a reopen with the same definitions shares the copy', () async {
      final backend = await _openBackend();
      final first = await _open(backend);
      final firstCopyId = first.copyIdOf(_kView);
      await first.close();

      final second = await _open(backend);
      final copies = await _copies(backend);
      expect(copies, hasLength(1), reason: 'no second copy was created');
      expect(second.copyIdOf(_kView), firstCopyId);
      await second.close();
    });

    // Verifies: EVS-DEV-view-convergence/A+B+D
    test('a new minor creates a second copy', () async {
      final backend = await _openBackend();
      final older = await _open(
        backend,
        registered: const EntryTypeVersion(1, 0),
      );
      final olderCopyId = older.copyIdOf(_kView);
      await older.close();

      final newer = await _open(
        backend,
        registered: const EntryTypeVersion(1, 1),
      );
      final copies = await _copies(backend);
      expect(copies, hasLength(2));
      expect(newer.copyIdOf(_kView), isNot(olderCopyId));
      final olderCopy = copies.singleWhere((c) => c.copyId == olderCopyId);
      expect(
        olderCopy.markedForDeletion,
        isTrue,
        reason:
            "the opening build does not register the older minor's "
            'fingerprint (T3: opening build only, live registrations in T5)',
      );
      final current = copies.singleWhere(
        (c) => c.copyId == newer.copyIdOf(_kView),
      );
      expect(current.markedForDeletion, isFalse);
      await newer.close();
    });

    // Verifies: EVS-DEV-view-convergence/D
    test("a dropped view's copy is marked", () async {
      final backend = await _openBackend();
      final withBoth = await _open(
        backend,
        projections: const <ProjectionSpec>[_kSpecV1, _kOtherSpec],
      );
      final droppedCopyId = withBoth.copyIdOf(_kOtherView);
      await withBoth.close();

      final withOne = await _open(backend);
      final copies = await _copies(backend);
      final dropped = copies.singleWhere((c) => c.copyId == droppedCopyId);
      expect(dropped.markedForDeletion, isTrue);
      final kept = copies.singleWhere((c) => c.copyId != droppedCopyId);
      expect(kept.markedForDeletion, isFalse);
      expect(kept.copyId, withOne.copyIdOf(_kView));
      await withOne.close();
    });

    // Verifies: EVS-DEV-view-convergence/E
    test(
      'an append into a current copy folds and moves the watermark',
      () async {
        final backend = await _openBackend();
        final store = await _open(backend);
        final copyId = store.copyIdOf(_kView);
        final before = (await _copies(backend)).single;
        expect(before.watermark, 0);

        final event = await store.append(
          entryType: _kType,
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'title': 'hello'},
          initiator: const UserInitiator('boot-copy-user'),
        );

        final after = (await _copies(backend)).single;
        expect(after.watermark, event!.sequenceNumber);
        final row = (await store.reader.findViewRows(_kView)).rows;
        expect(row, hasLength(1));
        expect(row.single['title'], 'hello');
        await store.close();
        // Never touched raw backend by name; the copy id is opaque.
        expect(copyId, isNotEmpty);
      },
    );

    // Verifies: EVS-DEV-view-convergence/F
    test('an append beside a converging copy leaves its rows and watermark '
        'unchanged', () async {
      final backend = await _openBackend();
      // Seed the log with an event the view's interest matches, through
      // a build that does not register the view, so the view's future
      // copy starts behind.
      final seeder = await _open(
        backend,
        projections: const <ProjectionSpec>[],
      );
      await seeder.append(
        entryType: _kType,
        aggregateId: 'agg-behind',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'title': 'before'},
        initiator: const UserInitiator('boot-copy-user'),
      );
      await seeder.close();

      // The view's catch-up driver (EVS-DEV-view-convergence) would fold
      // the seeded event into the copy in the background; failing every
      // catch-up attempt backs it off for a second, long enough for this
      // append and its assertions to run against the copy still behind.
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(onCatchUpStep: _throwToPauseCatchUp),
        () async {
          // The view's copy is created empty at this open, behind the
          // event above (EVS-DEV-view-convergence Terms: a copy behind is
          // not current when an event its definition folds lies past its
          // watermark).
          final store = await _open(backend);
          final beforeAppend = (await _copies(
            backend,
          )).singleWhere((c) => c.viewName == _kView);
          expect(beforeAppend.watermark, 0);

          await store.append(
            entryType: _kType,
            aggregateId: 'agg-after',
            aggregateType: 'note',
            eventType: 'finalized',
            data: const <String, Object?>{'title': 'after'},
            initiator: const UserInitiator('boot-copy-user'),
          );

          final afterAppend = (await _copies(
            backend,
          )).singleWhere((c) => c.viewName == _kView);
          expect(
            afterAppend.watermark,
            0,
            reason: 'a converging copy is left entirely unchanged',
          );
          expect((await store.reader.findViewRows(_kView)).rows, isEmpty);
          await store.close();
        },
      );
    });

    // Discriminates "considered-through" watermark semantics from
    // watermark == sequenceNumber - 1: a view whose copy starts behind a
    // log of events its interest does NOT fold is still current, because
    // "current" is decided against what the copy's own definition folds.
    // Verifies: EVS-DEV-view-convergence/E
    test('a view registered over a log of events its interest does not fold: '
        'the first append folds and moves the watermark past the unfolded '
        'events', () async {
      final backend = await _openBackend();
      final other = await _open(
        backend,
        projections: const <ProjectionSpec>[
          AggregateProjectionSpec(
            viewName: 'boot_copy_unrelated',
            interest: SubscriptionFilter(entryTypes: <String>{'unrelated'}),
            tombstoneEventTypes: <String>{},
          ),
        ],
      );
      final entryTypes = EntryTypeRegistry()
        ..register(
          const EntryTypeDefinition(
            id: 'unrelated',
            registeredVersion: EntryTypeVersion(1, 0),
            name: 'unrelated',
          ),
        )
        ..register(
          const EntryTypeDefinition(
            id: _kType,
            registeredVersion: EntryTypeVersion(1, 0),
            name: _kType,
          ),
        );
      // Reopen sharing the registry so both entry types are known, and
      // append three events the view below does not fold.
      final seeder = await EventStore.openForTest(
        storage: backend,
        entryTypes: entryTypes,
        source: const Source(
          hopId: 'boot-copy-hop',
          identifier: 'boot-copy-install',
          softwareVersion: 'boot-copy-test',
        ),
        securityContexts: SembastSecurityContextStore(backend: backend),
        projections: ProjectionRegistry()
          ..register(other.projections.lookup('boot_copy_unrelated')!),
      );
      for (var i = 0; i < 3; i++) {
        await seeder.append(
          entryType: 'unrelated',
          aggregateId: 'agg-u$i',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'x': 1},
          initiator: const UserInitiator('boot-copy-user'),
        );
      }
      await seeder.close();
      await other.close();

      final store = await _open(backend);
      final copy = (await _copies(
        backend,
      )).singleWhere((c) => c.viewName == _kView);
      expect(copy.watermark, 0);

      final event = await store.append(
        entryType: _kType,
        aggregateId: 'agg-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'title': 'hello'},
        initiator: const UserInitiator('boot-copy-user'),
      );

      final after = (await _copies(
        backend,
      )).singleWhere((c) => c.viewName == _kView);
      expect(
        after.watermark,
        event!.sequenceNumber,
        reason:
            'the copy was current despite starting behind, because none '
            'of the events past its watermark matched its definition',
      );
      expect((await store.reader.findViewRows(_kView)).rows, hasLength(1));
      await store.close();
    });

    // Verifies: EVS-DEV-event-store-open/N
    test('the boot touches no view row', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend);
      await seeder.append(
        entryType: _kType,
        aggregateId: 'agg-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'title': 'before-reopen'},
        initiator: const UserInitiator('boot-copy-user'),
      );
      final rowsBefore = (await seeder.reader.findViewRows(_kView)).rows;
      expect(rowsBefore, hasLength(1));
      await seeder.close();

      // A build that adds a second, unregistered-before view: its boot
      // creates a new empty copy without folding anything into it -- read
      // coverage here is by inspection of _runBoot (EVS-DEV-event-store-open/E),
      // which creates and marks copies only after every write decision,
      // and by this state check: the new view's copy stays empty and at
      // watermark 0 even though the log already holds a matching event.
      final reopened = await _open(
        backend,
        projections: const <ProjectionSpec>[_kSpecV1, _kOtherSpec],
      );
      final otherCopy = (await _copies(
        backend,
      )).singleWhere((c) => c.viewName == _kOtherView);
      expect(otherCopy.watermark, 0);
      expect((await reopened.reader.findViewRows(_kOtherView)).rows, isEmpty);
      // The pre-existing view's copy and rows are exactly as the seeder
      // left them: the boot read, wrote and deleted no view row.
      expect((await reopened.reader.findViewRows(_kView)).rows, rowsBefore);
      await reopened.close();
    });
  });
}
