// Verifies: EVS-DEV-view-convergence/U
// rebuildView marks the fingerprint's current unmarked copy for deletion,
//   resolved from stored state rather than a cached copy id, and creates
//   an empty copy of the same fingerprint, in one transaction: the copy id
//   changes and the old copy's rows are gone once catch-up has run. It
//   does not throw when another instance already rebuilt or marked the
//   copy.
// Verifies: EVS-DEV-view-convergence/T
// finding no unmarked copy of the fingerprint stored (another instance
//   marked it without replacing it), rebuildView creates one.
// Verifies: EVS-DEV-view-convergence/V
// rebuildView returns once the new copy is current for the instance, and
//   throws ViewConvergenceTimeout, naming the view and the copy's
//   progress, once the caller-supplied deadline passes first.
// Verifies: EVS-PRD-materializer/B
// the rows a rebuild's replacement copy converges to equal the rows the
//   log already derived.

@Tags(['timing'])
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/view_fingerprint.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/record_fixtures.dart';
import '../test_support/test_backends.dart';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var _dbCounter = 0;

const _kEntryType = 'sample_event';
const _kView = 'toy_view';

const _kAggSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kEntryType}),
  tombstoneEventTypes: <String>{'tombstone'},
);

/// A generous deadline: long enough for an ordinary rebuild of a modest
/// log to converge in a test process, short enough that a real hang still
/// fails the test instead of the suite's own timeout.
DateTime _farDeadline() =>
    DateTime.now().toUtc().add(const Duration(seconds: 20));

Future<EventStore> _openStore() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'rebuild-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final proj = ProjectionRegistry()..register(_kAggSpec);
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kEntryType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kEntryType,
      ),
    );
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'test',
      identifier: 'test-device',
      softwareVersion: 't',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: proj,
  );
  trackTestBackend(store, backend);
  return store;
}

/// Appends through the store's own path, so the event folds inline where
/// the copy is current, exactly as any ordinary caller's append does.
Future<StoredEvent?> _appendNote(
  EventStore store,
  String aggregateId, {
  String entryType = _kEntryType,
  Map<String, Object?> data = const <String, Object?>{'title': 'note'},
}) => store.append(
  entryType: entryType,
  aggregateId: aggregateId,
  aggregateType: 'SampleAggregate',
  eventType: 'finalized',
  data: data,
  initiator: const UserInitiator('u1'),
);

/// Polls, yielding to the event loop between checks, until [copyId] is no
/// longer among the backend's stored view copies. Fails the test after too
/// many polls rather than hanging forever.
Future<void> _waitUntilCopyGone(EventStore store, String copyId) async {
  final backend = testBackendOf(store);
  for (var i = 0; i < 2000; i++) {
    final all = await backend.transaction(backend.readViewCopiesInTxn);
    if (all.every((c) => c.copyId != copyId)) return;
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  fail('copy "$copyId" was not deleted in time');
}

void main() {
  group('rebuildView (fingerprinted view copies)', () {
    test('missing ProjectionSpec raises StateError', () async {
      _dbCounter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'rebuild-no-spec-$_dbCounter.db',
      );
      final backend = SembastBackend(database: db);
      final store = await EventStore.openForTest(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: const Source(
          hopId: 'test',
          identifier: 'test-device',
          softwareVersion: 't',
        ),
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      trackTestBackend(store, backend);
      await expectLater(
        rebuildView(store: store, viewName: _kView, deadline: _farDeadline()),
        throwsStateError,
      );
      await testBackendOf(store).close();
    });

    test('rebuild replaces the copy: rows match, copy id changed, old copy '
        'gone after catch-up', () async {
      final store = await _openStore();
      await _appendNote(store, 'agg-1', data: const {'intensity': 'mild'});
      await _appendNote(store, 'agg-2', data: const {'intensity': 'severe'});
      final beforeRows = (await store.reader.findViewRows(_kView)).rows;
      expect(beforeRows, hasLength(2));
      final oldCopyId = store.copyIdOf(_kView);

      await rebuildView(
        store: store,
        viewName: _kView,
        deadline: _farDeadline(),
      );

      final newCopyId = store.copyIdOf(_kView);
      expect(newCopyId, isNot(oldCopyId));
      final afterRead = await store.reader.findViewRows(_kView);
      expect(afterRead.state, ViewConvergenceState.current);
      expect(
        {for (final r in afterRead.rows) r['aggregateId']: r['intensity']},
        {for (final r in beforeRows) r['aggregateId']: r['intensity']},
      );

      await _waitUntilCopyGone(store, oldCopyId);
      await testBackendOf(store).close();
    });

    test('a garbage row not derivable from the log does not survive the '
        'replacement copy', () async {
      final store = await _openStore();
      final oldCopyId = store.copyIdOf(_kView);
      await testBackendOf(store).transaction((txn) async {
        await testBackendOf(store).upsertViewRowInTxn(
          txn,
          oldCopyId,
          'garbage-agg',
          <String, Object?>{'aggregateId': 'garbage-agg', 'garbage': true},
        );
      });
      await _appendNote(store, 'agg-1');

      await rebuildView(
        store: store,
        viewName: _kView,
        deadline: _farDeadline(),
      );

      final rows = (await store.reader.findViewRows(_kView)).rows;
      expect(rows, hasLength(1));
      expect(rows.single['aggregateId'], 'agg-1');
      await testBackendOf(store).close();
    });

    test('a deadline that passes before the copy converges throws '
        'ViewConvergenceTimeout naming the view and its progress', () async {
      // A registered major of 1 with a stray event stamped at major 2:
      // the fold step refuses it every attempt (EVS-DEV-version-
      // compatibility), so the replacement copy never converges and any
      // deadline is guaranteed to pass first.
      final store = await _openStore();
      await _appendNote(store, 'agg-1');
      final backend = testBackendOf(store);
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        final previous = await backend.readLatestEventHash(txn);
        await backend.appendEvent(
          txn,
          StoredEvent(
            key: 0,
            eventId: 'bad-event',
            aggregateId: 'agg-bad',
            aggregateType: 'SampleAggregate',
            entryType: _kEntryType,
            entryTypeVersion: const EntryTypeVersion(2, 0),
            libFormatVersion: LibVersion.dataFormat,
            eventType: 'finalized',
            sequenceNumber: seq,
            data: const <String, Object?>{'intensity': 'future'},
            metadata: const <String, dynamic>{},
            initiator: const UserInitiator('u1'),
            clientTimestamp: DateTime.now().toUtc(),
            eventHash: 'hash-bad-event',
            previousEventHash: previous,
            causal: kRootVersionCausal,
          ),
        );
      });

      final deadline = DateTime.now().toUtc().add(
        const Duration(milliseconds: 300),
      );
      await expectLater(
        rebuildView(store: store, viewName: _kView, deadline: deadline),
        throwsA(
          isA<ViewConvergenceTimeout>()
              .having(
                (e) => e.converging.map((s) => s.viewName),
                'converging view names',
                contains(_kView),
              )
              .having(
                (e) => e.converging.firstWhere((s) => s.viewName == _kView),
                "the view's copy progress",
                predicate<ViewCopyStatus>(
                  (s) => s.watermark < s.logHead,
                  'watermark behind the log head (still converging)',
                ),
              ),
        ),
      );
      await testBackendOf(store).close();
    });

    test('an appending loop during the rebuild never waits past one '
        'catch-up transaction', () async {
      final store = await _openStore();
      const totalEvents = 300;
      for (var i = 0; i < totalEvents; i++) {
        await _appendNote(store, 'agg-${i % 20}', data: {'index': i});
      }

      var rebuildDone = false;
      final rebuildFuture = rebuildView(
        store: store,
        viewName: _kView,
        deadline: _farDeadline(),
      ).whenComplete(() => rebuildDone = true);

      var appendsWhileConverging = 0;
      for (var i = 0; i < 20; i++) {
        final stopwatch = Stopwatch()..start();
        await _appendNote(
          store,
          'other-agg-$i',
          entryType: _kEntryType,
          data: const {'unrelated': true},
        );
        stopwatch.stop();
        expect(
          stopwatch.elapsed,
          lessThan(const Duration(seconds: 2)),
          reason:
              'an append never waits for the whole rebuild to finish, '
              'only for at most the catch-up transaction in flight',
        );
        if (!rebuildDone) appendsWhileConverging++;
      }
      await rebuildFuture;
      expect(
        appendsWhileConverging,
        greaterThan(0),
        reason:
            'at least one append interleaved with the still-converging '
            'rebuild rather than waiting behind it',
      );
      await testBackendOf(store).close();
    });

    test('rebuildView resolves the copy by fingerprint, not the stale '
        'in-memory copy id: it does not throw when another instance already '
        'rebuilt the view', () async {
      final store = await _openStore();
      await _appendNote(store, 'agg-1');
      final backend = testBackendOf(store);
      final staleCopyId = store.copyIdOf(_kView);
      final spec = store.projections.lookup(_kView)!;
      final fingerprint = viewFingerprint(
        spec,
        store.entryTypes,
        store.promoters,
      );

      // Stands in for another instance's rebuild: it marks the copy this
      // instance's cache still names and creates the replacement, all
      // before this instance's own catch-up has refreshed its cache.
      final otherInstanceCopyId = await backend.transaction<String>((
        txn,
      ) async {
        await backend.markViewCopyForDeletionInTxn(txn, staleCopyId);
        return backend.createViewCopyInTxn(txn, _kView, fingerprint, 0);
      });

      await rebuildView(
        store: store,
        viewName: _kView,
        deadline: _farDeadline(),
      );

      final copies = await backend.transaction(backend.readViewCopiesInTxn);
      final unmarkedOfFingerprint = copies.where(
        (c) => c.fingerprint == fingerprint && !c.markedForDeletion,
      );
      expect(unmarkedOfFingerprint, hasLength(1));
      expect(unmarkedOfFingerprint.single.copyId, isNot(otherInstanceCopyId));
      expect(unmarkedOfFingerprint.single.copyId, isNot(staleCopyId));

      final afterRead = await store.reader.findViewRows(_kView);
      expect(afterRead.state, ViewConvergenceState.current);
      expect(afterRead.rows, hasLength(1));
      await testBackendOf(store).close();
    });

    test('rebuildView creates a replacement when another instance already '
        'marked the copy without creating one', () async {
      final store = await _openStore();
      await _appendNote(store, 'agg-1');
      final backend = testBackendOf(store);
      final staleCopyId = store.copyIdOf(_kView);

      await backend.transaction((txn) async {
        await backend.markViewCopyForDeletionInTxn(txn, staleCopyId);
      });

      await rebuildView(
        store: store,
        viewName: _kView,
        deadline: _farDeadline(),
      );

      final afterRead = await store.reader.findViewRows(_kView);
      expect(afterRead.state, ViewConvergenceState.current);
      expect(afterRead.rows, hasLength(1));
      await testBackendOf(store).close();
    });
  });
}
