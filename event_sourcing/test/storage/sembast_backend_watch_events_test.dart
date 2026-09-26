// Verifies: EVS-PRD-event-log/D
// watchEvents emits events in
//   sequence_number order from any starting position; replay + live merge
//   delivers all committed events without gaps.
// Verifies: EVS-PRD-portability/D
// watchEvents is a SembastBackend-specific
//   reactive surface built on top of the abstract StorageBackend contract.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/record_fixtures.dart';

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

Future<StoredEvent> _appendEvent(
  SembastBackend backend, {
  required String eventId,
  String aggregateId = 'agg-1',
  bool rollBack = false,
}) {
  return backend.transaction((txn) async {
    final seq = await backend.nextSequenceNumber(txn);
    final event = StoredEvent(
      key: 0,
      eventId: eventId,
      aggregateId: aggregateId,
      aggregateType: 'note',
      entryType: 'epistaxis_event',
      entryTypeVersion: const EntryTypeVersion(1, 0),
      libFormatVersion: LibVersion.dataFormat,
      eventType: 'finalized',
      sequenceNumber: seq,
      data: const <String, dynamic>{},
      metadata: const <String, dynamic>{},
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 4, 22, 10),
      eventHash: 'hash-$eventId',
      causal: kRootVersionCausal,
    );
    await backend.appendEvent(txn, event);
    if (rollBack) throw StateError('injected rollback');
    return event;
  });
}

/// A backend that runs [afterReplayRead] once, after the replay's read of
/// the log has returned and before the watcher attaches to live events:
/// the window a replay-to-live hand-off must close.
class _HandoffBackend extends SembastBackend {
  _HandoffBackend({required super.database});

  Future<void> Function()? afterReplayRead;

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final read = await super.findAllEvents(
      afterSequence: afterSequence,
      limit: limit,
      originatorHopId: originatorHopId,
      originatorIdentifier: originatorIdentifier,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
    final hook = afterReplayRead;
    afterReplayRead = null;
    if (hook != null) await hook();
    return read;
  }
}

/// Pumps the event queue until [done] holds, failing after a bounded
/// number of rounds.
Future<void> _waitFor(bool Function() done) async {
  for (var round = 0; round < 100; round++) {
    if (done()) return;
    await pumpEventQueue();
  }
  fail('condition not reached');
}

void main() {
  group('SembastBackend.watchEvents', () {
    late SembastBackend backend;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('watch-events-$dbCounter.db');
    });

    tearDown(() async {
      await backend.close();
    });

    // The replay delivers the stored events; once it has, the next append
    // can only reach the watcher through the live path.
    test('watchEvents replays then transitions to live', () async {
      await _appendEvent(backend, eventId: 'e1');
      await _appendEvent(backend, eventId: 'e2');

      final received = <String>[];
      final sub = backend.watchEvents().listen((e) => received.add(e.eventId));
      addTearDown(sub.cancel);
      await _waitFor(() => received.length == 2);
      expect(received, ['e1', 'e2']);

      await _appendEvent(backend, eventId: 'e3');
      await pumpEventQueue();
      expect(received, ['e1', 'e2', 'e3']);
    });

    // An append that commits after the replay has read the log and before
    // the watcher attaches to live events is delivered, once.
    test('an append committed between the replay read and the live attach '
        'is delivered once', () async {
      final b = _HandoffBackend(
        database: await newDatabaseFactoryMemory().openDatabase(
          'watch-events-window-$dbCounter.db',
        ),
      );
      addTearDown(b.close);
      await _appendEvent(b, eventId: 'e1');
      await _appendEvent(b, eventId: 'e2');
      var committed = false;
      b.afterReplayRead = () async {
        await _appendEvent(b, eventId: 'e3');
        committed = true;
      };
      final received = <String>[];
      final sub = b.watchEvents().listen((e) => received.add(e.eventId));
      addTearDown(sub.cancel);
      await _waitFor(() => committed);
      await pumpEventQueue();
      expect(received, ['e1', 'e2', 'e3']);
    });

    test('watchEvents skips replay events at or below afterSequence', () async {
      final e1 = await _appendEvent(backend, eventId: 'e1');
      await _appendEvent(backend, eventId: 'e2');

      final received = <String>[];
      final sub = backend
          .watchEvents(afterSequence: e1.sequenceNumber)
          .listen((e) => received.add(e.eventId));
      addTearDown(sub.cancel);
      await pumpEventQueue();
      expect(received, ['e2']);
    });

    // sequences.
    test('watchEvents is broadcast (multiple subscribers)', () async {
      final stream = backend.watchEvents();
      final sub1 = <String>[];
      final sub2 = <String>[];
      final s1 = stream.listen((e) => sub1.add(e.eventId));
      final s2 = stream.listen((e) => sub2.add(e.eventId));

      await _appendEvent(backend, eventId: 'e1');
      await _appendEvent(backend, eventId: 'e2');
      await Future<void>.delayed(Duration.zero);

      await s1.cancel();
      await s2.cancel();
      expect(sub1, ['e1', 'e2']);
      expect(sub2, ['e1', 'e2']);
    });

    // and subsequent watchEvents throws StateError.
    test('watchEvents closes on backend close, then throws', () async {
      final stream = backend.watchEvents();
      final completer = expectLater(stream, emitsDone);
      await backend.close();
      await completer;
      expect(() => backend.watchEvents(), throwsStateError);
      // Re-open a fresh backend so tearDown's close doesn't double-close.
      backend = await _openBackend('watch-events-reopen-$dbCounter.db');
    });

    // Ingest routes through appendEvent and shares the broadcast
    // controller with origin appends, so a single watchEvents
    // subscription sees both write paths.
    test('watchEvents emits ingested events (unified store)', () async {
      const destSource = Source(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );
      final registry = EntryTypeRegistry()
        ..register(
          const EntryTypeDefinition(
            id: 'epistaxis_event',
            registeredVersion: EntryTypeVersion(1, 0),
            name: 'Epistaxis Event',
          ),
        );
      final secCtx = SembastSecurityContextStore(backend: backend);
      final destStore = await EventStore.openForTest(
        storage: backend,
        entryTypes: registry,
        source: destSource,
        securityContexts: secCtx,
      );

      // Originate a single event in a separate originator backend.
      final origDb = await newDatabaseFactoryMemory().openDatabase(
        'watch-events-orig-$dbCounter.db',
      );
      final origBackend = SembastBackend(database: origDb);
      final origSecCtx = SembastSecurityContextStore(backend: origBackend);
      final origStore = await EventStore.openForTest(
        storage: origBackend,
        entryTypes: registry,
        source: const Source(
          hopId: 'mobile-device',
          identifier: 'device-1',
          softwareVersion: 'my_app@1.0.0',
        ),
        securityContexts: origSecCtx,
      );
      try {
        final origEvent = await origStore.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-watch-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{
            'answers': {'q': 'a'},
          },
          initiator: const UserInitiator('u1'),
        );
        expect(origEvent, isNotNull);

        final stream = backend.watchEvents();
        final received = <String>[];
        final sub = stream.listen((e) => received.add(e.eventId));
        await Future<void>.delayed(Duration.zero);

        // Ingest the originated event into dest. The receiver-hop event
        // routes through appendEvent under unification, so it must
        // surface on the stream.
        await destStore.ingestEvent(origEvent!);
        await Future<void>.delayed(Duration.zero);

        await sub.cancel();
        expect(received, contains(origEvent.eventId));
      } finally {
        await origBackend.close();
      }
    });
  });

  // Verifies: EVS-PRD-subscription/E
  // a committed transaction's event reaches watchers exactly once and a
  //   rolled-back one's never, when the two transactions are started
  //   together on one backend. sembast serializes transaction bodies under
  //   its lock, so the pair runs one after the other (commit then rollback,
  //   or rollback then commit); the tests show the publication of one run
  //   is unaffected by the run before it, not an interleaving of the two.
  group('SembastBackend.watchEvents across a commit and a rollback', () {
    late SembastBackend backend;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('watch-events-concurrent-$dbCounter.db');
    });

    tearDown(() async {
      await backend.close();
    });

    /// Starts both appends without awaiting either (sembast runs their
    /// bodies in turn), and returns what a live watcher received.
    Future<List<StoredEvent>> runPair({required bool firstRollsBack}) async {
      final received = <StoredEvent>[];
      final sub = backend.watchEvents().listen(received.add);
      await pumpEventQueue();

      final first = _appendEvent(
        backend,
        eventId: 'first',
        rollBack: firstRollsBack,
      );
      final second = _appendEvent(
        backend,
        eventId: 'second',
        rollBack: !firstRollsBack,
      );
      final outcomes = await Future.wait<Object?>([
        first.then<Object?>((e) => e, onError: (Object e) => e),
        second.then<Object?>((e) => e, onError: (Object e) => e),
      ]);
      expect(outcomes.whereType<StateError>(), hasLength(1));
      await pumpEventQueue();
      await sub.cancel();
      return received;
    }

    test('commit then rollback: only the first is delivered, once', () async {
      final received = await runPair(firstRollsBack: false);
      final stored = await backend.findAllEvents();
      expect(stored.map((e) => e.eventId), ['first']);
      expect(received.map((e) => e.eventId), ['first']);
      expect(received.single.sequenceNumber, stored.single.sequenceNumber);
    });

    test('rollback then commit: only the second is delivered, once', () async {
      final received = await runPair(firstRollsBack: true);
      final stored = await backend.findAllEvents();
      expect(stored.map((e) => e.eventId), ['second']);
      expect(received.map((e) => e.eventId), ['second']);
      expect(received.single.sequenceNumber, stored.single.sequenceNumber);
    });
  });
}
