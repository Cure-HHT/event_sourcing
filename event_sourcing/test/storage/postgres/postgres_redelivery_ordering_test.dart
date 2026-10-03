// A converging AggregateMode subscription's redelivery read races a
// concurrent append on Postgres, where the redelivery read and the append
// run on independent pooled connections instead of sharing one storage
// lock. Gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const _kType = 'redelivery_note';
const _kView = 'redelivery_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kSource = Source(
  hopId: 'redelivery-hop',
  identifier: 'redelivery-install',
  softwareVersion: 'redelivery-test',
);

Future<EventStore> _openStore(PostgresBackend backend) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry()..register(_kSpec);
  final security = PostgresSecurityContextStore(backend: backend);
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, security),
    entryTypes: entryTypes,
    source: _kSource,
    projections: registry,
  );
}

Future<StoredEvent?> _append(
  EventStore store,
  String aggregateId,
  String title,
) => store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: 'note',
  eventType: 'finalized',
  data: {'title': title},
  initiator: const UserInitiator('redelivery-user'),
);

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'redeliv');
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
  });

  tearDown(() async {
    for (final backend in backends.reversed) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-PRD-subscription/C
  // a live change published while the redelivery read is in flight is
  //   buffered until the redelivered rows are emitted.
  test('an append that commits while the redelivery read is in flight is '
      "buffered: its Delta never precedes the redelivered aggregate's "
      'Snapshot in the subscription stream', () async {
    if (db == null) return;

    final seedBackend = await db.open(provision: true);
    backends.add(seedBackend);
    final seeder = await _openStore(seedBackend);
    final settled = (await _append(seeder, 'agg-1', 'first'))!;
    await _append(seeder, 'agg-2', 'second');
    final copyId = seeder.copyIdOf(_kView);
    await seeder.close();
    backends.remove(seedBackend);

    // Rewind the stored watermark directly at the backend, so the copy
    // this test's own store opens finds a converging view (agg-2's
    // event lies past the watermark) without a competing instance.
    final rewindBackend = await db.open();
    backends.add(rewindBackend);
    await rewindBackend.transaction(
      (txn) => rewindBackend.setViewCopyWatermarkInTxn(
        txn,
        copyId,
        settled.sequenceNumber,
      ),
    );
    await rewindBackend.close();
    backends.remove(rewindBackend);

    var paused = true;
    Future<void> catchUpHook(String id, String eventId) async {
      if (id != copyId) return;
      if (paused) {
        throw const InjectedFailure('paused for redelivery ordering test');
      }
    }

    var armed = false;
    var hookRan = false;
    late EventStore store;
    final thirdAppended = Completer<void>();
    Future<void> afterStateReadHook() async {
      if (!armed || hookRan) return;
      hookRan = true;
      // The redelivery read runs as one REPEATABLE READ / SERIALIZABLE
      // READ ONLY transaction, on a connection independent of this
      // append's: fully awaiting the append here -- unlike the
      // Sembast tests' unawaited-and-queue dance, needed only because
      // Sembast serializes every transaction on one database-wide lock
      // -- lets it commit and publish its live Delta while the read's
      // transaction is still open. Snapshot isolation guarantees the
      // read's own row fetch, right after this hook returns, still
      // reflects the pre-append state, so the redelivered Snapshot is
      // deterministically stale relative to the Delta this append
      // publishes immediately on commit.
      await _append(store, 'agg-2', 'third');
      thirdAppended.complete();
    }

    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onCatchUpStep: catchUpHook,
        afterViewStateReadBeforeRows: afterStateReadHook,
      ),
      () async {
        final backend = await db.open();
        backends.add(backend);
        store = await _openStore(backend);
        expect(store.copyIdOf(_kView), copyId);

        final updates = <Update<Map<String, Object?>?>>[];
        final firstMarker = Completer<EndOfReplay<Map<String, Object?>?>>();
        final secondMarker = Completer<EndOfReplay<Map<String, Object?>?>>();
        final sub = store
            .subscribe(
              const SubscriptionFilter(),
              AggregateMode<Map<String, Object?>?>(
                viewName: _kView,
                mapper: (m) => m,
                aggregates: const {'agg-1', 'agg-2'},
              ),
            )
            .listen((u) {
              updates.add(u);
              if (u is EndOfReplay<Map<String, Object?>?>) {
                if (!firstMarker.isCompleted) {
                  firstMarker.complete(u);
                } else if (!secondMarker.isCompleted) {
                  secondMarker.complete(u);
                }
              }
            });

        final marker1 = await firstMarker.future.timeout(
          const Duration(seconds: 10),
        );
        expect(marker1.state, ViewConvergenceState.converging);

        armed = true;
        paused = false;

        final marker2 = await secondMarker.future.timeout(
          const Duration(seconds: 20),
        );
        expect(marker2.state, ViewConvergenceState.current);
        expect(hookRan, isTrue);
        await thirdAppended.future.timeout(const Duration(seconds: 10));

        // Wait for the live path to settle so the third append's Delta,
        // wherever it lands, is present in `updates` by the time this
        // checks ordering.
        for (var i = 0; i < 200; i++) {
          if (updates.any(
            (u) =>
                u is Delta<Map<String, Object?>?> &&
                u.value?['aggregateId'] == 'agg-2' &&
                u.value?['title'] == 'third',
          )) {
            break;
          }
          await Future<void>.delayed(const Duration(milliseconds: 20));
        }

        // The redelivered row for agg-2 must never be followed by a
        // Delta carrying an OLDER sequence than one already delivered
        // for agg-2 -- the ordering EVS-PRD-subscription/C requires.
        var lastAgg2Sequence = -1;
        for (final u in updates) {
          switch (u) {
            case Snapshot<Map<String, Object?>?>(:final value, :final sequence)
                when value != null && value['aggregateId'] == 'agg-2':
              expect(
                sequence,
                greaterThanOrEqualTo(lastAgg2Sequence),
                reason:
                    'a Snapshot must never carry an older sequence than '
                    'an already-delivered update for the same aggregate',
              );
              lastAgg2Sequence = sequence;
            case Delta<Map<String, Object?>?>(:final value, :final sequence)
                when value != null && value['aggregateId'] == 'agg-2':
              expect(sequence, greaterThanOrEqualTo(lastAgg2Sequence));
              lastAgg2Sequence = sequence;
            default:
          }
        }
        expect(
          updates.any(
            (u) =>
                (u is Snapshot<Map<String, Object?>?> &&
                    u.value?['title'] == 'third') ||
                (u is Delta<Map<String, Object?>?> &&
                    u.value?['title'] == 'third'),
          ),
          isTrue,
          reason: 'the concurrent append must have reached the subscriber',
        );

        await sub.cancel();
      },
    );
  });
}
