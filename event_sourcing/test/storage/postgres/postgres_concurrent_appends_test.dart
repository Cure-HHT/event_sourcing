// Verifies: EVS-PRD-event-log/E
// concurrent appends from several instances make progress: two event
//   stores on two PostgresBackends, each appending in a tight loop, never
//   exhaust the transaction's retry bound, and every append commits once,
//   with gapless sequence numbers.
// Verifies: EVS-PRD-event-log/E
// the same holds while a delivery cycle drains the database at a short
//   cadence: its pass starts, fills and outcomes write the table the
//   appends' sequence counter lives in, and no append, and no transaction
//   of the cycle, exhausts the retry bound; every event is delivered.
// Verifies: EVS-PRD-subscription/C
// live subscribers of one event store receive its events in log order when
//   its appends run concurrently on several pool connections.
//
// Gated on PG_TEST_URL; files that reset the schema run one at a time.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: 'concurrent_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'concurrent_note',
);

/// Accepts every send and records the event ids it received.
class _Sink extends Destination {
  final List<String> received = <String>[];

  @override
  String get id => 'soak';

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'concurrent_note'});

  @override
  String get wireFormat => 'soak-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < 20;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async => WirePayload(
    bytes: Uint8List.fromList(
      utf8.encode(
        jsonEncode(<String, Object?>{
          'ids': <String>[for (final e in batch) e.eventId],
        }),
      ),
    ),
    contentType: 'application/json',
    transformVersion: 'soak-v1',
  );

  @override
  Future<SendResult> send(WirePayload payload) async {
    received.addAll(
      ((jsonDecode(utf8.decode(payload.bytes)) as Map<String, Object?>)['ids']!
              as List<Object?>)
          .cast<String>(),
    );
    return const SendOk();
  }
}

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final d in kSystemEntryTypes) {
    registry.register(d);
  }
  return registry..register(_noteDef);
}

Future<EventStore> _openStore(PostgresBackend backend, String installId) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: _entryTypes(),
      source: Source(
        hopId: 'test',
        identifier: installId,
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );

Future<void> _append(EventStore store, String id) => store.append(
  entryType: 'concurrent_note',
  aggregateId: id,
  aggregateType: 'note',
  eventType: 'noted',
  data: <String, Object?>{'id': id},
  initiator: const UserInitiator('u'),
);

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  tearDownAll(db.drop);

  final backends = <PostgresBackend>[];
  setUp(db.reset);
  tearDown(() async {
    for (final b in backends) {
      await b.close();
    }
    backends.clear();
  });

  Future<PostgresBackend> open() async {
    final b = await db.open(provision: true);
    backends.add(b);
    return b;
  }

  test('two instances appending in tight loops never exhaust the retry '
      'bound', () async {
    const perInstance = 200;
    final a = await _openStore(
      await open(),
      'aaaa0001-0000-4000-8000-0000000000a1',
    );
    final b = await _openStore(
      await open(),
      'aaaa0001-0000-4000-8000-0000000000b1',
    );
    Future<void> loop(EventStore store, String prefix) async {
      for (var i = 0; i < perInstance; i++) {
        await _append(store, '$prefix-$i');
      }
    }

    await Future.wait(<Future<void>>[loop(a, 'a'), loop(b, 'b')]);
    final notes = <StoredEvent>[
      for (final e in await a.reader.findAllEvents())
        if (e.entryType == 'concurrent_note') e,
    ];
    expect(notes, hasLength(2 * perInstance));
    final all = await a.reader.findAllEvents();
    expect(
      <int>[for (final e in all) e.sequenceNumber],
      <int>[for (var n = 1; n <= all.length; n++) n],
      reason: 'gapless sequence numbers',
    );
  }, timeout: const Timeout(Duration(minutes: 5)));

  test('two instances appending in tight loops while a delivery cycle drains '
      'at a short cadence', () async {
    const perInstance = 200;
    final log = <LibraryLogRecord>[];
    await runWithDeliveryTestHooks(DeliveryTestHooks(onLog: log.add), () async {
      final a = await _openStore(
        await open(),
        'aaaa0001-0000-4000-8000-0000000000e1',
      );
      final b = await _openStore(
        await open(),
        'aaaa0001-0000-4000-8000-0000000000f1',
      );
      final registry = DestinationRegistry(eventStore: a);
      final sink = _Sink();
      await registry.addDestination(
        sink,
        initiator: const AutomationInitiator(service: 'soak'),
      );
      await registry.setStartDate(
        sink.id,
        DateTime.utc(2000),
        initiator: const AutomationInitiator(service: 'soak'),
      );
      final cycle = await SyncCycle.start(
        registry: registry,
        cadence: const Duration(milliseconds: 200),
      );
      try {
        expect(cycle.state, SyncCycleState.running);
        Future<void> loop(EventStore store, String prefix) async {
          for (var i = 0; i < perInstance; i++) {
            await _append(store, '$prefix-$i');
          }
        }

        await Future.wait(<Future<void>>[loop(a, 'a'), loop(b, 'b')]);
        final appended = <String>[
          for (final e in await a.reader.findAllEvents())
            if (e.entryType == 'concurrent_note') e.eventId,
        ];
        expect(appended, hasLength(2 * perInstance));
        final deadline = DateTime.now().add(const Duration(seconds: 30));
        while (sink.received.toSet().length < appended.length) {
          if (DateTime.now().isAfter(deadline)) {
            for (final r in log.take(20)) {
              // ignore: avoid_print
              print(r);
            }
            fail(
              'delivered ${sink.received.toSet().length} of '
              '${appended.length} within 30 s',
            );
          }
          await Future<void>.delayed(const Duration(milliseconds: 50));
        }
        expect(sink.received.toSet(), appended.toSet());
        final all = await a.reader.findAllEvents();
        expect(
          <int>[for (final e in all) e.sequenceNumber],
          <int>[for (var n = 1; n <= all.length; n++) n],
          reason: 'gapless sequence numbers',
        );
        final failures = <LibraryLogRecord>[
          for (final r in log)
            if (r.error is TransactionRetryExhaustedException ||
                (r.error is ServerException &&
                    (r.error! as ServerException).code == '40P01') ||
                r.level == LibraryLogLevel.severe)
              r,
        ];
        expect(failures, isEmpty, reason: 'no failed transaction logged');
      } finally {
        await cycle.close(timeout: const Duration(seconds: 5));
      }
    });
  }, timeout: const Timeout(Duration(minutes: 5)));

  test(
    'concurrent appends on one store are delivered in log order',
    () async {
      final store = await _openStore(
        await open(),
        'aaaa0001-0000-4000-8000-0000000000c1',
      );
      final delivered = <int>[];
      final sub = store
          .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
          .listen((u) {
            if (u is Delta<StoredEvent>) delivered.add(u.value.sequenceNumber);
          });
      for (var round = 0; round < 20; round++) {
        await Future.wait(<Future<void>>[
          for (var i = 0; i < 8; i++) _append(store, 'r$round-$i'),
        ]);
      }
      await Future<void>.delayed(const Duration(milliseconds: 200));
      await sub.cancel();
      final stored = <int>[
        for (final e in await store.reader.findAllEvents())
          if (e.entryType == 'concurrent_note') e.sequenceNumber,
      ];
      expect(delivered, stored);
    },
    timeout: const Timeout(Duration(minutes: 5)),
  );

  test('a later commit whose continuation resumes first is delivered after '
      'the earlier one', () async {
    final store = await _openStore(
      await open(),
      'aaaa0001-0000-4000-8000-0000000000d1',
    );
    final delivered = <String>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value.aggregateId);
        });
    final late = Completer<void>();
    var calls = 0;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        afterCommitBeforePublish: () async {
          calls += 1;
          if (calls == 1) await late.future;
        },
      ),
      () async {
        final first = _append(store, 'first');
        while (calls == 0) {
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
        await _append(store, 'second');
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(delivered, isEmpty, reason: 'the second waits for the first');
        late.complete();
        await first;
      },
    );
    await Future<void>.delayed(const Duration(milliseconds: 100));
    await sub.cancel();
    expect(delivered, <String>['first', 'second']);
  });
}
