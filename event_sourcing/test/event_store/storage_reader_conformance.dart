// Backend-agnostic scenarios for the storage reader an event store hands
// the application, and for the binding of every transaction handle to the
// event store or reader that issued it. Run on Sembast by
// test/event_store/storage_reader_test.dart and on Postgres by
// test/storage/postgres/postgres_storage_reader_test.dart.
//
// Traceability lives on the individual tests below.
//
// The dynamic calls on the reader are deliberate: they are what code that
// bypasses the static type runs.
// ignore_for_file: avoid_dynamic_calls
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:flutter_test/flutter_test.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

const _kType = 'reader_note';
const _kView = 'reader_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kSource = Source(
  hopId: 'reader-hop',
  identifier: 'reader-install',
  softwareVersion: 'reader-test',
);

const _kInitiator = UserInitiator('reader-user');

Future<EventStore> _openStore(VersionTestDatabase db, StorageBackend backend) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  return EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: _kSource,
    securityContexts: db.securityFor(backend),
    projections: ProjectionRegistry()..register(_kSpec),
  );
}

Future<StoredEvent> _appendNote(
  EventStore store,
  String aggregateId,
  Map<String, Object?> data, {
  SecurityDetails? security,
}) async => (await store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: _kType,
  eventType: 'noted',
  data: data,
  initiator: _kInitiator,
  security: security,
))!;

Future<StoredEvent?> _appendInTxn(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String aggregateId,
) => store.appendInTxn(
  txn,
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: _kType,
  eventType: 'noted',
  data: const <String, Object?>{'n': 1},
  initiator: _kInitiator,
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

/// Runs the storage-reader scenarios. [openDatabase] and [openOtherDatabase]
/// return two distinct databases; [skip] skips the group when set.
void runStorageReaderScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required Future<VersionTestDatabase?> Function() openOtherDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('storage reader ($backendLabel)', skip: skip, () {
    late VersionTestDatabase db;
    late StorageBackend backend;
    late EventStore store;
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open(
      VersionTestDatabase database, {
      StorageBackend? backend,
    }) async {
      final s = await _openStore(
        database,
        backend ?? await database.openBackend(),
      );
      opened.add(s);
      return s;
    }

    setUp(() async {
      db = (await openDatabase())!;
      databases.add(db);
      backend = await db.openBackend();
      store = await open(db, backend: backend);
    });

    tearDown(() async {
      for (final s in opened.reversed) {
        await s.close();
      }
      opened.clear();
      for (final d in databases.reversed) {
        await d.close();
      }
      databases.clear();
    });

    Future<int> eventCount() async =>
        (await store.reader.findAllEvents()).length;

    // Verifies: EVS-DEV-storage-capability/E
    // the reader the event store hands out is an object of its own that is
    //   no storage backend and declares no writing member; its reads return
    //   what the store holds.
    group('the reader is a separate read-only object', () {
      test('it is not a storage backend, and a downcast fails', () {
        final Object reader = store.reader;
        expect(reader is StorageBackend, isFalse);
        expect(() => reader as StorageBackend, throwsA(isA<TypeError>()));
      });

      test('a dynamic call of a backend writer finds no such member', () {
        final dynamic reader = store.reader;
        expect(
          () => reader.appendEvent(null, null),
          throwsA(isA<NoSuchMethodError>()),
        );
        expect(
          () =>
              reader.upsertViewRowInTxn(null, _kView, 'k', <String, Object?>{}),
          throwsA(isA<NoSuchMethodError>()),
        );
        expect(
          () => reader.enqueueFifoTxn(
            null,
            'dest',
            const <StoredEvent>[],
            wirePayload: null,
          ),
          throwsA(isA<NoSuchMethodError>()),
        );
        expect(
          () => reader.writeSchemaVersion(null, 1),
          throwsA(isA<NoSuchMethodError>()),
        );
        expect(() => reader.backend, throwsA(isA<NoSuchMethodError>()));
        // A tear-off would fail at the property read, before the call.
        // ignore: unnecessary_lambdas
        expect(() => reader.close(), throwsA(isA<NoSuchMethodError>()));
      });
    });

    // Verifies: EVS-DEV-storage-capability/E
    // the reader the event store hands out is an object of its own that is
    //   no storage backend and declares no writing member; its reads return
    //   what the store holds.
    group('the reads return what the store holds', () {
      test('outside a transaction', () async {
        final first = await _appendNote(store, 'agg-1', const <String, Object?>{
          'n': 1,
        }, security: const SecurityDetails(ipAddress: '192.0.2.1'));
        final second = await _appendNote(
          store,
          'agg-2',
          const <String, Object?>{'n': 2},
        );
        final reader = store.reader;

        final all = await reader.findAllEvents();
        expect(
          all.map((e) => e.eventId),
          containsAllInOrder(<String>[first.eventId, second.eventId]),
        );
        expect(
          (await reader.findAllEvents(
            afterSequence: first.sequenceNumber,
          )).map((e) => e.eventId),
          <String>[second.eventId],
        );
        expect(
          (await reader.findEventById(first.eventId))?.eventHash,
          first.eventHash,
        );
        expect(await reader.findEventById('no-such-event'), isNull);
        expect(
          (await reader.findEventsForAggregate('agg-2')).map((e) => e.eventId),
          <String>[second.eventId],
        );
        expect(await reader.readSequenceCounter(), second.sequenceNumber);
        expect(
          (await reader.readEventsReverse().first).eventId,
          second.eventId,
        );

        final rows = await reader.findViewRows(_kView);
        expect(rows, hasLength(2));
        final byKey = await reader.readViewRowsByKeys(_kView, <String>{
          'agg-1',
        });
        expect(byKey.keys, <String>['agg-1']);
        expect(byKey['agg-1']!['n'], 1);

        expect(await reader.readFifoHead('no-such-destination'), isNull);
        expect(await reader.listFifoEntries('no-such-destination'), isEmpty);
        expect(await reader.readFifoRow('no-such-destination', 'e'), isNull);
        expect(await reader.hasFifoWedged(), isFalse);
        expect(await reader.wedgedFifos(), isEmpty);
        expect(await reader.readFillCursor('no-such-destination'), -1);
        expect(await reader.readSchedule('no-such-destination'), isNull);
        expect(await reader.listSchedules(), isEmpty);
        expect(await reader.readSchemaVersion(), isNonNegative);

        final audit = await reader.queryAudit(ipAddress: '192.0.2.1');
        expect(audit.rows.map((r) => r.event.eventId), <String>[first.eventId]);
      });

      test("inside the reader's transaction", () async {
        final first = await _appendNote(store, 'agg-1', const <String, Object?>{
          'n': 1,
        });
        final reader = store.reader;
        await reader.transaction((txn) async {
          expect(
            (await reader.findEventByIdInTxn(txn, first.eventId))?.eventId,
            first.eventId,
          );
          expect(
            (await reader.findEventsForAggregateInTxn(
              txn,
              'agg-1',
            )).map((e) => e.eventId),
            <String>[first.eventId],
          );
          expect(
            (await reader.findAllEventsInTxn(txn)).map((e) => e.eventId),
            contains(first.eventId),
          );
          expect(await reader.readLatestEventHash(txn), first.eventHash);
          expect(
            (await reader.readViewRowInTxn(txn, _kView, 'agg-1'))?['n'],
            1,
          );
          expect(
            await reader.findViewRowsInTxn(
              txn,
              _kView,
              where: const <String, Object?>{'n': 1},
            ),
            hasLength(1),
          );
          expect(
            await reader.readViewTargetVersionInTxn(txn, _kView, _kType),
            const EntryTypeVersion(1, 0),
          );
          expect(
            await reader.readAllViewTargetVersionsInTxn(txn, _kView),
            <String, EntryTypeVersion>{_kType: const EntryTypeVersion(1, 0)},
          );
          expect(
            await reader.readViewTargetsForEntryTypeInTxn(txn, _kType),
            <String, EntryTypeVersion>{_kView: const EntryTypeVersion(1, 0)},
          );
          expect(
            await reader.readViewTargetBehindInTxn(txn, _kView, _kType),
            isFalse,
          );
        });
      });

      test("inside the event store's transaction, which the reader reads "
          'in so a decision and the append it guards share one '
          'transaction', () async {
        await _appendNote(store, 'agg-1', const <String, Object?>{'n': 1});
        final rows = await store.runTransaction(
          (txn, collector) => store.reader.findViewRowsInTxn(txn, _kView),
        );
        expect(rows, hasLength(1));
      });
    });

    // Verifies: EVS-DEV-storage-capability/G
    // a transaction handle used in an event store or reader other than the
    //   one that issued it (the reader accepting its own event store's
    //   live handles), or after its body returned, is refused with
    //   StateError and nothing is written.
    group('a transaction handle is bound to its issuer and its body', () {
      test('an event store handle used after its body returns is refused '
          'by appendInTxn, which writes nothing', () async {
        late Transaction escaped;
        late PublishCollector escapedCollector;
        await store.runTransaction((txn, collector) async {
          escaped = txn;
          escapedCollector = collector;
        });
        final before = await eventCount();
        await expectLater(
          _appendInTxn(store, escaped, escapedCollector, 'agg-late'),
          throwsStateError,
        );
        expect(await eventCount(), before);
      });

      test('an event store handle used after its body returns is refused '
          'by the reader', () async {
        late Transaction escaped;
        await store.runTransaction((txn, collector) async => escaped = txn);
        await expectLater(
          store.reader.findViewRowsInTxn(escaped, _kView),
          throwsStateError,
        );
      });

      test('a live handle of one event store is refused by another over '
          'the same backend, which writes nothing', () async {
        final other = await open(db, backend: backend);
        final before = await eventCount();
        await store.runTransaction((txn, collector) async {
          await expectLater(
            _appendInTxn(other, txn, collector, 'agg-foreign'),
            throwsStateError,
          );
        });
        expect(await eventCount(), before);
        expect(
          await store.reader.findEventsForAggregate('agg-foreign'),
          isEmpty,
        );
      });

      test('a live handle of one event store is refused by an event store '
          'over another database, which writes nothing', () async {
        final otherDb = (await openOtherDatabase())!;
        databases.add(otherDb);
        final other = await open(otherDb);
        final otherBefore = (await other.reader.findAllEvents()).length;
        await store.runTransaction((txn, collector) async {
          await expectLater(
            _appendInTxn(other, txn, collector, 'agg-foreign'),
            throwsStateError,
          );
          await expectLater(
            other.reader.findViewRowsInTxn(txn, _kView),
            throwsStateError,
          );
        });
        expect((await other.reader.findAllEvents()).length, otherBefore);
      });

      test("a reader handle passed to the event store's appendInTxn is "
          'refused, and nothing is written', () async {
        late PublishCollector closedCollector;
        await store.runTransaction((txn, collector) async {
          closedCollector = collector;
        });
        final before = await eventCount();
        await store.reader.transaction((txn) async {
          await expectLater(
            _appendInTxn(store, txn, closedCollector, 'agg-reader'),
            throwsStateError,
          );
        });
        expect(await eventCount(), before);
      });

      test('a reader handle used after its body returns is refused', () async {
        late Transaction escaped;
        await store.reader.transaction((txn) async => escaped = txn);
        await expectLater(
          store.reader.findViewRowsInTxn(escaped, _kView),
          throwsStateError,
        );
        await expectLater(
          store.reader.findEventByIdInTxn(escaped, 'e'),
          throwsStateError,
        );
      });

      test('a reader handle is refused by the reader of an event store over '
          'another database', () async {
        final otherDb = (await openOtherDatabase())!;
        databases.add(otherDb);
        final other = await open(otherDb);
        await store.reader.transaction((txn) async {
          await expectLater(
            other.reader.findViewRowsInTxn(txn, _kView),
            throwsStateError,
          );
        });
      });
    });
  });
}
