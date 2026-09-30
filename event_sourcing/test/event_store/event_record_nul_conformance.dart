// Backend-agnostic scenarios for the U+0000 refusal on append: an append
// carrying the character U+0000 in any string of the record, a key
// included, at any depth, is refused before any write, naming the
// top-level field that carries it, and nothing of the refused append is
// stored -- not even the sequence number a later append would otherwise
// skip. Run on Sembast by test/event_store/event_record_nul_test.dart and
// on Postgres by test/storage/postgres/postgres_event_record_nul_test.dart.
//
// Traceability lives on the individual tests below.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

const String _kType = 'nul_note';

const _kInitiator = UserInitiator('nul-user');

EntryTypeRegistry _registry() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: _kType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kType,
    ),
  );

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runEventRecordNulScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('an append carrying U+0000 is refused ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open() async {
      final db = (await openDatabase())!;
      databases.add(db);
      final backend = await db.openBackend();
      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
        entryTypes: _registry(),
        source: const Source(
          hopId: 'nul-hop',
          identifier: 'nul-install',
          softwareVersion: 'nul-app@1.0.0',
        ),
      );
      opened.add(store);
      return store;
    }

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

    Future<StoredEvent?> appendOk(EventStore store, String aggregateId) =>
        store.append(
          entryType: _kType,
          aggregateId: aggregateId,
          aggregateType: _kType,
          eventType: 'finalized',
          data: <String, Object?>{'n': 1},
          initiator: _kInitiator,
        );

    /// Attempts [data]/[metadata]/[initiator] via [EventStore.append] and
    /// checks it is refused before any write, naming [field].
    Future<void> expectAppendRefused(
      EventStore store, {
      required String field,
      Map<String, Object?>? data,
      Map<String, Object?>? metadata,
      Initiator? initiator,
    }) async {
      await expectLater(
        () => store.append(
          entryType: _kType,
          aggregateId: 'nul-agg',
          aggregateType: _kType,
          eventType: 'finalized',
          data: data ?? <String, Object?>{'n': 1},
          metadata: metadata,
          initiator: initiator ?? _kInitiator,
        ),
        throwsA(isA<ArgumentError>().having((e) => e.name, 'name', field)),
      );
    }

    // Verifies: EVS-DEV-event-record/L
    // Verifies: EVS-DEV-event-record/M
    test('a direct string value of data carrying U+0000 is refused, naming '
        'data', () async {
      final store = await open();
      await expectAppendRefused(
        store,
        field: 'data',
        data: <String, Object?>{'note': 'x\u0000y'},
      );
      expect(await store.reader.findAllEvents(entryType: _kType), isEmpty);
    });

    // Verifies: EVS-DEV-event-record/L
    // Verifies: EVS-DEV-event-record/M
    test('a nested map key of data carrying U+0000 is refused, naming '
        'data', () async {
      final store = await open();
      await expectAppendRefused(
        store,
        field: 'data',
        data: <String, Object?>{
          'nested': <String, Object?>{'k\u0000ey': 1},
        },
      );
    });

    // Verifies: EVS-DEV-event-record/L
    // Verifies: EVS-DEV-event-record/M
    test('a value inside a list of data carrying U+0000 is refused, naming '
        'data', () async {
      final store = await open();
      await expectAppendRefused(
        store,
        field: 'data',
        data: <String, Object?>{
          'items': <Object?>['a', 'x\u0000y'],
        },
      );
    });

    // Verifies: EVS-DEV-event-record/L
    // Verifies: EVS-DEV-event-record/M
    test(
      'a metadata value carrying U+0000 is refused, naming metadata',
      () async {
        final store = await open();
        await expectAppendRefused(
          store,
          field: 'metadata',
          metadata: <String, Object?>{'note': 'x\u0000y'},
        );
      },
    );

    // Verifies: EVS-DEV-event-record/L
    // Verifies: EVS-DEV-event-record/M
    test('an initiator value carrying U+0000 is refused, naming '
        'initiator', () async {
      final store = await open();
      await expectAppendRefused(
        store,
        field: 'initiator',
        initiator: const UserInitiator('u\u0000ser'),
      );
    });

    // Verifies: EVS-DEV-event-record/M
    test('appendInTxn refuses the same way as append', () async {
      final store = await open();
      await expectLater(
        () => store.runTransaction(
          (txn, collector) => store.appendInTxn(
            txn,
            entryType: _kType,
            aggregateId: 'nul-agg',
            aggregateType: _kType,
            eventType: 'finalized',
            data: <String, Object?>{'note': 'x\u0000y'},
            initiator: _kInitiator,
            flowToken: null,
            metadata: null,
            security: null,
            checkpointReason: null,
            changeReason: null,
            dedupeByContent: false,
            collector: collector,
          ),
        ),
        throwsA(isA<ArgumentError>().having((e) => e.name, 'name', 'data')),
      );
    });

    // Verifies: EVS-DEV-event-record/M
    test('a refused append reserves no sequence number: the next append '
        'follows the last successful one with no gap', () async {
      final store = await open();
      final before = (await appendOk(store, 'nul-agg-before'))!;
      await expectAppendRefused(
        store,
        field: 'data',
        data: <String, Object?>{'note': 'x\u0000y'},
      );
      final after = (await appendOk(store, 'nul-agg-after'))!;
      expect(after.sequenceNumber, before.sequenceNumber + 1);
    });
  });
}
