import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/deliveries.dart' show ingestEventForTest;
import '../test_support/destination_wedges_view_conformance.dart'
    show forgedEvent;

const Source _source = Source(
  hopId: 'server',
  identifier: 'lineage-test-install',
  softwareVersion: 'test@1.0.0',
);

/// Crafts a succession event naming [databaseId] as having succeeded
/// [predecessorDatabaseId], with one restored channel, and returns it as an
/// event a peer authored (not this store's own identity).
StoredEvent _successionEvent({
  required String databaseId,
  required String predecessorDatabaseId,
  String registrationId = 'reg-1',
}) {
  final data = SenderSuccessionData(
    id: 'destination-1',
    registrationId: registrationId,
    databaseId: databaseId,
    predecessorDatabaseId: predecessorDatabaseId,
    predecessorChannels: <SenderSuccessionChannel>[
      SenderSuccessionChannel(
        channel: DeliveryChannel(
          senderDatabaseId: predecessorDatabaseId,
          destinationId: 'destination-1',
          registrationId: registrationId,
          generation: 1,
        ),
        deliveryNumber: 3,
        deliveryHash: 'h' * 64,
      ),
    ],
  );
  return forgedEvent(
    entryType: kDestinationSenderSucceededEntryType,
    aggregateType: kDestinationAuditAggregateType,
    eventType: kDestinationSenderSucceededEventType,
    data: data.toJson(),
    originDatabaseId: databaseId,
  );
}

Future<EventStore> _openStore() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'succession-lineage-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry(),
    source: _source,
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
  return store;
}

void main() {
  group('succession lineage read', () {
    // Verifies: EVS-DEV-sender-succession/G
    // a chain of succession events is walked transitively: C's lineage
    //   names both B and A, earliest first.
    test(
      'walks a chain of predecessors transitively, earliest first',
      () async {
        final store = await _openStore();
        addTearDown(store.close);
        await ingestEventForTest(
          store,
          _successionEvent(databaseId: 'B', predecessorDatabaseId: 'A'),
        );
        await ingestEventForTest(
          store,
          _successionEvent(databaseId: 'C', predecessorDatabaseId: 'B'),
        );

        final ofC = await store.reader.successionLineageOf('C');
        expect(ofC.predecessors, <String>['A', 'B']);
        expect(ofC.successor, isNull);

        final ofB = await store.reader.successionLineageOf('B');
        expect(ofB.predecessors, <String>['A']);
        expect(ofB.successor, 'C');

        final ofA = await store.reader.successionLineageOf('A');
        expect(ofA.predecessors, isEmpty);
        expect(ofA.successor, 'B');
      },
    );

    // Verifies: EVS-DEV-sender-succession/G
    // an identity the log holds no succession event for has no predecessors
    //   and no successor.
    test('an identity with no succession has an empty lineage', () async {
      final store = await _openStore();
      addTearDown(store.close);
      await ingestEventForTest(
        store,
        _successionEvent(databaseId: 'B', predecessorDatabaseId: 'A'),
      );

      final lineage = await store.reader.successionLineageOf('unrelated');
      expect(lineage.predecessors, isEmpty);
      expect(lineage.successor, isNull);
    });

    // Verifies: EVS-DEV-sender-succession/G
    // the successor of an identity is read from the succession event that
    //   names it as a predecessor.
    test('reads the successor of a succeeded identity', () async {
      final store = await _openStore();
      addTearDown(store.close);
      await ingestEventForTest(
        store,
        _successionEvent(databaseId: 'B', predecessorDatabaseId: 'A'),
      );

      final lineage = await store.reader.successionLineageOf('A');
      expect(lineage.successor, 'B');
      expect(lineage.predecessors, isEmpty);
    });

    // Verifies: EVS-DEV-sender-succession/G
    // a held cycle of succession events (here, a self-succession) does not
    //   loop forever: the walk stops at the first identity seen twice.
    test(
      'a held succession cycle does not hang the read',
      () async {
        final store = await _openStore();
        addTearDown(store.close);
        await ingestEventForTest(
          store,
          _successionEvent(databaseId: 'A', predecessorDatabaseId: 'A'),
        );

        final lineage = await store.reader.successionLineageOf('A');
        expect(lineage.predecessors, isEmpty);
      },
      timeout: const Timeout(Duration(seconds: 10)),
    );

    // Verifies: EVS-DEV-sender-succession/G
    // a held succession event that does not parse is skipped rather than
    //   thrown, so it does not poison the lineage of another identity: the
    //   read writes nothing and so cannot record a finding for it.
    test(
      'a malformed held succession event does not poison other lookups',
      () async {
        final store = await _openStore();
        addTearDown(store.close);
        await ingestEventForTest(
          store,
          forgedEvent(
            entryType: kDestinationSenderSucceededEntryType,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kDestinationSenderSucceededEventType,
            data: const <String, Object?>{
              'id': 'destination-1',
              'database_id': 'malformed-successor',
              // Missing registration_id, predecessor_database_id and
              // predecessor_channels.
            },
            originDatabaseId: 'malformed-successor',
          ),
        );
        await ingestEventForTest(
          store,
          _successionEvent(databaseId: 'B', predecessorDatabaseId: 'A'),
        );

        final lineage = await store.reader.successionLineageOf('B');
        expect(lineage.predecessors, <String>['A']);
      },
    );
  });

  group('SenderSuccessionData parsing', () {
    test('round-trips through JSON', () {
      final data = SenderSuccessionData(
        id: 'dest',
        registrationId: 'reg',
        databaseId: 'C',
        predecessorDatabaseId: 'B',
        predecessorChannels: <SenderSuccessionChannel>[
          SenderSuccessionChannel(
            channel: const DeliveryChannel(
              senderDatabaseId: 'B',
              destinationId: 'dest',
              registrationId: 'reg',
              generation: 2,
            ),
            deliveryNumber: 7,
            deliveryHash: 'a' * 64,
          ),
        ],
      );
      final parsed = SenderSuccessionData.fromJson(data.toJson());
      expect(parsed, data);
    });

    test('refuses a data object missing a required key', () {
      expect(
        () => SenderSuccessionData.fromJson(const <String, Object?>{
          'id': 'dest',
          'registration_id': 'reg',
          'database_id': 'C',
          'predecessor_channels': <Object?>[],
        }),
        throwsFormatException,
      );
    });
  });
}
