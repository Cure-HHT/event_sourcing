// Backend-agnostic scenarios for the receiver's accept path: a native
// delivery presented to the event store's receiver endpoint is accepted
// only when it follows the receiver's record of its channel, which the
// receiver derives from the `ingest.delivery_accepted` audits it authored;
// every answer carries that record. Run on Sembast by
// test/ingest/delivery_receiver_sembast_test.dart and on Postgres by
// test/storage/postgres/delivery_receiver_postgres_test.dart.
//
// This file exposes [runDeliveryReceiverScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionChannel, SenderSuccessionData;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kIngestAuditEntryType;
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'ingest_record_findings_conformance.dart' show sealedRecord;
import 'record_fixtures.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

/// The entry type of the records the scenarios deliver.
const String kDeliveryNoteType = 'finding_note';

const Source _receiverSource = Source(
  hopId: 'receiver-hop',
  identifier: 'receiver-install',
  softwareVersion: 'receiver-app@1.0.0',
);

/// The channel the scenarios deliver on, from [kPeerDatabaseId] unless
/// [senderDatabaseId] names another sender.
DeliveryChannel deliveryChannel({
  String senderDatabaseId = kPeerDatabaseId,
  String destinationId = 'hub',
  String registrationId = 'registration-1',
  int generation = 1,
}) => DeliveryChannel(
  senderDatabaseId: senderDatabaseId,
  destinationId: destinationId,
  registrationId: registrationId,
  generation: generation,
);

/// A delivery on [channel] numbered [number], linking to [link], carrying
/// [records] (by default one fresh record the channel's sender authored).
DeliveryEnvelope sealedDelivery({
  DeliveryChannel? channel,
  int number = 1,
  String? link,
  List<Map<String, Object?>>? records,
  Map<String, Object?> attributes = const <String, Object?>{},
}) {
  final c = channel ?? deliveryChannel();
  return DeliveryEnvelope.seal(
    batchId: 'delivery-${c.senderDatabaseId}-${c.generation}-$number',
    senderHop: 'peer-hop',
    senderIdentifier: 'peer-install',
    senderSoftwareVersion: 'peer-app@1.0.0',
    sentAt: DateTime.utc(2026, 9, 1, 12),
    channel: c,
    deliveryNumber: number,
    previousDeliveryHash: link,
    events:
        records ??
        <Map<String, Object?>>[sealedRecord(databaseId: c.senderDatabaseId)],
    attributes: attributes,
  );
}

/// The database identity a succession event names as the predecessor in
/// the scenarios below.
const String kPredecessorDatabaseId = 'predecessor-database';

/// The delivery hash [successionRecord] names by default.
const String _kPredecessorDeliveryHash =
    'hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh';

/// The channel a hand-built succession event names as restored from
/// [predecessorDatabaseId], by default.
DeliveryChannel successionPredecessorChannel({
  String predecessorDatabaseId = kPredecessorDatabaseId,
}) => DeliveryChannel(
  senderDatabaseId: predecessorDatabaseId,
  destinationId: 'destination-1',
  registrationId: 'registration-1',
  generation: 1,
);

/// A hand-built succession event (`system.destination_sender_succeeded`)
/// naming [databaseId] as having succeeded [predecessorDatabaseId], sealed
/// as its provenance's originator, naming [predecessorChannel] restored up
/// to [predecessorDeliveryNumber] (hashed [predecessorDeliveryHash]).
Map<String, Object?> successionRecord({
  String databaseId = kPeerDatabaseId,
  String predecessorDatabaseId = kPredecessorDatabaseId,
  DeliveryChannel? predecessorChannel,
  int predecessorDeliveryNumber = 3,
  String predecessorDeliveryHash = _kPredecessorDeliveryHash,
}) => sealedRecord(
  databaseId: databaseId,
  entryType: kDestinationSenderSucceededEntryType,
  aggregateType: kDestinationAuditAggregateType,
  eventType: kDestinationSenderSucceededEventType,
  data: SenderSuccessionData(
    id: 'destination-1',
    registrationId: 'registration-1',
    databaseId: databaseId,
    predecessorDatabaseId: predecessorDatabaseId,
    predecessorChannels: <SenderSuccessionChannel>[
      SenderSuccessionChannel(
        channel:
            predecessorChannel ??
            successionPredecessorChannel(
              predecessorDatabaseId: predecessorDatabaseId,
            ),
        deliveryNumber: predecessorDeliveryNumber,
        deliveryHash: predecessorDeliveryHash,
      ),
    ],
  ).toJson(),
);

/// The record a receiver answers with after accepting [delivery].
DeliveryRecord recordAfter(DeliveryEnvelope delivery) => DeliveryRecord(
  deliveryNumber: delivery.deliveryNumber,
  deliveryHash: delivery.deliveryHash,
);

/// The `ingest.delivery_accepted` audits [store] holds as authored, in log
/// order.
Future<List<StoredEvent>> authoredDeliveryAudits(EventStore store) async =>
    <StoredEvent>[
      for (final e in await store.reader.findAllEvents(
        entryType: kIngestAuditEntryType,
      ))
        if (e.eventType == 'ingest.delivery_accepted' &&
            (e.metadata['provenance']! as List).length == 1)
          e,
    ];

/// The findings [store] holds as authored, in log order.
Future<List<Map<String, Object?>>> authoredFindings(EventStore store) async =>
    <Map<String, Object?>>[
      for (final e in await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      ))
        if ((e.metadata['provenance']! as List).length == 1)
          Map<String, Object?>.from(e.data),
    ];

/// Opens an event store over a new backend of [db], registering the
/// scenarios' entry type.
Future<EventStore> openReceiverStore(
  VersionTestDatabase db, {
  Source source = _receiverSource,
}) async {
  final backend = await db.openBackend();
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: kDeliveryNoteType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: kDeliveryNoteType,
        ),
      ),
    source: source,
  );
}

/// Runs the scenarios. [openDatabase] returns a fresh database; [skip]
/// skips the group when set.
void runDeliveryReceiverScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('receiver accept path ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<VersionTestDatabase> database() async {
      final db = (await openDatabase())!;
      databases.add(db);
      return db;
    }

    Future<EventStore> open([VersionTestDatabase? db]) async {
      final store = await openReceiverStore(db ?? await database());
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

    Future<ReceiverResponse> present(
      EventStore store,
      DeliveryEnvelope delivery, {
      Set<String>? senders,
    }) => store.receiverEndpoint.accept(
      delivery.encode(),
      senderDatabaseIds: senders ?? <String>{delivery.channel.senderDatabaseId},
    );

    ReceiverAcknowledgement acknowledgement(
      EventStore store,
      DeliveryEnvelope delivery,
      AcknowledgementOutcome outcome,
    ) => ReceiverAcknowledgement(
      channel: delivery.channel,
      receiverDatabaseId: store.databaseId,
      record: recordAfter(delivery),
      outcome: outcome,
    );

    // Verifies: EVS-PRD-delivery-channel/C
    // Verifies: EVS-DEV-delivery-receiver/G
    // Verifies: EVS-DEV-delivery-receiver/L
    // Verifies: EVS-PRD-ingest/G
    test('delivery 1 is accepted with exactly one accepted-delivery audit '
        'carrying exactly its keys', () async {
      final store = await open();
      final first = sealedRecord();
      final second = sealedRecord();
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[first, second],
      );

      final response = await present(store, delivery);

      expect(
        response,
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );
      for (final record in <Map<String, Object?>>[first, second]) {
        expect(
          await store.reader.findEventById(record['event_id']! as String),
          isNotNull,
          reason: 'every event of an accepted delivery is admitted',
        );
      }
      final audits = await authoredDeliveryAudits(store);
      expect(audits, hasLength(1));
      expect(audits.single.data, <String, Object?>{
        'database_id': store.databaseId,
        'channel': delivery.channel.toJson(),
        'delivery_number': 1,
        'delivery_hash': delivery.deliveryHash,
        'previous_delivery_hash': null,
        'event_ids': <Object?>[first['event_id'], second['event_id']],
        'event_hashes': <Object?>[first['event_hash'], second['event_hash']],
        'attributes': <String, Object?>{},
      });
    });

    // Verifies: EVS-DEV-delivery-receiver/C
    // Verifies: EVS-DEV-delivery-receiver/L
    // Verifies: EVS-PRD-delivery-channel/E
    test('a re-presentation of the last accepted delivery is acknowledged as '
        'represented and appends no event', () async {
      final store = await open();
      final delivery = sealedDelivery();
      await present(store, delivery);
      final before = await store.reader.readSequenceCounter();

      final response = await present(store, delivery);

      expect(
        response,
        acknowledgement(store, delivery, AcknowledgementOutcome.represented),
      );
      expect(
        await store.reader.readSequenceCounter(),
        before,
        reason: 'a re-presentation appends nothing, not even an audit',
      );
    });

    // Verifies: EVS-PRD-delivery-channel/C
    // Verifies: EVS-DEV-delivery-receiver/D
    // Verifies: EVS-DEV-delivery-receiver/M
    // Verifies: EVS-PRD-delivery-channel/E
    test('a gap and a wrong link are each refused out_of_sequence with the '
        'record, appending nothing', () async {
      final store = await open();
      final first = sealedDelivery();
      await present(store, first);
      final before = await store.reader.readSequenceCounter();
      final expectedRefusal = ReceiverRefusal(
        channel: first.channel,
        receiverDatabaseId: store.databaseId,
        record: recordAfter(first),
        refusal: RefusalKind.outOfSequence,
      );

      final gap = sealedDelivery(number: 3, link: first.deliveryHash);
      expect(await present(store, gap), expectedRefusal);
      final wrongLink = sealedDelivery(number: 2, link: 'not-the-last-hash');
      expect(await present(store, wrongLink), expectedRefusal);
      final behind = sealedDelivery();
      expect(
        await present(store, behind),
        expectedRefusal,
        reason: 'another delivery 1 is neither the next nor a re-presentation',
      );

      expect(await store.reader.readSequenceCounter(), before);
      expect(await authoredDeliveryAudits(store), hasLength(1));
    });

    // Verifies: EVS-PRD-delivery-channel/D
    // Verifies: EVS-DEV-delivery-receiver/B
    // Verifies: EVS-DEV-delivery-receiver/I
    test(
      'the record is derived from the log after a reopen, and an '
      'accepted-delivery audit another database authored is ignored',
      () async {
        final db = await database();
        final store = await open(db);
        final first = sealedDelivery();
        await present(store, first);
        final ownAudit = (await authoredDeliveryAudits(store)).single;

        // Another database's audit naming the same channel, far ahead, lands
        // on the same aggregate through a delivery of that database's own.
        final otherSender = deliveryChannel(senderDatabaseId: 'other-receiver');
        final foreignAudit = sealedRecord(
          databaseId: 'other-receiver',
          entryType: kIngestAuditEntryType,
          aggregateType: 'ingest-audit',
          eventType: 'ingest.delivery_accepted',
          aggregateId: ownAudit.aggregateId,
          data: <String, Object?>{
            'database_id': 'other-receiver',
            'channel': first.channel.toJson(),
            'delivery_number': 7,
            'delivery_hash': 'seven',
            'previous_delivery_hash': 'six',
            'event_ids': <Object?>['x'],
            'event_hashes': <Object?>['y'],
            'attributes': <String, Object?>{},
          },
        );
        final carrier = sealedDelivery(
          channel: otherSender,
          records: <Map<String, Object?>>[foreignAudit],
        );
        expect(
          await present(store, carrier),
          isA<ReceiverAcknowledgement>().having(
            (a) => a.outcome,
            'outcome',
            AcknowledgementOutcome.accepted,
          ),
        );
        final held = await store.reader.findEventById(
          foreignAudit['event_id']! as String,
        );
        expect(held, isNotNull, reason: 'the other audit is stored');
        expect(held!.aggregateId, ownAudit.aggregateId);

        await db.stop(store);
        opened.remove(store);
        final reopened = await open(db);
        final second = sealedDelivery(number: 2, link: first.deliveryHash);

        expect(
          await present(reopened, second),
          acknowledgement(reopened, second, AcknowledgementOutcome.accepted),
        );
      },
    );

    // Verifies: EVS-DEV-delivery-receiver/N
    test('a delivery naming a sender the caller may not act for is refused '
        'before anything is written', () async {
      final store = await open();
      final before = await store.reader.readSequenceCounter();

      await expectLater(
        present(store, sealedDelivery(), senders: <String>{'someone-else'}),
        throwsA(
          isA<DeliveryAuthenticationRefused>().having(
            (e) => e.senderDatabaseId,
            'senderDatabaseId',
            kPeerDatabaseId,
          ),
        ),
      );
      await expectLater(
        present(store, sealedDelivery(), senders: const <String>{}),
        throwsA(isA<DeliveryAuthenticationRefused>()),
      );

      expect(await store.reader.readSequenceCounter(), before);
    });

    // Verifies: EVS-DEV-sender-succession/F
    // Verifies: EVS-DEV-delivery-receiver/N
    test('a delivery carrying a succession event the receiver does not hold is '
        'refused when the caller may not act for its predecessor', () async {
      final store = await open();
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[successionRecord()],
      );

      await expectLater(
        present(store, delivery, senders: <String>{kPeerDatabaseId}),
        throwsA(
          isA<DeliveryAuthenticationRefused>().having(
            (e) => e.senderDatabaseId,
            'senderDatabaseId',
            kPredecessorDatabaseId,
          ),
        ),
      );
      expect(await authoredDeliveryAudits(store), isEmpty);
      expect(
        await store.reader.findEventById(
          delivery.events.single['event_id']! as String,
        ),
        isNull,
      );
    });

    // Verifies: EVS-DEV-sender-succession/F
    test('a delivery carrying a succession event the receiver does not hold is '
        'accepted when the caller may act for both the successor and the '
        'predecessor', () async {
      final store = await open();
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[successionRecord()],
      );

      final response = await present(
        store,
        delivery,
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      expect(
        response,
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );
    });

    // Verifies: EVS-DEV-sender-succession/E
    test('a succession event the receiver already holds is a duplicate; no '
        'succession check applies to it', () async {
      final store = await open();
      final succession = successionRecord();
      final first = sealedDelivery(records: <Map<String, Object?>>[succession]);
      await present(
        store,
        first,
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      final second = sealedDelivery(
        number: 2,
        link: first.deliveryHash,
        records: <Map<String, Object?>>[succession, sealedRecord()],
      );
      final response = await present(
        store,
        second,
        senders: <String>{kPeerDatabaseId},
      );

      expect(
        response,
        acknowledgement(store, second, AcknowledgementOutcome.accepted),
      );
    });

    // Verifies: EVS-DEV-sender-succession/K
    test('a succession event names a channel the receiver never accepted a '
        'delivery on: no succession_ahead finding', () async {
      final store = await open();
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[
          successionRecord(predecessorDeliveryNumber: 1),
        ],
      );

      await present(
        store,
        delivery,
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      expect(await authoredFindings(store), isEmpty);
    });

    // Verifies: EVS-DEV-sender-succession/K
    test("a succession event names a delivery at the receiver's record of "
        'a channel: no succession_ahead finding', () async {
      final store = await open();
      final channel = successionPredecessorChannel();
      final first = sealedDelivery(channel: channel);
      final second = sealedDelivery(
        channel: channel,
        number: 2,
        link: first.deliveryHash,
      );
      await present(store, first, senders: <String>{kPredecessorDatabaseId});
      await present(store, second, senders: <String>{kPredecessorDatabaseId});

      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[
          successionRecord(
            predecessorChannel: channel,
            predecessorDeliveryNumber: 2,
            predecessorDeliveryHash: second.deliveryHash,
          ),
        ],
      );
      await present(
        store,
        delivery,
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      expect(await authoredFindings(store), isEmpty);
    });

    // Verifies: EVS-DEV-sender-succession/K
    // Verifies: EVS-DEV-security-findings/F
    // Verifies: EVS-DEV-security-findings/R
    test("a succession event names a delivery above the receiver's record "
        'of a channel: one succession_ahead finding, idempotent on a second '
        'succession event naming the same gap', () async {
      final store = await open();
      final channel = successionPredecessorChannel();
      final first = sealedDelivery(channel: channel);
      final second = sealedDelivery(
        channel: channel,
        number: 2,
        link: first.deliveryHash,
      );
      await present(store, first, senders: <String>{kPredecessorDatabaseId});
      await present(store, second, senders: <String>{kPredecessorDatabaseId});

      final succession = successionRecord(
        predecessorChannel: channel,
        predecessorDeliveryNumber: 3,
        predecessorDeliveryHash: _kPredecessorDeliveryHash,
      );
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[succession],
      );
      final response = await present(
        store,
        delivery,
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      expect(
        response,
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );
      expect(
        await store.reader.findEventById(succession['event_id']! as String),
        isNotNull,
      );

      final evidence = <String, Object?>{
        'channel': channel.toJson(),
        'receiver_record': DeliveryRecord(
          deliveryNumber: 2,
          deliveryHash: second.deliveryHash,
        ).toJson(),
        'succession_record': const DeliveryRecord(
          deliveryNumber: 3,
          deliveryHash: _kPredecessorDeliveryHash,
        ).toJson(),
      };
      final expectedFinding = <String, Object?>{
        'finding_id': expectedFindingId(
          databaseId: store.databaseId,
          role: 'ingest',
          kind: 'succession_ahead',
          evidence: evidence,
        ),
        'kind': 'succession_ahead',
        'evidence': evidence,
        'aggregates': <Object?>[],
        'detector': <String, Object?>{
          'database_id': store.databaseId,
          'role': 'ingest',
          'library_version': LibVersion.version,
        },
      };
      expect(await authoredFindings(store), <Map<String, Object?>>[
        expectedFinding,
      ]);
      // The finding is appended in the transaction that stores the
      //   succession event and its channel's accepted-delivery audit: its
      //   local sequence number immediately precedes the audit's.
      final findingEvent = (await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      )).single;
      final audit = (await authoredDeliveryAudits(store)).lastWhere(
        (a) => DeliveryChannel.fromJson(a.data['channel']) == delivery.channel,
      );
      expect(audit.sequenceNumber, findingEvent.sequenceNumber + 1);

      // A second, distinct succession event naming the same channel and the
      // same gap computes the same finding identity and records nothing
      // more.
      final secondSuccession = successionRecord(
        predecessorChannel: channel,
        predecessorDeliveryNumber: 3,
        predecessorDeliveryHash: _kPredecessorDeliveryHash,
      );
      expect(secondSuccession['event_id'], isNot(succession['event_id']));
      await present(
        store,
        sealedDelivery(
          number: 2,
          link: delivery.deliveryHash,
          records: <Map<String, Object?>>[secondSuccession],
        ),
        senders: <String>{kPeerDatabaseId, kPredecessorDatabaseId},
      );

      expect(await authoredFindings(store), <Map<String, Object?>>[
        expectedFinding,
      ]);
    });

    // Verifies: EVS-DEV-delivery-receiver/W
    // Verifies: EVS-DEV-security-findings/R
    // Verifies: EVS-PRD-delivery-channel/Z
    test(
      'a delivery hash that does not recompute is refused '
      'delivery_hash_mismatch with one finding, the same on a repeat',
      () async {
        final store = await open();
        final sound = sealedDelivery();
        final json = sound.toJson()..['delivery_hash'] = 'not-its-hash';
        final bytes = Uint8List.fromList(utf8.encode(jsonEncode(json)));
        Future<ReceiverResponse> presentBytes() => store.receiverEndpoint
            .accept(bytes, senderDatabaseIds: <String>{kPeerDatabaseId});

        final response = await presentBytes();

        expect(
          response,
          ReceiverRefusal(
            channel: sound.channel,
            receiverDatabaseId: store.databaseId,
            record: DeliveryRecord.none,
            refusal: RefusalKind.deliveryHashMismatch,
          ),
        );
        final evidence = <String, Object?>{
          'channel': sound.channel.toJson(),
          'delivery_number': 1,
          'carried_hash': 'not-its-hash',
          'recomputed_hash': sound.deliveryHash,
        };
        final findings = await authoredFindings(store);
        expect(findings, <Map<String, Object?>>[
          <String, Object?>{
            'finding_id': expectedFindingId(
              databaseId: store.databaseId,
              role: 'ingest',
              kind: 'delivery_hash_mismatch',
              evidence: evidence,
            ),
            'kind': 'delivery_hash_mismatch',
            'evidence': evidence,
            'aggregates': <Object?>[],
            'detector': <String, Object?>{
              'database_id': store.databaseId,
              'role': 'ingest',
              'library_version': LibVersion.version,
            },
          },
        ]);
        expect(
          await store.reader.findEventById(
            sound.events.single['event_id']! as String,
          ),
          isNull,
          reason:
              'the transaction that records the finding writes nothing else',
        );
        expect(await authoredDeliveryAudits(store), isEmpty);

        final counter = await store.reader.readSequenceCounter();
        expect(await presentBytes(), response);
        expect(await authoredFindings(store), findings);
        expect(await store.reader.readSequenceCounter(), counter);
      },
    );

    // Verifies: EVS-DEV-delivery-receiver/T
    // Verifies: EVS-DEV-security-findings/R
    // Verifies: EVS-PRD-delivery-channel/Z
    test('an event the channel sender did not author is stored with one '
        'foreign_event finding', () async {
      final store = await open();
      final own = sealedRecord();
      final foreign = sealedRecord(databaseId: 'third-party-db');
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[own, foreign],
      );

      expect(
        await present(store, delivery),
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );

      expect(
        await store.reader.findEventById(foreign['event_id']! as String),
        isNotNull,
        reason: 'the foreign event is stored as received',
      );
      final evidence = <String, Object?>{
        'channel': delivery.channel.toJson(),
        'delivery_number': 1,
        'event_id': foreign['event_id'],
        'sealed_hash': foreign['event_hash'],
      };
      expect(await authoredFindings(store), <Map<String, Object?>>[
        <String, Object?>{
          'finding_id': expectedFindingId(
            databaseId: store.databaseId,
            role: 'ingest',
            kind: 'foreign_event',
            evidence: evidence,
          ),
          'kind': 'foreign_event',
          'evidence': evidence,
          'aggregates': <Object?>[foreign['aggregate_id']],
          'detector': <String, Object?>{
            'database_id': store.databaseId,
            'role': 'ingest',
            'library_version': LibVersion.version,
          },
        },
      ]);
    });

    // Verifies: EVS-DEV-delivery-receiver/X
    test('attributes the receiver does not know are kept as carried in the '
        'audit and recompute to the delivery hash', () async {
      final store = await open();
      final delivery = sealedDelivery(
        attributes: <String, Object?>{
          'later_fact': 1.5e3,
          'ratio': 0.1,
          'huge': 1e300,
          'nested': <String, Object?>{
            'list': <Object?>[1, 'two', null, true],
          },
        },
      );
      // As the receiver decodes it from the wire.
      final carried = DeliveryEnvelope.decode(delivery.encode());

      expect(
        await present(store, delivery),
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );

      final audit = (await authoredDeliveryAudits(store)).single.data;
      expect(audit['attributes'], carried.attributes);
      expect(
        computeDeliveryHash(
          channel: DeliveryChannel.fromJson(audit['channel']),
          deliveryNumber: audit['delivery_number']! as int,
          previousDeliveryHash: audit['previous_delivery_hash'] as String?,
          eventHashes: audit['event_hashes']! as List<Object?>,
          attributes: (audit['attributes']! as Map).cast<String, Object?>(),
        ),
        delivery.deliveryHash,
        reason: 'the audit keeps every hashed field as carried',
      );
    });

    // Verifies: EVS-DEV-delivery-receiver/H
    test('each event ingested from a delivery carries the delivery in its '
        'receiver provenance entry', () async {
      final store = await open();
      final first = sealedDelivery();
      await present(store, first);
      final record = sealedRecord();
      final second = sealedDelivery(
        number: 2,
        link: first.deliveryHash,
        records: <Map<String, Object?>>[record],
      );
      await present(store, second);

      final stored = await store.reader.findEventById(
        record['event_id']! as String,
      );
      final entry = ProvenanceEntry.fromJson(
        ((stored!.metadata['provenance']! as List).last as Map)
            .cast<String, Object?>(),
      );
      expect(entry.databaseId, store.databaseId);
      expect(entry.delivery?.toJson(), <String, Object?>{
        'channel': second.channel.toJson(),
        'delivery_number': 2,
      });
    });

    // Verifies: EVS-DEV-delivery-receiver/M
    // Verifies: EVS-PRD-delivery-channel/E
    test('a batch that does not decode as a delivery is refused rejected '
        'naming the reason and the record, appending nothing', () async {
      final store = await open();
      final first = sealedDelivery();
      await present(store, first);
      final before = await store.reader.readSequenceCounter();
      final json = sealedDelivery(number: 2, link: first.deliveryHash).toJson()
        ..['events'] = <Object?>[];

      final response = await store.receiverEndpoint.accept(
        Uint8List.fromList(utf8.encode(jsonEncode(json))),
        senderDatabaseIds: <String>{kPeerDatabaseId},
      );

      expect(
        response,
        ReceiverRefusal(
          channel: first.channel,
          receiverDatabaseId: store.databaseId,
          record: recordAfter(first),
          refusal: RefusalKind.rejected,
          reason: IngestDecodeFailure.noEvents,
        ),
      );
      expect(await store.reader.readSequenceCounter(), before);
    });

    // Verifies: EVS-DEV-delivery-receiver/M
    test('an event of another data-format major is refused rejected naming '
        'the event, appending nothing', () async {
      final store = await open();
      final other = sealedRecord();
      other['lib_format_version'] = const DataFormatVersion(9, 0).toJson();
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[sealedRecord(), other],
      );

      final response = await present(store, delivery);

      expect(
        response,
        isA<ReceiverRefusal>()
            .having((r) => r.refusal, 'refusal', RefusalKind.rejected)
            .having((r) => r.record, 'record', DeliveryRecord.none)
            .having(
              (r) => r.refusedEventId,
              'refusedEventId',
              other['event_id'],
            )
            .having((r) => r.reason, 'reason', isNotNull),
      );
      expect(await authoredDeliveryAudits(store), isEmpty);
      expect(
        await store.reader.findEventById(
          delivery.events.first['event_id']! as String,
        ),
        isNull,
        reason: 'the refusal rolls back the whole delivery',
      );
    });
  });
}
