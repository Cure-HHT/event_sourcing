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
import 'package:event_sourcing/src/projections/integrity_marks.dart'
    show integrityFindingIdsOf;
import 'package:event_sourcing/src/projections/view_fingerprint.dart'
    show viewFingerprint;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kIngestAuditEntryType, kIngestDuplicateReceivedEventType;
import 'package:event_sourcing/src/storage/chain_coordinates.dart'
    show ChainCoordinates;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart'
    show DeliveryTestHooks, runWithDeliveryTestHooks;
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'ingest_record_findings_conformance.dart' show sealedRecord;
import 'manual_timers.dart' show neverFiringTimer;
import 'record_fixtures.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

/// The entry type of the records the scenarios deliver.
const String kDeliveryNoteType = 'finding_note';

/// A table view keyed on `data.k`, so a delivered record whose data has no
/// `k` throws from the fold rather than being stored under a substitute
/// key: a fold failure leaves that copy behind without aborting the
/// delivery (`EVS-PRD-ingest/G`).
const String _kKeyedTableView = 'keyed_delivery_notes';

const TableProjectionSpec _kKeyedTableSpec = TableProjectionSpec(
  viewName: _kKeyedTableView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: CompositeKey(<String>['data.k']),
  rowData: WholePayload(),
);

/// An aggregate view of the same entry type as [_kKeyedTableSpec], keyed by
/// aggregate id rather than a data field: it folds every one of the
/// scenarios' records without a fold failure, so it demonstrates that the
/// aggregate a `fold_failed` finding names carries the outstanding-finding
/// mark in every default view, not only the one whose copy failed
/// (`EVS-PRD-materializer/D`).
const String _kAggregateNotesView = 'aggregate_delivery_notes';

const AggregateProjectionSpec _kAggregateNotesSpec = AggregateProjectionSpec(
  viewName: _kAggregateNotesView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  tombstoneEventTypes: <String>{},
);

/// A view whose interest includes the receiver's own `ingest.delivery_accepted`
/// audit, so an accepted delivery's raw audit must itself fold into it for
/// the view to stay current (`EVS-DEV-view-convergence/E`).
const String _kAuditWatchingView = 'audit_watching_notes';

const AggregateProjectionSpec _kAuditWatchingSpec = AggregateProjectionSpec(
  viewName: _kAuditWatchingView,
  interest: SubscriptionFilter(
    includeSystemEvents: true,
    eventTypes: <String>{'ingest.delivery_accepted'},
  ),
  tombstoneEventTypes: <String>{},
);

/// A view whose interest includes the receiver's own `ingest.duplicate_received`
/// audit, so a re-presented event's raw audit must itself fold into it for
/// the view to stay current (`EVS-DEV-view-convergence/E`).
const String _kDuplicateAuditWatchingView = 'duplicate_audit_watching_notes';

const AggregateProjectionSpec _kDuplicateAuditWatchingSpec =
    AggregateProjectionSpec(
      viewName: _kDuplicateAuditWatchingView,
      interest: SubscriptionFilter(
        includeSystemEvents: true,
        eventTypes: <String>{kIngestDuplicateReceivedEventType},
      ),
      tombstoneEventTypes: <String>{},
    );

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
/// scenarios' entry type and, when given, [projections].
Future<EventStore> openReceiverStore(
  VersionTestDatabase db, {
  Source source = _receiverSource,
  ProjectionRegistry? projections,
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
    projections: projections,
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

    Future<EventStore> open([
      VersionTestDatabase? db,
      ProjectionRegistry? projections,
    ]) async {
      final store = await openReceiverStore(
        db ?? await database(),
        projections: projections,
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

    // Verifies: EVS-DEV-delivery-receiver/Y
    test('attributes carrying U+0000, in a value and in a key, are stored '
        'in the audit as the base64 of their canonical JSON, decoding back '
        'to what was sent', () async {
      final store = await open();
      final delivery = sealedDelivery(
        attributes: <String, Object?>{'note': 'x\u0000y', 'x\u0000y': 'value'},
      );
      final carried = DeliveryEnvelope.decode(delivery.encode());

      expect(
        await present(store, delivery),
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
      );

      expect(
        await store.reader.findEventById(
          carried.events.single['event_id']! as String,
        ),
        isNotNull,
        reason: "the delivery's event is stored",
      );

      final audit = (await authoredDeliveryAudits(store)).single.data;
      expect(
        audit['attributes'],
        isA<String>(),
        reason: 'attributes carrying U+0000 are not stored verbatim',
      );
      final decodedAttributes =
          jsonDecode(utf8.decode(base64.decode(audit['attributes']! as String)))
              as Map<String, Object?>;
      expect(decodedAttributes, carried.attributes);
      expect(
        computeDeliveryHash(
          channel: DeliveryChannel.fromJson(audit['channel']),
          deliveryNumber: audit['delivery_number']! as int,
          previousDeliveryHash: audit['previous_delivery_hash'] as String?,
          eventHashes: audit['event_hashes']! as List<Object?>,
          attributes: decodedAttributes,
        ),
        delivery.deliveryHash,
        reason: 'the encoded attributes still recompute the delivery hash',
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

    // Verifies: EVS-PRD-ingest/G
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-DEV-view-convergence/F
    // Verifies: EVS-PRD-materializer/I
    // Verifies: EVS-DEV-security-findings/S
    test('a delivered event a table fold cannot key is passed over; the '
        'delivery is accepted whole, the copy stays current and one '
        'fold_failed finding is recorded', () async {
      final registry = ProjectionRegistry()
        ..register(_kKeyedTableSpec)
        ..register(_kAggregateNotesSpec);
      final store = await open(null, registry);
      final ok1 = sealedRecord(data: <String, Object?>{'k': 'x'});
      final bad = sealedRecord(data: <String, Object?>{'title': 'no key'});
      final ok2 = sealedRecord(data: <String, Object?>{'k': 'y'});
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[ok1, bad, ok2],
      );

      final response = await present(store, delivery);

      expect(
        response,
        acknowledgement(store, delivery, AcknowledgementOutcome.accepted),
        reason: 'a fold failure on one event never rolls back the delivery',
      );
      for (final record in <Map<String, Object?>>[ok1, bad, ok2]) {
        expect(
          await store.reader.findEventById(record['event_id']! as String),
          isNotNull,
          reason:
              'every event of an accepted delivery is stored, whatever '
              'a view fold makes of it',
        );
      }
      final badEvent = (await store.reader.findEventById(
        bad['event_id']! as String,
      ))!;
      final ok2Event = (await store.reader.findEventById(
        ok2['event_id']! as String,
      ))!;

      // The copy passes over the event whose fold failed, in the same
      // storing transaction as the delivery: its watermark reaches the
      // delivery's last event and it reads current at once, with no wait
      // on catch-up.
      final progress = (await store.reader.viewProgress()).singleWhere(
        (p) => p.viewName == _kKeyedTableView,
      );
      expect(
        progress.watermark,
        greaterThanOrEqualTo(ok2Event.sequenceNumber),
        reason:
            'the copy passes over the failed event and keeps folding the '
            "delivery's later events, so its watermark reaches at least "
            "the delivery's last event, in the storing transaction itself",
      );
      expect(
        progress.state,
        ViewConvergenceState.current,
        reason: 'a copy that passes over a failed fold stays current',
      );

      final rows = await store.reader.findViewRows(_kKeyedTableView);
      final keys = rows.rows.map((r) => r['k']).toList();
      expect(
        keys,
        unorderedEquals(<String>['x', 'y']),
        reason: 'no row is written for the event whose fold failed',
      );

      final findingEvents = await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      );
      expect(findingEvents, hasLength(1));
      expect(
        findingEvents.single.sequenceNumber,
        allOf(
          greaterThan(badEvent.sequenceNumber),
          lessThan(ok2Event.sequenceNumber),
        ),
        reason:
            'the finding is recorded in the same storing transaction as '
            'the delivery, right after the event whose fold failed and '
            'before the next event of the delivery',
      );
      final finding = findingEvents.single.data;
      expect(finding['kind'], 'fold_failed');
      final expectedFingerprint = viewFingerprint(
        _kKeyedTableSpec,
        store.entryTypes,
        store.promoters,
      );
      expect(finding['evidence'], <String, Object?>{
        'view': _kKeyedTableView,
        'definition_fingerprint': expectedFingerprint,
        'event_id': badEvent.eventId,
        'sealed_hash': ChainCoordinates.of(badEvent).sealedHash,
        'reason': 'row_key_failed',
      });
      expect((finding['detector']! as Map<String, Object?>)['role'], 'fold');
      expect(finding['aggregates'], <String>[badEvent.aggregateId]);

      // The aggregate the finding names carries the outstanding-finding
      // mark in every default view, not only the one whose copy failed
      // (EVS-PRD-materializer/D): the aggregate view, whose fold of bad's
      // event does not fail, still marks its row.
      final badRow = await store.reader.transaction(
        (txn) => store.reader.readViewRowInTxn(
          txn,
          _kAggregateNotesView,
          badEvent.aggregateId,
        ),
      );
      expect(badRow.row, isA<SettledRow>());
      final markedRow = (badRow.row as SettledRow).data;
      expect(integrityFindingIdsOf(markedRow), <String>[
        findingEvents.single.aggregateId,
      ]);
    });

    // Verifies: EVS-DEV-security-findings/T
    // Verifies: EVS-DEV-security-findings/S
    test('a view whose fold cannot key a security finding event records one '
        'fold_failed finding about the delivered event and none about the '
        'fold_failed finding itself', () async {
      // Keyed on `data.k` and interested in both the delivered entry
      // type and security findings, so once the delivered event's fold
      // fails and the library appends a fold_failed finding, this same
      // copy tries to key that finding event too -- and cannot, since a
      // finding's data has no `k`.
      const view = 'keyed_notes_and_findings';
      const spec = TableProjectionSpec(
        viewName: view,
        interest: SubscriptionFilter(
          entryTypes: <String>{kDeliveryNoteType, kSecurityFindingEntryType},
          includeSystemEvents: true,
        ),
        insertEventTypes: <String>{
          'finalized',
          kSecurityFindingRecordedEventType,
        },
        removeEventTypes: <String>{},
        rowKey: CompositeKey(<String>['data.k']),
        rowData: WholePayload(),
      );
      final begins = <String>[];
      final bad = sealedRecord(data: <String, Object?>{'title': 'no key'});
      final delivery = sealedDelivery(records: <Map<String, Object?>>[bad]);
      late EventStore store;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: neverFiringTimer,
          onCatchUpTransactionBegin: begins.add,
        ),
        () async {
          final registry = ProjectionRegistry()..register(spec);
          store = await open(null, registry);
          // The view's initial catch-up pass (over the empty log at open)
          // must settle before the delivery below, or the copy reads
          // converging and the delivered event's fold is left to a later
          // catch-up instead of failing inline, in this same transaction.
          await _waitUntilFirstCatchUpPassSettles(store, begins, <String>[
            view,
          ]);

          await present(store, delivery);
        },
      );

      final badEvent = (await store.reader.findEventById(
        bad['event_id']! as String,
      ))!;
      final findingEvents = await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      );
      expect(
        findingEvents,
        hasLength(1),
        reason:
            'exactly the one fold_failed finding about the delivered '
            'event; the fold failure of the finding event itself, in '
            'the same copy, is passed over with nothing recorded '
            '(EVS-DEV-security-findings/T)',
      );
      expect(findingEvents.single.data['kind'], 'fold_failed');
      expect(
        findingEvents.single.data['evidence'],
        containsPair('event_id', badEvent.eventId),
      );

      final progress = (await store.reader.viewProgress()).singleWhere(
        (p) => p.viewName == view,
      );
      expect(
        progress.watermark,
        greaterThanOrEqualTo(findingEvents.single.sequenceNumber),
        reason:
            'the copy passes over both the delivered event and the '
            'finding event, staying current through the finding append',
      );
      expect(progress.state, ViewConvergenceState.current);

      // Verifies: EVS-DEV-security-findings/E
      // Re-presenting the delivery is a no-op the receiver recognizes
      // from its record before anything is re-folded, so no second
      // fold_failed finding of the same identity is ever attempted.
      await present(store, delivery);
      expect(
        await store.reader.findAllEvents(entryType: kSecurityFindingEntryType),
        hasLength(1),
        reason: 're-presenting the same delivery records no second finding',
      );
    });

    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-ingest/G
    test('a view whose interest includes ingest.delivery_accepted stays '
        'current after an accepted delivery', () async {
      // The catch-up driver's own re-checks run on a timer that never
      // fires here, so once its first pass (scheduled at open, over an
      // empty log) commits, it can never run a second one: a "current"
      // reading right after present() can only be the delivery's own
      // storing transaction, not a catch-up pass this test cannot rule
      // out otherwise. The first pass's own transaction is awaited by its
      // begin/commit signals below, so it is provably finished before the
      // delivery is presented.
      final begins = <String>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: neverFiringTimer,
          onCatchUpTransactionBegin: begins.add,
        ),
        () async {
          final registry = ProjectionRegistry()..register(_kAuditWatchingSpec);
          final store = await open(null, registry);
          await _waitUntilFirstCatchUpPassSettles(store, begins, <String>[
            _kAuditWatchingView,
          ]);
          final delivery = sealedDelivery();

          await present(store, delivery);

          final audit = (await authoredDeliveryAudits(store)).single;
          final progress = (await store.reader.viewProgress()).singleWhere(
            (p) => p.viewName == _kAuditWatchingView,
          );
          expect(
            progress.watermark,
            audit.sequenceNumber,
            reason:
                'the raw delivery_accepted audit folds in the same '
                'transaction that appends it, like any other stored event -- '
                'a watermark a blocked catch-up driver could not have '
                'produced on its own',
          );
          expect(progress.state, ViewConvergenceState.current);
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-PRD-ingest/G
    // Verifies: EVS-PRD-ingest/F
    test('a view whose interest includes ingest.duplicate_received stays '
        'current after a duplicate delivery', () async {
      final begins = <String>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: neverFiringTimer,
          onCatchUpTransactionBegin: begins.add,
        ),
        () async {
          final registry = ProjectionRegistry()
            ..register(_kDuplicateAuditWatchingSpec);
          final store = await open(null, registry);
          await _waitUntilFirstCatchUpPassSettles(store, begins, <String>[
            _kDuplicateAuditWatchingView,
          ]);
          final record = sealedRecord();
          final first = sealedDelivery(records: <Map<String, Object?>>[record]);
          await present(store, first);
          final second = sealedDelivery(
            number: 2,
            link: first.deliveryHash,
            records: <Map<String, Object?>>[record],
          );

          await present(store, second);

          // The delivery's own storing transaction appends the
          // duplicate_received audit and then the delivery's own
          // delivery_accepted audit; a copy that folded the duplicate audit
          // inline stays current and so its watermark reaches the
          // transaction's last event (the second delivery_accepted audit)
          // too -- a copy that missed the duplicate audit is stuck
          // converging at the duplicate audit's own position, and the later
          // fold skips it, since it is no longer current.
          final tip = await store.reader.readSequenceCounter();
          final progress = (await store.reader.viewProgress()).singleWhere(
            (p) => p.viewName == _kDuplicateAuditWatchingView,
          );
          expect(
            progress.watermark,
            tip,
            reason:
                'the raw duplicate_received audit folds in the same '
                'transaction that appends it, like any other stored event -- '
                'a watermark a blocked catch-up driver could not have '
                'produced on its own',
          );
          expect(progress.state, ViewConvergenceState.current);
        },
      );
    });
  });
}

/// Waits for the catch-up driver's own first pass -- scheduled at open,
/// over the log as it stood then -- to have begun and committed at least
/// one transaction (observed through [begins], the `onCatchUpTransactionBegin`
/// test seam), then for every view named in [viewNames] to read current.
/// Presenting a delivery only after this returns, with the driver's
/// `timerFactory` a seam that never fires again, means the driver cannot
/// run a second pass afterward: a "current" reading right after present()
/// can only be the delivery's own storing transaction. A view already
/// current before any pass ever runs (an empty log needs no catch-up)
/// would otherwise let a test move on before the driver's first pass
/// commits, which is why this waits on [begins] rather than on the
/// view's progress alone.
Future<void> _waitUntilFirstCatchUpPassSettles(
  EventStore store,
  List<String> begins,
  List<String> viewNames,
) async {
  for (var i = 0; i < 400; i++) {
    if (begins.isNotEmpty) break;
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  if (begins.isEmpty) {
    fail('the catch-up driver never began a pass before the timeout');
  }
  // The observed pass is over the log as it stood at open -- no events --
  // so its own transaction commits almost at once; this margin lets it
  // finish (and the driver settle into its now-permanent idle wait)
  // before the delivery below is presented.
  await Future<void>.delayed(const Duration(milliseconds: 50));
  for (var i = 0; i < 400; i++) {
    final progress = await store.reader.viewProgress();
    final current = <String, bool>{
      for (final name in viewNames)
        name:
            progress.singleWhere((p) => p.viewName == name).state ==
            ViewConvergenceState.current,
    };
    if (current.values.every((c) => c)) return;
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  fail('views $viewNames never reached "current" before the timeout');
}
