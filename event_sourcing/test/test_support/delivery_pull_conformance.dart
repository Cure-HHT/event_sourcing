// Backend-agnostic scenarios for the receiver endpoint's pull: a range of a
// channel's deliveries is served from the receiver's log (the
// `ingest.delivery_accepted` audit of each delivery and the records of the
// events it lists, in its order), and a channel listing names every channel
// of every generation the log records for a sender, each with the
// receiver's record. Run on Sembast by test/ingest/delivery_pull_sembast_test.dart
// and on Postgres by test/storage/postgres/delivery_pull_postgres_test.dart.
//
// This file exposes [runDeliveryPullScenarios] and registers no `main()` of
// its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionChannel, SenderSuccessionData;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kIngestAuditEntryType;
import 'package:flutter_test/flutter_test.dart';

import 'delivery_receiver_conformance.dart';
import 'ingest_record_findings_conformance.dart'
    show asReceived, resealed, sealedRecord;
import 'record_fixtures.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

/// The hash [record] carried when the receiver [receiverDatabaseId] was
/// presented it: for a stored copy, the arrival hash of the receiver's own
/// entry, its last; for a record served as received (from a finding's
/// evidence), its own `event_hash`.
///
/// Derived from the served record alone, independently of the receiver's
/// audit, so a recompute through it checks what the pull served.
Object? carriedHashOf(Map<String, Object?> record, String receiverDatabaseId) {
  final provenance = (record['metadata']! as Map)['provenance']! as List;
  final last = provenance.last as Map;
  if (provenance.length > 1 && last['database_id'] == receiverDatabaseId) {
    return last['arrival_hash'];
  }
  return record['event_hash'];
}

/// The delivery hash [served], a delivery of [channel] a receiver
/// [receiverDatabaseId] served, recomputes to from its served content.
String recomputedHashOf(
  ServedDelivery served,
  DeliveryChannel channel,
  String receiverDatabaseId,
) => computeDeliveryHash(
  channel: channel,
  deliveryNumber: served.deliveryNumber,
  previousDeliveryHash: served.previousDeliveryHash,
  eventHashes: <Object?>[
    for (final e in served.events) carriedHashOf(e, receiverDatabaseId),
  ],
  attributes: served.attributes,
);

/// Pulls [request] from [store]'s endpoint as a caller that may act for
/// [senders] (by default the sender the request names), and decodes the
/// answer through the library's pull-response decoder, as a destination's
/// pull operation reads it.
Future<PullResponse> pullThroughDecoder(
  EventStore store,
  PullRequest request, {
  Set<String>? senders,
}) async {
  final sender = switch (request) {
    ChannelListingPull(:final senderDatabaseId) => senderDatabaseId,
    DeliveryRangePull(:final channel) => channel.senderDatabaseId,
  };
  final response = await store.receiverEndpoint.pull(
    request,
    senderDatabaseIds: senders ?? <String>{sender},
  );
  final outcome = decodePullResponse(response.encode());
  expect(outcome, isA<PullServed>());
  return (outcome as PullServed).response;
}

/// Runs the scenarios. [openDatabase] returns a fresh database; [skip]
/// skips the group when set.
void runDeliveryPullScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('receiver pull and channel listing ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open() async {
      final db = (await openDatabase())!;
      databases.add(db);
      final store = await openReceiverStore(db);
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

    Future<void> accept(
      EventStore store,
      DeliveryEnvelope delivery, {
      Set<String>? senders,
    }) async {
      final response = await store.receiverEndpoint.accept(
        delivery.encode(),
        senderDatabaseIds:
            senders ?? <String>{delivery.channel.senderDatabaseId},
      );
      expect(
        response,
        isA<ReceiverAcknowledgement>().having(
          (a) => a.outcome,
          'outcome',
          AcknowledgementOutcome.accepted,
        ),
      );
    }

    /// Accepts [count] deliveries on [channel], each carrying one fresh
    /// record, and returns them.
    Future<List<DeliveryEnvelope>> acceptRun(
      EventStore store,
      DeliveryChannel channel,
      int count,
    ) async {
      final run = <DeliveryEnvelope>[];
      for (var n = 1; n <= count; n++) {
        final d = sealedDelivery(
          channel: channel,
          number: n,
          link: run.isEmpty ? null : run.last.deliveryHash,
          records: <Map<String, Object?>>[
            sealedRecord(databaseId: channel.senderDatabaseId),
          ],
        );
        await accept(store, d);
        run.add(d);
      }
      return run;
    }

    // Verifies: EVS-DEV-delivery-receiver/O
    // Verifies: EVS-DEV-delivery-receiver/X
    // Verifies: EVS-PRD-delivery-channel/P
    test('each pulled delivery recomputes to its hash from the served '
        'events, in its audit order, with its link and attributes as '
        'carried', () async {
      final store = await open();
      final a = sealedRecord();
      final b = sealedRecord();
      final c = sealedRecord();
      final d = sealedRecord();
      final first = sealedDelivery(records: <Map<String, Object?>>[a, b]);
      // The second carries a new event before one the receiver already
      // holds, so its audit order is not the log's order, and carries
      // attributes the receiver does not know.
      final second = sealedDelivery(
        number: 2,
        link: first.deliveryHash,
        records: <Map<String, Object?>>[c, a],
        attributes: <String, Object?>{
          'later_fact': 1.5e3,
          'ratio': 0.1,
          'nested': <String, Object?>{
            'list': <Object?>[1, 'two', null, true],
          },
        },
      );
      final third = sealedDelivery(
        number: 3,
        link: second.deliveryHash,
        records: <Map<String, Object?>>[d],
      );
      final sent = <DeliveryEnvelope>[first, second, third];
      for (final delivery in sent) {
        await accept(store, delivery);
      }

      final range =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: first.channel,
                  fromDeliveryNumber: 1,
                  toDeliveryNumber: 3,
                ),
              )
              as DeliveryRange;

      expect(range.receiverDatabaseId, store.databaseId);
      expect(range.channel, first.channel);
      expect(range.record, recordAfter(third));
      expect(range.unservableDeliveryNumber, isNull);
      expect(range.deliveries, hasLength(3));
      for (var i = 0; i < sent.length; i++) {
        final served = range.deliveries[i];
        final carried = DeliveryEnvelope.decode(sent[i].encode());
        expect(served.deliveryNumber, carried.deliveryNumber);
        expect(served.previousDeliveryHash, carried.previousDeliveryHash);
        expect(served.deliveryHash, carried.deliveryHash);
        expect(served.attributes, carried.attributes);
        expect(
          <Object?>[for (final e in served.events) e['event_id']],
          <Object?>[for (final e in carried.events) e['event_id']],
          reason: 'delivery ${i + 1} serves its events in its audit order',
        );
        for (final e in served.events) {
          final stored = await store.reader.findEventById(
            e['event_id']! as String,
          );
          expect(
            e,
            asReceived(stored!.toMap()),
            reason: 'a held event is served as the receiver stores it',
          );
        }
        expect(
          recomputedHashOf(served, first.channel, store.databaseId),
          carried.deliveryHash,
          reason: 'delivery ${i + 1} recomputes from what the pull served',
        );
      }
    });

    // Verifies: EVS-DEV-delivery-receiver/Y
    test('a delivery whose attributes carry U+0000 is served with its '
        'original attributes object and a hash that recomputes', () async {
      final store = await open();
      final delivery = sealedDelivery(
        attributes: <String, Object?>{'note': 'x\u0000y', 'x\u0000y': 'value'},
      );
      final carried = DeliveryEnvelope.decode(delivery.encode());
      await accept(store, delivery);

      final range =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: delivery.channel,
                  fromDeliveryNumber: 1,
                  toDeliveryNumber: 1,
                ),
              )
              as DeliveryRange;

      final served = range.deliveries.single;
      expect(served.attributes, carried.attributes);
      expect(
        recomputedHashOf(served, delivery.channel, store.databaseId),
        delivery.deliveryHash,
        reason:
            'the delivery hash recomputes from the decoded attributes '
            'the pull serves',
      );
    });

    // Verifies: EVS-DEV-delivery-receiver/P
    test('a delivery above the record cannot be served, and is named, after '
        'the deliveries below it', () async {
      final store = await open();
      final run = await acceptRun(store, deliveryChannel(), 3);

      final range =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: run.first.channel,
                  fromDeliveryNumber: 2,
                  toDeliveryNumber: 5,
                ),
              )
              as DeliveryRange;

      expect(range.record, recordAfter(run.last));
      expect(
        <int>[for (final d in range.deliveries) d.deliveryNumber],
        <int>[2, 3],
      );
      expect(range.unservableDeliveryNumber, 4);

      final unseen = deliveryChannel(registrationId: 'never-delivered');
      final none =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: unseen,
                  fromDeliveryNumber: 1,
                  toDeliveryNumber: 1,
                ),
              )
              as DeliveryRange;
      expect(none.record, DeliveryRecord.none);
      expect(none.deliveries, isEmpty);
      expect(none.unservableDeliveryNumber, 1);
    });

    // Verifies: EVS-DEV-delivery-receiver/O
    test('an event kept only in an event_malformed finding is served from '
        'the finding', () async {
      final store = await open();
      final good = sealedRecord();
      final malformed = sealedRecord(
        data: <String, Object?>{r'$integrity': 'forged', 'title': 't'},
      );
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[good, malformed],
      );
      await accept(store, delivery);
      expect(
        await store.reader.findEventById(malformed['event_id']! as String),
        isNull,
        reason: 'the malformed record is held only in the finding',
      );

      final range =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: delivery.channel,
                  fromDeliveryNumber: 1,
                  toDeliveryNumber: 1,
                ),
              )
              as DeliveryRange;

      final served = range.deliveries.single;
      expect(served.events.last, asReceived(malformed));
      expect(
        recomputedHashOf(served, delivery.channel, store.databaseId),
        delivery.deliveryHash,
      );
    });

    // Verifies: EVS-DEV-delivery-receiver/O
    test('an event whose identifier the receiver holds under another hash is '
        'served from its identity_mismatch finding', () async {
      final store = await open();
      final held = sealedRecord();
      final first = sealedDelivery(records: <Map<String, Object?>>[held]);
      await accept(store, first);
      final other = resealed(held, const <String, Object?>{
        'data': <String, Object?>{'title': 'another event, same identifier'},
      });
      final second = sealedDelivery(
        number: 2,
        link: first.deliveryHash,
        records: <Map<String, Object?>>[other],
      );
      await accept(store, second);

      final range =
          await pullThroughDecoder(
                store,
                DeliveryRangePull(
                  channel: first.channel,
                  fromDeliveryNumber: 1,
                  toDeliveryNumber: 2,
                ),
              )
              as DeliveryRange;

      expect(range.deliveries.last.events.single, asReceived(other));
      for (var i = 0; i < 2; i++) {
        expect(
          recomputedHashOf(
            range.deliveries[i],
            first.channel,
            store.databaseId,
          ),
          <DeliveryEnvelope>[first, second][i].deliveryHash,
        );
      }
    });

    // Verifies: EVS-DEV-delivery-receiver/R
    // Verifies: EVS-PRD-delivery-channel/P
    test('the listing names every channel of every generation the log '
        'records for the sender, each with its record', () async {
      final store = await open();
      final gen1 = await acceptRun(store, deliveryChannel(), 2);
      final gen2 = await acceptRun(store, deliveryChannel(generation: 2), 1);
      final elsewhere = await acceptRun(
        store,
        deliveryChannel(destinationId: 'archive'),
        1,
      );
      await acceptRun(
        store,
        deliveryChannel(senderDatabaseId: 'another-sender'),
        2,
      );
      // Another database's audit naming a channel of the sender that this
      // receiver never accepted on is not a channel the log records.
      final foreignAudit = sealedRecord(
        databaseId: 'other-receiver',
        entryType: kIngestAuditEntryType,
        aggregateType: 'ingest-audit',
        eventType: 'ingest.delivery_accepted',
        data: <String, Object?>{
          'database_id': 'other-receiver',
          'channel': deliveryChannel(registrationId: 'registration-9').toJson(),
          'delivery_number': 4,
          'delivery_hash': 'four',
          'previous_delivery_hash': 'three',
          'event_ids': <Object?>['x'],
          'event_hashes': <Object?>['y'],
          'attributes': <String, Object?>{},
        },
      );
      await accept(
        store,
        sealedDelivery(
          channel: deliveryChannel(senderDatabaseId: 'other-receiver'),
          records: <Map<String, Object?>>[foreignAudit],
        ),
      );

      final listing = await pullThroughDecoder(
        store,
        const ChannelListingPull(senderDatabaseId: kPeerDatabaseId),
      );

      expect(
        listing,
        ChannelListing(
          receiverDatabaseId: store.databaseId,
          senderDatabaseId: kPeerDatabaseId,
          channels: <ListedChannel>[
            ListedChannel(
              channel: elsewhere.last.channel,
              record: recordAfter(elsewhere.last),
            ),
            ListedChannel(
              channel: gen1.last.channel,
              record: recordAfter(gen1.last),
            ),
            ListedChannel(
              channel: gen2.last.channel,
              record: recordAfter(gen2.last),
            ),
          ],
        ),
      );

      expect(
        await pullThroughDecoder(
          store,
          const ChannelListingPull(senderDatabaseId: 'unknown-sender'),
        ),
        ChannelListing(
          receiverDatabaseId: store.databaseId,
          senderDatabaseId: 'unknown-sender',
          channels: const <ListedChannel>[],
        ),
      );
    });

    // Verifies: EVS-DEV-delivery-receiver/R
    // Verifies: EVS-PRD-delivery-channel/P
    test(
      "the listing for a successor includes its predecessor's channels",
      () async {
        final store = await open();
        const predecessorId = 'predecessor-db';
        const successorId = 'successor-db';
        final predecessorChannel = deliveryChannel(
          senderDatabaseId: predecessorId,
        );
        final predecessorRun = await acceptRun(store, predecessorChannel, 2);

        final successionData = SenderSuccessionData(
          id: predecessorChannel.destinationId,
          registrationId: predecessorChannel.registrationId,
          databaseId: successorId,
          predecessorDatabaseId: predecessorId,
          predecessorChannels: <SenderSuccessionChannel>[
            SenderSuccessionChannel(
              channel: predecessorChannel,
              deliveryNumber: predecessorRun.last.deliveryNumber,
              deliveryHash: predecessorRun.last.deliveryHash,
            ),
          ],
        );
        final successionRecord = sealedRecord(
          databaseId: successorId,
          entryType: kDestinationSenderSucceededEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationSenderSucceededEventType,
          data: successionData.toJson(),
        );
        final successorChannel = deliveryChannel(senderDatabaseId: successorId);
        final successionDelivery = sealedDelivery(
          channel: successorChannel,
          records: <Map<String, Object?>>[successionRecord],
        );
        await accept(
          store,
          successionDelivery,
          senders: <String>{successorId, predecessorId},
        );

        final listing = await pullThroughDecoder(
          store,
          const ChannelListingPull(senderDatabaseId: successorId),
        );

        expect(
          listing,
          ChannelListing(
            receiverDatabaseId: store.databaseId,
            senderDatabaseId: successorId,
            channels: <ListedChannel>[
              ListedChannel(
                channel: predecessorChannel,
                record: recordAfter(predecessorRun.last),
              ),
              ListedChannel(
                channel: successorChannel,
                record: recordAfter(successionDelivery),
              ),
            ],
          ),
        );
      },
    );

    // Verifies: EVS-DEV-delivery-receiver/N
    test(
      'a pull naming a sender the caller may not act for is refused',
      () async {
        final store = await open();
        await acceptRun(store, deliveryChannel(), 1);

        for (final request in <PullRequest>[
          const ChannelListingPull(senderDatabaseId: kPeerDatabaseId),
          DeliveryRangePull(
            channel: deliveryChannel(),
            fromDeliveryNumber: 1,
            toDeliveryNumber: 1,
          ),
        ]) {
          for (final senders in <Set<String>>[
            <String>{'someone-else'},
            const <String>{},
          ]) {
            await expectLater(
              store.receiverEndpoint.pull(request, senderDatabaseIds: senders),
              throwsA(
                isA<DeliveryAuthenticationRefused>().having(
                  (e) => e.senderDatabaseId,
                  'senderDatabaseId',
                  kPeerDatabaseId,
                ),
              ),
            );
          }
        }
      },
    );
  });
}
