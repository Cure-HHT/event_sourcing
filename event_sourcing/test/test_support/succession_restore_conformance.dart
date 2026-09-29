// Backend-agnostic scenarios for the succession-restore operation
// (`EventStore.restoreFromReceiver`): pulling every channel a receiver
// lists for a predecessor identity and storing, in one transaction, every
// carried event this database does not hold, plus the succession event
// that records the restore. Run on Sembast by
// test/sync/succession_restore_sembast_test.dart and on Postgres by
// test/storage/postgres/postgres_succession_restore_test.dart.
//
// This file exposes [runSuccessionRestoreScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionChannel, SenderSuccessionData;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'deliveries.dart' show TestDelivery, deliverTo, testChannel;
import 'ingest_record_findings_conformance.dart' show resealed, sealedRecord;
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const String _kType = 'finding_note';

const Source _receiverSource = Source(
  hopId: 'restore-receiver-hop',
  identifier: 'restore-receiver-install',
  softwareVersion: 'restore-receiver-app@1.0.0',
);

const Source _successorSource = Source(
  hopId: 'restore-successor-hop',
  identifier: 'restore-successor-install',
  softwareVersion: 'restore-successor-app@1.0.0',
);

const String _destinationId = 'restore-destination';

const EntryTypeDefinition _entryType = EntryTypeDefinition(
  id: _kType,
  registeredVersion: EntryTypeVersion(1, 0),
  name: _kType,
);

/// A [Destination] whose pull routes directly to [receiver]'s receiver
/// endpoint, authenticated for the sender identity each request names. It
/// is never drained: the restore scenarios never enqueue anything to it.
class ReceiverPullDestination extends Destination {
  ReceiverPullDestination(this.receiver, {this.id = _destinationId});

  final EventStore receiver;

  @override
  final String id;

  // Matches nothing: the restore scenarios never enqueue to this
  // destination, and it is never drained.
  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'restore-fixture-unused'});

  @override
  String get wireFormat => DeliveryEnvelope.wireFormat;

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool get serializesNatively => true;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw UnimplementedError('restore scenarios never drain this fixture');

  @override
  Future<SendResult> send(WirePayload payload) =>
      throw UnimplementedError('restore scenarios never drain this fixture');

  @override
  ChannelPull get channelPull => _pull;

  Future<PullOutcome> _pull(PullRequest request) async {
    final sender = switch (request) {
      ChannelListingPull(:final senderDatabaseId) => senderDatabaseId,
      DeliveryRangePull(:final channel) => channel.senderDatabaseId,
    };
    final response = await receiver.receiverEndpoint.pull(
      request,
      senderDatabaseIds: <String>{sender},
    );
    return decodePullResponse(response.encode());
  }
}

/// A [ReceiverPullDestination] that truncates a delivery-range pull's
/// served deliveries to [truncateTo], without naming
/// `unservableDeliveryNumber`: exercises the restore's "served fewer
/// deliveries than asked" refusal path on its own, apart from the
/// receiver's own unservable-number signal.
class TruncatingRangeDestination extends ReceiverPullDestination {
  TruncatingRangeDestination(super.receiver, {required this.truncateTo});

  /// Deliveries above this count are dropped from any served range.
  final int truncateTo;

  @override
  ChannelPull get channelPull => _truncatingPull;

  Future<PullOutcome> _truncatingPull(PullRequest request) async {
    final outcome = await super.channelPull(request);
    if (outcome is PullServed && outcome.response is DeliveryRange) {
      final range = outcome.response as DeliveryRange;
      if (range.deliveries.length > truncateTo) {
        return PullServed(
          DeliveryRange(
            receiverDatabaseId: range.receiverDatabaseId,
            channel: range.channel,
            record: range.record,
            deliveries: range.deliveries.sublist(0, truncateTo),
          ),
        );
      }
    }
    return outcome;
  }
}

/// A [ReceiverPullDestination] that rewrites a delivery-range pull's served
/// [DeliveryRange] through [tamper] before returning it: exercises the
/// restore's verification of what a receiver serves, apart from what the
/// receiver itself actually stored.
class TamperingRangeDestination extends ReceiverPullDestination {
  TamperingRangeDestination(super.receiver, {required this.tamper});

  final DeliveryRange Function(DeliveryRange range) tamper;

  @override
  ChannelPull get channelPull => _tamperingPull;

  Future<PullOutcome> _tamperingPull(PullRequest request) async {
    final outcome = await super.channelPull(request);
    if (outcome is PullServed && outcome.response is DeliveryRange) {
      return PullServed(tamper(outcome.response as DeliveryRange));
    }
    return outcome;
  }
}

/// Opens a fresh receiver store over [db].
Future<EventStore> openRestoreReceiver(VersionTestDatabase db) async {
  final backend = await db.openBackend();
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: EntryTypeRegistry()..register(_entryType),
    source: _receiverSource,
  );
}

/// Opens a fresh successor bundle over [db], with one native destination
/// registered whose pull reaches [receiver]: [destination], when given, in
/// place of the default [ReceiverPullDestination].
Future<EventStoreBundle> openRestoreSuccessor(
  VersionTestDatabase db,
  EventStore receiver, {
  Destination? destination,
  ProjectionRegistry? projections,
}) async {
  final backend = await db.openBackend();
  return bootstrapEventStore(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    source: _successorSource,
    entryTypes: const <EntryTypeDefinition>[_entryType],
    destinations: <Destination>[
      destination ?? ReceiverPullDestination(receiver),
    ],
    projections: projections,
  );
}

/// A table view keyed on `data.k`, so a served predecessor event whose
/// data has no `k` throws from the fold rather than being stored under a
/// substitute key: a fold failure leaves that copy behind without
/// aborting the restore (`EVS-PRD-ingest/G`, `EVS-DEV-sender-succession/H`).
const String _kKeyedTableView = 'keyed_restore_notes';

const TableProjectionSpec _kKeyedTableSpec = TableProjectionSpec(
  viewName: _kKeyedTableView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: CompositeKey(<String>['data.k']),
  rowData: WholePayload(),
);

/// A hand-built record of database identity [databaseId] at origin
/// position [sequenceNumber].
Map<String, Object?> _recordAt(String databaseId, int sequenceNumber) =>
    resealed(
      sealedRecord(databaseId: databaseId, entryType: _kType),
      <String, Object?>{'sequence_number': sequenceNumber},
    );

/// The findings [store] holds as authored (one provenance entry).
Future<List<Map<String, Object?>>> _ownFindings(EventStore store) async =>
    <Map<String, Object?>>[
      for (final e in await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      ))
        if ((e.metadata['provenance']! as List).length == 1)
          Map<String, Object?>.from(e.data),
    ];

/// [record] with provenance entry [index] (negative counts from the end)
/// replaced by the result of [mutate].
Map<String, Object?> _withProvenanceEntry(
  Map<String, Object?> record,
  int index,
  Map<String, Object?> Function(Map<String, Object?> entry) mutate,
) {
  final metadata = Map<String, Object?>.from(record['metadata']! as Map);
  final provenance = List<Object?>.from(metadata['provenance']! as List);
  final i = index < 0 ? provenance.length + index : index;
  final entry = Map<String, Object?>.from(provenance[i]! as Map);
  provenance[i] = mutate(entry);
  metadata['provenance'] = provenance;
  return <String, Object?>{...record, 'metadata': metadata};
}

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runSuccessionRestoreScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('succession restore ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> receiverStore() async {
      final db = (await openDatabase())!;
      databases.add(db);
      final store = await openRestoreReceiver(db);
      opened.add(store);
      return store;
    }

    Future<EventStoreBundle> successorBundle(
      EventStore receiver, {
      Destination? destination,
      ProjectionRegistry? projections,
    }) async {
      final db = (await openDatabase())!;
      databases.add(db);
      final bundle = await openRestoreSuccessor(
        db,
        receiver,
        destination: destination,
        projections: projections,
      );
      opened.add(bundle.eventStore);
      return bundle;
    }

    tearDown(() async {
      for (final s in opened.reversed) {
        await s.close();
      }
      opened.clear();
      for (final db in databases.reversed) {
        await db.close();
      }
      databases.clear();
    });

    // Verifies: EVS-DEV-sender-succession/A
    // Verifies: EVS-DEV-sender-succession/C
    // Verifies: EVS-DEV-sender-succession/D
    // Verifies: EVS-PRD-delivery-channel/Q
    // Verifies: EVS-DEV-event-record/D
    // Verifies: EVS-DEV-event-record/G
    test('stores every event the receiver holds for the predecessor, each '
        'with the successor provenance entry naming the channel and '
        'delivery it was pulled from, and appends the succession event '
        'naming the restored channel and its last delivery', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-happy';
      final channel = testChannel(predecessorId);
      final records = <Map<String, Object?>>[
        for (var i = 0; i < 3; i++)
          sealedRecord(databaseId: predecessorId, entryType: _kType),
      ];
      TestDelivery? last;
      for (final record in records) {
        last = await deliverTo(receiver, <Map<String, Object?>>[
          record,
        ], channel: channel);
      }
      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;

      final succession = await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      for (var i = 0; i < records.length; i++) {
        final record = records[i];
        final held = await successor.reader.findEventById(
          record['event_id']! as String,
        );
        expect(held, isNotNull, reason: 'restored ${record['event_id']}');
        expect(
          held!.sealedHash,
          record['event_hash'],
          reason: 'the sealed hash is stable across every hop',
        );
        final provenance = held.metadata['provenance']! as List;
        expect(
          provenance.length,
          3,
          reason:
              'keeps the originator and the receiver entries the pulled '
              'record carried and adds its own',
        );
        expect(
          (provenance.last as Map)['database_id'],
          successor.databaseId,
          reason: 'names the restoring database',
        );
        final delivery = (provenance.last as Map)['delivery'] as Map?;
        expect(delivery, isNotNull);
        final deliveredChannel = delivery!['channel'] as Map;
        expect(deliveredChannel['sender_database_id'], predecessorId);
        expect(deliveredChannel['destination_id'], channel.destinationId);
        expect(deliveredChannel['registration_id'], channel.registrationId);
        expect(deliveredChannel['generation'], channel.generation);
        expect(
          delivery['delivery_number'],
          i + 1,
          reason:
              'each record was delivered on its own delivery, numbered in '
              'the order it was sent',
        );
      }

      expect(succession.entryType, kDestinationSenderSucceededEntryType);
      final data = SenderSuccessionData.fromJson(succession.data);
      expect(data.databaseId, successor.databaseId);
      expect(data.predecessorDatabaseId, predecessorId);
      expect(data.predecessorChannels, hasLength(1));
      expect(data.predecessorChannels.single.channel, channel);
      expect(data.predecessorChannels.single.deliveryNumber, records.length);
      expect(
        data.predecessorChannels.single.deliveryHash,
        last!.envelope.deliveryHash,
      );

      final storedSuccessions = await successor.reader.findAllEvents(
        entryType: kDestinationSenderSucceededEntryType,
      );
      expect(storedSuccessions, hasLength(1));
    });

    // Verifies: EVS-PRD-ingest/G
    // Verifies: EVS-DEV-view-convergence/E
    // Verifies: EVS-DEV-view-convergence/Q
    // Verifies: EVS-DEV-sender-succession/C
    // Verifies: EVS-DEV-sender-succession/H
    test(
      'a served history containing an event a table fold cannot key '
      'restores without throwing, and every served event is stored',
      () async {
        final receiver = await receiverStore();
        const predecessorId = 'predecessor-unkeyable';
        final channel = testChannel(predecessorId);
        final ok1 = sealedRecord(
          databaseId: predecessorId,
          entryType: _kType,
          data: <String, Object?>{'k': 'x'},
        );
        final bad = sealedRecord(
          databaseId: predecessorId,
          entryType: _kType,
          data: <String, Object?>{'title': 'no key'},
        );
        final ok2 = sealedRecord(
          databaseId: predecessorId,
          entryType: _kType,
          data: <String, Object?>{'k': 'y'},
        );
        for (final record in <Map<String, Object?>>[ok1, bad, ok2]) {
          await deliverTo(receiver, <Map<String, Object?>>[
            record,
          ], channel: channel);
        }

        final bundle = await successorBundle(
          receiver,
          projections: ProjectionRegistry()..register(_kKeyedTableSpec),
        );
        final successor = bundle.eventStore;

        // The unkeyable event's fold throws inside the restore's one
        // transaction; it must not propagate as a refusal outside the
        // enumerated list in EVS-DEV-sender-succession/H.
        await successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: predecessorId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        );

        for (final record in <Map<String, Object?>>[ok1, bad, ok2]) {
          expect(
            await successor.reader.findEventById(record['event_id']! as String),
            isNotNull,
            reason:
                'the restore stores every served event whatever a view '
                "fold makes of it (${record['event_id']})",
          );
        }
      },
    );

    // Verifies: EVS-DEV-sender-succession/C
    test('an event served on more than one generation of a channel is '
        'stored once', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-dup';
      final genOne = testChannel(predecessorId, generation: 1);
      final genTwo = testChannel(predecessorId, generation: 2);
      final shared = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        shared,
      ], channel: genOne);
      // A resumed channel starts its new generation from the start of the
      // sender's log: the shared record is delivered again as delivery 1.
      await deliverTo(receiver, <Map<String, Object?>>[
        shared,
      ], channel: genTwo);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final all = await successor.reader.findAllEvents(entryType: _kType);
      expect(all.where((e) => e.eventId == shared['event_id']), hasLength(1));
    });

    // Verifies: EVS-DEV-sender-succession/B
    // Verifies: EVS-DEV-sender-succession/C
    // Verifies: EVS-DEV-security-findings/G
    test('an event_id served under two different hashes across generations '
        'stores the genuine record and records an identity_mismatch finding '
        'for the other, instead of dropping it', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-resealed';
      final genOne = testChannel(predecessorId, generation: 1);
      final genTwo = testChannel(predecessorId, generation: 2);
      final genuine = sealedRecord(
        databaseId: predecessorId,
        entryType: _kType,
      );
      final tampered = resealed(genuine, <String, Object?>{
        'data': <String, Object?>{'title': 'other'},
      });
      await deliverTo(receiver, <Map<String, Object?>>[
        genuine,
      ], channel: genOne);
      // A resumed channel resends the predecessor's log from the start;
      // here the predecessor's own copy of the record has changed, so
      // the receiver is served a different record under the same
      // event_id and keeps it in an identity_mismatch finding of its
      // own instead of the held event.
      await deliverTo(receiver, <Map<String, Object?>>[
        tampered,
      ], channel: genTwo);

      final receiverFindings = await _ownFindings(receiver);
      final receiverMismatch = receiverFindings.where(
        (f) => f['kind'] == 'identity_mismatch',
      );
      expect(receiverMismatch, hasLength(1));
      expect(
        await receiver.reader.findEventById(genuine['event_id']! as String),
        isNotNull,
      );

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final held = await successor.reader.findEventById(
        genuine['event_id']! as String,
      );
      expect(held, isNotNull);
      expect(held!.sealedHash, genuine['event_hash']);

      final findings = await _ownFindings(successor);
      final mismatch = findings.where((f) => f['kind'] == 'identity_mismatch');
      expect(
        mismatch,
        hasLength(1),
        reason:
            'the record served under the second occurrence is checked '
            'and kept in its own finding, never dropped',
      );
      expect(
        (mismatch.single['detector']! as Map)['role'],
        'restore',
        reason: 'the restore records its own checks under role restore',
      );
      final evidence = mismatch.single['evidence']! as Map;
      expect(evidence['event_id'], genuine['event_id']);
      expect(
        (evidence['record']! as Map)['event_hash'],
        tampered['event_hash'],
        reason:
            "the finding's evidence carries the record that was "
            'served but not stored',
      );
    });

    // Verifies: EVS-DEV-sender-succession/B
    // Verifies: EVS-DEV-sender-succession/C
    // Verifies: EVS-DEV-security-findings/G
    test('an event_id served twice in one delivery under two different '
        'hashes is checked both times: one is stored and the other is kept '
        'in an identity_mismatch finding', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-resealed-one-delivery';
      final channel = testChannel(predecessorId);
      final genuine = sealedRecord(
        databaseId: predecessorId,
        entryType: _kType,
      );
      final tampered = resealed(genuine, <String, Object?>{
        'data': <String, Object?>{'title': 'other'},
      });
      await deliverTo(receiver, <Map<String, Object?>>[
        genuine,
        tampered,
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final all = await successor.reader.findAllEvents(entryType: _kType);
      expect(
        all.where((e) => e.eventId == genuine['event_id']),
        hasLength(1),
        reason: 'exactly one of the two hashes is stored as the event',
      );

      final findings = await _ownFindings(successor);
      final mismatch = findings.where((f) => f['kind'] == 'identity_mismatch');
      expect(mismatch, hasLength(1));
    });

    // Verifies: EVS-DEV-sender-succession/B
    test('a distinct-hash occurrence that ends up stored, because the chosen '
        'occurrence of its event_id was kept only in its own finding, gets '
        'the same originator and receiver-entry checks as any other newly '
        'stored event', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-extra-stored';
      final genOne = testChannel(predecessorId, generation: 1);
      final genTwo = testChannel(predecessorId, generation: 2);
      // Sorts lowest (generation 1) and is unstorable, so the successor
      // holds nothing under this event_id once it is processed.
      final malformed = sealedRecord(
        databaseId: predecessorId,
        entryType: _kType,
        data: const <String, Object?>{r'$integrity': 'forged'},
      );
      // Same event_id, well-formed data, a different hash: this is the
      // occurrence that ends up stored, as an extra.
      final wellFormed = resealed(malformed, <String, Object?>{
        'data': <String, Object?>{'title': 'ok'},
      });
      await deliverTo(receiver, <Map<String, Object?>>[
        malformed,
      ], channel: genOne);
      await deliverTo(receiver, <Map<String, Object?>>[
        wellFormed,
      ], channel: genTwo);

      final bundle = await successorBundle(
        receiver,
        destination: TamperingRangeDestination(
          receiver,
          tamper: (range) {
            if (range.channel != genTwo) return range;
            return DeliveryRange(
              receiverDatabaseId: range.receiverDatabaseId,
              channel: range.channel,
              record: range.record,
              deliveries: <ServedDelivery>[
                for (final d in range.deliveries)
                  ServedDelivery(
                    deliveryNumber: d.deliveryNumber,
                    previousDeliveryHash: d.previousDeliveryHash,
                    deliveryHash: d.deliveryHash,
                    attributes: d.attributes,
                    events: <Map<String, Object?>>[
                      for (final e in d.events)
                        _withProvenanceEntry(
                          e,
                          -1,
                          (entry) => <String, Object?>{
                            ...entry,
                            'database_id': 'not-the-receiver',
                          },
                        ),
                    ],
                  ),
              ],
            );
          },
        ),
      );
      final successor = bundle.eventStore;

      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final held = await successor.reader.findEventById(
        malformed['event_id']! as String,
      );
      expect(
        held,
        isNotNull,
        reason: 'the well-formed occurrence is stored as the event',
      );
      expect(held!.sealedHash, wellFormed['event_hash']);

      final findings = await _ownFindings(successor);
      final unverified = findings.where(
        (f) => f['kind'] == 'restore_unverified',
      );
      expect(
        unverified.where(
          (f) => (f['evidence']! as Map)['check'] == 'receiver_entry',
        ),
        hasLength(1),
        reason:
            'the extra occurrence that got stored is checked for its '
            'receiver entry exactly as the ordinary stored path checks '
            'it',
      );
    });

    // Verifies: EVS-DEV-sender-succession/C
    test('events are stored in lineage order ahead of origin position, '
        'whatever order their channels delivered them', () async {
      final receiver = await receiverStore();
      const rootId = 'predecessor-root';
      const leafId = 'predecessor-leaf';

      // The leaf's own channel first carries the succession event by which
      // it succeeded the root, then its own later events.
      final leafChannel = testChannel(leafId);
      final successionRecord = resealed(
        sealedRecord(
          databaseId: leafId,
          entryType: kDestinationSenderSucceededEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationSenderSucceededEventType,
          data: const SenderSuccessionData(
            id: 'root-destination',
            registrationId: 'root-registration',
            databaseId: leafId,
            predecessorDatabaseId: rootId,
            predecessorChannels: <SenderSuccessionChannel>[],
          ).toJson(),
        ),
        const <String, Object?>{'sequence_number': 1},
      );
      // The leaf's own event sits at an earlier origin position than the
      // root's, so origin position alone would put it first: only the
      // lineage rank (root precedes its successor) can put the root event
      // first.
      final leafEvent = _recordAt(leafId, 4);
      await deliverTo(receiver, <Map<String, Object?>>[
        successionRecord,
        leafEvent,
      ], channel: leafChannel);

      final rootChannel = testChannel(rootId);
      final rootEvent = _recordAt(rootId, 5);
      await deliverTo(receiver, <Map<String, Object?>>[
        rootEvent,
      ], channel: rootChannel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: leafId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final restored = await successor.reader.findAllEvents(entryType: _kType);
      restored.sort((a, b) => a.sequenceNumber.compareTo(b.sequenceNumber));
      expect(
        restored.map((e) => e.eventId),
        <Object?>[rootEvent['event_id'], leafEvent['event_id']],
        reason:
            "the root's event precedes the leaf's own, later-position "
            'event, solely because the root precedes its successor in '
            'the lineage',
      );
    });

    // Verifies: EVS-DEV-sender-succession/C
    test('events of one identity are ordered by ascending origin position '
        'across channels, not by the order their channels were pulled, '
        'and a duplicate at one origin position is stored under the '
        'channel whose (registration, generation, delivery number) sorts '
        'lowest', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-interleaved';

      // Channel A is registered ahead of channel B, and delivers first
      // below, but carries the later-origin-position event: pulling it
      // first must not put its event first in the stored order.
      final channelA = testChannel(
        predecessorId,
        registrationId: 'reg-a',
        generation: 1,
      );
      final channelB = testChannel(
        predecessorId,
        registrationId: 'reg-b',
        generation: 1,
      );
      final laterEvent = _recordAt(predecessorId, 9);
      final earlierEvent = _recordAt(predecessorId, 2);
      // A duplicate of the earlier event, served again on channel A at a
      // later delivery number: both occurrences name the same origin
      // position, so only the (registration, generation, delivery number)
      // tie-break decides which channel's provenance is kept.
      final duplicateOfEarlier = resealed(
        earlierEvent,
        const <String, Object?>{},
      );

      await deliverTo(receiver, <Map<String, Object?>>[
        laterEvent,
        duplicateOfEarlier,
      ], channel: channelA);
      await deliverTo(receiver, <Map<String, Object?>>[
        earlierEvent,
      ], channel: channelB);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final restored = await successor.reader.findAllEvents(entryType: _kType);
      restored.sort((a, b) => a.sequenceNumber.compareTo(b.sequenceNumber));
      expect(
        restored.map((e) => e.eventId),
        <Object?>[earlierEvent['event_id'], laterEvent['event_id']],
        reason:
            'ascending origin position orders the two events regardless '
            'of which channel delivered which first',
      );

      final held = await successor.reader.findEventById(
        earlierEvent['event_id']! as String,
      );
      final delivery =
          (held!.metadata['provenance']! as List).last as Map<String, Object?>;
      final deliveryInfo = delivery['delivery']! as Map<String, Object?>;
      expect(
        (deliveryInfo['channel'] as Map)['registration_id'],
        channelA.registrationId,
        reason:
            'reg-a sorts below reg-b, so the duplicate is kept under '
            "channel A's delivery even though channel B's was pulled and "
            'served too',
      );
    });

    // Verifies: EVS-DEV-sender-succession/D
    test('a failure after the store and before the succession event rolls '
        'back every restored event with it', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-atomic';
      final channel = testChannel(predecessorId);
      final record = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        record,
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(failRestoreStore: () => true),
        () => expectLater(
          successor.restoreFromReceiver(
            registry: bundle.destinations,
            destinationId: _destinationId,
            predecessorDatabaseId: predecessorId,
            initiator: const AutomationInitiator(service: 'restore-test'),
          ),
          throwsA(isA<InjectedFailure>()),
        ),
      );

      expect(
        await successor.reader.findEventById(record['event_id']! as String),
        isNull,
        reason: 'the restored event did not survive the rollback',
      );
      expect(
        await successor.reader.findAllEvents(
          entryType: kDestinationSenderSucceededEntryType,
        ),
        isEmpty,
        reason: 'the succession event was never appended',
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    // Verifies: EVS-PRD-delivery-channel/R
    test('refuses a restore into a successor whose log already holds an '
        'authored application event, storing nothing', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-app-event';
      final channel = testChannel(predecessorId);
      final record = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        record,
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.append(
        entryType: _kType,
        aggregateId: 'own-application-aggregate',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      await expectLater(
        successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: predecessorId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        ),
        throwsA(
          isA<SuccessionRestoreRefused>().having(
            (e) => e.reason,
            'reason',
            SuccessionRestoreRefused.applicationEventAuthored,
          ),
        ),
      );

      expect(
        await successor.reader.findEventById(record['event_id']! as String),
        isNull,
        reason: 'the predecessor event was not stored',
      );
      expect(
        await successor.reader.findAllEvents(
          entryType: kDestinationSenderSucceededEntryType,
        ),
        isEmpty,
        reason: 'no succession event was appended',
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    test('refuses a restore into a successor whose log already holds a '
        'succession event it authored, storing nothing', () async {
      final receiver = await receiverStore();
      const firstPredecessorId = 'predecessor-first-of-two';
      final firstChannel = testChannel(firstPredecessorId);
      await deliverTo(receiver, <Map<String, Object?>>[
        sealedRecord(databaseId: firstPredecessorId, entryType: _kType),
      ], channel: firstChannel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: firstPredecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      const secondPredecessorId = 'predecessor-second-of-two';
      final secondChannel = testChannel(secondPredecessorId);
      final secondRecord = sealedRecord(
        databaseId: secondPredecessorId,
        entryType: _kType,
      );
      await deliverTo(receiver, <Map<String, Object?>>[
        secondRecord,
      ], channel: secondChannel);

      await expectLater(
        successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: secondPredecessorId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        ),
        throwsA(
          isA<SuccessionRestoreRefused>().having(
            (e) => e.reason,
            'reason',
            SuccessionRestoreRefused.successionAlreadyAuthored,
          ),
        ),
      );

      expect(
        await successor.reader.findEventById(
          secondRecord['event_id']! as String,
        ),
        isNull,
        reason: 'the second predecessor event was not stored',
      );
      expect(
        await successor.reader.findAllEvents(
          entryType: kDestinationSenderSucceededEntryType,
        ),
        hasLength(1),
        reason: 'only the first restore appended a succession event',
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    test("refuses a restore naming the successor's own identity", () async {
      final receiver = await receiverStore();
      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;

      await expectLater(
        successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: successor.databaseId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        ),
        throwsA(
          isA<SuccessionRestoreRefused>().having(
            (e) => e.reason,
            'reason',
            SuccessionRestoreRefused.predecessorIsSelf,
          ),
        ),
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    test('refuses a restore for a predecessor the receiver lists no '
        'channel for', () async {
      final receiver = await receiverStore();
      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;

      await expectLater(
        successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: 'predecessor-never-delivered-to',
          initiator: const AutomationInitiator(service: 'restore-test'),
        ),
        throwsA(
          isA<SuccessionRestoreRefused>().having(
            (e) => e.reason,
            'reason',
            SuccessionRestoreRefused.noChannelListed,
          ),
        ),
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    test('refuses a restore whose pull serves fewer deliveries than it '
        'asked for, storing nothing', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-truncated';
      final channel = testChannel(predecessorId);
      final records = <Map<String, Object?>>[
        for (var i = 0; i < 3; i++)
          sealedRecord(databaseId: predecessorId, entryType: _kType),
      ];
      for (final record in records) {
        await deliverTo(receiver, <Map<String, Object?>>[
          record,
        ], channel: channel);
      }

      final bundle = await successorBundle(
        receiver,
        destination: TruncatingRangeDestination(receiver, truncateTo: 2),
      );
      final successor = bundle.eventStore;

      await expectLater(
        successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: predecessorId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        ),
        throwsA(
          isA<SuccessionRestoreRefused>().having(
            (e) => e.reason,
            'reason',
            SuccessionRestoreRefused.deliveryUnservable,
          ),
        ),
      );

      expect(
        await successor.reader.findAllEvents(entryType: _kType),
        isEmpty,
        reason: 'nothing was stored from the truncated range',
      );
    });

    // Verifies: EVS-DEV-sender-succession/H
    test('refuses inside the storing transaction when an application '
        'event is authored between the pull and the transaction', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-race';
      final channel = testChannel(predecessorId);
      await deliverTo(receiver, <Map<String, Object?>>[
        sealedRecord(databaseId: predecessorId, entryType: _kType),
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          beforeRestoreTransaction: () async {
            await successor.append(
              entryType: _kType,
              aggregateId: 'race-aggregate',
              aggregateType: 'note',
              eventType: 'finalized',
              data: const <String, Object?>{},
              initiator: const AutomationInitiator(service: 'restore-test'),
            );
          },
        ),
        () => expectLater(
          successor.restoreFromReceiver(
            registry: bundle.destinations,
            destinationId: _destinationId,
            predecessorDatabaseId: predecessorId,
            initiator: const AutomationInitiator(service: 'restore-test'),
          ),
          throwsA(
            isA<SuccessionRestoreRefused>().having(
              (e) => e.reason,
              'reason',
              SuccessionRestoreRefused.applicationEventAuthored,
            ),
          ),
        ),
      );

      expect(
        await successor.reader.findAllEvents(
          entryType: kDestinationSenderSucceededEntryType,
        ),
        isEmpty,
        reason: 'no succession event was appended',
      );
    });

    // Verifies: EVS-DEV-sender-succession/B
    // Verifies: EVS-DEV-security-findings/Q
    // Verifies: EVS-DEV-security-findings/R
    test('records restore_unverified check delivery_link for a served '
        'delivery whose link does not chain from the delivery before it, '
        'and still stores every carried event', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-bad-link';
      final channel = testChannel(predecessorId);
      final records = <Map<String, Object?>>[
        for (var i = 0; i < 2; i++)
          sealedRecord(databaseId: predecessorId, entryType: _kType),
      ];
      for (final record in records) {
        await deliverTo(receiver, <Map<String, Object?>>[
          record,
        ], channel: channel);
      }

      final bundle = await successorBundle(
        receiver,
        destination: TamperingRangeDestination(
          receiver,
          tamper: (range) => DeliveryRange(
            receiverDatabaseId: range.receiverDatabaseId,
            channel: range.channel,
            record: range.record,
            deliveries: <ServedDelivery>[
              for (final d in range.deliveries)
                if (d.deliveryNumber == 2)
                  ServedDelivery(
                    deliveryNumber: d.deliveryNumber,
                    previousDeliveryHash: 'tampered-link',
                    deliveryHash: d.deliveryHash,
                    attributes: d.attributes,
                    events: d.events,
                  )
                else
                  d,
            ],
          ),
        ),
      );
      final successor = bundle.eventStore;

      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final unverified = findings.where(
        (f) => f['kind'] == 'restore_unverified',
      );
      // Tampering the link also invalidates the delivery hash, which
      // covers it: both checks fail for delivery 2, and delivery 1, the
      // one left untouched, is not named by either.
      final linkFinding = unverified.singleWhere(
        (f) => (f['evidence']! as Map)['check'] == 'delivery_link',
      );
      final evidence = linkFinding['evidence']! as Map;
      expect(evidence['delivery_number'], 2);
      expect(evidence['event_id'], isNull);
      expect(
        (linkFinding['detector']! as Map)['role'],
        'restore',
        reason: 'the restore records its own checks under role restore',
      );
      final hashFinding = unverified.singleWhere(
        (f) => (f['evidence']! as Map)['check'] == 'delivery_hash',
      );
      expect((hashFinding['evidence']! as Map)['delivery_number'], 2);
      expect(
        unverified.every(
          (f) => (f['evidence']! as Map)['delivery_number'] == 2,
        ),
        isTrue,
      );

      for (final record in records) {
        expect(
          await successor.reader.findEventById(record['event_id']! as String),
          isNotNull,
          reason: 'a failed check does not stop the event from being stored',
        );
      }
    });

    // Verifies: EVS-DEV-sender-succession/B
    test('records restore_unverified check delivery_hash for a served '
        'delivery whose hash does not recompute', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-bad-delivery-hash';
      final channel = testChannel(predecessorId);
      final record = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        record,
      ], channel: channel);

      final bundle = await successorBundle(
        receiver,
        destination: TamperingRangeDestination(
          receiver,
          tamper: (range) => DeliveryRange(
            receiverDatabaseId: range.receiverDatabaseId,
            channel: range.channel,
            record: range.record,
            deliveries: <ServedDelivery>[
              for (final d in range.deliveries)
                ServedDelivery(
                  deliveryNumber: d.deliveryNumber,
                  previousDeliveryHash: d.previousDeliveryHash,
                  deliveryHash: 'tampered-delivery-hash',
                  attributes: d.attributes,
                  events: d.events,
                ),
            ],
          ),
        ),
      );
      final successor = bundle.eventStore;

      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final unverified = findings.where(
        (f) => f['kind'] == 'restore_unverified',
      );
      expect(unverified, hasLength(1));
      final evidence = unverified.single['evidence']! as Map;
      expect(evidence['check'], 'delivery_hash');
      expect(evidence['delivery_number'], 1);
      expect(evidence['event_id'], isNull);

      expect(
        await successor.reader.findEventById(record['event_id']! as String),
        isNotNull,
      );
    });

    // Verifies: EVS-DEV-sender-succession/B
    test('records restore_unverified check originator for a served event '
        "whose originator entry does not name the channel's sender", () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-bad-originator';
      final channel = testChannel(predecessorId);
      final record = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        record,
      ], channel: channel);

      final bundle = await successorBundle(
        receiver,
        destination: TamperingRangeDestination(
          receiver,
          tamper: (range) => DeliveryRange(
            receiverDatabaseId: range.receiverDatabaseId,
            channel: range.channel,
            record: range.record,
            deliveries: <ServedDelivery>[
              for (final d in range.deliveries)
                ServedDelivery(
                  deliveryNumber: d.deliveryNumber,
                  previousDeliveryHash: d.previousDeliveryHash,
                  deliveryHash: d.deliveryHash,
                  attributes: d.attributes,
                  events: <Map<String, Object?>>[
                    for (final e in d.events)
                      _withProvenanceEntry(
                        e,
                        0,
                        (entry) => <String, Object?>{
                          ...entry,
                          'database_id': 'not-the-sender',
                        },
                      ),
                  ],
                ),
            ],
          ),
        ),
      );
      final successor = bundle.eventStore;

      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final unverified = findings.where(
        (f) => f['kind'] == 'restore_unverified',
      );
      expect(unverified, hasLength(1));
      final evidence = unverified.single['evidence']! as Map;
      expect(evidence['check'], 'originator');
      expect(evidence['event_id'], record['event_id']);
    });

    // Verifies: EVS-DEV-sender-succession/B
    test('records restore_unverified check receiver_entry for a served '
        "event whose last provenance entry does not name the receiver's "
        'identity', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-bad-receiver-entry';
      final channel = testChannel(predecessorId);
      final record = sealedRecord(databaseId: predecessorId, entryType: _kType);
      await deliverTo(receiver, <Map<String, Object?>>[
        record,
      ], channel: channel);

      final bundle = await successorBundle(
        receiver,
        destination: TamperingRangeDestination(
          receiver,
          tamper: (range) => DeliveryRange(
            receiverDatabaseId: range.receiverDatabaseId,
            channel: range.channel,
            record: range.record,
            deliveries: <ServedDelivery>[
              for (final d in range.deliveries)
                ServedDelivery(
                  deliveryNumber: d.deliveryNumber,
                  previousDeliveryHash: d.previousDeliveryHash,
                  deliveryHash: d.deliveryHash,
                  attributes: d.attributes,
                  events: <Map<String, Object?>>[
                    for (final e in d.events)
                      _withProvenanceEntry(
                        e,
                        -1,
                        (entry) => <String, Object?>{
                          ...entry,
                          'database_id': 'not-the-receiver',
                        },
                      ),
                  ],
                ),
            ],
          ),
        ),
      );
      final successor = bundle.eventStore;

      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final unverified = findings.where(
        (f) => f['kind'] == 'restore_unverified',
      );
      // Tampering the last entry also invalidates the delivery hash: the
      // delivery hash is built from the receiver's own arrival hash for
      // each event, which this record no longer carries under a receiver
      // entry.
      final receiverEntryFinding = unverified.singleWhere(
        (f) => (f['evidence']! as Map)['check'] == 'receiver_entry',
      );
      expect(
        (receiverEntryFinding['evidence']! as Map)['event_id'],
        record['event_id'],
      );
      expect(
        unverified.every(
          (f) => (f['evidence']! as Map)['delivery_number'] == 1,
        ),
        isTrue,
      );
    });

    // Verifies: EVS-DEV-sender-succession/B
    // Verifies: EVS-DEV-chain-verification/L
    test('applies the reused ingest chain checks to what it stores and '
        'records their finding under role restore: two channels carrying '
        "different events at one of the predecessor's origin positions "
        'produce one position_reused finding', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-overlap';
      final genOne = testChannel(predecessorId, generation: 1);
      final genTwo = testChannel(predecessorId, generation: 2);
      final first = _recordAt(predecessorId, 3);
      final second = _recordAt(predecessorId, 3);
      await deliverTo(receiver, <Map<String, Object?>>[first], channel: genOne);
      await deliverTo(receiver, <Map<String, Object?>>[
        second,
      ], channel: genTwo);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final reused = findings.where((f) => f['kind'] == 'position_reused');
      expect(reused, hasLength(1));
      expect((reused.single['detector']! as Map)['role'], 'restore');
      expect((reused.single['evidence']! as Map)['origin_sequence_number'], 3);

      expect(
        await successor.reader.findEventById(first['event_id']! as String),
        isNotNull,
      );
      expect(
        await successor.reader.findEventById(second['event_id']! as String),
        isNotNull,
      );
    });

    // Verifies: EVS-DEV-security-findings/L
    // Verifies: EVS-PRD-hash-chain-integrity/J
    test(
      'records hash_mismatch under role restore for a served event '
      'whose event hash does not recompute, and stores it as received',
      () async {
        final receiver = await receiverStore();
        const predecessorId = 'predecessor-bad-event-hash';
        final channel = testChannel(predecessorId);
        final record = sealedRecord(
          databaseId: predecessorId,
          entryType: _kType,
        );
        await deliverTo(receiver, <Map<String, Object?>>[
          record,
        ], channel: channel);

        final bundle = await successorBundle(
          receiver,
          destination: TamperingRangeDestination(
            receiver,
            tamper: (range) => DeliveryRange(
              receiverDatabaseId: range.receiverDatabaseId,
              channel: range.channel,
              record: range.record,
              deliveries: <ServedDelivery>[
                for (final d in range.deliveries)
                  ServedDelivery(
                    deliveryNumber: d.deliveryNumber,
                    previousDeliveryHash: d.previousDeliveryHash,
                    deliveryHash: d.deliveryHash,
                    attributes: d.attributes,
                    events: <Map<String, Object?>>[
                      for (final e in d.events)
                        <String, Object?>{
                          ...e,
                          'event_hash': 'tampered-event-hash',
                        },
                    ],
                  ),
              ],
            ),
          ),
        );
        final successor = bundle.eventStore;

        await successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: predecessorId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        );

        final findings = await _ownFindings(successor);
        final mismatch = findings.where((f) => f['kind'] == 'hash_mismatch');
        expect(mismatch, hasLength(1));
        expect((mismatch.single['detector']! as Map)['role'], 'restore');
        final evidence = mismatch.single['evidence']! as Map;
        expect(evidence['event_id'], record['event_id']);
        expect(evidence['carried_hash'], 'tampered-event-hash');
        // The receiver's own arrival hash, not the tampered top-level
        // event_hash, feeds the delivery hash, so it still recomputes.
        expect(
          findings.where((f) => f['kind'] == 'restore_unverified'),
          isEmpty,
        );

        expect(
          await successor.reader.findEventById(record['event_id']! as String),
          isNotNull,
          reason: 'stored as received despite the mismatch',
        );
      },
    );

    // Verifies: EVS-DEV-security-findings/O
    test('records event_malformed, and no restore_unverified, for a served '
        'record the receiver could keep only in a finding', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-malformed';
      final channel = testChannel(predecessorId);
      final malformed = sealedRecord(
        databaseId: predecessorId,
        entryType: _kType,
        data: const <String, Object?>{r'$integrity': 'forged'},
      );
      await deliverTo(receiver, <Map<String, Object?>>[
        malformed,
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final malformedFindings = findings.where(
        (f) => f['kind'] == 'event_malformed',
      );
      expect(malformedFindings, hasLength(1));
      expect(
        (malformedFindings.single['evidence']! as Map)['reason'],
        'record_malformed',
      );
      expect(
        findings.where((f) => f['kind'] == 'restore_unverified'),
        isEmpty,
        reason:
            'a record kept only in a finding is not checked again as an '
            'event, and its delivery still recomputes from the arrival '
            'hash the record itself carries',
      );
      expect(
        await successor.reader.findEventById(malformed['event_id']! as String),
        isNull,
        reason:
            'no event is stored for a record the library cannot hold '
            'as one',
      );
    });

    // Verifies: EVS-DEV-security-findings/O
    // Verifies: EVS-DEV-sender-succession/B
    test('records event_malformed, and stores the rest of what it '
        'received, for a served record too structurally malformed to '
        'parse as an event', () async {
      final receiver = await receiverStore();
      const predecessorId = 'predecessor-structurally-malformed';
      final channel = testChannel(predecessorId);
      final noAggregateId = Map<String, Object?>.from(
        sealedRecord(databaseId: predecessorId, entryType: _kType),
      )..remove('aggregate_id');
      final nonStringEventId = Map<String, Object?>.from(
        sealedRecord(databaseId: predecessorId, entryType: _kType),
      )..['event_id'] = 12345;
      final wellFormed = sealedRecord(
        databaseId: predecessorId,
        entryType: _kType,
      );
      await deliverTo(receiver, <Map<String, Object?>>[
        noAggregateId,
        nonStringEventId,
        wellFormed,
      ], channel: channel);

      final bundle = await successorBundle(receiver);
      final successor = bundle.eventStore;
      // Neither the ordering step nor the dedup step this exercises can
      // parse event_id or origin position from either malformed record;
      // both must still reach storing instead of throwing.
      await successor.restoreFromReceiver(
        registry: bundle.destinations,
        destinationId: _destinationId,
        predecessorDatabaseId: predecessorId,
        initiator: const AutomationInitiator(service: 'restore-test'),
      );

      final findings = await _ownFindings(successor);
      final malformedFindings = findings.where(
        (f) => f['kind'] == 'event_malformed',
      );
      expect(
        malformedFindings,
        hasLength(2),
        reason:
            'one finding for the missing aggregate_id, one for the '
            'non-string event_id',
      );
      for (final finding in malformedFindings) {
        expect((finding['evidence']! as Map)['reason'], 'record_malformed');
      }

      expect(
        await successor.reader.findEventById(wellFormed['event_id']! as String),
        isNotNull,
        reason:
            'the rest of what the restore received is stored despite the '
            'malformed records beside it',
      );
    });

    // Verifies: EVS-DEV-chain-verification/K
    test(
      'records predecessor_break under role restore for a served event '
      'whose previous_event_hash names a held event of another database',
      () async {
        final receiver = await receiverStore();
        const rootId = 'predecessor-break-root';
        const leafId = 'predecessor-break-leaf';

        final leafChannel = testChannel(leafId);
        final successionRecord = resealed(
          sealedRecord(
            databaseId: leafId,
            entryType: kDestinationSenderSucceededEntryType,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kDestinationSenderSucceededEventType,
            data: const SenderSuccessionData(
              id: 'root-destination',
              registrationId: 'root-registration',
              databaseId: leafId,
              predecessorDatabaseId: rootId,
              predecessorChannels: <SenderSuccessionChannel>[],
            ).toJson(),
          ),
          const <String, Object?>{'sequence_number': 1},
        );
        await deliverTo(receiver, <Map<String, Object?>>[
          successionRecord,
        ], channel: leafChannel);

        final rootChannel = testChannel(rootId);
        final rootEvent = sealedRecord(databaseId: rootId, entryType: _kType);
        await deliverTo(receiver, <Map<String, Object?>>[
          rootEvent,
        ], channel: rootChannel);

        // A leaf event whose previous_event_hash wrongly names the root's
        // event: an event of another database.
        final leafEvent = resealed(
          sealedRecord(databaseId: leafId, entryType: _kType),
          <String, Object?>{'previous_event_hash': rootEvent['event_hash']},
        );
        await deliverTo(receiver, <Map<String, Object?>>[
          leafEvent,
        ], channel: leafChannel);

        final bundle = await successorBundle(receiver);
        final successor = bundle.eventStore;
        await successor.restoreFromReceiver(
          registry: bundle.destinations,
          destinationId: _destinationId,
          predecessorDatabaseId: leafId,
          initiator: const AutomationInitiator(service: 'restore-test'),
        );

        final findings = await _ownFindings(successor);
        final breaks = findings.where((f) => f['kind'] == 'predecessor_break');
        expect(breaks, hasLength(1));
        expect((breaks.single['detector']! as Map)['role'], 'restore');
        final evidence = breaks.single['evidence']! as Map;
        expect(evidence['database_id'], leafId);
        expect(evidence['previous_event_hash'], rootEvent['event_hash']);

        expect(
          await successor.reader.findEventById(
            leafEvent['event_id']! as String,
          ),
          isNotNull,
          reason: 'stored as received despite the break',
        );
      },
    );
  });
}
