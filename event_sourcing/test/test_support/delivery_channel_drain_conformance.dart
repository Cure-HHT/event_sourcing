// Backend-agnostic scenarios for the sender's side of a delivery channel:
// the drainer numbers each delivery at its pre-send fence from the sender
// channel record, sends it only while that record is unchanged, and reads
// the receiver's record returned with every answer (in step, a lost
// acknowledgement, an adopted record, a receiver behind, a sender
// regression, anything else). Sembast runs them from
// test/sync/delivery_channel_drain_test.dart and Postgres from
// test/storage/postgres/delivery_channel_drain_postgres_test.dart.
//
// This file exposes [runDeliveryChannelDrainScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'native_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show drainForTest, fillForTest;
import 'test_backends.dart';

const Initiator _init = AutomationInitiator(service: 'channel-scenarios');
const String _noteType = 'channel_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'channel-install',
  softwareVersion: 'test@1.0.0',
);

/// A policy with no backoff and an attempt budget of five.
const SyncPolicy _policy = SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: 5,
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// The keys the resume event's data carries.
const Set<String> _resumeEventKeys = <String>{
  'id',
  'database_id',
  'registration_id',
  'generation',
  'resume_after',
  'previous_record',
  'drainer_epoch',
};

class _World {
  _World(this.db);

  final QueueTestDatabase db;
  late StorageBackend backend;
  late EventStore store;
  late DestinationRegistry registry;
  DateTime eventTime = DateTime.utc(2026, 3, 1);

  Future<void> open() async {
    backend = await db.openBackend();
    final entryTypes = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: _noteType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: _noteType,
        ),
      );
    store = trackTestBackend(
      await EventStore.openForTest(
        storage: backend,
        entryTypes: entryTypes,
        source: _source,
        securityContexts: db.securityFor(backend),
        clock: () => eventTime,
      ),
      backend,
    );
    registry = DestinationRegistry(eventStore: store);
  }

  Future<StoredEvent> note(String id) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    final event = await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id},
      initiator: _init,
    );
    return event!;
  }

  Future<void> activate(Destination d) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  }

  /// Fills [d] until a fill enqueues nothing more and leaves the position.
  Future<void> fill(Destination d) async {
    for (var i = 0; i < 50; i++) {
      final before = (await backend.listFifoEntries(d.id)).length;
      final cursorBefore = await backend.readFillCursor(d.id);
      await fillForTest(d, backend: backend, source: _source, clock: _fillNow);
      final after = (await backend.listFifoEntries(d.id)).length;
      if (after == before &&
          await backend.readFillCursor(d.id) == cursorBefore) {
        return;
      }
    }
  }

  /// One drain of [d]'s queue.
  Future<void> drain(Destination d) => drainForTest(
    d,
    registry: registry,
    policy: _policy,
    clock: () => DateTime.utc(2100),
  );

  /// Fills and drains [d] until its queue holds no pending item, at most
  /// [rounds] times.
  Future<void> deliverAll(Destination d, {int rounds = 20}) async {
    for (var i = 0; i < rounds; i++) {
      await fill(d);
      final head = await backend.readFifoHead(d.id);
      if (head == null || head.finalStatus != null) return;
      await drain(d);
    }
  }

  Future<SenderChannelRecord?> senderRecord(String id) =>
      backend.transaction((txn) => backend.readSenderChannelRecordTxn(txn, id));

  Future<TransformFailureRecord?> transformFailureRecord(String id) => backend
      .transaction((txn) => backend.readTransformFailureRecordTxn(txn, id));

  Future<String> registrationId(String id) async {
    final schedule = await backend.transaction(
      (txn) => backend.readScheduleTxn(txn, id),
    );
    return schedule!.registrationId!;
  }

  Future<DeliveryChannel> channel(String id, {int generation = 1}) async =>
      DeliveryChannel(
        senderDatabaseId: store.databaseId,
        destinationId: id,
        registrationId: await registrationId(id),
        generation: generation,
      );

  Future<List<FifoEntry>> items(String id) => backend.listFifoEntries(id);

  Future<List<StoredEvent>> findings() async => <StoredEvent>[
    for (final e in await store.reader.findAllEvents(
      entryType: kSecurityFindingEntryType,
    ))
      e,
  ];

  Future<List<StoredEvent>> resumeEvents() => store.reader.findAllEvents(
    entryType: kDestinationChannelResumedEntryType,
  );

  Future<int?> drainEpoch() =>
      backend.transaction((txn) => backend.readDrainEpochTxn(txn));
}

/// The deliveries [d] was handed, decoded, in send order.
List<DeliveryEnvelope> _sentDeliveries(NativeDestination d) =>
    <DeliveryEnvelope>[
      for (final p in d.sent) DeliveryEnvelope.decode(p.bytes),
    ];

DeliveryRecord _recordOf(DeliveryEnvelope delivery) => DeliveryRecord(
  deliveryNumber: delivery.deliveryNumber,
  deliveryHash: delivery.deliveryHash,
);

/// Runs the scenarios. [databaseFactory] returns a fresh database, or null
/// when the backend is not available (every test is then skipped).
void runDeliveryChannelDrainScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('delivery channel drain ($label)', () {
    late _World w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = _World(db);
      await w.open();
    });

    tearDown(() async {
      if (!available) return;
      await w.store.close();
      await w.db.close();
    });

    // Verifies: EVS-DEV-delivery-channel/A
    // a destination that serializes natively is a channel: its items carry
    //   the channel and empty attributes, no number, and every send is an
    //   esd/batch@3 delivery.
    // Verifies: EVS-DEV-delivery-channel/G
    // each delivery is numbered one above the sender channel record, linked
    //   to its hash, at the pre-send fence.
    // Verifies: EVS-DEV-delivery-channel/I
    // the send fence record and the attempt carry the delivery's number and
    //   hash.
    // Verifies: EVS-DEV-delivery-channel/M
    // the head is marked sent on the record naming the delivery it sent,
    //   under its generation, number and hash.
    // Verifies: EVS-PRD-delivery-channel/F
    // an item is marked delivered only on a receiver record naming its
    //   delivery.
    test(
      'deliveries are numbered 1, 2, 3, each linked to the one before',
      () async {
        if (!available) return;
        final d = NativeDestination(id: 'hub');
        await w.activate(d);
        for (var i = 1; i <= 3; i++) {
          await w.note('n$i');
        }
        await w.fill(d);
        final channel = await w.channel('hub');
        final queued = await w.items('hub');
        expect(queued, hasLength(3));
        for (final item in queued) {
          expect(item.wireFormat, DeliveryEnvelope.wireFormat);
          expect(item.envelopeMetadata!.channel, channel);
          expect(item.envelopeMetadata!.attributes, isEmpty);
          expect(item.deliveryNumber, isNull);
        }

        await w.drain(d);

        final deliveries = _sentDeliveries(d);
        expect(
          <int>[for (final x in deliveries) x.deliveryNumber],
          <int>[1, 2, 3],
        );
        expect(deliveries[0].previousDeliveryHash, isNull);
        expect(deliveries[1].previousDeliveryHash, deliveries[0].deliveryHash);
        expect(deliveries[2].previousDeliveryHash, deliveries[1].deliveryHash);
        for (final x in deliveries) {
          expect(x.channel, channel);
          expect(x.attributes, isEmpty);
          expect(x.deliveryHash, x.recomputedDeliveryHash);
        }
        for (final p in d.sent) {
          expect(p.contentType, DeliveryEnvelope.wireFormat);
        }

        final sent = await w.items('hub');
        for (var i = 0; i < 3; i++) {
          final item = sent[i];
          expect(item.finalStatus, FinalStatus.sent);
          expect(item.deliveryGeneration, 1);
          expect(item.deliveryNumber, i + 1);
          expect(item.deliveryHash, deliveries[i].deliveryHash);
          expect(item.attempts.single.deliveryNumber, i + 1);
          expect(item.attempts.single.deliveryHash, deliveries[i].deliveryHash);
        }
        final fence = await w.backend.transaction(
          (txn) => w.backend.readSendFenceTxn(txn, 'hub'),
        );
        expect(fence!.deliveryNumber, 3);
        expect(fence.deliveryHash, deliveries[2].deliveryHash);
        expect(
          await w.senderRecord('hub'),
          SenderChannelRecord(
            generation: 1,
            receiverRecord: _recordOf(deliveries[2]),
            receiverDatabaseId: d.receiverDatabaseId,
          ),
        );
      },
    );

    // Verifies: EVS-DEV-destination-retry-budget/C
    // Verifies: EVS-DEV-destination-retry-budget/D
    test('a destination that declines, then accepts, sends the accepted '
        'delivery at number 1 with no gap', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub')
        ..enqueueScript(
          const SendNotAttempted(reason: 'receiver asked to wait'),
        );
      await w.activate(d);
      await w.note('n1');
      await w.fill(d);

      await w.drain(d);
      expect(d.sent, hasLength(1));
      final pending = (await w.items('hub')).single;
      expect(pending.finalStatus, isNull);
      expect(pending.attempts, isEmpty);
      final senderRecordAfterDecline = await w.senderRecord('hub');
      expect(senderRecordAfterDecline!.receiverRecord, DeliveryRecord.none);

      await w.drain(d);
      expect(d.sent, hasLength(2));
      // Both the declined attempt and the accepted retry carry the same
      // number and link: nothing was written in between, so the second
      // fence recomputed exactly what the first one did.
      final deliveries = _sentDeliveries(d);
      expect(<int>[for (final x in deliveries) x.deliveryNumber], [1, 1]);
      for (final x in deliveries) {
        expect(x.previousDeliveryHash, isNull);
      }

      final sent = (await w.items('hub')).single;
      expect(sent.finalStatus, FinalStatus.sent);
      expect(sent.deliveryNumber, 1);
      expect(sent.attempts.single.deliveryNumber, 1);
    });

    // Verifies: EVS-DEV-delivery-channel/H
    // a pre-send fence that reads a sender channel record other than the one
    //   the payload was built from writes nothing and starts no send; the
    //   payload is built again from the record the fence reads.
    test(
      'a fence that sees a changed sender channel record does not send',
      () async {
        if (!available) return;
        final d = NativeDestination(id: 'hub');
        await w.activate(d);
        await w.note('n1');
        await w.fill(d);
        var fences = 0;
        var changed = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeSendFence: (id) async {
              fences += 1;
              if (changed) return;
              changed = true;
              // Another writer establishes the receiver on the record between
              // the payload's build and the fence.
              await w.backend.transaction(
                (txn) => w.backend.writeSenderChannelRecordTxn(
                  txn,
                  'hub',
                  SenderChannelRecord(
                    generation: 1,
                    receiverRecord: DeliveryRecord.none,
                    receiverDatabaseId: d.receiverDatabaseId,
                  ),
                ),
              );
            },
          ),
          () => w.drain(d),
        );
        expect(
          fences,
          2,
          reason:
              'the first fence wrote nothing and started '
              'no send; the payload was built again',
        );
        expect(d.sent, hasLength(1));
        expect(_sentDeliveries(d).single.deliveryNumber, 1);
      },
    );

    // Verifies: EVS-DEV-delivery-resume/H
    // a record at the next number naming the delivery the send fence names
    //   marks the pending head sent under that delivery.
    // Verifies: EVS-PRD-delivery-channel/F
    // the lost acknowledgement's retry is answered `represented`, whose
    //   record names the delivery, and only then is the item delivered.
    test('a lost acknowledgement marks the in-flight item sent when the retry '
        'is answered represented', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub')..loseAnswer();
      await w.activate(d);
      await w.note('n1');
      await w.fill(d);

      await w.drain(d);
      final pending = (await w.items('hub')).single;
      expect(pending.finalStatus, isNull);
      expect(pending.attempts.single.outcome, 'transient');
      expect(
        d.accepted.values.single,
        hasLength(1),
        reason:
            'the receiver '
            'accepted the delivery',
      );

      await w.drain(d);
      final sentDeliveries = _sentDeliveries(d);
      expect(
        d.sent[1].bytes,
        d.sent[0].bytes,
        reason:
            'the retry re-presents '
            'the same delivery',
      );
      final answer = d.returned[1] as SendAnswered;
      expect(
        (answer.response as ReceiverAcknowledgement).outcome,
        AcknowledgementOutcome.represented,
      );
      final item = (await w.items('hub')).single;
      expect(item.finalStatus, FinalStatus.sent);
      expect(item.deliveryNumber, 1);
      expect(item.deliveryHash, sentDeliveries[0].deliveryHash);
      expect(
        (await w.senderRecord('hub'))!.receiverRecord,
        _recordOf(sentDeliveries[0]),
      );
    });

    /// Delivers `n1` on `hub` as delivery 1, losing its answer to a
    /// permanent refusal so the item wedges; recovers the wedged item and
    /// refills it with `n2`; and drains the refilled item, whose delivery 1
    /// differs from the accepted one, so the receiver answers with its
    /// record naming the wedged (now tombstoned) item's attempt.
    Future<(NativeDestination, FifoEntry, FifoEntry)>
    adoptTombstonedAttempt() async {
      final d = NativeDestination(id: 'hub', batchCapacity: 5)
        ..loseAnswer(const SendPermanent(error: 'answer lost'));
      await w.activate(d);
      await w.note('n1');
      await w.fill(d);
      await w.drain(d);
      final wedged = (await w.items('hub')).single;
      expect(wedged.finalStatus, FinalStatus.wedged);
      await w.note('n2');
      await w.registry.tombstoneAndRefill(
        'hub',
        wedged.entryId,
        initiator: _init,
      );
      await w.fill(d);
      final refilled = (await w.items('hub')).last;
      expect(refilled.eventIds, hasLength(2));
      await w.drain(d);
      return (d, wedged, refilled);
    }

    // Verifies: EVS-DEV-delivery-resume/H
    // a record at the next number naming an attempt a tombstoned item of the
    //   registration carries is adopted, with the responding receiver, and
    //   marks nothing sent.
    // Verifies: EVS-DEV-delivery-channel/N
    // an out_of_sequence refusal is read as the receiver's record, never as
    //   a permanent failure.
    // Verifies: EVS-PRD-delivery-channel/F
    // the record names the tombstoned item's delivery, not the head's, so
    //   the head is not delivered.
    test("an adopted record naming a tombstoned item's attempt marks nothing "
        'sent', () async {
      if (!available) return;
      final (d, wedged, refilled) = await adoptTombstonedAttempt();
      final deliveries = _sentDeliveries(d);
      expect(deliveries, hasLength(2));
      expect(deliveries[1].deliveryNumber, 1);
      expect(deliveries[1].deliveryHash, isNot(deliveries[0].deliveryHash));
      final answer = (d.returned[1] as SendAnswered).response;
      expect(answer, isA<ReceiverRefusal>());
      expect(answer.record, _recordOf(deliveries[0]));

      final items = await w.items('hub');
      expect(
        items.firstWhere((i) => i.entryId == wedged.entryId).finalStatus,
        FinalStatus.tombstoned,
      );
      final head = items.firstWhere((i) => i.entryId == refilled.entryId);
      expect(head.finalStatus, isNull, reason: 'nothing is marked sent');
      expect(head.attempts.single.deliveryNumber, 1);
      expect(
        await w.senderRecord('hub'),
        SenderChannelRecord(
          generation: 1,
          receiverRecord: _recordOf(deliveries[0]),
          receiverDatabaseId: d.receiverDatabaseId,
        ),
      );
      expect(await w.findings(), isEmpty);

      // The refilled item is delivered after the adopted record.
      await w.drain(d);
      final sentItem = (await w.items(
        'hub',
      )).firstWhere((i) => i.entryId == refilled.entryId);
      expect(sentItem.finalStatus, FinalStatus.sent);
      expect(sentItem.deliveryNumber, 2);
      expect(
        _sentDeliveries(d).last.previousDeliveryHash,
        deliveries[0].deliveryHash,
      );
    });

    // Verifies: EVS-DEV-delivery-channel/Q
    // an accepting outcome with no record wedges the head with cause
    //   acknowledgement_invalid in the attempt's transaction.
    // Verifies: EVS-PRD-destinations/Q
    // the wedge event records the cause acknowledgement_invalid.
    test(
      'an acceptance carrying no record wedges acknowledgement_invalid',
      () async {
        if (!available) return;
        final d = NativeDestination(
          id: 'hub',
          script: <SendResult>[const SendOk()],
        );
        await w.activate(d);
        await w.note('n1');
        await w.fill(d);
        await w.drain(d);
        final item = (await w.items('hub')).single;
        expect(item.finalStatus, FinalStatus.wedged);
        expect(item.attempts.single.outcome, 'ok');
        expect(item.attempts.single.deliveryNumber, 1);
        final wedges = await w.store.reader.findAllEvents(
          entryType: kDestinationWedgedEntryType,
        );
        expect(wedges.single.data['cause'], 'acknowledgement_invalid');
        expect(wedges.single.data['row_id'], item.entryId);
        final record = await w.backend.transaction(
          (txn) => w.backend.readWedgeRecordTxn(txn, 'hub'),
        );
        expect(record!.cause, WedgeCause.acknowledgementInvalid);
        expect(await w.senderRecord('hub'), SenderChannelRecord.initial);
      },
    );

    // Verifies: EVS-DEV-delivery-channel/N
    // a delivery_hash_mismatch refusal is retried as a transient failure; a
    //   rejected refusal wedges the head as a permanent failure.
    test(
      'a delivery hash mismatch is retried, a rejected delivery wedges',
      () async {
        if (!available) return;
        final channel = DeliveryChannel(
          senderDatabaseId: w.store.databaseId,
          destinationId: 'hub',
          registrationId: 'r',
          generation: 1,
        );
        ReceiverRefusal refusal(RefusalKind kind) => ReceiverRefusal(
          channel: channel,
          receiverDatabaseId: 'native-receiver',
          record: DeliveryRecord.none,
          refusal: kind,
          reason: kind == RefusalKind.rejected ? 'validation_failed' : null,
        );
        final d = NativeDestination(
          id: 'hub',
          script: <SendResult>[
            decodeReceiverAnswer(
              refusal(RefusalKind.deliveryHashMismatch).encode(),
            ),
          ],
        );
        await w.activate(d);
        await w.note('n1');
        await w.fill(d);
        await w.drain(d);
        var item = (await w.items('hub')).single;
        expect(item.finalStatus, isNull);
        expect(item.attempts.single.outcome, 'transient');
        await w.drain(d);
        item = (await w.items('hub')).single;
        expect(item.finalStatus, FinalStatus.sent);
        expect(item.deliveryNumber, 1);

        d.enqueueScript(
          decodeReceiverAnswer(refusal(RefusalKind.rejected).encode()),
        );
        await w.note('n2');
        await w.fill(d);
        await w.drain(d);
        final second = (await w.items('hub')).last;
        expect(second.finalStatus, FinalStatus.wedged);
        expect(second.attempts.single.outcome, 'permanent');
      },
    );

    // Verifies: EVS-DEV-delivery-resume/I
    // a record below the sender's, every number above it retained and the
    //   first linking to its hash, resumes the channel.
    // Verifies: EVS-DEV-delivery-resume/M
    // one resend item per number, in ascending order, carrying the retained
    //   delivery's events and attributes.
    // Verifies: EVS-DEV-delivery-resume/N
    // the resume retires the pending items (tombstoning one that carries
    //   attempts), rewinds the fill below their lowest event, removes the
    //   transform failure record, enqueues the resend items, sets the
    //   sender channel record to the receiver's and appends one resume
    //   event, in one transaction.
    // Verifies: EVS-DEV-delivery-resume/W
    // the retained delivery at a number is the item last marked sent there
    //   under the current generation.
    // Verifies: EVS-PRD-delivery-channel/H
    // every retained delivery after the receiver's record is sent again with
    //   the bytes it was first sent with, and the resume is one event.
    // Verifies: EVS-DEV-resume-event/A
    // the resume event carries exactly id, database_id, registration_id,
    //   generation, resume_after, previous_record and drainer_epoch.
    // Verifies: EVS-DEV-destination-drain/E
    // the resume enqueues the resend items; the fill enqueues the retired
    //   events again after them.
    test('a receiver behind gets the retained deliveries again exactly as '
        'sent, recorded in one resume event', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub');
      await w.activate(d);
      for (var i = 1; i <= 3; i++) {
        await w.note('n$i');
      }
      await w.deliverAll(d);
      final original = List<WirePayload>.of(d.sent);
      final first = _sentDeliveries(d);
      final channel = await w.channel('hub');
      d.restoreReceiverTo(channel, 1);

      final n4 = await w.note('n4');
      final n5 = await w.note('n5');
      await w.fill(d);
      final pending = (await w.items('hub')).skip(3).toList();
      expect(pending, hasLength(2));
      // A transform failure record left from before the resume is written
      // directly here; a receiver-behind resume removes it with the rewind.
      await w.backend.transaction(
        (txn) => w.backend.writeTransformFailureRecordTxn(
          txn,
          'hub',
          TransformFailureRecord(
            failureTimes: [DateTime.utc(2027, 1, 1)],
            sequenceRange: (firstSeq: 1, lastSeq: 1),
          ),
        ),
      );
      await w.drain(d);

      final items = await w.items('hub');
      expect(
        items.firstWhere((i) => i.entryId == pending[0].entryId).finalStatus,
        FinalStatus.tombstoned,
        reason: 'the pending item carrying an attempt is tombstoned',
      );
      expect(
        items.where((i) => i.entryId == pending[1].entryId),
        isEmpty,
        reason: 'the pending item carrying no attempt is deleted',
      );
      expect(
        await w.backend.readFillCursor('hub'),
        n4.sequenceNumber - 1,
        reason: "the fill is rewound below the retired item's events",
      );
      expect(
        await w.senderRecord('hub'),
        SenderChannelRecord(
          generation: 1,
          receiverRecord: _recordOf(first[0]),
          receiverDatabaseId: d.receiverDatabaseId,
        ),
      );
      final resumes = await w.resumeEvents();
      expect(resumes, hasLength(1));
      expect(resumes.single.data.keys.toSet(), _resumeEventKeys);
      expect(resumes.single.data, <String, Object?>{
        'id': 'hub',
        'database_id': w.store.databaseId,
        'registration_id': channel.registrationId,
        'generation': 1,
        'resume_after': _recordOf(first[0]).toJson(),
        'previous_record': _recordOf(first[2]).toJson(),
        'drainer_epoch': await w.drainEpoch(),
      });

      await w.deliverAll(d);
      // The resend of deliveries 2 and 3 carries the bytes first sent.
      final resent = d.sent.skip(4).take(2).toList();
      expect(resent[0].bytes, original[1].bytes);
      expect(resent[1].bytes, original[2].bytes);
      expect(
        <int>[for (final x in d.accepted[channel]!) x.deliveryNumber],
        <int>[1, 2, 3, 4, 5],
      );
      expect(
        <Object?>[
          for (final x in d.accepted[channel]!.skip(3))
            x.events.single['event_id'],
        ],
        <String>[n4.eventId, n5.eventId],
      );
      expect(await w.resumeEvents(), hasLength(1));
      expect(await w.findings(), isEmpty);
      expect(await w.transformFailureRecord('hub'), isNull);
    });

    // Verifies: EVS-DEV-delivery-resume/Y
    // a record below the sender's at a number the sender adopted, with no
    //   retained delivery there, starts a new generation with a
    //   channel_unexplained finding.
    // Verifies: EVS-DEV-delivery-resume/W
    // an adopted number has no retained delivery.
    // Verifies: EVS-DEV-delivery-resume/Z
    // the new generation records the finding under the role sender, retires
    //   the pending items, rewinds the fill to the start and sets the record
    //   to the next generation at number 0 with the responding receiver.
    // Verifies: EVS-PRD-delivery-channel/X
    // a record that calls for no resend continues the registration on a new
    //   generation from delivery 1, filled again from the start.
    // Verifies: EVS-PRD-delivery-channel/L
    // a receiver behind that the sender cannot resend to is realigned by a
    //   new generation.
    test('a record behind at an adopted, unretained number is unexplained and '
        'starts generation 2', () async {
      if (!available) return;
      final (d, _, _) = await adoptTombstonedAttempt();
      await w.drain(d);
      final before = await w.senderRecord('hub');
      expect(before!.receiverRecord.deliveryNumber, 2);
      final channel = await w.channel('hub');
      d.restoreReceiverTo(channel, 0);

      await w.note('n3');
      await w.fill(d);
      final retired = (await w.items('hub')).last;
      await w.drain(d);

      final findings = await w.findings();
      expect(findings, hasLength(1));
      final data = findings.single.data;
      final evidence = <String, Object?>{
        'channel': channel.toJson(),
        'sender_record': before.receiverRecord.toJson(),
        'receiver_record': DeliveryRecord.none.toJson(),
        'recorded_receiver_database_id': d.receiverDatabaseId,
        'responding_receiver_database_id': d.receiverDatabaseId,
      };
      expect(data['kind'], 'channel_unexplained');
      expect(data['evidence'], evidence);
      expect((data['detector']! as Map)['role'], 'sender');
      expect(
        data['finding_id'],
        expectedFindingId(
          databaseId: w.store.databaseId,
          role: 'sender',
          kind: 'channel_unexplained',
          evidence: evidence,
        ),
      );
      expect(
        (await w.items(
          'hub',
        )).firstWhere((i) => i.entryId == retired.entryId).finalStatus,
        FinalStatus.tombstoned,
      );
      expect(await w.backend.readFillCursor('hub'), -1);
      expect(
        await w.senderRecord('hub'),
        SenderChannelRecord(
          generation: 2,
          receiverRecord: DeliveryRecord.none,
          receiverDatabaseId: d.receiverDatabaseId,
        ),
      );

      await w.deliverAll(d);
      final generation2 = await w.channel('hub', generation: 2);
      final accepted = d.accepted[generation2]!;
      expect(accepted.first.deliveryNumber, 1);
      expect(
        <String>{
          for (final x in accepted)
            for (final e in x.events) e['event_id']! as String,
        },
        containsAll(<String>[
          for (final e in await w.store.reader.findAllEvents(
            entryType: _noteType,
          ))
            e.eventId,
        ]),
        reason: 'generation 2 is filled again from the start of the log',
      );
      expect(await w.findings(), hasLength(1));
    });

    // Verifies: EVS-DEV-delivery-resume/I
    // a record below the sender's resumes the channel only when the
    //   retained delivery numbered one above it links to its hash; a record
    //   of a forked or foreign receiver history, whose hash the retained
    //   delivery does not link to, does not resume it.
    // Verifies: EVS-DEV-delivery-resume/Y
    // a record below the sender's that assertion I does not resume starts a
    //   new generation with a channel_unexplained finding.
    // Verifies: EVS-PRD-delivery-channel/L
    // a receiver behind whose record does not prove a common point by
    //   content is realigned by a new generation, not a resume.
    test('a record behind whose hash the retained delivery does not link to '
        'is unexplained and starts generation 2', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub');
      await w.activate(d);
      for (var i = 1; i <= 3; i++) {
        await w.note('n$i');
      }
      await w.deliverAll(d);
      final channel = await w.channel('hub');
      final before = await w.senderRecord('hub');
      // A record naming delivery 1 of a forked or foreign receiver history:
      // its hash is not the one the retained delivery 2 links to.
      final forked = DeliveryRecord(deliveryNumber: 1, deliveryHash: 'f' * 64);
      d.enqueueScript(
        SendAnswered(
          ReceiverRefusal(
            channel: channel,
            receiverDatabaseId: d.receiverDatabaseId,
            record: forked,
            refusal: RefusalKind.outOfSequence,
          ),
        ),
      );
      await w.note('n4');
      await w.fill(d);
      await w.drain(d);

      expect(
        await w.resumeEvents(),
        isEmpty,
        reason:
            'the forked record does not prove a common point by '
            'content, so no resume is recorded',
      );
      final findings = await w.findings();
      expect(findings, hasLength(1));
      final data = findings.single.data;
      expect(data['kind'], 'channel_unexplained');
      expect(data['evidence'], <String, Object?>{
        'channel': channel.toJson(),
        'sender_record': before!.receiverRecord.toJson(),
        'receiver_record': forked.toJson(),
        'recorded_receiver_database_id': d.receiverDatabaseId,
        'responding_receiver_database_id': d.receiverDatabaseId,
      });
      expect((data['detector']! as Map)['role'], 'sender');
      expect(
        await w.senderRecord('hub'),
        SenderChannelRecord(
          generation: 2,
          receiverRecord: DeliveryRecord.none,
          receiverDatabaseId: d.receiverDatabaseId,
        ),
      );
    });

    // Verifies: EVS-DEV-delivery-resume/K
    // a record ahead of the sender's naming no delivery the sender attempted
    //   starts a new generation with a sender_regressed finding.
    // Verifies: EVS-PRD-delivery-channel/I
    // the finding of sender regression names both records.
    // Verifies: EVS-DEV-delivery-resume/Z
    // the new generation starts at delivery 1 with the fill at the start
    //   and removes the transform failure record.
    test('a record ahead naming nothing attempted records one sender_regressed '
        'finding and starts generation 2 at delivery 1', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub');
      await w.activate(d);
      await w.note('n1');
      await w.deliverAll(d);
      final channel = await w.channel('hub');
      final before = await w.senderRecord('hub');
      final ahead = DeliveryRecord(deliveryNumber: 5, deliveryHash: 'a' * 64);
      d.enqueueScript(
        SendAnswered(
          ReceiverRefusal(
            channel: channel,
            receiverDatabaseId: d.receiverDatabaseId,
            record: ahead,
            refusal: RefusalKind.outOfSequence,
          ),
        ),
      );
      await w.note('n2');
      await w.note('n3');
      await w.fill(d);
      final retiring = (await w.items('hub')).skip(1).toList();
      expect(retiring, hasLength(2));
      // A transform failure record left from before the new generation is
      // written directly here; a new generation removes it with the rewind.
      await w.backend.transaction(
        (txn) => w.backend.writeTransformFailureRecordTxn(
          txn,
          'hub',
          TransformFailureRecord(
            failureTimes: [DateTime.utc(2027, 1, 1)],
            sequenceRange: (firstSeq: 1, lastSeq: 1),
          ),
        ),
      );
      await w.drain(d);

      final findings = await w.findings();
      expect(findings, hasLength(1));
      expect(findings.single.data['kind'], 'sender_regressed');
      expect(findings.single.data['evidence'], <String, Object?>{
        'channel': channel.toJson(),
        'sender_record': before!.receiverRecord.toJson(),
        'receiver_record': ahead.toJson(),
        'recorded_receiver_database_id': d.receiverDatabaseId,
        'responding_receiver_database_id': d.receiverDatabaseId,
      });
      expect((findings.single.data['detector']! as Map)['role'], 'sender');
      expect(
        await w.senderRecord('hub'),
        SenderChannelRecord(
          generation: 2,
          receiverRecord: DeliveryRecord.none,
          receiverDatabaseId: d.receiverDatabaseId,
        ),
      );
      expect(await w.backend.readFillCursor('hub'), -1);
      final pending = [
        for (final i in await w.items('hub'))
          if (i.finalStatus == null) i,
      ];
      expect(pending, isEmpty, reason: 'the pending items are retired');
      final after = await w.items('hub');
      expect(
        after.firstWhere((i) => i.entryId == retiring[0].entryId).finalStatus,
        FinalStatus.tombstoned,
      );
      expect(after.where((i) => i.entryId == retiring[1].entryId), isEmpty);
      expect(await w.transformFailureRecord('hub'), isNull);

      final sentBefore = d.sent.length;
      await w.deliverAll(d);
      final generation2 = _sentDeliveries(d).skip(sentBefore).toList();
      expect(generation2.first.channel.generation, 2);
      expect(generation2.first.deliveryNumber, 1);
      expect(generation2.first.previousDeliveryHash, isNull);
      expect(await w.findings(), hasLength(1));
    });

    // Verifies: EVS-DEV-delivery-resume/Y
    // a record more than one ahead naming an attempt the sender made before
    //   a resume moved its record back is unexplained, not a regression.
    test('a record ahead naming a superseded attempt is unexplained', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub');
      await w.activate(d);
      await w.note('n1');
      await w.note('n2');
      await w.deliverAll(d);
      final channel = await w.channel('hub');
      d.restoreReceiverTo(channel, 0);
      await w.note('n3');
      await w.fill(d);
      await w.drain(d); // delivery 3 is refused; the channel resumes at 0.
      final superseded = _sentDeliveries(d).last;
      expect(superseded.deliveryNumber, 3);
      expect(await w.resumeEvents(), hasLength(1));

      // The receiver comes forward again, holding the superseded delivery.
      d.enqueueScript(
        SendAnswered(
          ReceiverAcknowledgement(
            channel: channel,
            receiverDatabaseId: d.receiverDatabaseId,
            record: _recordOf(superseded),
            outcome: AcknowledgementOutcome.accepted,
          ),
        ),
      );
      await w.drain(d);

      final findings = await w.findings();
      expect(findings, hasLength(1));
      expect(findings.single.data['kind'], 'channel_unexplained');
      expect((await w.senderRecord('hub'))!.generation, 2);
    });

    // Verifies: EVS-DEV-delivery-resume/Y
    // a response from another receiver database than the one the sender
    //   channel record holds is unexplained, whatever its record.
    // Verifies: EVS-DEV-delivery-resume/Z
    // the new generation holds the responding receiver identity.
    test('another receiver identity is unexplained', () async {
      if (!available) return;
      final d = NativeDestination(id: 'hub');
      await w.activate(d);
      await w.note('n1');
      await w.deliverAll(d);
      final channel = await w.channel('hub');
      final before = await w.senderRecord('hub');
      await w.note('n2');
      await w.fill(d);
      // The answer names the delivery in flight, from another receiver.
      final inFlight = DeliveryEnvelope.seal(
        batchId: 'x',
        senderHop: 'h',
        senderIdentifier: 'i',
        senderSoftwareVersion: 's',
        sentAt: DateTime.utc(2026),
        channel: channel,
        deliveryNumber: 2,
        previousDeliveryHash: before!.receiverRecord.deliveryHash,
        events: <Map<String, Object?>>[
          (await w.store.reader.findAllEvents(
            entryType: _noteType,
          )).last.toMap(),
        ],
      );
      d.enqueueScript(
        SendAnswered(
          ReceiverAcknowledgement(
            channel: channel,
            receiverDatabaseId: 'other-receiver',
            record: _recordOf(inFlight),
            outcome: AcknowledgementOutcome.accepted,
          ),
        ),
      );
      await w.drain(d);

      final findings = await w.findings();
      expect(findings, hasLength(1));
      expect(findings.single.data['kind'], 'channel_unexplained');
      expect(
        (findings.single.data['evidence']!
            as Map)['recorded_receiver_database_id'],
        d.receiverDatabaseId,
      );
      expect(
        (findings.single.data['evidence']!
            as Map)['responding_receiver_database_id'],
        'other-receiver',
      );
      expect(
        await w.senderRecord('hub'),
        const SenderChannelRecord(
          generation: 2,
          receiverRecord: DeliveryRecord.none,
          receiverDatabaseId: 'other-receiver',
        ),
      );
      final n2 = (await w.items('hub')).last;
      expect(
        n2.finalStatus,
        FinalStatus.tombstoned,
        reason: "nothing is marked sent on another receiver's record",
      );
    });
  });
}
