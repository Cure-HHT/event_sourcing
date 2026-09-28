// End to end over three event stores on Sembast: a predecessor delivers to
// a receiver's ReceiverEndpoint, a fresh successor restores the
// predecessor's deliveries from that receiver and registers its own native
// destination to the same receiver, then appends a new version of the
// aggregate the predecessor started and delivers it.
//
// Exercises, with no crafted events, that the fill enqueues the successor's
// succession event and its own new event but never re-enqueues the
// restored predecessor event, that the new event's causal parent names the
// predecessor's version with no fork finding, and that the receiver ends up
// holding one continuous aggregate history across both identities.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/queue_test_support.dart' show cycleOnce;
import '../test_support/test_backends.dart';

const Initiator _init = AutomationInitiator(service: 'succession-successor');
const String _noteType = 'succession_successor_note';

EntryTypeRegistry _entryTypes() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: _noteType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _noteType,
    ),
  );

/// A native destination that presents every delivery, and every pull, to
/// [receiver]'s receiver endpoint, authenticated for whichever sender
/// identity the request names. Records every payload it sends, in order.
final class _HubDestination extends Destination {
  _HubDestination(
    this.receiver, {
    this.id = 'hub',
    this.additionalSenderIds = const <String>{},
  });

  final EventStore receiver;

  @override
  final String id;

  /// Identities the deployment's authentication also binds this caller to,
  /// beyond the channel's own sender: a succession delivery needs the
  /// caller authenticated for the predecessor too
  /// (`EVS-DEV-sender-succession/F`).
  final Set<String> additionalSenderIds;

  @override
  SubscriptionFilter get filter => const SubscriptionFilter();

  @override
  String get wireFormat => DeliveryEnvelope.wireFormat;

  @override
  bool get serializesNatively => true;

  @override
  Duration get maxAccumulateTime => Duration.zero;

  final List<DeliveryEnvelope> sent = <DeliveryEnvelope>[];

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw StateError('a native destination has no transform');

  @override
  ChannelPull get channelPull => _pull;

  Future<PullOutcome> _pull(PullRequest request) async {
    final sender = switch (request) {
      ChannelListingPull(:final senderDatabaseId) => senderDatabaseId,
      DeliveryRangePull(:final channel) => channel.senderDatabaseId,
    };
    final response = await receiver.receiverEndpoint.pull(
      request,
      senderDatabaseIds: <String>{sender, ...additionalSenderIds},
    );
    return decodePullResponse(response.encode());
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    final envelope = DeliveryEnvelope.decode(payload.bytes);
    sent.add(envelope);
    final answer = await receiver.receiverEndpoint.accept(
      payload.bytes,
      senderDatabaseIds: <String>{
        envelope.channel.senderDatabaseId,
        ...additionalSenderIds,
      },
    );
    return decodeReceiverAnswer(answer.encode());
  }
}

Future<EventStore> _open(Source source) async {
  final backend = SembastBackend(
    database: await newDatabaseFactoryMemory().openDatabase(
      '${source.identifier}.db',
    ),
  );
  return trackTestBackend(
    await EventStore.open(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      entryTypes: _entryTypes(),
      source: source,
    ),
    backend,
  );
}

void main() {
  late EventStore predecessor;
  late EventStore receiver;
  late EventStore successor;
  late DestinationRegistry predecessorRegistry;
  late DestinationRegistry successorRegistry;
  late _HubDestination predecessorHub;
  late _HubDestination successorHub;

  setUp(() async {
    predecessor = await _open(
      const Source(
        hopId: 'predecessor-hop',
        identifier: 'predecessor-install',
        softwareVersion: 'app@1.0.0',
      ),
    );
    receiver = await _open(
      const Source(
        hopId: 'receiver-hop',
        identifier: 'receiver-install',
        softwareVersion: 'server@1.0.0',
      ),
    );

    predecessorHub = _HubDestination(receiver, id: 'predecessor-hub');
    predecessorRegistry = DestinationRegistry(eventStore: predecessor);
    await predecessorRegistry.addDestination(predecessorHub, initiator: _init);
    await predecessorRegistry.setStartDate(
      predecessorHub.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  });

  tearDown(() async {
    await predecessor.close();
    await receiver.close();
    await successor.close();
  });

  Future<StoredEvent> note(
    EventStore store,
    String aggregateId,
    String eventType,
  ) async => (await store.append(
    entryType: _noteType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: eventType,
    data: <String, Object?>{'aggregate_id': aggregateId},
    initiator: _init,
  ))!;

  Future<void> deliver(DestinationRegistry registry, {int passes = 4}) async {
    for (var i = 0; i < passes; i++) {
      await cycleOnce(registry, clock: () => DateTime.utc(2100));
    }
  }

  /// Restores a fresh successor from [receiver], holding [predecessor]'s
  /// deliveries, and registers its own native destination to [receiver].
  Future<void> restoreSuccessor() async {
    successor = await _open(
      const Source(
        hopId: 'successor-hop',
        identifier: 'successor-install',
        softwareVersion: 'app@2.0.0',
      ),
    );
    successorHub = _HubDestination(
      receiver,
      id: 'successor-hub',
      additionalSenderIds: <String>{predecessor.databaseId},
    );
    successorRegistry = DestinationRegistry(eventStore: successor);
    await successorRegistry.addDestination(successorHub, initiator: _init);
    await successorRegistry.setStartDate(
      successorHub.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
    await successor.restoreFromReceiver(
      registry: successorRegistry,
      destinationId: successorHub.id,
      predecessorDatabaseId: predecessor.databaseId,
      initiator: _init,
    );
  }

  // Verifies: EVS-PRD-delivery-channel/U
  // Verifies: EVS-DEV-destination-drain/X
  // the successor's fill enqueues the succession event on its natively
  //   serializing registration, whatever the destination's filter.
  // Verifies: EVS-DEV-destination-drain/V
  // a restored predecessor event, holding more than one provenance entry,
  //   is never enqueued again on the successor's channel.
  // Verifies: EVS-PRD-destinations/C
  // the successor's queue carries only what it authored (the succession
  //   event and its own new event), not an event it did not author.
  test('the successor enqueues its succession event and its own new event, '
      'never the restored predecessor event', () async {
    final a1 = await note(predecessor, 'agg1', 'created');
    await deliver(predecessorRegistry);
    expect(
      await receiver.reader.findEventById(a1.eventId),
      isNotNull,
      reason:
          'the receiver holds the predecessor event before the '
          'restore',
    );

    await restoreSuccessor();
    expect(
      await successor.reader.findEventById(a1.eventId),
      isNotNull,
      reason: 'the restore brought the predecessor event across',
    );

    final b1 = await note(successor, 'agg1', 'amended');
    await deliver(successorRegistry);

    final sentEntryTypes = <String>{
      for (final e in successorHub.sent)
        for (final ev in e.events) ev['entry_type']! as String,
    };
    expect(
      sentEntryTypes,
      containsAll(<String>[_noteType, kDestinationSenderSucceededEntryType]),
    );

    final sentEventIds = <String>{
      for (final e in successorHub.sent)
        for (final ev in e.events) ev['event_id']! as String,
    };
    expect(
      sentEventIds,
      isNot(contains(a1.eventId)),
      reason: 'the restored predecessor event is never re-enqueued',
    );
    expect(sentEventIds, contains(b1.eventId));
    final successionEventIds = <String>{
      for (final e in await successor.reader.findAllEvents(
        entryType: kDestinationSenderSucceededEntryType,
      ))
        e.eventId,
    };
    expect(successionEventIds, isNotEmpty);
    expect(sentEventIds, containsAll(successionEventIds));
  });

  // Verifies: EVS-PRD-delivery-channel/T
  // a version the successor appends to a predecessor's aggregate names the
  //   predecessor's latest version as its causal parent, with no fork
  //   finding: authorship continues through the ordinary stamping rule and
  //   the succession lineage read, with no separate canonicalization code.
  test("a version the successor appends names the predecessor's version as "
      'its causal parent, with no fork finding', () async {
    final a1 = await note(predecessor, 'agg1', 'created');
    await deliver(predecessorRegistry);
    await restoreSuccessor();

    final b1 = await note(successor, 'agg1', 'amended');
    final stored = await successor.reader.findEventById(b1.eventId);
    expect(stored, isNotNull);
    expect(stored!.causal!.parents.map((p) => p.eventId), <String>[a1.eventId]);
    expect(stored.causal!.parents.single.eventHash, a1.eventHash);

    await deliver(successorRegistry);

    for (final store in <EventStore>[predecessor, receiver, successor]) {
      expect(
        await store.reader.findAllEvents(entryType: kSecurityFindingEntryType),
        isEmpty,
        reason: 'no fork or reused-position finding is recorded anywhere',
      );
    }
  });

  // Verifies: EVS-PRD-delivery-channel/T
  // the receiver ingests the successor's delivery and ends up holding one
  //   continuous history of the aggregate across both identities: the
  //   Layer 2 TREAT of authorship-continuation, realised by the lineage
  //   read and the stamping rule rather than separate canonicalization
  //   code.
  test('the receiver ends up holding one continuous aggregate history across '
      'the predecessor and the successor', () async {
    final a1 = await note(predecessor, 'agg1', 'created');
    await deliver(predecessorRegistry);
    await restoreSuccessor();
    final b1 = await note(successor, 'agg1', 'amended');
    await deliver(successorRegistry);

    final held = await receiver.reader.findEventsForAggregate('agg1');
    expect(held.map((e) => e.eventId).toSet(), <String>{
      a1.eventId,
      b1.eventId,
    });
    final heldB1 = held.firstWhere((e) => e.eventId == b1.eventId);
    expect(heldB1.causal!.parents.map((p) => p.eventId), <String>[a1.eventId]);
  });
}
