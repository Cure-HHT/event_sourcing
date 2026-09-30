// End to end over two event stores on Sembast: a sender's delivery cycle
// delivers a natively serializing destination to a receiver's
// ReceiverEndpoint, and the receiver (and, in the second scenario, the
// sender too) is restored from a snapshot of its database taken earlier.
// The sender reads the receiver's record from the answer to its next
// delivery and resends exactly what the receiver lacks.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:sembast/utils/sembast_import_export.dart';

import '../test_support/queue_test_support.dart' show cycleOnce;
import '../test_support/test_backends.dart';

const Initiator _init = AutomationInitiator(service: 'end-to-end');
const String _noteType = 'e2e_note';

EntryTypeRegistry _entryTypes() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: _noteType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _noteType,
    ),
  );

/// One Sembast database an event store is opened over, and can be
/// snapshotted and restored.
final class _Side {
  _Side(this.name, this.source);

  final String name;
  final Source source;
  final DatabaseFactory _factory = newDatabaseFactoryMemory();
  late Database db;
  late EventStore store;
  var _restores = 0;

  Future<void> create() async {
    db = await _factory.openDatabase('$name.db');
    await _open();
  }

  Future<void> _open() async {
    final backend = SembastBackend(database: db);
    store = trackTestBackend(
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

  Future<Map<String, Object?>> snapshot() => exportDatabase(db);

  /// Closes the store and reopens it over a database restored from
  /// [snapshot], as a restore of the database from a backup does.
  Future<void> restore(Map<String, Object?> snapshot) async {
    await store.close();
    await db.close();
    _restores += 1;
    db = await importDatabase(snapshot, _factory, '$name-$_restores.db');
    await _open();
  }

  Future<void> close() async {
    await store.close();
    await db.close();
  }
}

/// A destination that serializes natively and presents each delivery to
/// [receiver]'s receiver endpoint, authenticated for the sender the
/// delivery's channel names, passing the answer through the library's
/// decoder, as a transport carrying the receiver's answer does.
final class _EndpointDestination extends Destination {
  _EndpointDestination(
    this.receiver, {
    this.additionalSenderIds = const <String>{},
  });

  final _Side receiver;

  /// Identities the deployment's authentication also binds this caller to,
  /// beyond the channel's own sender: a succession delivery needs the
  /// caller authenticated for the predecessor too
  /// (`EVS-DEV-sender-succession/F`).
  final Set<String> additionalSenderIds;

  /// Every payload handed to [send], in order.
  final List<WirePayload> sent = <WirePayload>[];

  @override
  String get id => 'hub';

  @override
  SubscriptionFilter get filter => const SubscriptionFilter();

  @override
  String get wireFormat => DeliveryEnvelope.wireFormat;

  @override
  bool get serializesNatively => true;

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw StateError('a native destination has no transform');

  @override
  ChannelPull get channelPull => (request) async {
    final sender = switch (request) {
      ChannelListingPull(:final senderDatabaseId) => senderDatabaseId,
      DeliveryRangePull(:final channel) => channel.senderDatabaseId,
    };
    final response = await receiver.store.receiverEndpoint.pull(
      request,
      senderDatabaseIds: <String>{sender, ...additionalSenderIds},
    );
    return decodePullResponse(response.encode());
  };

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    final sender = DeliveryEnvelope.decode(
      payload.bytes,
    ).channel.senderDatabaseId;
    final answer = await receiver.store.receiverEndpoint.accept(
      payload.bytes,
      senderDatabaseIds: <String>{sender, ...additionalSenderIds},
    );
    return decodeReceiverAnswer(answer.encode());
  }
}

void main() {
  late _Side sender;
  late _Side receiver;
  _Side? successor;
  late _EndpointDestination hub;
  late _EndpointDestination successorHub;
  late DestinationRegistry registry;

  Future<void> register() async {
    registry = DestinationRegistry(eventStore: sender.store);
    await registry.addDestination(hub, initiator: _init);
  }

  setUp(() async {
    sender = _Side(
      'sender',
      const Source(
        hopId: 'mobile-device',
        identifier: 'sender-install',
        softwareVersion: 'app@1.0.0',
      ),
    );
    receiver = _Side(
      'receiver',
      const Source(
        hopId: 'server',
        identifier: 'receiver-install',
        softwareVersion: 'server@1.0.0',
      ),
    );
    await sender.create();
    await receiver.create();
    hub = _EndpointDestination(receiver);
    await register();
    await registry.setStartDate(
      'hub',
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  });

  tearDown(() async {
    await sender.close();
    await receiver.close();
    final side = successor;
    if (side != null) await side.close();
  });

  Future<StoredEvent> note(String id) async {
    return (await sender.store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id},
      initiator: _init,
    ))!;
  }

  Future<void> deliver({int passes = 4}) async {
    for (var i = 0; i < passes; i++) {
      await cycleOnce(registry, clock: () => DateTime.utc(2100));
    }
  }

  Future<List<String>> receivedNotes() async => <String>[
    for (final e in await receiver.store.reader.findAllEvents(
      entryType: _noteType,
    ))
      e.data['id']! as String,
  ];

  Future<ListedChannel> listedChannel() async {
    final listing =
        await receiver.store.receiverEndpoint.pull(
              ChannelListingPull(senderDatabaseId: sender.store.databaseId),
              senderDatabaseIds: <String>{sender.store.databaseId},
            )
            as ChannelListing;
    return listing.channels.single;
  }

  List<DeliveryEnvelope> decoded(Iterable<WirePayload> payloads) =>
      <DeliveryEnvelope>[
        for (final p in payloads) DeliveryEnvelope.decode(p.bytes),
      ];

  /// The findings [side] holds of kind [kind] (the security finding's wire
  /// `kind`, e.g. `FindingKind.senderRegressed.wire`).
  Future<List<Map<String, Object?>>> findingsOf(_Side side, String kind) async {
    return <Map<String, Object?>>[
      for (final e in await side.store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      ))
        if (e.data['kind'] == kind) e.data,
    ];
  }

  /// Every channel [receiver] lists for [senderDatabaseId] and its
  /// succession lineage, across every generation.
  Future<List<ListedChannel>> allListedChannels(String senderDatabaseId) async {
    final listing =
        await receiver.store.receiverEndpoint.pull(
              ChannelListingPull(senderDatabaseId: senderDatabaseId),
              senderDatabaseIds: <String>{senderDatabaseId},
            )
            as ChannelListing;
    return listing.channels;
  }

  /// The event ids of every `_noteType` event [side] holds.
  Future<Set<String>> noteEventIdsOf(_Side side) async => <String>{
    for (final e in await side.store.reader.findAllEvents(entryType: _noteType))
      e.eventId,
  };

  /// Opens a fresh, freshly identified successor database, registers its
  /// own native destination to [receiver] (bound to [predecessorId] too, so
  /// its succession delivery authenticates under `EVS-DEV-sender-succession
  /// /F`), and restores [predecessorId]'s deliveries into it. Records the
  /// successor as [successor] (closed in `tearDown`) and returns its
  /// registry, for draining.
  Future<DestinationRegistry> rebuildSuccessor(String predecessorId) async {
    final side = _Side(
      'successor',
      const Source(
        hopId: 'mobile-device',
        identifier: 'successor-install',
        softwareVersion: 'app@2.0.0',
      ),
    );
    await side.create();
    successor = side;
    successorHub = _EndpointDestination(
      receiver,
      additionalSenderIds: <String>{predecessorId},
    );
    final successorRegistry = DestinationRegistry(eventStore: side.store);
    await successorRegistry.addDestination(successorHub, initiator: _init);
    await successorRegistry.setStartDate(
      successorHub.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
    await side.store.restoreFromReceiver(
      registry: successorRegistry,
      destinationId: successorHub.id,
      predecessorDatabaseId: predecessorId,
      initiator: _init,
    );
    return successorRegistry;
  }

  // Verifies: EVS-PRD-delivery-channel/H
  // a receiver restored to an earlier point is sent again, exactly as first
  //   sent, every delivery it lacks, and the resume is one event.
  // Verifies: EVS-DEV-delivery-resume/I
  // the sender resumes the channel on the restored receiver's record.
  test('a receiver restored to an earlier point gets the deliveries it lacks '
      'again, byte for byte', () async {
    await note('n1');
    await deliver();
    final restorePoint = await receiver.snapshot();
    await note('n2');
    await note('n3');
    await deliver();
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3']);
    final firstSends = List<Uint8List>.of(<Uint8List>[
      for (final p in hub.sent) p.bytes,
    ]);
    expect(firstSends, hasLength(3));

    await receiver.restore(restorePoint);
    expect(await receivedNotes(), <String>['n1']);

    await note('n4');
    await deliver();

    expect(await receivedNotes(), <String>['n1', 'n2', 'n3', 'n4']);
    final later = hub.sent.skip(3).toList();
    final laterDeliveries = decoded(later);
    // Delivery 4 is refused on the restored record, 2 and 3 are sent again
    // exactly as first sent, and n4 follows as delivery 4.
    expect(laterDeliveries.map((d) => d.deliveryNumber).toList(), <int>[
      4,
      2,
      3,
      4,
    ]);
    expect(later[1].bytes, firstSends[1]);
    expect(later[2].bytes, firstSends[2]);
    final resumes = await sender.store.reader.findAllEvents(
      entryType: kDestinationChannelResumedEntryType,
    );
    expect(resumes, hasLength(1));
    expect(resumes.single.data['resume_after'], <String, Object?>{
      'delivery_number': 1,
      'delivery_hash': decoded(<WirePayload>[
        hub.sent.first,
      ]).single.deliveryHash,
    });
    expect((await listedChannel()).record.deliveryNumber, 4);
    for (final side in <_Side>[sender, receiver]) {
      expect(
        await side.store.reader.findAllEvents(
          entryType: kSecurityFindingEntryType,
        ),
        isEmpty,
        reason: '${side.name} records no finding',
      );
    }
  });

  // Verifies: EVS-PRD-delivery-channel/L
  // with both ends moved back, the sender resends the retained deliveries
  //   the receiver lacks and continues; the delivery neither end holds is
  //   unknown to both, its number is used again, and nothing is recorded
  //   for it.
  test('both ends restored: the resend covers what the receiver lacks and '
      'the number neither end holds is used again', () async {
    await note('n1');
    await deliver();
    final receiverPoint = await receiver.snapshot();
    await note('n2');
    await deliver();
    final senderPoint = await sender.snapshot();
    await note('n3');
    await deliver();
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3']);
    final firstSends = <Uint8List>[for (final p in hub.sent) p.bytes];

    await receiver.restore(receiverPoint);
    await sender.restore(senderPoint);
    await register();

    await note('n3b');
    await deliver();

    expect(await receivedNotes(), <String>['n1', 'n2', 'n3b']);
    final later = hub.sent.skip(3).toList();
    expect(decoded(later).map((d) => d.deliveryNumber).toList(), <int>[
      3,
      2,
      3,
    ]);
    expect(
      later[1].bytes,
      firstSends[1],
      reason:
          'delivery 2 is resent as '
          'first sent',
    );
    expect(later[2].bytes, isNot(firstSends[2]));
    expect((await listedChannel()).record.deliveryNumber, 3);
    for (final side in <_Side>[sender, receiver]) {
      expect(
        await side.store.reader.findAllEvents(
          entryType: kSecurityFindingEntryType,
        ),
        isEmpty,
        reason: '${side.name} records nothing for the delivery neither holds',
      );
    }
  });

  // Verifies: EVS-PRD-delivery-channel/I
  // a receiver record ahead of the sender's, naming no delivery the sender
  //   attempted, is recorded as a sender-regression finding.
  // Verifies: EVS-DEV-delivery-resume/K
  // the drainer starts a new generation on such a record, recording the
  //   finding.
  // Verifies: EVS-DEV-sender-succession/A
  // Verifies: EVS-DEV-sender-succession/C
  // the application rebuilds the regressed sender as a successor: the
  //   restore pulls every channel the receiver lists for the predecessor
  //   and stores, in one transaction, every carried event the successor
  //   does not hold.
  test('a sender rolled back is recorded as regressed and rebuilt through '
      'the restore, holding every event the receiver holds, both branches '
      'included', () async {
    final n1 = await note('n1');
    await deliver();
    final senderPoint = await sender.snapshot();
    final n2 = await note('n2');
    final n3 = await note('n3');
    await deliver();
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3']);
    final predecessorId = sender.store.databaseId;

    // The sender forgets n2 and n3 it authored and delivered; the receiver
    // keeps everything.
    await sender.restore(senderPoint);
    await register();

    // A fresh event at the reused origin position: the second branch the
    // regression produces.
    final n2b = await note('n2b');
    await deliver(passes: 8);

    expect(
      await findingsOf(sender, 'sender_regressed'),
      hasLength(1),
      reason:
          'the receiver record, ahead and naming no delivery the '
          'sender attempted, is recorded exactly once',
    );
    final channels = await allListedChannels(predecessorId);
    expect(
      channels.map((c) => c.channel.generation),
      containsAll(<int>[1, 2]),
      reason: 'delivery continued on a new generation, alongside the first',
    );

    final receiverIds = await noteEventIdsOf(receiver);
    expect(receiverIds, <String>{
      n1.eventId,
      n2.eventId,
      n3.eventId,
      n2b.eventId,
    }, reason: 'the receiver holds both branches of the fork');

    final successorRegistry = await rebuildSuccessor(predecessorId);
    expect(
      await noteEventIdsOf(successor!),
      receiverIds,
      reason:
          'the rebuilt successor holds every event the receiver holds '
          'for the predecessor, including both branches',
    );

    for (var i = 0; i < 4; i++) {
      await cycleOnce(successorRegistry, clock: () => DateTime.utc(2100));
    }
    final sentEntryTypes = <String>{
      for (final p in successorHub.sent)
        for (final e in DeliveryEnvelope.decode(p.bytes).events)
          e['entry_type']! as String,
    };
    expect(
      sentEntryTypes,
      isNot(contains(_noteType)),
      reason:
          'nothing was appended to the successor after the restore, so its '
          'queue carries no application event of its own',
    );
    expect(
      sentEntryTypes,
      contains(kDestinationSenderSucceededEntryType),
      reason: 'the succession event goes on every channel',
    );
  });

  // Verifies: EVS-PRD-delivery-channel/L
  // Verifies: EVS-PRD-delivery-channel/Q
  // Verifies: EVS-DEV-sender-succession/A
  // Verifies: EVS-DEV-sender-succession/C
  // both a receiver-behind resend and a sender rebuild, applied in the same
  //   channel's history, each fill the end that fell behind: the resend
  //   fills the receiver, and the rebuilt successor fills in for the
  //   sender, so each end ends up holding the other's missing events.
  test('double regression: the receiver-behind resend fills the receiver '
      'and the sender rebuild fills the successor', () async {
    final n1 = await note('n1');
    await deliver();
    final receiverPoint = await receiver.snapshot();
    final n2 = await note('n2');
    final n3 = await note('n3');
    await deliver();
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3']);

    // The receiver forgets n2 and n3; a fresh delivery attempt discovers
    // the receiver is behind and the sender resends them, with no finding,
    // filling the receiver back in.
    await receiver.restore(receiverPoint);
    final n4 = await note('n4');
    await deliver(passes: 8);
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3', 'n4']);
    expect(
      await findingsOf(sender, 'channel_unexplained'),
      isEmpty,
      reason: 'the resend alone explains the receiver record',
    );
    expect(
      await sender.store.reader.findAllEvents(
        entryType: kDestinationChannelResumedEntryType,
      ),
      hasLength(1),
      reason: 'the gap is closed by a resume, not a new generation',
    );
    expect(
      (await allListedChannels(
        sender.store.databaseId,
      )).map((c) => c.channel.generation),
      <int>[1],
      reason:
          'the channel is still on its first generation after the '
          'resend',
    );

    // Now the sender regresses: it forgets n5 it authored and delivered,
    // and the events it appends after the restore reuse n5's position.
    final senderPoint = await sender.snapshot();
    final n5 = await note('n5');
    await deliver();
    expect(await receivedNotes(), <String>['n1', 'n2', 'n3', 'n4', 'n5']);
    final predecessorId = sender.store.databaseId;

    await sender.restore(senderPoint);
    await register();
    final n5b = await note('n5b');
    await deliver(passes: 8);

    expect(
      await findingsOf(sender, 'sender_regressed'),
      hasLength(1),
      reason: 'exactly the sender regression is recorded',
    );

    final receiverIds = await noteEventIdsOf(receiver);
    expect(
      receiverIds,
      <String>{
        n1.eventId,
        n2.eventId,
        n3.eventId,
        n4.eventId,
        n5.eventId,
        n5b.eventId,
      },
      reason:
          'the receiver holds what the resend filled in and both branches '
          'of the sender regression',
    );

    final successorRegistry = await rebuildSuccessor(predecessorId);
    expect(
      await noteEventIdsOf(successor!),
      receiverIds,
      reason:
          'each end now holds the other end had ever been missing: the '
          'successor rebuild fills in for the regressed sender exactly as '
          'the earlier resend filled in for the receiver',
    );

    for (var i = 0; i < 4; i++) {
      await cycleOnce(successorRegistry, clock: () => DateTime.utc(2100));
    }
    final sentEntryTypes = <String>{
      for (final p in successorHub.sent)
        for (final e in DeliveryEnvelope.decode(p.bytes).events)
          e['entry_type']! as String,
    };
    expect(
      sentEntryTypes,
      isNot(contains(_noteType)),
      reason:
          'a restore with nothing appended after it drains no application '
          'event of the successor',
    );
    expect(
      sentEntryTypes,
      contains(kDestinationSenderSucceededEntryType),
      reason: 'the succession event goes on every channel',
    );
  });
}
