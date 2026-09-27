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
  _EndpointDestination(this.receiver);

  final _Side receiver;

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
      senderDatabaseIds: <String>{sender},
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
      senderDatabaseIds: <String>{sender},
    );
    return decodeReceiverAnswer(answer.encode());
  }
}

void main() {
  late _Side sender;
  late _Side receiver;
  late _EndpointDestination hub;
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
}
