// Implements: EVS-DEV-sender-succession/D
// the succession event's data carries exactly id, registration_id,
//   database_id, predecessor_database_id and predecessor_channels, each
//   channel entry holding exactly channel, delivery_number and
//   delivery_hash.
// Implements: EVS-DEV-sender-succession/G
// the library offers a read of the succession lineage of a sender database
//   identity, derived solely from the succession events the log holds.
import 'package:event_sourcing/src/ingest/delivery_channel.dart'
    show DeliveryChannel, requireExactObject, requirePositiveInt, requireString;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kDestinationSenderSucceededEntryType;
import 'package:event_sourcing/src/storage/storage_backend.dart'
    show StorageBackend;
import 'package:event_sourcing/src/storage/stored_event.dart' show StoredEvent;
import 'package:event_sourcing/src/storage/transaction.dart' show Transaction;
import 'package:meta/meta.dart' show immutable, internal;

/// One channel a sender-succession event restored: encoded as an object
/// with exactly `channel`, `delivery_number` and `delivery_hash` (the last
/// delivery the restore pulled on that channel).
///
/// Shared, as the library-internal parser and builder of the succession
/// event's data shape, by the restore operation that emits it, the
/// receiver's acceptance of it, and the succession-lineage read.
@immutable
@internal
final class SenderSuccessionChannel {
  const SenderSuccessionChannel({
    required this.channel,
    required this.deliveryNumber,
    required this.deliveryHash,
  });

  /// Decodes a `predecessor_channels` entry, refusing with a
  /// [FormatException] one without exactly its keys.
  factory SenderSuccessionChannel.fromJson(Object? json) {
    final map = requireExactObject(json, 'predecessor_channels entry', _keys);
    return SenderSuccessionChannel(
      channel: DeliveryChannel.fromJson(map['channel']),
      deliveryNumber: requirePositiveInt(
        map,
        'delivery_number',
        'predecessor_channels entry',
      ),
      deliveryHash: requireString(
        map,
        'delivery_hash',
        'predecessor_channels entry',
      ),
    );
  }

  static const Set<String> _keys = <String>{
    'channel',
    'delivery_number',
    'delivery_hash',
  };

  /// The restored channel.
  final DeliveryChannel channel;

  /// The number of the last delivery restored on [channel].
  final int deliveryNumber;

  /// The hash of the last delivery restored on [channel].
  final String deliveryHash;

  Map<String, Object?> toJson() => <String, Object?>{
    'channel': channel.toJson(),
    'delivery_number': deliveryNumber,
    'delivery_hash': deliveryHash,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SenderSuccessionChannel &&
          channel == other.channel &&
          deliveryNumber == other.deliveryNumber &&
          deliveryHash == other.deliveryHash;

  @override
  int get hashCode => Object.hash(channel, deliveryNumber, deliveryHash);

  @override
  String toString() =>
      'SenderSuccessionChannel($channel, $deliveryNumber, $deliveryHash)';
}

/// The data a sender-succession event
/// (`system.destination_sender_succeeded`) carries: encoded as an object
/// with exactly `id`, `registration_id`, `database_id`,
/// `predecessor_database_id` and `predecessor_channels`.
///
/// Shared, as the library-internal parser and builder of the succession
/// event's data shape, by the restore operation that emits it, the
/// receiver's acceptance of it, and the succession-lineage read.
@immutable
@internal
final class SenderSuccessionData {
  const SenderSuccessionData({
    required this.id,
    required this.registrationId,
    required this.databaseId,
    required this.predecessorDatabaseId,
    required this.predecessorChannels,
  });

  /// Decodes a succession event's data, refusing with a [FormatException]
  /// one without exactly its keys or whose `predecessor_channels` is not a
  /// list of well-formed entries.
  factory SenderSuccessionData.fromJson(Map<String, Object?> data) {
    final map = requireExactObject(data, 'sender succession', _keys);
    final rawChannels = map['predecessor_channels'];
    if (rawChannels is! List) {
      throw const FormatException(
        'sender succession: "predecessor_channels" must be a list',
      );
    }
    return SenderSuccessionData(
      id: requireString(map, 'id', 'sender succession'),
      registrationId: requireString(
        map,
        'registration_id',
        'sender succession',
      ),
      databaseId: requireString(map, 'database_id', 'sender succession'),
      predecessorDatabaseId: requireString(
        map,
        'predecessor_database_id',
        'sender succession',
      ),
      predecessorChannels: <SenderSuccessionChannel>[
        for (final entry in rawChannels)
          SenderSuccessionChannel.fromJson(entry),
      ],
    );
  }

  static const Set<String> _keys = <String>{
    'id',
    'registration_id',
    'database_id',
    'predecessor_database_id',
    'predecessor_channels',
  };

  /// The successor's destination the restore pulled through.
  final String id;

  /// The registration identifier of that destination.
  final String registrationId;

  /// The successor's database identity.
  final String databaseId;

  /// The predecessor's database identity.
  final String predecessorDatabaseId;

  /// Every channel the restore that appended this event restored.
  final List<SenderSuccessionChannel> predecessorChannels;

  Map<String, Object?> toJson() => <String, Object?>{
    'id': id,
    'registration_id': registrationId,
    'database_id': databaseId,
    'predecessor_database_id': predecessorDatabaseId,
    'predecessor_channels': <Map<String, Object?>>[
      for (final channel in predecessorChannels) channel.toJson(),
    ],
  };

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! SenderSuccessionData ||
        id != other.id ||
        registrationId != other.registrationId ||
        databaseId != other.databaseId ||
        predecessorDatabaseId != other.predecessorDatabaseId ||
        predecessorChannels.length != other.predecessorChannels.length) {
      return false;
    }
    for (var i = 0; i < predecessorChannels.length; i++) {
      if (predecessorChannels[i] != other.predecessorChannels[i]) {
        return false;
      }
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(
    id,
    registrationId,
    databaseId,
    predecessorDatabaseId,
    Object.hashAll(predecessorChannels),
  );

  @override
  String toString() =>
      'SenderSuccessionData($databaseId succeeded $predecessorDatabaseId, '
      '$id/$registrationId, $predecessorChannels)';
}

/// The succession lineage of a sender database identity, derived solely
/// from the succession events the log holds: the predecessors it
/// succeeded, transitively, earliest first, and its successor, if any.
@immutable
final class SuccessionLineage {
  SuccessionLineage({required List<String> predecessors, this.successor})
    : predecessors = List<String>.unmodifiable(predecessors);

  /// The identities this database succeeded, transitively, earliest
  /// predecessor first.
  final List<String> predecessors;

  /// The identity that succeeded this database, or null when it has none.
  final String? successor;

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! SuccessionLineage ||
        successor != other.successor ||
        predecessors.length != other.predecessors.length) {
      return false;
    }
    for (var i = 0; i < predecessors.length; i++) {
      if (predecessors[i] != other.predecessors[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(Object.hashAll(predecessors), successor);

  @override
  String toString() =>
      'SuccessionLineage($predecessors, successor: $successor)';
}

/// `StorageReader.successionLineageOf`, computed by reading every
/// `system.destination_sender_succeeded` event [backend] holds (whether
/// authored or received) and walking the `predecessor_database_id` links
/// from [databaseId]: no other state is consulted.
// Implements: EVS-DEV-sender-succession/G
// the succession lineage read is derived solely from the succession events
//   the log holds, keyed by each event's database_id and
//   predecessor_database_id, walking predecessors transitively and finding
//   the successor, if any.
@internal
Future<SuccessionLineage> computeSuccessionLineage(
  StorageBackend backend,
  String databaseId,
) async =>
    lineageFromSuccessions(await readSenderSuccessions(backend), databaseId);

/// [computeSuccessionLineage] read inside [txn], so a caller that already
/// holds a transaction (the receiver's channel listing, the outstanding-
/// finding marks) makes no read outside it.
@internal
Future<SuccessionLineage> computeSuccessionLineageInTxn(
  Transaction txn,
  StorageBackend backend,
  String databaseId,
) async => lineageFromSuccessions(
  await readSenderSuccessionsInTxn(txn, backend),
  databaseId,
);

/// Every `system.destination_sender_succeeded` event [backend] holds
/// (whether authored or received), parsed. A malformed held event (the
/// receiver's shape check passed it, since that check reads only `id` and
/// `database_id`) is skipped rather than thrown: this read writes nothing
/// and so cannot record a finding for it, and one bad event must not
/// poison every other lookup.
@internal
Future<List<SenderSuccessionData>> readSenderSuccessions(
  StorageBackend backend,
) async => _parseAll(
  await backend.findAllEvents(entryType: kDestinationSenderSucceededEntryType),
);

/// [readSenderSuccessions] read inside [txn].
@internal
Future<List<SenderSuccessionData>> readSenderSuccessionsInTxn(
  Transaction txn,
  StorageBackend backend,
) async => _parseAll(
  await backend.findAllEventsInTxn(
    txn,
    entryType: kDestinationSenderSucceededEntryType,
  ),
);

List<SenderSuccessionData> _parseAll(List<StoredEvent> events) {
  final successions = <SenderSuccessionData>[];
  for (final event in events) {
    final data = _tryParse(event.data);
    if (data != null) successions.add(data);
  }
  return successions;
}

/// The succession lineage of [databaseId], computed solely from
/// [successions]: the predecessors it succeeded, transitively, earliest
/// first, and its successor, if any.
// Implements: EVS-DEV-sender-succession/G
// the succession lineage read is derived solely from the succession events
//   the log holds, keyed by each event's database_id and
//   predecessor_database_id, walking predecessors transitively and finding
//   the successor, if any.
@internal
SuccessionLineage lineageFromSuccessions(
  List<SenderSuccessionData> successions,
  String databaseId,
) {
  // Keyed by the successor's identity: its succession event. Last in log
  // order wins on the rare case of two stored events naming the same
  // successor.
  final bySuccessor = <String, SenderSuccessionData>{
    for (final s in successions) s.databaseId: s,
  };
  // Keyed by the predecessor's identity: the identity that succeeded it.
  // Last in log order wins on the rare case of two stored events naming
  // the same predecessor.
  final successorOf = <String, String>{
    for (final s in successions) s.predecessorDatabaseId: s.databaseId,
  };

  // A visited set guards against a held cycle (a succession event naming
  // its own successor as predecessor, directly or through others): the
  // walk stops at the first identity seen twice instead of looping.
  final nearestFirst = <String>[];
  final visited = <String>{databaseId};
  var current = databaseId;
  while (bySuccessor.containsKey(current)) {
    final predecessor = bySuccessor[current]!.predecessorDatabaseId;
    if (!visited.add(predecessor)) break;
    nearestFirst.add(predecessor);
    current = predecessor;
  }

  return SuccessionLineage(
    predecessors: nearestFirst.reversed.toList(growable: false),
    successor: successorOf[databaseId],
  );
}

/// The identities [lineage] (the succession lineage of [databaseId]) names,
/// [databaseId] itself included: its predecessors and its successor, if
/// any.
@internal
Set<String> lineageSetOf(SuccessionLineage lineage, String databaseId) =>
    <String>{
      databaseId,
      ...lineage.predecessors,
      if (lineage.successor != null) lineage.successor!,
    };

/// [SenderSuccessionData.fromJson] of [data], or null when it does not
/// parse.
SenderSuccessionData? _tryParse(Map<String, Object?> data) {
  try {
    return SenderSuccessionData.fromJson(data);
  } on FormatException {
    return null;
  }
}
