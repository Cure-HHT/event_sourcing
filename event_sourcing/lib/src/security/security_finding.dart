// Implements: EVS-PRD-portability/C
// pure Dart value types and functions; a finding's identity serialises and
//   hashes identically on every Dart-supported runtime.
import 'dart:convert';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:meta/meta.dart' show immutable, internal;

/// The role in which a database detected an integrity anomaly, recorded as
/// `detector.role` on the security finding and covered by its identity:
/// each role of each database records an anomaly once.
// Implements: EVS-DEV-security-findings/Q
// a finding names the role of the operation that detected it: ingest, the
//   restore, the sender (the drainer) or the walk (the chain verification).
enum FindingRole {
  /// An ingest entry point.
  ingest('ingest'),

  /// The restore operation.
  restore('restore'),

  /// The drainer, as a channel's sender.
  sender('sender'),

  /// The chain verification operation.
  walk('walk');

  const FindingRole(this.wire);

  /// The string recorded as `detector.role`.
  final String wire;
}

/// The kind of integrity anomaly a security finding records.
///
/// The kind is an open value: a later release of the data-format major may
/// add a kind, and a finding of a kind this build does not know, stored by
/// ingest as received, reads with its string verbatim ([FindingKind.fromWire] never
/// throws; [isKnown] is false for it). The library records findings only of
/// the kinds in [values].
// Implements: EVS-DEV-destination-drain/L
// the security finding's kind is an open value: a value this build does not
//   know is carried verbatim wherever it is read.
@immutable
final class FindingKind {
  const FindingKind._(this.wire, {required this.isKnown});

  /// The kind whose recorded string is [value]: one of [values], or, for a
  /// string this build does not know, a kind carrying it verbatim.
  factory FindingKind.fromWire(String value) {
    for (final kind in values) {
      if (kind.wire == value) return kind;
    }
    return FindingKind._(value, isKnown: false);
  }

  /// An event's hash, or an arrival hash, does not recompute.
  static const hashMismatch = FindingKind._('hash_mismatch', isKnown: true);

  /// An event identifier arrives that the holder holds under another sealed
  /// hash.
  static const identityMismatch = FindingKind._(
    'identity_mismatch',
    isKnown: true,
  );

  /// A received record the library does not store as an event.
  static const eventMalformed = FindingKind._('event_malformed', isKnown: true);

  /// A delivery hash does not recompute.
  static const deliveryHashMismatch = FindingKind._(
    'delivery_hash_mismatch',
    isKnown: true,
  );

  /// A held predecessor of another database, or at a later origin position.
  static const predecessorBreak = FindingKind._(
    'predecessor_break',
    isKnown: true,
  );

  /// A second event of one database after one predecessor, at another
  /// origin position.
  static const forkUnrecorded = FindingKind._('fork_unrecorded', isKnown: true);

  /// A second event of one database at one origin position.
  static const positionReused = FindingKind._('position_reused', isKnown: true);

  /// An event the receiving database's own identity originated arrives by
  /// ingest.
  static const ownEventIngested = FindingKind._(
    'own_event_ingested',
    isKnown: true,
  );

  /// An event arrives on a channel whose sender did not author it.
  static const foreignEvent = FindingKind._('foreign_event', isKnown: true);

  /// A receiver record no automatic path explains.
  static const channelUnexplained = FindingKind._(
    'channel_unexplained',
    isKnown: true,
  );

  /// A receiver record of a channel ahead of the sender's.
  static const senderRegressed = FindingKind._(
    'sender_regressed',
    isKnown: true,
  );

  /// A served delivery or event fails a check other than a hash that does
  /// not recompute.
  static const restoreUnverified = FindingKind._(
    'restore_unverified',
    isKnown: true,
  );

  /// A succession event names a delivery of a channel above the receiver's
  /// record of it.
  static const successionAhead = FindingKind._(
    'succession_ahead',
    isKnown: true,
  );

  /// A storage-chain link of the holder's log does not hold.
  static const storageLinkBreak = FindingKind._(
    'storage_link_break',
    isKnown: true,
  );

  /// A local sequence number of the holder's log holds no event.
  static const sequenceMissing = FindingKind._(
    'sequence_missing',
    isKnown: true,
  );

  /// A parent an event names is not an eligible version of its aggregate
  /// the holder holds under that hash.
  static const parentInvalid = FindingKind._('parent_invalid', isKnown: true);

  /// An event the holder authored names other parents than the stamping
  /// rule yields.
  static const parentsNotStamped = FindingKind._(
    'parents_not_stamped',
    isKnown: true,
  );

  /// Every kind this build records, in the order the requirement lists
  /// them.
  // Implements: EVS-DEV-security-findings/H
  // the closed list of kinds the library records a finding with.
  static const List<FindingKind> values = <FindingKind>[
    hashMismatch,
    identityMismatch,
    eventMalformed,
    deliveryHashMismatch,
    predecessorBreak,
    forkUnrecorded,
    positionReused,
    ownEventIngested,
    foreignEvent,
    channelUnexplained,
    senderRegressed,
    restoreUnverified,
    successionAhead,
    storageLinkBreak,
    sequenceMissing,
    parentInvalid,
    parentsNotStamped,
  ];

  /// The string recorded as the finding's `kind`.
  final String wire;

  /// Whether this build knows the kind, so that it may record it.
  final bool isKnown;

  @override
  bool operator ==(Object other) => other is FindingKind && other.wire == wire;

  @override
  int get hashCode => wire.hashCode;

  @override
  String toString() => 'FindingKind($wire)';
}

/// One evidence key with, where the kind fixes one, the closed list of
/// values it may hold.
typedef _Key = ({String name, Set<String>? closed, bool deliveryRecord});

_Key _k(String name) => (name: name, closed: null, deliveryRecord: false);

_Key _closed(String name, Set<String> values) =>
    (name: name, closed: values, deliveryRecord: false);

_Key _delivery(String name) => (name: name, closed: null, deliveryRecord: true);

/// The keys of a delivery record in the evidence: the delivery number and
/// the delivery hash.
const Set<String> _deliveryRecordKeys = <String>{
  'delivery_number',
  'delivery_hash',
};

// Implements: EVS-DEV-security-findings/J+K+L+M+R
// the exact evidence keys of every kind, with the closed lists of reasons,
//   checks and fields some kinds name, so every release of one data-format
//   major spells the evidence of one anomaly alike.
final Map<String, List<_Key>> _evidenceByKind = <String, List<_Key>>{
  FindingKind.forkUnrecorded.wire: <_Key>[
    _k('database_id'),
    _k('previous_event_hash'),
  ],
  FindingKind.positionReused.wire: <_Key>[
    _k('database_id'),
    _k('origin_sequence_number'),
  ],
  FindingKind.hashMismatch.wire: <_Key>[
    _k('event_id'),
    _k('carried_hash'),
    _k('recomputed_hash'),
  ],
  FindingKind.predecessorBreak.wire: <_Key>[
    _k('database_id'),
    _k('event_hash'),
    _k('previous_event_hash'),
  ],
  FindingKind.identityMismatch.wire: <_Key>[
    _k('event_id'),
    _k('held_hash'),
    _k('record'),
  ],
  FindingKind.eventMalformed.wire: <_Key>[
    _closed('reason', const <String>{
      'record_malformed',
      'reserved_type_undeclared',
      'audit_identity_invalid',
    }),
    _k('record'),
  ],
  FindingKind.deliveryHashMismatch.wire: <_Key>[
    _k('channel'),
    _k('delivery_number'),
    _k('carried_hash'),
    _k('recomputed_hash'),
  ],
  FindingKind.ownEventIngested.wire: <_Key>[
    _k('event_id'),
    _k('sealed_hash'),
    _k('record'),
  ],
  FindingKind.foreignEvent.wire: <_Key>[
    _k('channel'),
    _k('delivery_number'),
    _k('event_id'),
    _k('sealed_hash'),
  ],
  FindingKind.channelUnexplained.wire: <_Key>[
    _k('channel'),
    _delivery('sender_record'),
    _delivery('receiver_record'),
    _k('recorded_receiver_database_id'),
    _k('responding_receiver_database_id'),
  ],
  FindingKind.senderRegressed.wire: <_Key>[
    _k('channel'),
    _delivery('sender_record'),
    _delivery('receiver_record'),
    _k('recorded_receiver_database_id'),
    _k('responding_receiver_database_id'),
  ],
  FindingKind.restoreUnverified.wire: <_Key>[
    _k('channel'),
    _k('delivery_number'),
    _k('event_id'),
    _closed('check', const <String>{
      'delivery_link',
      'delivery_hash',
      'originator',
      'receiver_entry',
    }),
  ],
  FindingKind.successionAhead.wire: <_Key>[
    _k('channel'),
    _delivery('receiver_record'),
    _delivery('succession_record'),
  ],
  FindingKind.storageLinkBreak.wire: <_Key>[
    _k('local_sequence_number'),
    _k('event_id'),
    _closed('field', const <String>{
      'ingest_sequence_number',
      'previous_ingest_hash',
    }),
    _k('expected'),
    _k('actual'),
  ],
  FindingKind.sequenceMissing.wire: <_Key>[_k('local_sequence_number')],
  FindingKind.parentInvalid.wire: <_Key>[
    _k('local_sequence_number'),
    _k('event_id'),
    _k('parent'),
    _closed('reason', const <String>{
      'other_aggregate',
      'annotation',
      'ineligible',
      'held_under_other_hash',
    }),
  ],
  FindingKind.parentsNotStamped.wire: <_Key>[
    _k('local_sequence_number'),
    _k('event_id'),
    _k('expected'),
    _k('actual'),
  ],
};

/// Throws [ArgumentError] unless [kind] is a kind this build records and
/// [evidence] holds exactly the keys its kind fixes, every named reason,
/// check or field one of its closed list, and every delivery record an
/// object with exactly `delivery_number` and `delivery_hash`.
// Implements: EVS-DEV-security-findings/D
// a finding's evidence holds only the fixed keys of its kind, so it carries
//   identifiers, hashes, positions, identities, channels, delivery records,
//   named reasons and checks, compared values and the record concerned.
@internal
void checkFindingEvidence(FindingKind kind, Map<String, Object?> evidence) {
  final keys = _evidenceByKind[kind.wire];
  if (!kind.isKnown || keys == null) {
    throw ArgumentError.value(
      kind.wire,
      'kind',
      'is not a kind of security finding this library records',
    );
  }
  final expected = <String>{for (final k in keys) k.name};
  final actual = evidence.keys.toSet();
  if (actual.length != expected.length || !actual.containsAll(expected)) {
    throw ArgumentError.value(
      (actual.toList()..sort()).join(', '),
      'evidence',
      'the evidence of a ${kind.wire} finding holds exactly '
          '${(expected.toList()..sort()).join(', ')}',
    );
  }
  for (final key in keys) {
    final value = evidence[key.name];
    final closed = key.closed;
    if (closed != null && (value is! String || !closed.contains(value))) {
      throw ArgumentError.value(
        value,
        'evidence.${key.name}',
        'a ${kind.wire} finding names one of '
            '${(closed.toList()..sort()).join(', ')}',
      );
    }
    if (key.deliveryRecord &&
        (value is! Map ||
            value.length != _deliveryRecordKeys.length ||
            !_deliveryRecordKeys.every(value.containsKey))) {
      throw ArgumentError.value(
        value,
        'evidence.${key.name}',
        'a delivery record is an object with exactly delivery_number and '
            'delivery_hash',
      );
    }
  }
}

/// The identity of a finding: the SHA-256, in lowercase hexadecimal, of the
/// canonical JSON of the detecting database [databaseId], the detector's
/// [role], the [kind] and the [evidence]. The library version that detected
/// it is not part of it.
// Implements: EVS-DEV-security-findings/C
// the finding identity is the digest of the detecting database, the role,
//   the kind and the evidence, and nothing else.
@internal
String securityFindingId({
  required String databaseId,
  required FindingRole role,
  required FindingKind kind,
  required Map<String, Object?> evidence,
}) => sha256
    .convert(
      utf8.encode(
        canonicalize(<String, Object?>{
          'database_id': databaseId,
          'role': role.wire,
          'kind': kind.wire,
          'evidence': evidence,
        }),
      ),
    )
    .toString();

/// The data of the security finding [findingId] a detector records: exactly
/// `finding_id`, `kind`, `evidence`, `aggregates` (in ascending order, each
/// once) and `detector` (the detecting database, its role and the library
/// version that detected it).
// Implements: EVS-DEV-security-findings/B
// a finding's data carries exactly its identity, kind, evidence, the
//   aggregates in ascending order, and the detector's database, role and
//   library version.
@internal
Map<String, Object?> securityFindingData({
  required String findingId,
  required FindingKind kind,
  required Map<String, Object?> evidence,
  required Iterable<String> aggregates,
  required String databaseId,
  required FindingRole role,
  required String libraryVersion,
}) => <String, Object?>{
  'finding_id': findingId,
  'kind': kind.wire,
  'evidence': evidence,
  'aggregates': aggregates.toSet().toList()..sort(),
  'detector': <String, Object?>{
    'database_id': databaseId,
    'role': role.wire,
    'library_version': libraryVersion,
  },
};
