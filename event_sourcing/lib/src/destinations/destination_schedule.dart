// Implements: EVS-PRD-destinations/A
// DestinationSchedule value type
// representing the wall-clock window configuration (startDate/endDate)
// that is part of each destination's configuration on the deployment.
import 'package:event_sourcing/src/destinations/destination.dart';

/// Persisted schedule for a registered `Destination`.
///
/// The pair `(startDate, endDate)` defines the wall-clock window during
/// which events match this destination's time-window filter
///. Both fields are nullable at construction:
///
/// - `startDate == null` is the "dormant" state a destination enters on
///   initial `addDestination` before any `setStartDate` call. Dormant
///   destinations do not accept any FIFO rows regardless of current time
///  .
/// - `endDate == null` is "no scheduled end" — the destination is active
///   for all time once `startDate` has elapsed. A later `setEndDate` call
///   may populate it.
///
/// The persisted schedule also carries the destination's registration
/// identity ([registrationId], the event id of the
/// `system.destination_registered` event that created it) and the
/// hard-delete opt-in in effect ([allowHardDelete], written by the latest
/// registration), and the keying the latest registration recorded
/// ([keying]). The registry's date, recovery and deletion operations act
/// on this persisted record, so any process can run them for any destination
/// the database knows.
///
/// The value type is deliberately immutable; `DestinationRegistry`
/// mutations construct a new `DestinationSchedule` and persist it.
class DestinationSchedule {
  /// Construct a schedule. Either date may be null — see class doc.
  const DestinationSchedule({
    this.startDate,
    this.endDate,
    this.registrationId,
    this.allowHardDelete = false,
    this.keying,
  });

  /// Inverse of [toJson]. `null` fields parse back to `null` DateTime.
  factory DestinationSchedule.fromJson(Map<String, Object?> json) {
    final start = json['start_date'] as String?;
    final end = json['end_date'] as String?;
    return DestinationSchedule(
      startDate: start == null ? null : DateTime.parse(start),
      endDate: end == null ? null : DateTime.parse(end),
      registrationId: json['registration_id'] as String?,
      allowHardDelete: (json['allow_hard_delete'] as bool?) ?? false,
      keying: switch (json['keying']) {
        final Map<Object?, Object?> k => DestinationKeying.fromJson(
          Map<String, Object?>.from(k),
        ),
        _ => null,
      },
    );
  }

  /// Wall-clock time at which this destination starts accepting events.
  /// Null means "dormant" — no `startDate` has been assigned yet.
  final DateTime? startDate;

  /// Wall-clock time at which this destination stops accepting new events.
  /// Null means "no scheduled end" — active indefinitely once `startDate`
  /// has elapsed.
  final DateTime? endDate;

  /// Event id of the `system.destination_registered` event that created this
  /// persisted schedule. A destination deleted and registered again under
  /// the same id gets a new one. Null only on a schedule no registration
  /// wrote (a value built in memory).
  final String? registrationId;

  /// The hard-delete opt-in in effect: the value the latest registration of
  /// the destination declared. A deletion acts on, and records, this value.
  final bool allowHardDelete;

  /// The keying the latest registration of the destination recorded: whether
  /// it is keyed and the name of its key scheme. Null when no registration
  /// has recorded one, as for a schedule written before keyings were
  /// recorded; the next registration records its own.
  final DestinationKeying? keying;

  /// True when no `startDate` has been assigned; the destination is
  /// registered but not yet active for any wall-clock time.
  bool get isDormant => startDate == null;

  /// True iff `startDate <= now < endDate`, treating a null `endDate` as
  /// an open-ended right bound. A dormant schedule (null `startDate`) is
  /// never active — dormant is strictly pre-active.
  bool isActiveAt(DateTime now) =>
      startDate != null &&
      startDate!.compareTo(now) <= 0 &&
      (endDate == null || endDate!.compareTo(now) > 0);

  /// JSON representation used by `StorageBackend.writeScheduleTxn`. Both
  /// fields serialize to either an ISO-8601 string or `null`.
  Map<String, Object?> toJson() => {
    'start_date': startDate?.toIso8601String(),
    'end_date': endDate?.toIso8601String(),
    'registration_id': registrationId,
    'allow_hard_delete': allowHardDelete,
    if (keying != null) 'keying': keying!.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DestinationSchedule &&
          startDate == other.startDate &&
          endDate == other.endDate &&
          registrationId == other.registrationId &&
          allowHardDelete == other.allowHardDelete &&
          keying == other.keying;

  @override
  int get hashCode =>
      Object.hash(startDate, endDate, registrationId, allowHardDelete, keying);

  @override
  String toString() =>
      'DestinationSchedule(startDate: $startDate, endDate: $endDate, '
      'registrationId: $registrationId, allowHardDelete: $allowHardDelete, '
      'keying: $keying)';
}

/// How a destination names the block key of each event it queues, as its
/// registration recorded it in the persisted schedule: whether it is
/// [keyed], and, for a keyed destination, the name of its key scheme
/// ([keyScheme], `Destination.keyScheme`). Two registrations that record
/// different keyings may compute different block keys for one subject, so
/// the registry refuses the change while the destination has key lists, and
/// a fill or a hold under a keying other than the recorded one writes
/// nothing (`EVS-DEV-destination-key-hold-guards`).
// Implements: EVS-DEV-destination-key-hold-guards/A
// the keying a registration records: whether the destination is keyed and
//   its key scheme's name.
class DestinationKeying {
  /// A keying. A destination that is not [keyed] records no [keyScheme].
  const DestinationKeying({required this.keyed, this.keyScheme})
    : assert(keyed || keyScheme == null, 'an unkeyed keying names no scheme');

  /// The keying of [destination]: its `keyed` declaration and, when keyed,
  /// its `keyScheme`. Reads consumer code, so a caller reads it once,
  /// outside any transaction.
  factory DestinationKeying.of(Destination destination) {
    final keyed = destination.keyed;
    return DestinationKeying(
      keyed: keyed,
      keyScheme: keyed ? destination.keyScheme : null,
    );
  }

  /// Inverse of [toJson].
  factory DestinationKeying.fromJson(Map<String, Object?> json) =>
      DestinationKeying(
        keyed: json['keyed']! as bool,
        keyScheme: json['key_scheme'] as String?,
      );

  /// The keying of a destination that is not keyed.
  static const DestinationKeying unkeyed = DestinationKeying(keyed: false);

  /// Whether the destination names a block key for each event.
  final bool keyed;

  /// The name of the keyed destination's key scheme; null when not keyed.
  final String? keyScheme;

  Map<String, Object?> toJson() => <String, Object?>{
    'keyed': keyed,
    'key_scheme': keyScheme,
  };

  @override
  bool operator ==(Object other) =>
      other is DestinationKeying &&
      other.keyed == keyed &&
      other.keyScheme == keyScheme;

  @override
  int get hashCode => Object.hash(keyed, keyScheme);

  @override
  String toString() => keyed ? 'keyed by $keyScheme' : 'not keyed';
}

/// Return code from `DestinationRegistry.setEndDate`.
///
/// Exactly one of the three is returned per call:
///
/// - `closed` — the call transitions the destination from currently active
///   to currently closed (new `endDate <= now`, prior state was active).
/// - `scheduled` — the new `endDate` is in the future, either from an
///   active state (closure is scheduled later) or from a previously-closed
///   state reopened with a future end.
/// - `applied` — no change in the current active-vs-closed classification
///   relative to `now`. For example, overwriting a past `endDate` with a
///   different past value, or replacing a future-dated `endDate` with
///   another future-dated one that does not cross the `now` boundary.
enum SetEndDateResult { closed, scheduled, applied }

/// Result of `tombstoneAndRefill`
///
/// Carries three operator-visible values: the `entry_id` of the wedged
/// row flipped to `tombstoned`, the count of trail null rows deleted in
/// the same transaction, and the value `fill_cursor` was rewound to.
class TombstoneAndRefillResult {
  const TombstoneAndRefillResult({
    required this.rowId,
    required this.deletedTrailCount,
    required this.rewoundTo,
  });

  /// `entry_id` of the tombstoned row, recorded as `row_id` on the
  /// recovery audit.
  final String rowId;

  /// Count of null-finalStatus rows whose sequence_in_queue was strictly
  /// greater than the tombstoned row's sequence_in_queue that were deleted from
  /// the FIFO store in the same transaction.
  final int deletedTrailCount;

  /// Value the per-destination fill_cursor was rewound to: one below the
  /// lowest `event_id_range.first_seq` among the tombstoned head and the
  /// swept trail items.
  final int rewoundTo;
}

/// Result of `DestinationRegistry.releaseHold`: the release applied, the
/// key already releasing under the named hold, or the named hold stale.
sealed class ReleaseHoldResult {
  const ReleaseHoldResult();
}

/// The release moved the key's list to releasing and appended the release
/// event [releaseEventId].
final class ReleaseHoldApplied extends ReleaseHoldResult {
  const ReleaseHoldApplied(this.releaseEventId);

  /// The id of the `system.destination_hold_released` event the release
  /// appended.
  final String releaseEventId;

  @override
  bool operator ==(Object other) =>
      other is ReleaseHoldApplied && other.releaseEventId == releaseEventId;

  @override
  int get hashCode => Object.hash(ReleaseHoldApplied, releaseEventId);

  @override
  String toString() => 'ReleaseHoldApplied($releaseEventId)';
}

/// The key's list was already releasing under the named hold: nothing
/// changed, and [releaseEventId] is the release event the earlier release
/// appended.
final class ReleaseHoldAlreadyReleasing extends ReleaseHoldResult {
  const ReleaseHoldAlreadyReleasing(this.releaseEventId);

  /// The id of the `system.destination_hold_released` event the earlier
  /// release appended.
  final String releaseEventId;

  @override
  bool operator ==(Object other) =>
      other is ReleaseHoldAlreadyReleasing &&
      other.releaseEventId == releaseEventId;

  @override
  int get hashCode => Object.hash(ReleaseHoldAlreadyReleasing, releaseEventId);

  @override
  String toString() => 'ReleaseHoldAlreadyReleasing($releaseEventId)';
}

/// The named hold is not the key's: the key has no list (a null
/// [currentHoldId]) or its list is under the hold [currentHoldId]. Nothing
/// changed.
final class ReleaseHoldStale extends ReleaseHoldResult {
  const ReleaseHoldStale(this.currentHoldId);

  /// The hold id of the key's list, or null when the key has none.
  final String? currentHoldId;

  @override
  bool operator ==(Object other) =>
      other is ReleaseHoldStale && other.currentHoldId == currentHoldId;

  @override
  int get hashCode => Object.hash(ReleaseHoldStale, currentHoldId);

  @override
  String toString() => 'ReleaseHoldStale($currentHoldId)';
}
