// Implements: EVS-PRD-destinations/A
// DestinationSchedule value type
// representing the wall-clock window configuration (startDate/endDate)
// that is part of each destination's configuration on the deployment.
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
/// registration). The registry's date, recovery and deletion operations act
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
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DestinationSchedule &&
          startDate == other.startDate &&
          endDate == other.endDate &&
          registrationId == other.registrationId &&
          allowHardDelete == other.allowHardDelete;

  @override
  int get hashCode =>
      Object.hash(startDate, endDate, registrationId, allowHardDelete);

  @override
  String toString() =>
      'DestinationSchedule(startDate: $startDate, endDate: $endDate, '
      'registrationId: $registrationId, allowHardDelete: $allowHardDelete)';
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
