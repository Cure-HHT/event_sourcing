// Implements: EVS-PRD-portability/C
// pure Dart value types; serialise identically on every Dart-supported
//   runtime.
// Implements: EVS-DEV-destination-drain/F+T
// the records the drainer keeps beside the queues: its declared
//   configuration and unserved destinations as of its latest pass, its
//   heartbeat, the refill guard an accepted recovery of a reconfigure halt
//   leaves, and the persisted delivery status read from them.
import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';

/// Why a delivery cycle does not serve a destination: it neither fills nor
/// sends it.
enum UnservedReason {
  /// The database holds a schedule for the destination, and the cycle's
  /// registry does not register it (it is registered by another process,
  /// or was removed from this process's code). The cycle still honours a
  /// halt request on it, so an operator can halt it and then delete it.
  notRegisteredHere('not_registered_here'),

  /// The cycle's registry holds the destination, and the database holds no
  /// schedule for it (another process deleted it).
  deletedInStorage('deleted_in_storage'),

  /// A recovery of a reconfigure halt left a refill guard naming the
  /// configuration the drainer still declares for the destination, so the
  /// drainer does not refill it until it declares another configuration.
  refillAwaitsChangedConfiguration('refill_awaits_changed_configuration'),

  /// The cycle's registry holds the destination under another registration
  /// than the one the database holds: another process deleted the
  /// destination and registered it again, possibly with another
  /// configuration. The cycle does not fill it under the configuration it
  /// holds, and still honours a halt request on it; registering the
  /// destination again in the draining process serves it.
  registrationMismatch('registration_mismatch');

  const UnservedReason(this.wire);

  /// The string the drainer's declaration records.
  final String wire;

  /// The reason whose [wire] string is [value]. Throws [FormatException] for
  /// any other value.
  static UnservedReason fromWire(String value) {
    for (final reason in values) {
      if (reason.wire == value) return reason;
    }
    throw FormatException('UnservedReason: unknown reason "$value"');
  }
}

/// What the current drainer declared at the start of its latest pass: the
/// drain epoch it holds, the configuration it declares for each destination
/// it registers (and its fingerprint), and the destinations it does not
/// serve.
///
/// Persisted under `backend_state` key `drainer_declaration`, written by the
/// pass-start transaction when it differs from the stored one.
class DrainerDeclaration {
  const DrainerDeclaration({
    required this.epoch,
    required this.configurationVersion,
    required this.configurations,
    required this.fingerprints,
    required this.unserved,
    required this.declaredAt,
  });

  /// Decode from the persisted JSON form.
  factory DrainerDeclaration.fromJson(Map<String, Object?> json) {
    final epoch = json['epoch'];
    if (epoch is! int) {
      throw const FormatException(
        'DrainerDeclaration: missing or non-integer "epoch"',
      );
    }
    final version = json['configuration_version'];
    if (version != null && version is! String) {
      throw const FormatException(
        'DrainerDeclaration: non-string "configuration_version"',
      );
    }
    final configurations = json['configurations'];
    final fingerprints = json['fingerprints'];
    final unserved = json['unserved'];
    if (configurations is! Map || fingerprints is! Map || unserved is! Map) {
      throw const FormatException(
        'DrainerDeclaration: missing "configurations", "fingerprints" or '
        '"unserved"',
      );
    }
    final at = json['declared_at'];
    if (at is! String) {
      throw const FormatException(
        'DrainerDeclaration: missing or non-string "declared_at"',
      );
    }
    return DrainerDeclaration(
      epoch: epoch,
      configurationVersion: version as String?,
      configurations: <String, Map<String, Object?>>{
        for (final e in configurations.entries)
          e.key as String: Map<String, Object?>.from(e.value as Map),
      },
      fingerprints: <String, String>{
        for (final e in fingerprints.entries)
          e.key as String: e.value as String,
      },
      unserved: <String, UnservedReason>{
        for (final e in unserved.entries)
          e.key as String: UnservedReason.fromWire(e.value as String),
      },
      declaredAt: DateTime.parse(at).toUtc(),
    );
  }

  /// The drain epoch the declaring drainer holds.
  final int epoch;

  /// The `configurationVersion` the drainer's delivery cycle was started
  /// with.
  final String? configurationVersion;

  /// The declared configuration of each destination the drainer serves.
  final Map<String, Map<String, Object?>> configurations;

  /// The fingerprint of each declared configuration.
  final Map<String, String> fingerprints;

  /// The destinations the drainer does not serve, and why.
  final Map<String, UnservedReason> unserved;

  /// When the drainer wrote the declaration, by its clock.
  final DateTime declaredAt;

  /// True when [other] declares the same epoch, configuration and unserved
  /// set, whenever it was written.
  bool declaresSameAs(DrainerDeclaration other) =>
      other.epoch == epoch &&
      other.configurationVersion == configurationVersion &&
      const DeepCollectionEquality().equals(
        other.configurations,
        configurations,
      ) &&
      const DeepCollectionEquality().equals(other.fingerprints, fingerprints) &&
      const DeepCollectionEquality().equals(other.unserved, unserved);

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'epoch': epoch,
    'configuration_version': configurationVersion,
    'configurations': <String, Object?>{
      for (final e in configurations.entries) e.key: e.value,
    },
    'fingerprints': Map<String, Object?>.of(fingerprints),
    'unserved': <String, Object?>{
      for (final e in unserved.entries) e.key: e.value.wire,
    },
    'declared_at': declaredAt.toUtc().toIso8601String(),
  };

  @override
  bool operator ==(Object other) =>
      other is DrainerDeclaration &&
      declaresSameAs(other) &&
      other.declaredAt.isAtSameMomentAs(declaredAt);

  @override
  int get hashCode => Object.hash(
    epoch,
    configurationVersion,
    const DeepCollectionEquality().hash(configurations),
    const DeepCollectionEquality().hash(fingerprints),
    const DeepCollectionEquality().hash(unserved),
    declaredAt.microsecondsSinceEpoch,
  );

  @override
  String toString() =>
      'DrainerDeclaration(epoch: $epoch, configurationVersion: '
      '$configurationVersion, fingerprints: $fingerprints, unserved: '
      '${unserved.map((k, v) => MapEntry(k, v.wire))}, declaredAt: '
      '$declaredAt)';
}

/// The drainer's liveness record: the drain epoch it holds, the number of
/// its latest pass under that epoch, and when that pass started.
///
/// Persisted under `backend_state` key `drain_heartbeat`, written by every
/// pass-start transaction.
class DrainHeartbeat {
  const DrainHeartbeat({
    required this.epoch,
    required this.pass,
    required this.at,
  });

  /// Decode from the persisted JSON form.
  factory DrainHeartbeat.fromJson(Map<String, Object?> json) {
    final epoch = json['epoch'];
    final pass = json['pass'];
    final at = json['at'];
    if (epoch is! int || pass is! int || at is! String) {
      throw const FormatException(
        'DrainHeartbeat: missing or mistyped "epoch", "pass" or "at"',
      );
    }
    return DrainHeartbeat(
      epoch: epoch,
      pass: pass,
      at: DateTime.parse(at).toUtc(),
    );
  }

  /// The drain epoch the drainer holds.
  final int epoch;

  /// The pass number under [epoch], from 1.
  final int pass;

  /// When the pass started, by the drainer's clock.
  final DateTime at;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'epoch': epoch,
    'pass': pass,
    'at': at.toUtc().toIso8601String(),
  };

  @override
  bool operator ==(Object other) =>
      other is DrainHeartbeat &&
      other.epoch == epoch &&
      other.pass == pass &&
      other.at.isAtSameMomentAs(at);

  @override
  int get hashCode => Object.hash(epoch, pass, at.microsecondsSinceEpoch);

  @override
  String toString() => 'DrainHeartbeat(epoch: $epoch, pass: $pass, at: $at)';
}

/// The guard an accepted recovery of a reconfigure halt leaves: until a
/// fill under another declared configuration has refilled the recovery's
/// rewound range (its fill position reaches [refillThrough]), a drainer
/// that declares [fingerprint] for the destination does not fill it.
///
/// Persisted under `backend_state` key `refill_guard_<destinationId>`.
class RefillGuard {
  const RefillGuard({
    required this.fingerprint,
    required this.recoveryEventId,
    required this.refillThrough,
  });

  /// Decode from the persisted JSON form.
  factory RefillGuard.fromJson(Map<String, Object?> json) {
    final fingerprint = json['fingerprint'];
    final recovery = json['recovery_event_id'];
    final through = json['refill_through'];
    if (fingerprint is! String || recovery is! String || through is! int) {
      throw const FormatException(
        'RefillGuard: missing or mistyped "fingerprint", '
        '"recovery_event_id" or "refill_through"',
      );
    }
    return RefillGuard(
      fingerprint: fingerprint,
      recoveryEventId: recovery,
      refillThrough: through,
    );
  }

  /// The fingerprint of the configuration in effect when the halt was
  /// honoured.
  final String fingerprint;

  /// `event_id` of the recovery event that set the guard.
  final String recoveryEventId;

  /// The fill position before the recovery rewound it: the guard stays
  /// until a fill has advanced the position to at least this sequence
  /// number, so the whole rewound range is refilled under one
  /// configuration.
  final int refillThrough;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'fingerprint': fingerprint,
    'recovery_event_id': recoveryEventId,
    'refill_through': refillThrough,
  };

  @override
  bool operator ==(Object other) =>
      other is RefillGuard &&
      other.fingerprint == fingerprint &&
      other.recoveryEventId == recoveryEventId &&
      other.refillThrough == refillThrough;

  @override
  int get hashCode => Object.hash(fingerprint, recoveryEventId, refillThrough);

  @override
  String toString() =>
      'RefillGuard(fingerprint: $fingerprint, refillThrough: $refillThrough, '
      'recoveryEventId: '
      '$recoveryEventId)';
}

/// One destination's persisted delivery state.
class DestinationDeliveryStatus {
  const DestinationDeliveryStatus({
    required this.schedule,
    required this.openHaltRequest,
    required this.wedge,
    required this.refillGuard,
    required this.unserved,
  });

  /// The persisted schedule.
  final DestinationSchedule schedule;

  /// The open halt request, or null.
  final HaltRequest? openHaltRequest;

  /// The open wedge, or null.
  final WedgeRecord? wedge;

  /// The refill guard, or null.
  final RefillGuard? refillGuard;

  /// Why the current drainer does not serve the destination, as of its
  /// latest pass, or null when it serves it (or no drainer has declared
  /// since the current epoch).
  final UnservedReason? unserved;

  @override
  String toString() =>
      'DestinationDeliveryStatus(schedule: $schedule, openHaltRequest: '
      '$openHaltRequest, wedge: $wedge, refillGuard: $refillGuard, '
      'unserved: ${unserved?.wire})';
}

/// The persisted delivery status of a database: the current drainer's
/// declaration and heartbeat, and each persisted destination's state.
class DeliveryStatus {
  const DeliveryStatus({
    required this.drainer,
    required this.heartbeat,
    required this.destinations,
  });

  /// The declaration of the drainer that holds the current drain epoch, or
  /// null when no drainer has declared since that epoch began.
  final DrainerDeclaration? drainer;

  /// The latest pass-start heartbeat, of whichever epoch.
  final DrainHeartbeat? heartbeat;

  /// Each persisted destination's state, by destination id.
  final Map<String, DestinationDeliveryStatus> destinations;

  @override
  String toString() =>
      'DeliveryStatus(drainer: $drainer, heartbeat: $heartbeat, '
      'destinations: $destinations)';
}
