// Implements: EVS-DEV-destination-drain-lock/F
// the test seams are read only inside an
//   assertion, so a build without assertions never reads them and installed
//   seams have no effect.
import 'dart:async';

import 'package:event_sourcing/src/logging.dart';
import 'package:meta/meta.dart';

/// Private zone key under which [runWithDeliveryTestHooks] installs seams.
final Object _zoneKey = Object();

/// The library's test seams. Every seam is optional and absent by default.
///
/// Library code reads the installed seams only through [current], which
/// returns null unless assertions are enabled. The seams observe the
/// library's log lines ([onLog]) and make a destination-registry operation
/// fail after it appends its audit event ([failRegistryAuditAppend]); none
/// can make an operation succeed that would otherwise fail, none can change
/// what the library does by throwing, and none receives a database handle
/// or a transaction.
@internal
@immutable
class DeliveryTestHooks {
  @internal
  const DeliveryTestHooks({this.onLog, this.failRegistryAuditAppend});

  /// Observes every line the library logs. An exception it throws is
  /// reported and does not reach the code that logged.
  final void Function(LibraryLogRecord record)? onLog;

  /// Consulted after a destination-registry operation appends its audit
  /// event of `entryType`, inside the operation's transaction. Returning
  /// true makes the operation throw [InjectedFailure] there, so the
  /// transaction rolls back.
  final bool Function(String entryType)? failRegistryAuditAppend;

  /// The seams installed for the current zone, or null. Always null when
  /// assertions are disabled: the zone is read only inside an assertion.
  static DeliveryTestHooks? get current {
    DeliveryTestHooks? hooks;
    assert(() {
      hooks = Zone.current[_zoneKey] as DeliveryTestHooks?;
      return true;
    }(), 'reads the installed test seams');
    return hooks;
  }
}

/// Runs [body] with [hooks] installed as the test seams of its zone.
@internal
R runWithDeliveryTestHooks<R>(DeliveryTestHooks hooks, R Function() body) =>
    runZoned(body, zoneValues: <Object?, Object?>{_zoneKey: hooks});

/// The failure a test seam injects at a named point.
@internal
class InjectedFailure implements Exception {
  @internal
  const InjectedFailure(this.point);

  /// The named point the failure was injected at.
  final String point;

  @override
  String toString() => 'InjectedFailure at $point';
}
