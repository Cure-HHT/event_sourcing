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
/// library's log lines ([onLog]) and each run of a registry operation's
/// transaction body ([onRegistryBodyRun]); make a destination-registry
/// operation fail after it appends its audit event
/// ([failRegistryAuditAppend]), a drain outcome's transaction fail after
/// its writes ([failOutcomeTransaction]) or a fill's transaction fail after
/// its writes ([failFillTransaction]); and run interleaving operations at
/// named points between transactions ([beforeRegistryTransaction],
/// [insideTransform], [afterFillReads]). None can make an operation succeed
/// that would otherwise fail, an exception thrown by an observing seam
/// ([onLog], [onRegistryBodyRun]) is reported and does not reach the
/// library code that called it, and none receives a database handle or a
/// transaction.
@internal
@immutable
class DeliveryTestHooks {
  @internal
  const DeliveryTestHooks({
    this.onLog,
    this.failRegistryAuditAppend,
    this.failOutcomeTransaction,
    this.failFillTransaction,
    this.beforeRegistryTransaction,
    this.onRegistryBodyRun,
    this.insideTransform,
    this.afterFillReads,
  });

  /// Observes every line the library logs. An exception it throws is
  /// reported and does not reach the code that logged.
  final void Function(LibraryLogRecord record)? onLog;

  /// Consulted after the last write of a destination-registry operation
  /// whose audit event is of `entryType` (the audit append, and for a
  /// registration the schedule it writes), inside the operation's
  /// transaction. Returning true makes the operation throw [InjectedFailure]
  /// there, so the transaction rolls back.
  final bool Function(String entryType)? failRegistryAuditAppend;

  /// Consulted inside a drain outcome's transaction after its writes (the
  /// attempt, and the status it produces). `outcome` is the attempt's
  /// outcome (`ok`, `transient` or `permanent`). Returning true makes the
  /// transaction throw [InjectedFailure], so it rolls back.
  final bool Function(String destinationId, String outcome)?
  failOutcomeTransaction;

  /// Consulted inside a fill's compare-and-set transaction after its writes
  /// (the queue items, the fill position and the cleared replay request it
  /// commits). Returning true makes the transaction throw
  /// [InjectedFailure], so it rolls back.
  final bool Function(String destinationId)? failFillTransaction;

  /// Awaited before a destination-registry operation opens its transaction.
  /// `op` names the operation (for example `tombstoneAndRefill`). Runs
  /// between transactions, so it may run other library operations.
  final Future<void> Function(String op)? beforeRegistryTransaction;

  /// Observes each run of a destination-registry operation's transaction
  /// body, at its start. A backend may run a body more than once. An
  /// exception it throws is reported and does not reach the operation.
  final void Function(String op)? onRegistryBodyRun;

  /// Awaited while the fill runs a destination's transform, which always
  /// runs outside any transaction.
  final Future<void> Function(String destinationId)? insideTransform;

  /// Awaited after the fill's reads and before its compare-and-set
  /// transaction.
  final Future<void> Function(String destinationId)? afterFillReads;

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
