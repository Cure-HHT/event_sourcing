// Implements: EVS-DEV-destination-drain-lock/F
// the test seams are read only inside an
//   assertion, so a build without assertions never reads them and installed
//   seams have no effect.
import 'dart:async';

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/versions.dart';
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
/// its writes ([failOutcomeTransaction]), the drainer's wedge fail inside
/// its transaction ([afterWedgeHeadInTxn]) or report failure after it
/// committed ([afterWedgeTransaction]), or a fill's transaction fail after
/// its writes ([failFillTransaction]); and run interleaving operations at
/// named points between transactions ([beforeRegistryTransaction],
/// [insideTransform], [afterFillReads]). The boot of `EventStore.open` has
/// an observing seam ([onBootBodyRun]) and a failure injection after its
/// library-version append ([afterBootVersionEvent]). One seam is an input
/// substitution: [buildDeclaration] replaces the package and data-format
/// versions the boot decides with and records, so that one test process can
/// play two builds of the library against one database; it changes what
/// the boot decides, as a build of those versions would. Apart from
/// [buildDeclaration], under which an open succeeds or fails as the
/// declared build's would, none can make an operation succeed that would
/// otherwise fail; an exception thrown by an
/// observing seam ([onLog], [onRegistryBodyRun], [onBootBodyRun]) is
/// reported and does not reach the library code that called it, and none
/// receives a database handle or a transaction.
@internal
@immutable
class DeliveryTestHooks {
  @internal
  const DeliveryTestHooks({
    this.onLog,
    this.failRegistryAuditAppend,
    this.failOutcomeTransaction,
    this.afterWedgeHeadInTxn,
    this.afterWedgeTransaction,
    this.failFillTransaction,
    this.beforeRegistryTransaction,
    this.onRegistryBodyRun,
    this.insideTransform,
    this.afterFillReads,
    this.onBootBodyRun,
    this.afterBootVersionEvent,
    this.buildDeclaration,
  });

  /// Observes every line the library logs. An exception it throws is
  /// reported and does not reach the code that logged.
  final void Function(LibraryLogRecord record)? onLog;

  /// Consulted after the last write of a destination-registry operation
  /// whose audit event is of `entryType` (the audit append, and for a
  /// registration the schedule it writes), inside the operation's
  /// transaction. Returning true makes the operation throw [InjectedFailure]
  /// there, so the transaction rolls back.
  ///
  /// For the drainer's wedge (`entryType` is the wedge event's entry type)
  /// it is consulted in place of the wedge event's append, after the
  /// `wedged` status write: returning true makes the append fail, so the
  /// transaction rolls back the status write with it.
  final bool Function(String entryType)? failRegistryAuditAppend;

  /// Consulted inside a drain outcome's transaction after its writes (the
  /// attempt, and the status it produces). `outcome` is the attempt's
  /// outcome (`ok`, `transient` or `permanent`). Returning true makes the
  /// transaction throw [InjectedFailure], so it rolls back.
  final bool Function(String destinationId, String outcome)?
  failOutcomeTransaction;

  /// Consulted inside the transaction that wedges a destination's queue
  /// head, after its writes (the `wedged` status, the wedge event and the
  /// wedge record). Returning true makes the transaction throw
  /// [InjectedFailure], so it rolls back.
  final bool Function(String destinationId)? afterWedgeHeadInTxn;

  /// Consulted after a transaction that wedges a destination's queue head
  /// committed. Returning true makes the drain see [InjectedFailure] as
  /// that transaction's failure, although it committed: the point where a
  /// lost commit acknowledgement would surface.
  final bool Function(String destinationId)? afterWedgeTransaction;

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
  /// body, at its start, and each run of the drainer's wedge (`op` is
  /// `wedgeHeadInTxn`) at the start of the registry's part of the wedge
  /// transaction. A backend may run a body more than once. An exception it
  /// throws is reported and does not reach the operation.
  final void Function(String op)? onRegistryBodyRun;

  /// Awaited while the fill runs a destination's transform, which always
  /// runs outside any transaction.
  final Future<void> Function(String destinationId)? insideTransform;

  /// Awaited after the fill's reads and before its compare-and-set
  /// transaction.
  final Future<void> Function(String destinationId)? afterFillReads;

  /// Observes each run of `EventStore.open`'s boot transaction body, at its
  /// start. A backend may run the body more than once. An exception it
  /// throws is reported and does not reach the boot.
  final void Function()? onBootBodyRun;

  /// Consulted inside `EventStore.open`'s boot transaction right after it
  /// appends a library-version event. Returning true makes the boot throw
  /// [InjectedFailure] there, so the transaction rolls back.
  final bool Function()? afterBootVersionEvent;

  /// Input substitution: the package version and data-format version that
  /// `EventStore.open`'s boot decides with and records in the
  /// library-version event it appends, in place of the compiled
  /// `LibVersion.version` and `LibVersion.dataFormat`.
  final ({String version, DataFormatVersion dataFormat})? buildDeclaration;

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
