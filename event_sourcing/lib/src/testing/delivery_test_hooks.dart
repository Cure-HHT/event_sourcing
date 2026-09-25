// Implements: EVS-DEV-destination-drain-lock/F
// the test seams are read only inside an
//   assertion, so a build without assertions never reads them and installed
//   seams have no effect.
import 'dart:async';

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_migration.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart';

/// Private zone key under which [runWithDeliveryTestHooks] installs seams.
final Object _zoneKey = Object();

/// The library's test seams. Every seam is optional and absent by default.
///
/// Library code reads the installed seams only through [current], which
/// returns null unless assertions are enabled. The seams observe the
/// library's log lines ([onLog]), each run of a registry operation's
/// transaction body ([onRegistryBodyRun]) and each run of the drainer's
/// pre-send fence body ([onFenceBodyRun]); make a destination-registry
/// operation fail after it appends its audit event
/// ([failRegistryAuditAppend]), a drain outcome's transaction fail after
/// its writes ([failOutcomeTransaction]), the drainer's wedge fail inside
/// its transaction ([afterWedgeHeadInTxn]) or report failure after it
/// committed ([afterWedgeTransaction]), a fill's transaction fail after
/// its writes ([failFillTransaction]), or a delivery-cycle pass's read of
/// the persisted schedules fail ([failListSchedules]); and run
/// interleaving operations at named points between transactions
/// ([beforeRegistryTransaction], [insideTransform], [afterFillReads],
/// [afterHaltLoopTopRead], [beforeSendFence], [afterSendBeforeOutcome],
/// [afterCommitBeforePublish], [beforeGrantDelivered]). The delivery cycle
/// and its drain lock have seams that observe ([onInboundPoll]), hold an
/// epoch raise or a queue-changing transaction while a test interleaves
/// another party ([afterLockAcquireBeforeEpochBump],
/// [insideEpochBumpBeforeCommit], [beforeQueueWrites]), and make an
/// acquisition or a heartbeat fail ([failLockAcquisition],
/// [failAfterExclusionObtained], [failDrainLockVerification],
/// [failEpochBumpWithSerializationFailure], [stallEpochBumpPastQueryTimeout],
/// [holdDrainKeyOutsideLibrary], [failNextHeartbeat]); an observing seam
/// sees each wake of the delivery cycle ([onDeliveryWake]), and a cycle
/// can be started so that no wake runs a pass of it ([handDrivenCycle]).
/// The boot of
/// `EventStore.open` has an observing seam ([onBootBodyRun]) and a failure
/// injection after its library-version append ([afterBootVersionEvent]).
/// The incompatible-generation guard and the Postgres lock session have
/// seams that delay ([insideBootLock]), replace the timers of the lock
/// session's probe, the delivery cycle's cadence and heartbeat and a
/// drain-lock request's retry ([timerFactory]), make an operation fail
/// ([failGenerationRegistration], [failNextLockHeartbeat],
/// [stallLockHeartbeatPastQueryTimeout], [failOldSessionTermination],
/// [failLostSessionClose], [failProvisioningBeforeVersionWrite],
/// [webLocksUnavailable]), or make the lock session's check fail the way a
/// transaction-mode pooler would ([splitLockSessionStatements]). One seam is
/// an environment signal: [pageVisibility] narrows the page visibility the
/// browser's drain lock follows (it can make a visible page count as hidden,
/// never a hidden one as visible). Two seams
/// are input substitutions: [buildDeclaration] replaces the package and
/// data-format versions the boot decides with and records, and
/// [schemaDeclaration] replaces the Postgres migration list (and so the
/// schema version and its minimum) of the backends and provisionings
/// started in the zone, so that one test process can play two builds of
/// the library against one database; each changes what the checks decide,
/// as a build of those versions would. Apart from those two, under which an
/// open or a provisioning succeeds or fails as the declared build's would,
/// none can make an operation succeed that would otherwise fail; an
/// exception thrown by an observing seam ([onLog], [onRegistryBodyRun],
/// [onBootBodyRun], [onFenceBodyRun], [onDeliveryWake]) is reported and does not reach the
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
    this.afterWedgeHeadInTxn,
    this.afterWedgeTransaction,
    this.failFillTransaction,
    this.failListSchedules,
    this.beforeRegistryTransaction,
    this.onRegistryBodyRun,
    this.insideTransform,
    this.afterFillReads,
    this.afterHaltLoopTopRead,
    this.beforeSendFence,
    this.onFenceBodyRun,
    this.onBootBodyRun,
    this.afterBootVersionEvent,
    this.buildDeclaration,
    this.insideBootLock,
    this.splitLockSessionStatements = false,
    this.failGenerationRegistration,
    this.failNextLockHeartbeat,
    this.stallLockHeartbeatPastQueryTimeout,
    this.failOldSessionTermination,
    this.failLostSessionClose,
    this.timerFactory,
    this.failProvisioningBeforeVersionWrite,
    this.webLocksUnavailable = false,
    this.schemaDeclaration,
    this.failLockAcquisition,
    this.failAfterExclusionObtained,
    this.beforeGrantDelivered,
    this.onInboundPoll,
    this.afterLockAcquireBeforeEpochBump,
    this.insideEpochBumpBeforeCommit,
    this.beforeQueueWrites,
    this.afterSendBeforeOutcome,
    this.failDrainLockVerification,
    this.failEpochBumpWithSerializationFailure,
    this.stallEpochBumpPastQueryTimeout,
    this.holdDrainKeyOutsideLibrary,
    this.failNextHeartbeat,
    this.afterCommitBeforePublish,
    this.pageVisibility,
    this.onDeliveryWake,
    this.handDrivenCycle = false,
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

  /// Consulted when a delivery-cycle pass reads the persisted destination
  /// schedules, before the read. Returning true makes the read throw
  /// [InjectedFailure].
  final bool Function()? failListSchedules;

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

  /// Awaited after the drainer's non-transactional read of a destination's
  /// halt request at the top of each iteration of its loop, before the
  /// transaction that honours an open request and before the pre-send
  /// fence.
  final Future<void> Function(String destinationId)? afterHaltLoopTopRead;

  /// Awaited after the drainer built a send's payload and before the
  /// pre-send fence transaction opens.
  final Future<void> Function(String destinationId)? beforeSendFence;

  /// Observes each run of the drainer's pre-send fence transaction body, at
  /// its start. A backend may run a body more than once. An exception it
  /// throws is reported and does not reach the drainer.
  final void Function(String destinationId)? onFenceBodyRun;

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

  /// Awaited while the incompatible-generation guard holds a database's
  /// exclusive boot lock, after it inspected the live generations and
  /// before it registers its own; on Postgres it runs inside the lock
  /// session's operation, so no probe runs meanwhile.
  final Future<void> Function()? insideBootLock;

  /// When true, the Postgres lock session's check sends its statements
  /// alternately over the lock connection and a second connection the
  /// library opens for the purpose, as a transaction-mode pooler would
  /// route them, so the check fails.
  final bool splitLockSessionStatements;

  /// Consulted by the Postgres guard after it took the first shared lock of
  /// a registration. Returning true makes the registration throw
  /// [InjectedFailure] there.
  final bool Function()? failGenerationRegistration;

  /// Consulted by each probe of the Postgres lock session. Returning true
  /// makes the probe fail as a failed statement would, so the session is
  /// declared lost.
  final bool Function()? failNextLockHeartbeat;

  /// Consulted by each probe of the Postgres lock session. Returning true
  /// makes the probe a statement that runs one second longer than the lock
  /// session's query timeout, so the driver cancels it on a live session.
  final bool Function()? stallLockHeartbeatPastQueryTimeout;

  /// Consulted when the Postgres backend ends the server session of a lost
  /// lock session that still holds a library lock. Returning true makes the
  /// library treat the termination as refused.
  final bool Function()? failOldSessionTermination;

  /// Consulted when the Postgres backend closes a lock session it declared
  /// lost. Returning true makes the library treat the close as failed: the
  /// connection is set aside unclosed (and closed when the backend closes),
  /// so its server session stays alive until the library ends it.
  final bool Function()? failLostSessionClose;

  /// Creates the timers of the Postgres lock session's probe, of the
  /// delivery cycle's cadence and heartbeat, and of a drain-lock request's
  /// retry, in place of `Timer.periodic` (a one-shot timer is a periodic
  /// one cancelled at its first tick).
  final Timer Function(Duration period, void Function(Timer timer) callback)?
  timerFactory;

  /// Consulted by `PostgresBackend.provision` after the DDL of its
  /// migration steps and before it writes the schema version pair.
  /// Returning true makes the provisioning throw [InjectedFailure] there,
  /// so its transaction rolls back.
  final bool Function()? failProvisioningBeforeVersionWrite;

  /// When true, the browser lock-manager wrapper behaves as if the page had
  /// no `navigator.locks`.
  final bool webLocksUnavailable;

  /// Input substitution: the Postgres migration list, in place of the
  /// compiled one, for the backends opened and the provisionings run in
  /// the zone; its last step gives the schema version and the minimum
  /// compatible schema version they require and record.
  final List<PostgresMigrationStep>? schemaDeclaration;

  /// Consulted at the start of every drain-lock acquisition, before the
  /// backend obtains its exclusion primitive. Returning true makes the
  /// acquisition throw [InjectedFailure] there, as an unreachable database
  /// would.
  final bool Function()? failLockAcquisition;

  /// Consulted by a drain-lock acquisition on a backend over one database
  /// handle after it obtained its exclusion primitive, inside the
  /// transaction that raises the drain epoch, after its write. Returning
  /// true makes the acquisition throw [InjectedFailure] there, so the epoch
  /// is not raised and the primitive is given up.
  final bool Function()? failAfterExclusionObtained;

  /// Awaited by a drain-lock request after an acquisition succeeded and
  /// before the request delivers the lock, so a test can cancel the request
  /// in between.
  final Future<void> Function()? beforeGrantDelivered;

  /// Observes each run of the delivery cycle's inbound poll, after every
  /// destination of the pass was drained. An exception it throws is
  /// reported and does not reach the cycle.
  final void Function()? onInboundPoll;

  /// Awaited by a Postgres drain-lock acquisition after it took the drain
  /// key on the lock session and before the transaction that raises the
  /// drain epoch. Runs between the acquisition's statements, on no
  /// transaction.
  final Future<void> Function()? afterLockAcquireBeforeEpochBump;

  /// Awaited by a Postgres drain-lock acquisition inside the transaction
  /// that raises the drain epoch, after its write and before its commit.
  /// It may only await a test-side signal.
  final Future<void> Function()? insideEpochBumpBeforeCommit;

  /// Awaited inside each queue-changing transaction of the drainer, after
  /// the drain-lock check and before the transaction's writes. It may only
  /// await a test-side signal.
  final Future<void> Function(String destinationId)? beforeQueueWrites;

  /// Awaited by the drainer after a send returned and before the
  /// transaction that records its outcome opens.
  final Future<void> Function(String destinationId)? afterSendBeforeOutcome;

  /// Consulted by a Postgres drain-lock acquisition in place of the
  /// verification, through the pool, that the lock session holds the drain
  /// key. Returning true makes the verification report a mismatch, so the
  /// acquisition gives the key up and throws
  /// `DrainLockConfigurationException`.
  final bool Function()? failDrainLockVerification;

  /// Consulted by a Postgres drain-lock acquisition inside the transaction
  /// that raises the drain epoch, after its write. Returning true makes the
  /// library raise a serialization failure (SQLSTATE 40001) from the server
  /// there, so the transaction rolls back.
  final bool Function()? failEpochBumpWithSerializationFailure;

  /// Consulted by a Postgres drain-lock acquisition inside the transaction
  /// that raises the drain epoch, after its write. Returning true makes the
  /// library run a statement on the lock session that lasts one second
  /// longer than the lock session's query timeout, so the driver cancels it
  /// on a live session.
  final bool Function()? stallEpochBumpPastQueryTimeout;

  /// Consulted by a Postgres drain-lock acquisition before it checks
  /// whether the lock session already holds the drain key. Returning true
  /// makes the library take the drain key on the lock session without
  /// creating a drain lock, as something other than the library would.
  final bool Function()? holdDrainKeyOutsideLibrary;

  /// Consulted by each drain-lock heartbeat. Returning true makes the
  /// heartbeat fail, so the lock is reported lost.
  final bool Function()? failNextHeartbeat;

  /// Awaited by the event store after a transaction that appended events
  /// committed and before it publishes them, as a continuation that resumes
  /// late would wait.
  final Future<void> Function()? afterCommitBeforePublish;

  /// Environment signal: narrows the visibility of the page
  /// (`document.visibilityState`, `pagehide`, `freeze`) for the drain locks
  /// acquired and requested in the zone: the page counts as visible only
  /// when both this seam and the page itself say so. Two tab models in one
  /// page share one document, and a test cannot set the document's
  /// visibility. Read only by the browser's drain lock; a hidden page makes
  /// a holder hand the lock over and a request wait, and the lock itself is
  /// still granted only by the browser's lock manager.
  final TestPageVisibility? pageVisibility;

  /// Observes each wake of an event store's delivery cycle (after an
  /// append, a committed registry operation, a committed dispatch or a
  /// security-context operation), before the trigger fires. `cycleWoken`
  /// is true when a started, not yet closed delivery cycle held the store's
  /// trigger slot. An exception it throws is reported and does not reach
  /// the operation that woke.
  final void Function(bool cycleWoken)? onDeliveryWake;

  /// Read once by `SyncCycle.start`. When true, the started cycle holds its
  /// event store's trigger slot as any cycle does, but a wake runs no pass
  /// of it: only a call of the cycle, its cadence and its lock requests
  /// run passes, so a test drives the passes itself.
  final bool handDrivenCycle;

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

/// A page visibility a test sets, for the [DeliveryTestHooks.pageVisibility]
/// seam.
@internal
final class TestPageVisibility {
  @internal
  TestPageVisibility({bool visible = true}) : _visible = visible;

  bool _visible;
  final StreamController<void> _changes = StreamController<void>.broadcast();

  /// Whether the page is visible.
  bool get visible => _visible;

  /// Sets the page's visibility; a change is announced on [changes].
  set visible(bool value) {
    if (value == _visible) return;
    _visible = value;
    _changes.add(null);
  }

  /// An event after each change of [visible].
  Stream<void> get changes => _changes.stream;
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
