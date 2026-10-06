// Implements: EVS-PRD-event-log/A
// EventStore is the append-only, immutable
//   log; append/appendInTxn are its sole write paths.
// Implements: EVS-PRD-event-log/B
// the storage backend preserves a stable
//   total order; EventStore surfaces it via read/findAll/subscribe.
// Implements: EVS-PRD-event-log/D
// EventStore.read and subscribe both
//   accept a starting sequence position for replay from any offset.
// Implements: EVS-DEV-event-store-open/A
// EventStore.open is the sole
//   production constructor; EventStore._ is private and library-internal
//   only; EventStore.openForTest is visible for testing only.
// Implements: EVS-DEV-event-store-open/B
// open appends lib_version_initialized
//   when the database holds no locally appended library-version event
//   (_runBoot).
// Implements: EVS-DEV-event-store-open/C
// open appends lib_version_changed
//   when its package version or data format differs from the latest locally
//   recorded one, older ones included (_runBoot).
// Implements: EVS-DEV-event-store-open/D
// every library-version event records
//   the package version and data format; a different recorded data-format
//   major throws DataFormatIncompatibleError before any write (_runBoot).
// Implements: EVS-DEV-event-store-open/E
// the whole boot runs in one
//   bootTransaction, refusals first, then the library-version event and
//   registry audit, then creating and marking view copies, the generation
//   record and the boot record (_runBoot).
// Implements: EVS-DEV-event-store-open/N
// the boot transaction reads, writes and deletes no view row; it creates
//   and marks copy records only.
// Implements: EVS-DEV-event-store-open/O
// the boot reads no event other than the library-version events, the
//   registry audit events and the latest event of the log.
// Implements: EVS-DEV-event-store-open/F
// the database identity is minted
//   or adopted at the first open, recorded in lib_version_initialized, and
//   checked at every later open; a database an earlier data format wrote
//   is refused as one to reset before any write (_runBoot).
// Implements: EVS-DEV-append-stamps-registered-version/A
// append looks up
//   entryTypes.byId(entryType).registeredVersion and stamps its major and
//   minor on the event.
// Implements: EVS-DEV-append-stamps-registered-version/B
// appendInTxn
//   stamps the same registered major and minor as append.
// Implements: EVS-DEV-append-stamps-registered-version/C
// entryTypeVersion
//   does not appear on the public append/appendInTxn signatures.
// Implements: EVS-DEV-version-compatibility/C
// every append path stamps LibVersion.dataFormat as the event's
//   lib_format_version.
// Implements: EVS-DEV-view-convergence/B
// _runBoot creates an empty copy, watermark before the first event of the
//   log, for each registered view whose fingerprint has no stored unmarked
//   copy.
// Implements: EVS-DEV-view-convergence/D
// _runBoot marks for deletion every stored copy whose fingerprint the
//   opening build does not register.
// Implements: EVS-DEV-entry-type-downgrade-refusal/A
// EntryTypeVersionDowngradeError
//   is thrown from open when a registered major is below the major the
//   database's generation record holds for that entry type.
// Implements: EVS-DEV-entry-type-downgrade-refusal/C
// EntryTypeVersionDowngradeError
//   carries the entryType id and the recorded and registered versions, each
//   a major and a minor, for diagnostic logging.

import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:typed_data';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:event_sourcing/src/actions/action_context.dart';
import 'package:event_sourcing/src/actions/action_registry.dart';
import 'package:event_sourcing/src/actions/action_submission.dart';
import 'package:event_sourcing/src/actions/authorization_decision.dart'
    show Deny, DenyReason;
import 'package:event_sourcing/src/actions/authorization_policy.dart';
import 'package:event_sourcing/src/actions/denial_events.dart';
import 'package:event_sourcing/src/actions/dispatch_result.dart';
import 'package:event_sourcing/src/actions/execution_result.dart';
import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:event_sourcing/src/actions/idempotency_errors.dart';
import 'package:event_sourcing/src/actions/idempotency_store.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart' show UserPrincipal;
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/causal_record.dart';
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/default_destination_wedges_spec.dart';
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/halt_purpose.dart';
import 'package:event_sourcing/src/destinations/receiver_response.dart';
import 'package:event_sourcing/src/destinations/wedge_cause.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/ingest/chain_checks.dart';
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/ingest/delivery_envelope.dart';
import 'package:event_sourcing/src/ingest/ingest_errors.dart';
import 'package:event_sourcing/src/ingest/ingest_result.dart';
import 'package:event_sourcing/src/ingest/sender_succession.dart';
import 'package:event_sourcing/src/lifecycle/boot_errors.dart';
import 'package:event_sourcing/src/lifecycle/boot_progress.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/permissions/wait_for_current_views.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/fold_failure.dart'
    show FoldFailureReason, foldFailedFindingEvidence;
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/projections/view_catch_up.dart';
import 'package:event_sourcing/src/projections/view_fingerprint.dart';
import 'package:event_sourcing/src/projections/view_read.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/security_details.dart';
import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:event_sourcing/src/security/security_retention_policy.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/boot_check.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/event_hash.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/record_characters.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/storage_description.dart';
import 'package:event_sourcing/src/storage/storage_reader.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/transaction_rerun_limit.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:event_sourcing/src/subscriptions/subscription_engine.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/declared_configuration.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:event_sourcing/src/verification/chain_verification_verdict.dart';
import 'package:event_sourcing/src/verification/chain_walk.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:provenance/provenance.dart';
import 'package:uuid/uuid.dart';

part 'actions/action_dispatcher.dart';
// Implements: EVS-DEV-storage-capability/C
// the library's writers that take a handed-out object -- the bootstrap, the
//   destination registry, the delivery cycle and its fill and drain, the
//   view rebuild and the action dispatcher -- share the event store's Dart
//   library, so the backend, the reserved append, the trigger slot, the
//   wake and the collector's publish stay private to it.
// Implements: EVS-PRD-storage-barrier/B
// no handed-out object carries a member that writes the library's state
//   outside its public operations; those members are library-private.
// Implements: EVS-PRD-storage-barrier/E
// no handed-out object yields the storage backend or a handle under it.
// Implements: EVS-DEV-storage-capability/D
// the members of the handed-out types that are accessible outside their
//   declaring Dart library are pinned, by name and signature, in a list
//   committed with the library's tests; the members declared here and in
//   its parts are the event store's side of that surface.
part 'bootstrap.dart';
part 'destinations/destination_registry.dart';
part 'ingest/receiver_endpoint.dart';
part 'projections/rebuild.dart';
part 'sync/drain.dart';
part 'sync/fill_batch.dart';
part 'sync/historical_replay.dart';
part 'sync/succession_restore.dart';
part 'sync/sync_cycle.dart';

/// The delivery cycle's trigger, held in an event store's trigger slot.
typedef _DeliveryTrigger = Future<void> Function();

/// Accumulates the [StoredEvent]s and [AggregateFoldChange]s that
/// [EventStore.appendInTxn] produces inside one run of a transaction body,
/// so that [EventStore.runTransaction] can publish them to the subscription
/// bus after that run commits.
///
/// A collector belongs to exactly one run of one transaction body: the
/// event store creates it for the run, binds it to the run's
/// [Transaction], and closes it when the run ends. [EventStore.appendInTxn]
/// refuses a collector that is bound to another transaction or whose run
/// has ended, so an append can neither publish through a collector whose
/// transaction does not commit it nor commit without being published.
///
/// A consumer never constructs a collector; it receives one as the second
/// argument of a [EventStore.runTransaction] body and passes it to
/// [EventStore.appendInTxn].
class PublishCollector {
  PublishCollector._(this._transaction, this._onFirstEvent);

  final Transaction _transaction;
  final void Function(int sequenceNumber) _onFirstEvent;
  bool _open = true;
  final List<StoredEvent> _events = <StoredEvent>[];
  final List<AggregateFoldChange> _rowChanges = <AggregateFoldChange>[];

  // Implements: EVS-PRD-storage-barrier/D
  // publishing to live subscribers is private to the event store's library.
  void _add(StoredEvent event) {
    _checkOpen();
    if (_events.isEmpty) _onFirstEvent(event.sequenceNumber);
    _events.add(event);
  }

  void _addRowChanges(Iterable<AggregateFoldChange> changes) {
    _checkOpen();
    _rowChanges.addAll(changes);
  }

  void _checkOpen() {
    if (!_open) {
      throw StateError(
        'PublishCollector used after the transaction run it belongs to '
        'ended',
      );
    }
  }

  List<StoredEvent> get events => List<StoredEvent>.unmodifiable(_events);

  List<AggregateFoldChange> get rowChanges =>
      List<AggregateFoldChange>.unmodifiable(_rowChanges);
}

/// A committed transaction's publication, waiting for its turn in sequence
/// order.
final class _Publication {
  _Publication(this.publish);

  final void Function() publish;
}

/// Result of `EventStore.applyRetentionPolicy`: counts of rows touched by
/// the compact and purge sweeps.
class RetentionResult {
  const RetentionResult({
    required this.compactedCount,
    required this.purgedCount,
  });
  final int compactedCount;
  final int purgedCount;
}

/// Thrown by [EventStore.open] when a registered entry type's major is
/// below the highest major the database's generation record holds for it:
/// an earlier boot registered that major, so events or copies of that
/// major may already exist, which this build cannot read. A higher
/// recorded minor of the same major is not a downgrade. The resolution is
/// a build whose registered major is at least [fromVersion]'s major.
// Implements: EVS-DEV-entry-type-downgrade-refusal/A
class EntryTypeVersionDowngradeError extends Error {
  EntryTypeVersionDowngradeError({
    required this.entryType,
    required this.fromVersion,
    required this.toVersion,
  });

  /// The entry type whose registered major is below its recorded major.
  final String entryType;

  /// The highest major the generation record holds for [entryType], with
  /// minor 0.
  final EntryTypeVersion fromVersion;

  /// The version this build registers for [entryType].
  final EntryTypeVersion toVersion;

  @override
  String toString() =>
      'EntryTypeVersionDowngradeError: entry type "$entryType" was '
      'registered at major $fromVersion by an earlier open of the '
      'database (its generation record), but this build registers version '
      '$toVersion. A build whose registered major (${toVersion.major}) is '
      'below the recorded major (${fromVersion.major}) is refused. Run a '
      'build that registers major ${fromVersion.major} or higher for '
      '"$entryType".';
}

/// The substrate's append-only event log. Serves callers across mobile and
/// server deployments via one `append` method that takes per-field
/// arguments plus optional `SecurityDetails`.
///
/// `EventStore` is permission-blind: it exposes unguarded
/// read/write APIs to anything holding a reference. All access control
/// lives in the widget layer (Flutter widgets client-side, request
/// handlers server-side).
class EventStore {
  EventStore._({
    required StorageBackend backend,
    required this.entryTypes,
    required this.source,
    required MutableSecurityContextStore securityContexts,
    required OpenedStorage? storage,
    required this.databaseId,
    required Map<String, String> viewCopyIds,
    required GenerationRegistration registration,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    Clock? clock,
    Uuid? uuid,
  }) : _backend = backend,
       _viewCopyIds = viewCopyIds,
       _interpreter = ProjectionInterpreter(
         projections: projections ?? ProjectionRegistry(),
         promoters: promoters ?? PromoterRegistry(),
         entryTypes: entryTypes,
       ),
       _promoters = promoters ?? PromoterRegistry(),
       _securityContexts = securityContexts,
       securityContexts = _SecurityContextReader(securityContexts),
       _storage = storage,
       _registration = registration,
       _clock = clock,
       _uuid = uuid ?? const Uuid() {
    // _catchUp's recordFoldFailedFinding callback closes over this
    // instance's _recordCatchUpFoldFailedFinding, which is only legal once
    // construction has reached the constructor body.
    _catchUp = ViewCatchUpDriver(
      backend: backend,
      entryTypes: entryTypes,
      projections: projections ?? ProjectionRegistry(),
      promoters: promoters ?? PromoterRegistry(),
      viewCopyIds: viewCopyIds,
      databaseId: databaseId,
      recordFoldFailedFinding: _recordCatchUpFoldFailedFinding,
      clock: clock,
    );
  }

  /// The storage this store appends to and reads from. It is private to
  /// the event store's Dart library: the library's own writers (the
  /// destination registry, the delivery cycle, the view rebuild, the
  /// bootstrap) share that library and read it directly.
  // Implements: EVS-DEV-storage-capability/C
  // the backend is reachable only inside the event store's Dart library.
  final StorageBackend _backend;
  final EntryTypeRegistry entryTypes;
  final Source source;

  /// The id of this instance's current copy of each registered view, by
  /// view name, as the boot decided it (EVS-DEV-view-convergence/B+D). Row
  /// storage addresses a view's rows by its copy id, never by its name.
  final Map<String, String> _viewCopyIds;

  /// Catches up this instance's copies that are behind after boot, and
  /// deletes copies marked for deletion, in transactions bounded to run
  /// after this store's construction and before it closes
  /// (EVS-DEV-view-convergence).
  late final ViewCatchUpDriver _catchUp;

  /// The copy id of [viewName]'s current copy for this instance. Throws
  /// [StateError] for a view no registered [ProjectionSpec] names.
  String _copyIdOf(String viewName) {
    final copyId = _viewCopyIds[viewName];
    if (copyId == null) {
      throw StateError(
        'EventStore: "$viewName" names no view this instance registered '
        'at EventStore.open.',
      );
    }
    return copyId;
  }

  /// Test-only access to [_copyIdOf]: the id of this instance's current
  /// copy of [viewName]'s view, the key row storage methods take in place
  /// of the view's name.
  @visibleForTesting
  String copyIdOf(String viewName) => _copyIdOf(viewName);

  /// Test-only access to the catch-up driver's in-memory progress of
  /// [copyId]: its last failure and the backoff it is retrying under, if
  /// it is behind, or null when no catch-up transaction on it has failed.
  @visibleForTesting
  ViewCopyProgress? catchUpProgressOf(String copyId) =>
      _catchUp.progressOf(copyId);

  /// The security contexts stored beside this store's events, for reading:
  /// an object of its own that declares no writing member. The event
  /// store's own operations (append, redaction, retention) write them.
  // Implements: EVS-DEV-storage-capability/E
  // the security-context store handed to the application is a separate
  //   object declaring only the reads.
  final SecurityContextStore securityContexts;

  /// The writing store over the same contexts, private to the event store.
  final MutableSecurityContextStore _securityContexts;

  /// Reads of this store's storage: an object of its own that declares no
  /// writing member. Its transactions run for reads only (`READ ONLY` on
  /// Postgres), and its `...InTxn` reads accept the handles it issued and
  /// those [runTransaction] issued, each while its body runs.
  // Implements: EVS-DEV-storage-capability/E
  // the storage reader handed to the application is a separate object
  //   delegating only the backend's reads.
  late final StorageReader reader = _StorageReader(this);

  /// This store's receiver endpoint: it accepts the native deliveries a
  /// sender presents on its delivery channels.
  // Implements: EVS-PRD-delivery-channel/P
  // the library provides the receiver endpoint over the event store; it is
  //   never built from a backend.
  late final ReceiverEndpoint receiverEndpoint = ReceiverEndpoint._(this);

  /// This store's succession-restore operation, private to the library:
  /// [restoreFromReceiver] is the public entry point.
  late final _SuccessionRestore _successionRestore = _SuccessionRestore._(this);

  /// Restores, through [destinationId] (a destination [registry] holds
  /// registered, whose [Destination.channelPull] this pulls with), every
  /// channel the destination's receiver lists for [predecessorDatabaseId]
  /// and that identity's succession lineage, storing every carried event
  /// this database does not hold and appending the succession event
  /// (`system.destination_sender_succeeded`) naming [predecessorDatabaseId]
  /// and each restored channel's last delivery, all in one transaction.
  ///
  /// Pulls the channel listing and, for each listed channel, its deliveries
  /// from 1 up to the listing's record, outside any transaction. Every
  /// carried event, deduplicated across the channels and generations that
  /// carry it (an event served on a channel's earlier generation and its
  /// current one is the ordinary case after that channel resumed on a new
  /// generation), is stored in lineage order (the earliest predecessor
  /// first, derived from the succession events among the pulled records),
  /// then ascending origin position, then ascending registration
  /// identifier, generation and delivery number of the lowest pulled
  /// delivery that carried it. [initiator] names who asked for the restore.
  ///
  /// Throws [SuccessionRestoreRefused], storing nothing, when the restore
  /// is refused (see its `reason` constants): the successor's log already
  /// holds an authored application event or an authored succession event,
  /// [predecessorDatabaseId] names the successor's own identity, the
  /// receiver lists no channel for it, or a pull cannot serve a delivery
  /// the restore asks for.
  // Implements: EVS-DEV-sender-succession/A
  // the restore operation pulls the channels a receiver lists for a
  //   predecessor identity and each listed channel's deliveries from 1 up
  //   to the receiver's record.
  // Implements: EVS-DEV-sender-succession/C
  // every carried event this database does not hold is stored, in one
  //   transaction, in lineage order, then ascending origin position, then
  //   ascending registration identifier, generation and delivery number of
  //   the lowest pulled delivery carrying it, each with the successor's
  //   provenance entry naming the channel and delivery it was pulled from.
  // Implements: EVS-DEV-sender-succession/D
  // the succession event is appended only in the transaction that stores
  //   the predecessor's events, naming the successor's destination and
  //   registration, the successor, the predecessor and each restored
  //   channel's last delivery.
  // Implements: EVS-PRD-delivery-channel/Q
  // a database that restores a predecessor's deliveries records a
  //   succession event in the transaction that stores them.
  // Implements: EVS-PRD-event-log/C
  // the restore stores one identity's events in the order that identity
  //   wrote them.
  // Implements: EVS-DEV-event-record/D+G
  // the restore's provenance entry names the restoring database and keeps
  //   every provenance entry the record carries exactly as carried.
  // Implements: EVS-PRD-storage-barrier/C
  // the restore operation is one of the public operations that may append
  //   a reserved event.
  // Implements: EVS-PRD-destinations/K
  // the restore operation is one of the public operations exempted from
  //   the internal-mutator rule.
  // Implements: EVS-DEV-sender-succession/H
  // the restore refuses, before storing anything, a restore into a
  //   successor whose log holds an authored application event, one whose
  //   log holds an authored succession event, one naming the successor's
  //   own identity, one the receiver lists no channel for, and one whose
  //   pull cannot serve a delivery asked for; throws
  //   SuccessionRestoreRefused naming the refusal.
  // Implements: EVS-PRD-delivery-channel/R
  // the restore refuses, before storing anything, into a database that has
  //   authored an event of an application entry type.
  Future<StoredEvent> restoreFromReceiver({
    required DestinationRegistry registry,
    required String destinationId,
    required String predecessorDatabaseId,
    required Initiator initiator,
  }) => _successionRestore._run(
    registry: registry,
    destinationId: destinationId,
    predecessorDatabaseId: predecessorDatabaseId,
    initiator: initiator,
  );

  /// The idempotency store over this store's storage, for an action
  /// dispatcher, when it runs on Postgres: its outcomes persist in the
  /// database's `idempotency` table, and every lookup, record and sweep runs
  /// in the backend's fenced transactions. Null on any other backend, where
  /// the application supplies an idempotency store of its own.
  // Implements: EVS-DEV-storage-capability/I
  // the library builds the idempotency store over the storage it opened.
  late final IdempotencyStore? idempotencyStore = _backend
      .idempotencyStoreOverThis();

  /// The transaction handles this store has issued whose body is running.
  final Set<Transaction> _liveHandles = Set<Transaction>.identity();

  /// The storage [open] opened from its description, which [close] closes
  /// when the library opened it; null for [openForTest], whose backend the
  /// caller keeps.
  final OpenedStorage? _storage;

  /// The trigger slot: the trigger of the one started, not yet closed
  /// delivery cycle over this store, or null.
  _DeliveryTrigger? _deliveryTrigger;

  /// Wakes the delivery cycle that holds the trigger slot, if any, without
  /// waiting for it. Nothing it raises reaches the caller: a trigger that
  /// throws, synchronously or through its future, is logged.
  // Implements: EVS-DEV-destination-drain-lock/D
  // a delivery-cycle trigger never raises into the operation that fires it.
  void _wakeDeliveryCycle() {
    final trigger = _deliveryTrigger;
    _observeDeliveryWake(cycleWoken: trigger != null);
    if (trigger == null) return;
    void report(Object e, StackTrace st) => libraryLog(
      'event_store',
      'the delivery cycle trigger failed',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
    try {
      unawaited(trigger().then((_) {}, onError: report));
    } on Object catch (e, st) {
      report(e, st);
    }
  }

  static void _observeDeliveryWake({required bool cycleWoken}) {
    final seam = DeliveryTestHooks.current?.onDeliveryWake;
    if (seam == null) return;
    try {
      seam(cycleWoken);
    } on Object catch (e, st) {
      libraryLog(
        'event_store',
        'the onDeliveryWake test seam threw',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  final ProjectionInterpreter _interpreter;

  /// The database identity: a random identifier minted at the database's
  /// first open, recorded in its `lib_version_initialized` event, and the
  /// same for every event store over the database, whatever its [source].
  final String databaseId;

  /// This store's registration with the backend's incompatible-generation
  /// guard, released by [close].
  final GenerationRegistration _registration;

  /// Sealed registry of promoter specs, threaded in from [EventStore.open] (or
  /// supplied directly on the constructor). Used by `rebuildView` to apply
  /// promoter chains during replay.
  final PromoterRegistry _promoters;

  /// Exposes the projection registry for `rebuildView`.
  ProjectionRegistry get projections => _interpreter.projections;

  /// Exposes the promoter registry for `rebuildView`.
  PromoterRegistry get promoters => _promoters;

  final Clock? _clock;
  final Uuid _uuid;
  final SubscriptionEngine _subs = SubscriptionEngine();

  /// Opens an [EventStore] over the storage [storage] describes: the single
  /// production entry point. The returned store is fully configured and
  /// ready for use.
  ///
  /// For a [SembastStorage] or a `PostgresStorage` description (exported by
  /// `package:event_sourcing/postgres.dart`) the library opens the storage
  /// itself, with the Sembast factory it selects for the description or
  /// with `PostgresBackend.open`, builds the security-context
  /// store over it, and holds both: [close] closes that storage, and an open
  /// that fails after the storage opened closes it before the error reaches
  /// the caller. An [ApplicationSuppliedStorage] carries a backend the
  /// application constructed, with its security-context store; the
  /// application keeps it and closes it, and neither [close] nor a failed
  /// open does.
  ///
  /// The open first registers, in [entryTypes] and [projections], every
  /// reserved system entry type ([kSystemEntryTypes]) and the library's
  /// default destination-wedges view ([defaultDestinationWedgesSpec]) they
  /// lack, then seals [projections]; the reserved types are therefore part
  /// of the build's data generation. A registry that already holds a
  /// reserved entry-type id, or a spec under the default view's name, is
  /// accepted only when it holds the library's own object (the exported
  /// definition or spec, which an earlier open with the same registries
  /// also leaves there); any other definition, or a sealed [projections]
  /// that lacks the view, throws [ArgumentError] naming the reserved id or
  /// view, before either registry changes and before anything is written.
  ///
  /// Before anything is written, the build registers its data generation
  /// (its data-format major and each registered entry type's major) with
  /// the backend's incompatible-generation guard: while another live
  /// instance on the database holds a conflicting generation -- another
  /// data-format major, or another major of an entry type both register --
  /// the open throws [IncompatibleGenerationException]. On Postgres the
  /// guard covers every process on the database, on the web every tab of
  /// the origin (a page without Web Locks throws
  /// [GenerationGuardConfigurationException]); a Sembast database outside
  /// the browser is used by one process. The registration lasts until
  /// [close], and a failed open releases it.
  ///
  /// The boot then runs in one storage transaction, under the guard's
  /// exclusive boot lock. It first decides, before writing anything,
  /// whether this build may open the database:
  ///
  /// - The database identity stored beside the log must equal the one the
  ///   database's first `lib_version_initialized` event records; a missing
  ///   or different identity throws [DatabaseIdentityMismatchError]. A
  ///   database written by a build that recorded no identity or no data
  ///   format, or whose stored shapes predate this data format, throws
  ///   [DatabaseResetRequiredError]: it must be reset.
  /// - The data-format major recorded by the latest library-version event,
  ///   and by the database's generation record, must equal this build's
  ///   ([LibVersion.dataFormat]); another major throws
  ///   [DataFormatIncompatibleError].
  /// - No registered entry type's major may be below the major recorded
  ///   for it in the generation record by an earlier boot; a lower one
  ///   throws [EntryTypeVersionDowngradeError].
  ///
  /// Only the library-version events this database appended itself count;
  /// a peer's library-version events it ingested are never read as its
  /// own. When the boot accepts, it writes, in this order: a
  /// `lib_version_initialized` event at the first open (minting the
  /// database identity, [databaseId]), or a `lib_version_changed` event
  /// when this build's package version or data format differs from the one
  /// recorded last, older ones included; an empty copy, at a watermark
  /// before the first event of the log, for every registered view whose
  /// fingerprinted definition has no stored unmarked copy (it catches up
  /// with the log after the open returns); every stored copy whose
  /// fingerprint this build does not register, marked for deletion; the
  /// generation record, merged with this build's generation; and a boot
  /// record. A refused boot writes nothing.
  ///
  /// Deployment. Builds with the same data-format major and the same
  /// entry-type majors share a database in any mix -- a canary beside the
  /// serving revision, several instances, a restart, a rollback to the
  /// previous release -- and every open by a different version is recorded
  /// in the log. The library stores a view's rows per fingerprint of its
  /// definition (its interest, shape, entry-type versions and promoter
  /// chains): builds that agree share one copy and fold each event into it
  /// as they store it, and a build whose definition is new or changed gets
  /// a copy of its own, which starts empty and catches up with the log
  /// after this open returns, in short bounded transactions
  /// (`EVS-DEV-view-convergence`). A build of another data-format major, or
  /// one that raises an entry-type major, is deployed stop-then-start:
  /// every instance of the old revision stops before the first instance of
  /// the new one opens the database, and the old revision's next open is
  /// refused afterwards. Recovery after such a deployment is a restore from
  /// a backup taken before the switch, or a roll-forward. Evolve
  /// compatibly where possible: add an optional field as a minor step, and
  /// make a real reshape a new entry type.
  ///
  /// On a backend whose transactions contend with concurrent appends, the
  /// boot first locks what every append writes, so the appends of a
  /// revision serving the same database wait for the boot to commit rather
  /// than abort it. The wait lasts for the whole boot: its reads of the
  /// library-version events, the registry audit events and the latest
  /// event of the log, its checks, and its creating and marking of view
  /// copies, a constant amount of work per registered view and stored
  /// copy. It folds no view row, so the pause does not grow with a view's
  /// size or the log's length.
  ///
  /// Progress. [onBootProgress], when given, observes the boot: it receives
  /// a [BootProgress] when the open starts its checks ([BootPhase.checks])
  /// and once the boot has committed, just before the open returns
  /// ([BootPhase.complete]). A refused open reports no completion. A boot
  /// transaction the backend runs again reports its phases again from
  /// [BootPhase.checks] when the new run starts; until then the discarded
  /// run's last report stands.
  ///
  /// The observer only observes: nothing it does changes what the boot
  /// decides or writes. The boot calls it synchronously and does not await
  /// a future it returns, so its synchronous work extends the boot -- on
  /// Postgres, the time every append to the database is held back -- and it
  /// must return quickly: record the progress (for a readiness endpoint,
  /// say) and act on it after the open returns. It runs in an error zone the
  /// library owns: what it throws, at once or from work it started, is
  /// logged and the boot continues. That zone outlives the boot, so an error
  /// the observer's later work raises is logged by the library rather than
  /// reaching the caller's zone, and a future created there that fails does
  /// not complete an await in another error zone.
  ///
  /// While the boot runs, a call from the observer, or from work it started
  /// in its zone, that opens an event store, runs a transaction of an event
  /// store (its writes, [runTransaction], ingest, and `rebuildView`) or starts a transaction on a storage backend the library
  /// ships throws [StateError], whichever database it is over. A callback
  /// the observer hands to code registered outside its zone (a stream
  /// listener subscribed elsewhere, say), a read a backend serves outside a
  /// transaction, and a transaction an application-supplied backend starts
  /// are not recognised.
  // Implements: EVS-DEV-event-store-open/A+B+C+D+E+F
  // the sole production constructor; the whole boot, refusals first, runs
  //   in one storage transaction (see _runBoot).
  // Implements: EVS-DEV-event-store-open/G+I+M
  // the boot reports its phases to an optional observer that decides nothing;
  //   the completion is reported after the boot committed; an open the
  //   observer calls while the boot runs is refused.
  // Implements: EVS-PRD-storage-barrier/I
  // an open that fails after the library opened its storage closes that
  //   storage before the failure reaches the caller.
  static Future<EventStore> open({
    required StorageDescription storage,
    required EntryTypeRegistry entryTypes,
    required Source source,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    Clock? clock,
    Uuid? uuid,
    void Function(BootProgress progress)? onBootProgress,
  }) async {
    refuseCallFromBootProgressObserver('EventStore.open');
    final progress = BootProgressReporter(onBootProgress);
    try {
      progress.report(BootPhase.checks, 0, 0);
      final effectiveProjections = projections ?? ProjectionRegistry();
      _registerLibraryDefinitions(entryTypes, effectiveProjections);
      entryTypes.seal();
      effectiveProjections.seal();
      final effectivePromoters = (promoters ?? PromoterRegistry())..seal();
      final opened = await openDescribedStorage(storage);
      final EventStore store;
      try {
        final (:databaseId, :copyIds, :registration) = await _guardedBoot(
          storage: opened.backend,
          entryTypes: entryTypes,
          projections: effectiveProjections,
          promoters: effectivePromoters,
          recordVersion: true,
          progress: progress,
        );
        store = EventStore._(
          backend: opened.backend,
          entryTypes: entryTypes,
          source: source,
          securityContexts: opened.securityContexts,
          storage: opened,
          databaseId: databaseId,
          viewCopyIds: copyIds,
          registration: registration,
          projections: effectiveProjections,
          promoters: effectivePromoters,
          clock: clock,
          uuid: uuid,
        );
      } catch (_) {
        await opened.close();
        rethrow;
      }
      progress
        ..bootFinished()
        ..report(BootPhase.complete, 0, 0);
      store._catchUp.onCaughtUp = store._subs.publishViewCaughtUp;
      store._catchUp.start();
      return store;
    } finally {
      progress.bootFinished();
    }
  }

  /// Opens an [EventStore] for a test: the boot of [open], refusals
  /// included, without its library-version event.
  ///
  /// It registers the reserved system entry types and the default
  /// destination-wedges view as [open] does, runs the incompatible-generation
  /// guard and refuses what [open] refuses (a reserved id or the view's name
  /// under a definition other than the library's, a sealed projection
  /// registry without the view, the database identity, the data format, an
  /// entry-type downgrade), and otherwise seeds, promotes and re-derives
  /// views and writes the generation and boot records as [open] does. It
  /// appends no `lib_version_initialized` or `lib_version_changed` event,
  /// so sequence numbers stay predictable, and at a first open it mints the
  /// database identity without a log record; a later [open] adopts that
  /// identity. The guarantee that the log records every version that opened
  /// the database holds for [open] only. [onBootProgress] observes the boot
  /// as it does for [open].
  ///
  /// The caller keeps [storage]: [close] does not close it, so several
  /// stores may run over one backend. In a build with assertions disabled
  /// it throws [StateError] before it touches [storage].
  // Implements: EVS-DEV-event-store-open/A
  // the test-only constructor: visible for testing, so the analyzer reports
  //   a call from production code; the refusals of open; no library-version
  //   event.
  // Implements: EVS-DEV-storage-capability/K
  // in a build with assertions disabled the test-only open refuses with
  //   StateError before it touches the backend.
  // Implements: EVS-PRD-storage-barrier/J
  // without assertions the test-only entry point admits no backend.
  @visibleForTesting
  static Future<EventStore> openForTest({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required Source source,
    required MutableSecurityContextStore securityContexts,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    Clock? clock,
    Uuid? uuid,
    void Function(BootProgress progress)? onBootProgress,
  }) async {
    var assertionsEnabled = false;
    assert(() {
      assertionsEnabled = true;
      return true;
    }(), 'admits the test-only open');
    if (!assertionsEnabled) {
      throw StateError(
        'EventStore.openForTest runs only in a build with assertions '
        'enabled; open an event store with EventStore.open',
      );
    }
    refuseCallFromBootProgressObserver('EventStore.openForTest');
    final progress = BootProgressReporter(onBootProgress);
    try {
      progress.report(BootPhase.checks, 0, 0);
      final effectiveProjections = projections ?? ProjectionRegistry();
      _registerLibraryDefinitions(entryTypes, effectiveProjections);
      entryTypes.seal();
      effectiveProjections.seal();
      final effectivePromoters = (promoters ?? PromoterRegistry())..seal();
      final (:databaseId, :copyIds, :registration) = await _guardedBoot(
        storage: storage,
        entryTypes: entryTypes,
        projections: effectiveProjections,
        promoters: effectivePromoters,
        recordVersion: false,
        progress: progress,
      );
      final store = EventStore._(
        backend: storage,
        entryTypes: entryTypes,
        source: source,
        securityContexts: securityContexts,
        storage: null,
        databaseId: databaseId,
        viewCopyIds: copyIds,
        registration: registration,
        projections: effectiveProjections,
        promoters: effectivePromoters,
        clock: clock,
        uuid: uuid,
      );
      progress
        ..bootFinished()
        ..report(BootPhase.complete, 0, 0);
      store._catchUp.onCaughtUp = store._subs.publishViewCaughtUp;
      store._catchUp.start();
      return store;
    } finally {
      progress.bootFinished();
    }
  }

  /// Registers, in the caller's registries, every reserved system entry
  /// type ([kSystemEntryTypes]) and the default destination-wedges view
  /// ([defaultDestinationWedgesSpec]) they lack, before the registries are
  /// sealed and the boot reads them.
  ///
  /// A registry that already holds a reserved entry-type id, or a spec under
  /// the default view's name, is accepted only when it holds the library's
  /// own object (the exported definition or spec, which is also what an
  /// earlier open with the same registries left there); anything else, or a
  /// sealed projection registry that lacks the view, throws [ArgumentError]
  /// naming the reserved id or view before either registry changes and
  /// before anything is written.
  // Implements: EVS-DEV-destination-drain/M
  // opening an event store registers the reserved system entry types and the
  //   default destination-wedges view, accepting only the library's own
  //   definitions already present.
  static void _registerLibraryDefinitions(
    EntryTypeRegistry entryTypes,
    ProjectionRegistry projections,
  ) {
    // Implements: EVS-DEV-destination-drain/L
    // an open refuses a registry holding an entry type of the reserved
    //   namespace that the library does not declare.
    for (final held in entryTypes.all()) {
      if (isReservedEntryType(held.id) &&
          !kReservedSystemEntryTypeIds.contains(held.id)) {
        throw ArgumentError.value(
          held.id,
          'entryTypes',
          'entryType id "${held.id}" is in the reserved entry-type namespace '
              '(every id beginning with "system." and '
              '${kReservedFixedEntryTypeIds.join(', ')}), which only the '
              'library declares',
        );
      }
    }
    for (final definition in kSystemEntryTypes) {
      final held = entryTypes.byId(definition.id);
      if (held != null && !identical(held, definition)) {
        throw ArgumentError.value(
          definition.id,
          'entryTypes',
          'entryType id "${definition.id}" is reserved for system events; '
              "the registry holds a definition other than the library's "
              "(kSystemEntryTypes). Register the library's own definition "
              'or none: EventStore.open registers it.',
        );
      }
    }
    final viewName = defaultDestinationWedgesSpec.viewName;
    final heldSpec = projections.lookup(viewName);
    if (heldSpec != null &&
        !identical(heldSpec, defaultDestinationWedgesSpec)) {
      throw ArgumentError.value(
        viewName,
        'projections',
        "view \"$viewName\" is reserved for the library's default "
            'destination-wedges view; the registry holds another spec under '
            'that name. Register defaultDestinationWedgesSpec or none: '
            'EventStore.open registers it.',
      );
    }
    if (heldSpec == null && projections.isSealed) {
      throw ArgumentError.value(
        viewName,
        'projections',
        "the projection registry is sealed and lacks the library's default "
            'destination-wedges view "$viewName", which EventStore.open '
            'registers; pass the registry unsealed, or register '
            'defaultDestinationWedgesSpec before sealing it.',
      );
    }
    for (final definition in kSystemEntryTypes) {
      if (entryTypes.byId(definition.id) == null) {
        entryTypes.register(definition);
      }
    }
    if (heldSpec == null) projections.register(defaultDestinationWedgesSpec);
  }

  /// The build this process runs as: the compiled [LibVersion] constants,
  /// or the declaration a test installed.
  static ({String version, DataFormatVersion dataFormat}) _build() =>
      DeliveryTestHooks.current?.buildDeclaration ??
      (version: LibVersion.version, dataFormat: LibVersion.dataFormat);

  /// The generation guard around the boot of [open] and [openForTest]:
  /// registers this build's generation with the backend's guard (refusing a
  /// conflicting live instance before any write), runs the boot transaction
  /// under the boot lock the registration holds, completes the boot (the
  /// registration joins the backend's active set and the boot lock is
  /// released). Any failure after the registration releases it before the
  /// error surfaces.
  // Implements: EVS-DEV-version-compatibility/F+G
  // register before any write; the boot transaction runs under the boot
  //   lock; a failed open releases its registration.
  static Future<
    ({
      String databaseId,
      Map<String, String> copyIds,
      GenerationRegistration registration,
    })
  >
  _guardedBoot({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required ProjectionRegistry projections,
    required PromoterRegistry promoters,
    required bool recordVersion,
    required BootProgressReporter progress,
  }) async {
    final build = _build();
    final descriptor = GenerationDescriptor(
      packageVersion: build.version,
      dataFormat: build.dataFormat,
      entryTypes: <String, EntryTypeVersion>{
        for (final definition in entryTypes.all())
          definition.id: definition.registeredVersion,
      },
      // Implements: EVS-DEV-view-convergence/C
      // the fingerprint of each registered view is registered with the
      //   guard's live components before the boot transaction.
      viewFingerprints: <String>{
        for (final spec in projections.all())
          viewFingerprint(spec, entryTypes, promoters),
      },
    );
    final registration = await storage.registerGeneration(descriptor);
    try {
      final (:databaseId, :copyIds) = await _runBoot(
        storage: storage,
        entryTypes: entryTypes,
        projections: projections,
        promoters: promoters,
        recordVersion: recordVersion,
        descriptor: descriptor,
        registration: registration,
        progress: progress,
      );
      await registration.completeBoot();
      return (
        databaseId: databaseId,
        copyIds: copyIds,
        registration: registration,
      );
    } catch (_) {
      await registration.release();
      rethrow;
    }
  }

  /// The boot of [open] (with [recordVersion]) and of [openForTest]
  /// (without), in one `bootTransaction` of [storage]. Returns the database
  /// identity and, for every view [projections] registers, the id of its
  /// current copy.
  ///
  /// Every refusal is decided before the first write: the stored shapes,
  /// the database identity, the data format (in the log, then in the
  /// generation record) and the entry-type majors (in the generation
  /// record). Then, in order: the library-version event (when
  /// [recordVersion] and one is due), an empty copy for every registered
  /// view whose fingerprint has no stored unmarked copy, marking for
  /// deletion every stored copy whose fingerprint the opening build does
  /// not register, the merged generation record and [registration]'s own
  /// records, and the boot record. The boot folds no view row: a new copy
  /// catches up with the log after the open returns. The whole body may
  /// run more than once (a serialization retry, or a browser database
  /// re-running it after another tab committed); each run decides again
  /// from what it reads.
  // Implements: EVS-DEV-event-store-open/E
  // one boot transaction; refusals before any write; the library-version
  //   event and registry audit before creating or marking any copy; the
  //   boot record on every accepted boot.
  // Implements: EVS-DEV-entry-type-downgrade-refusal/A
  // the downgrade refusal runs before any write of the boot transaction.
  // Implements: EVS-DEV-view-convergence/B+D
  // the boot creates an empty copy for every unfingerprinted registered
  //   view and marks for deletion every stored copy whose fingerprint the
  //   opening build does not register; sparing a copy a live registration
  //   of another instance names is added once the generation guard's live
  //   registrations exist.
  // Implements: EVS-DEV-version-compatibility/I
  // the generation record refuses, before any write, a build it does not
  //   admit, and every accepted boot merges its generation into it.
  static Future<({String databaseId, Map<String, String> copyIds})> _runBoot({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required ProjectionRegistry projections,
    required PromoterRegistry promoters,
    required bool recordVersion,
    required GenerationDescriptor descriptor,
    required GenerationRegistration registration,
    required BootProgressReporter progress,
  }) {
    final hooks = DeliveryTestHooks.current;
    final build = _build();
    return storage.bootTransaction<
      ({String databaseId, Map<String, String> copyIds})
    >((txn) async {
      _observeBootBodyRun(hooks);
      progress.beginBodyRun();

      // -------- Decide: nothing below writes until every refusal ran.
      await _refuseEarlierFormatEvents(storage, txn, build.dataFormat);
      final storedId = await storage.readDatabaseIdTxn(txn);
      final LocalLibVersionHistory history;
      try {
        history = await VersionCheck.readLocalInTxn(storage, txn);
      } on FormatException catch (e) {
        throw DatabaseResetRequiredError(
          'a library-version event is not in this data format: ${e.message}',
        );
      }
      final initialized = history.firstInitialized;
      final latest = history.latest;
      if (initialized == null && latest != null) {
        throw DatabaseResetRequiredError(
          'its log records library-version changes but no initialization',
        );
      }
      if (initialized != null) {
        final recordedId = initialized.databaseId;
        if (recordedId == null ||
            history.events.any((recorded) => recorded.dataFormat == null)) {
          throw DatabaseResetRequiredError(
            'its library-version events record no database identity or no '
            'data format',
          );
        }
        if (storedId != recordedId) {
          throw DatabaseIdentityMismatchError(
            recordedDatabaseId: recordedId,
            storedDatabaseId: storedId,
          );
        }
        final recordedFormat = latest!.dataFormat!;
        if (recordedFormat.major != build.dataFormat.major) {
          throw DataFormatIncompatibleError(
            recordedPackageVersion: latest.packageVersion ?? '(unrecorded)',
            recordedDataFormat: recordedFormat,
            packageVersion: build.version,
            dataFormat: build.dataFormat,
          );
        }
      }
      final record = await storage.readDataGenerationTxn(txn);
      if (record != null && record.dataFormatMajor != build.dataFormat.major) {
        final recordedFormat = latest?.dataFormat;
        throw DataFormatIncompatibleError(
          recordedPackageVersion: latest?.packageVersion ?? '(unrecorded)',
          recordedDataFormat:
              recordedFormat != null &&
                  recordedFormat.major == record.dataFormatMajor
              ? recordedFormat
              : DataFormatVersion(record.dataFormatMajor, 0),
          packageVersion: build.version,
          dataFormat: build.dataFormat,
        );
      }
      // Implements: EVS-DEV-entry-type-downgrade-refusal/A
      // the database's generation record is the one place that knows
      //   every major the database has been opened with, so the downgrade
      //   refusal reads it alone.
      if (record != null) {
        for (final entry in descriptor.entryTypes.entries) {
          final recordedMajor = record.entryTypeMajors[entry.key];
          if (recordedMajor != null && recordedMajor > entry.value.major) {
            throw EntryTypeVersionDowngradeError(
              entryType: entry.key,
              fromVersion: EntryTypeVersion(recordedMajor, 0),
              toVersion: entry.value,
            );
          }
        }
      }

      // -------- Write.
      final String databaseId;
      var versionEventAppended = false;
      if (initialized == null) {
        databaseId = await storage.readOrCreateDatabaseIdTxn(txn);
        if (recordVersion) {
          await _appendLibVersionEventInTxn(
            txn,
            storage,
            LibVersionEvents.initialized,
            <String, Object?>{
              'version': build.version,
              'data_format': build.dataFormat.toJson(),
              'database_id': databaseId,
              'initializedAt': DateTime.now().toUtc().toIso8601String(),
            },
            databaseId: databaseId,
          );
          versionEventAppended = true;
        }
      } else {
        databaseId = initialized.databaseId!;
        final recordedVersion = latest!.packageVersion;
        final recordedFormat = latest.dataFormat!;
        if (recordVersion &&
            (recordedVersion != build.version ||
                recordedFormat != build.dataFormat)) {
          await _appendLibVersionEventInTxn(
            txn,
            storage,
            LibVersionEvents.changed,
            <String, Object?>{
              'fromVersion': recordedVersion,
              'toVersion': build.version,
              'fromDataFormat': recordedFormat.toJson(),
              'toDataFormat': build.dataFormat.toJson(),
              'changedAt': DateTime.now().toUtc().toIso8601String(),
            },
            databaseId: databaseId,
          );
          versionEventAppended = true;
        }
      }
      if (versionEventAppended &&
          (hooks?.afterBootVersionEvent?.call() ?? false)) {
        throw const InjectedFailure('afterBootVersionEvent');
      }
      // Implements: EVS-DEV-view-convergence/A+B
      // at most one copy of a fingerprint that is not marked for deletion;
      //   an empty copy, watermark before the first event of the log, is
      //   created for every registered view whose fingerprint has none.
      final storedCopies = await storage.readViewCopiesInTxn(txn);
      final unmarkedByFingerprint = <String, ViewCopy>{
        for (final copy in storedCopies)
          if (!copy.markedForDeletion) copy.fingerprint: copy,
      };
      final registeredFingerprints = <String>{};
      final copyIds = <String, String>{};
      for (final spec in projections.all()) {
        final fingerprint = viewFingerprint(spec, entryTypes, promoters);
        registeredFingerprints.add(fingerprint);
        final existing = unmarkedByFingerprint[fingerprint];
        if (existing != null) {
          copyIds[spec.viewName] = existing.copyId;
          continue;
        }
        final copyId = await storage.createViewCopyInTxn(
          txn,
          spec.viewName,
          fingerprint,
          0,
        );
        copyIds[spec.viewName] = copyId;
        unmarkedByFingerprint[fingerprint] = ViewCopy(
          copyId: copyId,
          viewName: spec.viewName,
          fingerprint: fingerprint,
          watermark: 0,
          markedForDeletion: false,
        );
      }
      // Implements: EVS-DEV-view-convergence/D
      // every stored copy whose fingerprint neither the opening build nor
      //   a live registration of another instance names is marked for
      //   deletion.
      final liveFingerprints = registration.liveViewFingerprints;
      for (final copy in storedCopies) {
        if (!copy.markedForDeletion &&
            !registeredFingerprints.contains(copy.fingerprint) &&
            !liveFingerprints.contains(copy.fingerprint)) {
          await storage.markViewCopyForDeletionInTxn(txn, copy.copyId);
        }
      }
      final merged = record == null
          ? GenerationRecord.of(descriptor)
          : record.merge(descriptor);
      if (merged != record) {
        await storage.writeDataGenerationTxn(txn, merged);
      }
      await registration.recordInTxn(txn);
      // An accepted boot always writes, so a browser database checks this
      // transaction against other tabs' commits and re-runs it on fresh data
      // when one committed first.
      await storage.writeBootCheckTxn(
        txn,
        BootCheck(
          at: DateTime.now().toUtc(),
          packageVersion: build.version,
          dataFormat: build.dataFormat,
        ),
      );
      return (databaseId: databaseId, copyIds: copyIds);
    });
  }

  /// Throws [DatabaseResetRequiredError] when the latest event in the log
  /// is not in this data format's stored shape, or records a data-format
  /// major below this build's: a build of an earlier data format appended
  /// it. No build of this data format stores an event of an earlier major,
  /// since ingest refuses one, so the latest event decides for the log.
  // Implements: EVS-DEV-version-compatibility/O
  // the open refuses, before any write, a database holding an event an
  //   earlier data-format major appended, as one that must be reset.
  static Future<void> _refuseEarlierFormatEvents(
    StorageBackend storage,
    Transaction txn,
    DataFormatVersion dataFormat,
  ) async {
    StoredEvent? latest;
    try {
      await for (final event in storage.readEventsReverseInTxn(txn)) {
        latest = event;
        break;
      }
    } on FormatException catch (e) {
      throw DatabaseResetRequiredError(
        'its events are not in this data format: ${e.message}',
      );
    }
    if (latest != null && latest.libFormatVersion.major < dataFormat.major) {
      throw DatabaseResetRequiredError(
        'its latest event, ${latest.eventId}, was appended by a build of '
        'data format ${latest.libFormatVersion}',
      );
    }
  }

  static void _observeBootBodyRun(DeliveryTestHooks? hooks) {
    final seam = hooks?.onBootBodyRun;
    if (seam == null) return;
    try {
      seam();
    } on Object catch (e, st) {
      libraryLog(
        'event_store',
        'the onBootBodyRun test seam threw',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  /// Closes the subscription engine and the storage the library opened for
  /// this store, releasing their resources. A backend the application
  /// supplied ([ApplicationSuppliedStorage]), or handed to [openForTest],
  /// stays open: its holder closes it. Not safe to call concurrently with
  /// in-flight work.
  ///
  /// The generation registration is released last, once the store and the
  /// backend have stopped writing, so no write of this store runs after a
  /// conflicting build could register.
  // Implements: EVS-PRD-storage-barrier/H
  // closing the event store closes the storage the library opened for it.
  Future<void> close() async {
    // Implements: EVS-DEV-view-convergence/H
    // Implements: EVS-DEV-view-convergence/I
    // no catch-up transaction begins after close is called, and close
    //   awaits the one in flight, if any, before the storage it uses closes.
    await _catchUp.stop();
    await _subs.close();
    await _storage?.close();
    await _registration.release();
  }

  /// Run [body] inside a single `backend.transaction`, collecting every
  /// [StoredEvent] that [appendInTxn] records during [body]'s execution.
  /// After the transaction commits successfully, all collected events are
  /// published to the subscription bus in append order so [subscribe]
  /// listeners receive the same delivery they would from the public [append]
  /// method.
  ///
  /// This is the only transaction [appendInTxn] accepts: it requires the
  /// collector this method hands [body], and refuses an append inside a
  /// plain `backend.transaction`.
  ///
  /// Does not wake the delivery cycle: the library's operations that call
  /// it (action dispatch, the destination registry's operations) wake the
  /// cycle themselves after it returns. Events a consumer appends through it
  /// reach the drainer at its next pass, at the latest one cadence later; a
  /// consumer that wants them delivered sooner calls its started
  /// `SyncCycle` after this returns.
  ///
  /// On Postgres a run that wrote the table holding the sequence counter
  /// (every run that appended) and lost a serialization race is re-run
  /// holding a lock on that table, which holds back every other write to it,
  /// and so every append to the database, until the re-run ends. [body]
  /// therefore does not wait on anything outside the database (a network
  /// call, a timer, another transaction of this store's database).
  ///
  /// A live subscriber receives this store's events in log order, so the
  /// events of a transaction are published only once every transaction of
  /// this store that appended before it has committed or failed. [body]
  /// therefore does not wait for the live delivery of an event that a later
  /// transaction of this store appends: that delivery waits for [body]'s
  /// transaction to end.
  ///
  /// The backend may run [body] more than once before one run commits (see
  /// `StorageBackend.transaction`). Each run receives its own
  /// [PublishCollector], and only the committed run's collector is
  /// published. A caller keeps every value that describes a run -- event ids
  /// it appended, a decision it reached, results it accumulated -- inside
  /// [body] and returns it as [body]'s result, or resets it at the start of
  /// each run, so that what it reports reflects only the committed run.
  Future<T> runTransaction<T>(
    Future<T> Function(Transaction txn, PublishCollector collector) body,
  ) async {
    return _runInTxnWithPublish(body);
  }

  /// The first sequence number each transaction of this store has appended
  /// in its current run, while it has not committed or failed: the
  /// publication of a committed transaction waits while one with a lower
  /// first sequence number is still in flight.
  final SplayTreeMap<int, int> _inFlightFirstSequences =
      SplayTreeMap<int, int>();

  /// Committed transactions waiting to publish, by first sequence number.
  final SplayTreeMap<int, _Publication> _awaitingPublication =
      SplayTreeMap<int, _Publication>();

  void _holdSequence(int sequenceNumber) => _inFlightFirstSequences.update(
    sequenceNumber,
    (count) => count + 1,
    ifAbsent: () => 1,
  );

  void _releaseSequence(int sequenceNumber) {
    final count = _inFlightFirstSequences[sequenceNumber];
    if (count == null) return;
    if (count <= 1) {
      _inFlightFirstSequences.remove(sequenceNumber);
    } else {
      _inFlightFirstSequences[sequenceNumber] = count - 1;
    }
  }

  /// Publishes, in sequence order, every committed transaction that no
  /// in-flight transaction with a lower first sequence number precedes.
  void _publishInOrder() {
    while (_awaitingPublication.isNotEmpty) {
      final next = _awaitingPublication.firstKey()!;
      final lowestInFlight = _inFlightFirstSequences.isEmpty
          ? null
          : _inFlightFirstSequences.firstKey();
      if (lowestInFlight != null && lowestInFlight < next) return;
      _awaitingPublication.remove(next)!.publish();
    }
  }

  /// Internal helper: wraps a `backend.transaction` call with a
  /// [PublishCollector] and publishes all collected events and row changes
  /// after commit.
  // Implements: EVS-PRD-subscription/E
  // A backend may run the body more than once
  //   (Postgres re-runs it after a serialization conflict; sembast_web re-runs
  //   it after another tab commits first). Each run gets a fresh collector, and
  //   only the collector of the run that committed (the last one) publishes.
  //   Runs that overlap break that contract and are refused.
  // Implements: EVS-PRD-subscription/C
  // Transactions of one store commit in the order of the sequence numbers
  //   they append (each append advances the one sequence counter), but their
  //   continuations can resume in any order. A committed transaction's
  //   events are therefore published only once no transaction of this store
  //   that appended a lower sequence number is still in flight, so live
  //   subscribers receive the store's events in log order.
  Future<T> _runInTxnWithPublish<T>(
    Future<T> Function(Transaction txn, PublishCollector collector) body,
  ) async {
    refuseCallFromBootProgressObserver('An EventStore transaction');
    late PublishCollector collector;
    var runInProgress = false;
    int? heldSequence;
    void releaseHeld() {
      final held = heldSequence;
      if (held == null) return;
      heldSequence = null;
      _releaseSequence(held);
    }

    final T result;
    try {
      result = await _backend.transaction<T>((txn) async {
        if (runInProgress) {
          throw StateError(
            'StorageBackend.transaction started a run of the body while an '
            'earlier run was still in progress; runs must be sequential.',
          );
        }
        runInProgress = true;
        // A new run replaces whatever an earlier, discarded run appended.
        releaseHeld();
        _publishInOrder();
        final runCollector = PublishCollector._(txn, (sequenceNumber) {
          heldSequence = sequenceNumber;
          _holdSequence(sequenceNumber);
        });
        collector = runCollector;
        _liveHandles.add(txn);
        try {
          return await body(txn, runCollector);
        } finally {
          _liveHandles.remove(txn);
          runCollector._open = false;
          runInProgress = false;
        }
      });
    } catch (_) {
      releaseHeld();
      _publishInOrder();
      rethrow;
    }
    if (heldSequence != null) {
      await DeliveryTestHooks.current?.afterCommitBeforePublish?.call();
    }
    final events = collector.events;
    final rowChanges = collector.rowChanges;
    void publish() {
      for (final event in events) {
        _subs.publishEvent(event);
      }
      for (final change in rowChanges) {
        _subs.publishRowChange(change);
      }
    }

    final first = heldSequence;
    releaseHeld();
    if (first == null) {
      publish();
      _publishInOrder();
      return result;
    }
    // Not awaited: a caller whose body waits on another transaction of this
    // store must not wait on its own publication too.
    _awaitingPublication[first] = _Publication(publish);
    _publishInOrder();
    return result;
  }

  /// Subscribe to live updates. For [Events] mode: delivers a [Delta] for
  /// every event appended after this call (pre-existing history is NOT
  /// replayed). For [AggregateMode]: delivers an initial [Snapshot] per
  /// matching aggregate then [Delta] / [Tombstone] updates as events land.
  Stream<Update<T>> subscribe<T>(
    SubscriptionFilter filter,
    SubscriptionMode<T> mode,
  ) {
    switch (mode) {
      case Events():
        return _subs.events(filter) as Stream<Update<T>>;
      case AggregateMode<T>():
        return _subscribeAggregate<T>(filter, mode);
    }
  }

  /// Builds a live [AggregateMode] stream via a [StreamController].
  ///
  /// Atomic snapshot-then-attach: opens a single live listener FIRST
  /// (before reading the snapshot) so no changes are lost between the
  /// snapshot read and forward-mode delivery. A `replayDone` flag inside
  /// the listener routes events to a buffer during the snapshot read and
  /// directly to the output controller after it; a `redelivering` flag
  /// applies the same buffering, once replay is done, for the duration of
  /// a became-current redelivery read below, so a live change the read's
  /// own storage transaction predates is never overtaken by it.
  ///
  /// The initial replay reads the view's convergence state and its rows in
  /// one storage transaction (the [reader]'s `findViewRows` /
  /// `readViewRowsByKeys`, EVS-DEV-converging-view-reads/A): a named
  /// aggregate not yet settled is delivered as [Pending] rather than
  /// [Snapshot], and the replay ends with [EndOfReplay] carrying the
  /// view's state. While the copy converges, no `Delta`/`Tombstone` for it
  /// reaches this subscription -- an append folds inline only into a copy
  /// that is current (EVS-DEV-view-convergence/F) -- so nothing here needs
  /// to filter live updates by settledness. Once the copy is found current
  /// at the end of a catch-up transaction, this subscription re-reads the
  /// view (again in one storage transaction) and redelivers a [Snapshot]
  /// for every aggregate it named -- the already-settled ones unchanged,
  /// the formerly pending ones replacing their earlier [Pending] -- or
  /// every row of the view, if it named none; a live change that lands
  /// while this re-read is in flight is buffered and drained right after,
  /// so it can never reach the subscriber ahead of a redelivered row it
  /// postdates (EVS-PRD-subscription/C). Only then does the subscription
  /// emit a second [EndOfReplay] reporting the view current
  /// (EVS-DEV-converging-view-reads/G).
  ///
  /// The [StreamController] is closed when the subscriber cancels,
  /// preventing infinite blocking.
  Stream<Update<T>> _subscribeAggregate<T>(
    SubscriptionFilter filter,
    AggregateMode<T> mode,
  ) {
    late StreamController<Update<T>> controller;
    StreamSubscription<AggregateFoldChange>? liveSub;
    StreamSubscription<String>? caughtUpSub;

    Future<void> start() async {
      // Open ONE subscription that lasts the lifetime of this stream.
      // During the snapshot phase events go to liveBuffer; after
      // _replayDone is set they go directly to controller.
      var replayDone = false;
      var reportedCurrent = false;
      // Guards the re-read itself, not just its outcome: two caught-up
      // signals landing before either read returns must not both pass the
      // `reportedCurrent` check and both redeliver.
      var deliveringBecameCurrent = false;
      var caughtUpDuringReplay = false;
      var maxSequenceSeen = 0;
      final liveBuffer = <AggregateFoldChange>[];
      // While `deliverBecameCurrent`'s redelivery read is in flight, a live
      // change published by an append that lands concurrently is buffered
      // here rather than sent straight to the controller -- exactly as
      // `liveBuffer` holds changes during the initial replay -- so a
      // redelivered Snapshot (reflecting the read's pre-append state) can
      // never be followed by a Delta/Tombstone the append already
      // published before the read started (EVS-PRD-subscription/C).
      var redelivering = false;
      final redeliverBuffer = <AggregateFoldChange>[];

      // Drains changes buffered during a redelivery read: applied straight
      // to the controller, in arrival order, updating `maxSequenceSeen`.
      void drainRedeliverBuffer() {
        for (final change in redeliverBuffer) {
          if (controller.isClosed) break;
          final u = _changeToUpdate<T>(change, filter, mode);
          if (u != null) {
            if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
            controller.add(u);
          }
        }
        redeliverBuffer.clear();
      }

      // Implements: EVS-DEV-converging-view-reads/G
      // Redelivers on the view becoming current and reports it current,
      // only once, guarded by `reportedCurrent`. A signal that arrives
      // while the re-read still finds the copy converging (another build
      // wrote past the watermark again first) is a no-op: this listener
      // stays attached for the next one.
      Future<void> deliverBecameCurrent() async {
        if (reportedCurrent || deliveringBecameCurrent || controller.isClosed) {
          return;
        }
        deliveringBecameCurrent = true;
        // Implements: EVS-PRD-subscription/C
        // route concurrent live changes to a buffer for the
        // duration of the redelivery read, the same discipline the initial
        // replay uses, so no redelivered row is stale relative to a change
        // already published.
        redelivering = true;
        try {
          final aggregateIds = mode.aggregates;
          if (aggregateIds == null) {
            final read = await reader.findViewRows(mode.viewName);
            if (read.state != ViewConvergenceState.current) {
              drainRedeliverBuffer();
              return;
            }
            reportedCurrent = true;
            var maxSeq = 0;
            for (final row in read.rows) {
              if (controller.isClosed) return;
              final seq = (row['sequence'] as int?) ?? 0;
              if (seq > maxSeq) maxSeq = seq;
              controller.add(
                Snapshot<T>(value: mode.mapper(row), sequence: seq),
              );
            }
            drainRedeliverBuffer();
            if (maxSequenceSeen > maxSeq) maxSeq = maxSequenceSeen;
            if (!controller.isClosed) {
              controller.add(
                EndOfReplay<T>(sequence: maxSeq, state: read.state),
              );
            }
          } else {
            final read = await reader.readViewRowsByKeys(
              mode.viewName,
              aggregateIds,
            );
            if (read.state != ViewConvergenceState.current) {
              drainRedeliverBuffer();
              return;
            }
            reportedCurrent = true;
            var maxSeq = 0;
            for (final aggId in aggregateIds) {
              if (controller.isClosed) return;
              final data = read.rows[aggId]?.dataOrNull;
              final seq = (data?['sequence'] as int?) ?? 0;
              if (seq > maxSeq) maxSeq = seq;
              controller.add(
                Snapshot<T>(
                  value: data == null ? null : mode.mapper(data),
                  sequence: seq,
                ),
              );
            }
            drainRedeliverBuffer();
            if (maxSequenceSeen > maxSeq) maxSeq = maxSequenceSeen;
            if (!controller.isClosed) {
              controller.add(
                EndOfReplay<T>(sequence: maxSeq, state: read.state),
              );
            }
          }
        } finally {
          redelivering = false;
          deliveringBecameCurrent = false;
        }
        await caughtUpSub?.cancel();
        caughtUpSub = null;
      }

      // Attached before the snapshot read, like `liveSub`, so a copy that
      // reaches the log's tip during the read is not missed: the signal is
      // recorded and acted on right after the initial EndOfReplay.
      caughtUpSub = _subs.viewCaughtUp(mode.viewName).listen((_) {
        if (!replayDone) {
          caughtUpDuringReplay = true;
          return;
        }
        unawaited(deliverBecameCurrent());
      });

      liveSub = _subs.rowChanges(mode.viewName).listen((change) {
        if (controller.isClosed) return;
        if (!replayDone) {
          liveBuffer.add(change);
        } else if (redelivering) {
          redeliverBuffer.add(change);
        } else {
          final u = _changeToUpdate<T>(change, filter, mode);
          if (u != null) {
            if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
            controller.add(u);
          }
        }
      }, onDone: () => controller.close());

      // Snapshot read: through the reader, so the view's convergence
      // state and its rows come from one storage transaction
      // (EVS-DEV-converging-view-reads/A).
      final aggregateIds = mode.aggregates;
      var initialState = ViewConvergenceState.current;
      if (aggregateIds == null) {
        final read = await reader.findViewRows(mode.viewName);
        initialState = read.state;
        for (final row in read.rows) {
          if (controller.isClosed) return;
          final seq = (row['sequence'] as int?) ?? 0;
          if (seq > maxSequenceSeen) maxSequenceSeen = seq;
          controller.add(Snapshot<T>(value: mode.mapper(row), sequence: seq));
        }
      } else {
        // Implements: EVS-PRD-subscription/A
        // a filtered (row-scoped)
        // materialized-state snapshot. Materialize the whole allow-list in ONE
        // bulk read rather than a BEGIN/SELECT/COMMIT per aggregate id, which
        // would cost about three round trips per id against a networked
        // database. Each requested id emits a Snapshot, with a null value for
        // an absent row, so a tombstoned or absent row is still signalled
        // per id; a row the copy cannot yet confirm settled is delivered as
        // Pending instead (EVS-DEV-converging-view-reads/E).
        final read = await reader.readViewRowsByKeys(
          mode.viewName,
          aggregateIds,
        );
        initialState = read.state;
        for (final aggId in aggregateIds) {
          if (controller.isClosed) return;
          final row = read.rows[aggId];
          switch (row) {
            case SettledRow(:final data):
              final seq = (data['sequence'] as int?) ?? 0;
              if (seq > maxSequenceSeen) maxSequenceSeen = seq;
              controller.add(
                Snapshot<T>(value: mode.mapper(data), sequence: seq),
              );
            case AbsentRow():
            case null:
              controller.add(Snapshot<T>(value: null, sequence: 0));
            case PendingRow():
              controller.add(Pending<T>(aggregateId: aggId));
          }
        }
      }

      // Drain buffered live changes that arrived during snapshot read.
      for (final change in liveBuffer) {
        if (controller.isClosed) return;
        final u = _changeToUpdate<T>(change, filter, mode);
        if (u != null) {
          if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
          controller.add(u);
        }
      }
      liveBuffer.clear();

      // Snapshot phase + buffer drain are complete. Emit EndOfReplay BEFORE
      // flipping replayDone so the marker is ordered correctly relative to
      // any deltas that arrive after this point.
      if (!controller.isClosed) {
        controller.add(
          EndOfReplay<T>(sequence: maxSequenceSeen, state: initialState),
        );
      }

      replayDone = true;
      if (initialState == ViewConvergenceState.current) {
        // Already reported current by the initial EndOfReplay: a stray
        // caught-up signal (this view was never converging) must not
        // trigger a redelivery.
        reportedCurrent = true;
        await caughtUpSub?.cancel();
        caughtUpSub = null;
      } else if (caughtUpDuringReplay) {
        unawaited(deliverBecameCurrent());
      }
    }

    controller = StreamController<Update<T>>(
      onListen: start,
      onCancel: () async {
        await liveSub?.cancel();
        liveSub = null;
        await caughtUpSub?.cancel();
        caughtUpSub = null;
        if (!controller.isClosed) await controller.close();
      },
    );

    return controller.stream;
  }

  Update<T>? _changeToUpdate<T>(
    AggregateFoldChange c,
    SubscriptionFilter filter,
    AggregateMode<T> mode,
  ) {
    final aggSet = mode.aggregates;
    if (aggSet != null && !aggSet.contains(c.aggregateId)) return null;
    if (c.isTombstone) {
      return Tombstone<T>(aggregateId: c.aggregateId, sequence: c.sequence);
    }
    return Delta<T>(
      value: mode.mapper(c.newValue!),
      sequence: c.sequence,
      cause: c.cause,
    );
  }

  /// The current instant in UTC, whatever zone the injected clock returns
  /// it in: every time the store writes into a hashed field (an event's
  /// `client_timestamp`, a provenance entry's `received_at`) is written as
  /// a `Z`-suffixed string, which every backend stores and reads back
  /// unchanged.
  DateTime _now() => (_clock ?? DateTime.now)().toUtc();

  // Implements: EVS-DEV-causal-parents/G
  // the public append operations take no argument that sets causal.
  /// Append a new event. Returns the persisted `StoredEvent`, or `null`
  /// when `dedupeByContent` is true and the content matches the
  /// aggregate's most recent event of [entryType].
  ///
  /// Throws [ArgumentError], appending nothing, when [entryType] lies in
  /// the reserved entry-type namespace ([isReservedEntryType]): only the
  /// library appends reserved system events; and when [data] holds a
  /// top-level key beginning with `$`, which the default views reserve.
  ///
  /// The library stamps `entry_type_version` with the registered major and
  /// minor (`EntryTypeDefinition.registeredVersion` for [entryType]) and
  /// `lib_format_version` with its data-format version
  /// (`LibVersion.dataFormat`). The library is the single source of truth
  /// for both fields; callers do not (and cannot) supply them.
  // Implements: EVS-DEV-append-stamps-registered-version
  // substrate stamps
  //   entry_type_version with the registered major and minor; callers do not
  //   supply this field. dedupeByContent skips the append when content
  //   matches the prior event; any throw rolls back the entire append.
  Future<StoredEvent?> append({
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    String? flowToken,
    Map<String, Object?>? metadata,
    SecurityDetails? security,
    String? checkpointReason,
    String? changeReason,
    bool dedupeByContent = false,
  }) async {
    _refuseReservedEntryType(entryType);
    // appendInTxn runs the projection interpreter and threads row-changes
    // through the collector; _runInTxnWithPublish fires both events and
    // row changes to subscribers after the transaction commits.
    final event = await _runInTxnWithPublish<StoredEvent?>((
      txn,
      collector,
    ) async {
      return _appendInTxn(
        txn,
        collector: collector,
        entryType: entryType,
        aggregateId: aggregateId,
        aggregateType: aggregateType,
        eventType: eventType,
        data: data,
        initiator: initiator,
        flowToken: flowToken,
        metadata: metadata,
        security: security,
        checkpointReason: checkpointReason,
        changeReason: changeReason,
        dedupeByContent: dedupeByContent,
      );
    });

    if (event == null) return null;
    _wakeDeliveryCycle();
    return event;
  }

  /// Throws [ArgumentError] when [entryType] lies in the reserved
  /// namespace ([isReservedEntryType]), declared by this release or not: the
  /// public append operations never append one.
  // Implements: EVS-DEV-destination-drain/L
  // the event store's public append operations refuse every entry type in
  //   the reserved namespace.
  static void _refuseReservedEntryType(String entryType) {
    if (isReservedEntryType(entryType)) {
      throw ArgumentError.value(
        entryType,
        'entryType',
        'is in the reserved entry-type namespace (every id beginning with '
            '"system." and ${kReservedFixedEntryTypeIds.join(', ')}); only '
            'the library appends reserved system events',
      );
    }
  }

  /// Append a reserved system event inside the transaction of a
  /// [runTransaction] body: the library's counterpart of [appendInTxn] for
  /// the entry types [appendInTxn] refuses.
  ///
  /// Throws [ArgumentError], appending nothing, when [entryType] is not a
  /// reserved system entry type, when [aggregateType] and [eventType] are
  /// not a shape the library declares for [entryType], or when a destination
  /// audit's [data] lacks a destination identifier or a database identity
  /// that ingest admits; and [StateError] as
  /// [appendInTxn] does for a collector of another run. Returns null only
  /// when [dedupeByContent] is true and the content matches the latest event
  /// of [entryType] in the aggregate.
  // Implements: EVS-PRD-storage-barrier/C
  // the reserved append is private to the event store's library; only its
  //   public operations append reserved events.
  // Implements: EVS-DEV-security-findings/S
  // [mode] decides whether this reserved append's own fold failures are
  //   passed over and recorded (a record of an ingest, a restore or the
  //   drain, and every security finding) or fail the append to its caller
  //   (a public local operation such as destination_registered, an
  //   app-requested halt, or the boot).
  Future<StoredEvent?> _appendReservedInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    bool dedupeByContent = false,
    ApplyEventMode mode = ApplyEventMode.local,
  }) {
    checkReservedAppend(
      entryType: entryType,
      aggregateType: aggregateType,
      eventType: eventType,
      data: data,
    );
    return _appendInTxn(
      txn,
      collector: collector,
      entryType: entryType,
      aggregateId: aggregateId,
      aggregateType: aggregateType,
      eventType: eventType,
      data: data,
      initiator: initiator,
      flowToken: null,
      metadata: null,
      security: null,
      checkpointReason: null,
      changeReason: null,
      dedupeByContent: dedupeByContent,
      mode: mode,
    );
  }

  /// Verifies this database's log over the local sequence numbers [from] to
  /// [to], both inclusive, and records what it finds.
  ///
  /// It checks every event stored in the range, whatever path stored it:
  /// its hashes, its storage-chain link (the first event of the range
  /// against the event before it), the local sequence numbers holding no
  /// event, its origin-chain predecessor, the forks and reused origin
  /// positions it takes part in (each once), and its causal parents. An
  /// omitted lower bound, or 0, is the first local sequence number; the
  /// upper bound is the highest local sequence number stored when the
  /// verification starts, or [to] when that is lower, so events stored
  /// meanwhile are left to the next verification.
  ///
  /// The reads hold no transaction an append waits for. After them, each
  /// finding the returned verdict lists is recorded as a security finding
  /// under the detector role `walk`, in a short transaction of its own,
  /// unless this database already holds it: a second verification of the
  /// same log records nothing more. [StorageReader.verifyChains] returns
  /// the same verdict and records nothing.
  ///
  /// Throws [ArgumentError], before reading any event, for a negative bound
  /// or a lower bound above the upper.
  // Implements: EVS-PRD-hash-chain-integrity/C+F
  // any holder of the log verifies its storage chain and every origin-chain
  //   link it can resolve, from the log alone, the first event of a range
  //   included.
  // Implements: EVS-DEV-chain-verification/R
  // each finding of the verdict is recorded as a security finding of its
  //   kind and evidence, in a write transaction of its own committed after
  //   the read.
  // Implements: EVS-DEV-security-findings/Q
  // the chain verification records its findings under the detector role
  //   walk.
  // Implements: EVS-PRD-hash-chain-integrity/J
  // the operation run on an open event store records each anomaly once per
  //   detector.
  Future<ChainVerificationVerdict> verifyChains({int? from, int? to}) =>
      _verifyChains(from: from, to: to);

  Future<ChainVerificationVerdict> _verifyChains({
    int? from,
    int? to,
    int pageSize = kChainWalkPageSize,
    Future<void> Function()? afterPage,
  }) async {
    refuseCallFromBootProgressObserver('EventStore.verifyChains');
    final verdict = await verifyChainsOver(
      _backend,
      from: from,
      to: to,
      pageSize: pageSize,
      afterPage: afterPage,
    );
    for (final finding in verdict.findings) {
      await _runInTxnWithPublish<void>((txn, collector) async {
        await _recordFindingInTxn(
          txn,
          collector,
          role: FindingRole.walk,
          kind: finding.kind,
          evidence: finding.evidence,
          aggregates: await recordedAggregatesInTxn(_backend, txn, finding),
        );
      });
    }
    return verdict;
  }

  /// [evidence] with its `record` key, when it holds a record (an
  /// `identity_mismatch`, `event_malformed` or `own_event_ingested`
  /// finding), replaced by [findingRecordEvidence]'s encoding; [evidence]
  /// unchanged otherwise (no `record` key, or one already null).
  // Implements: EVS-DEV-security-findings/U
  // a finding's record evidence is the record itself when it is free of
  //   U+0000, otherwise its base64 encoding.
  static Map<String, Object?> _withStorableRecord(
    Map<String, Object?> evidence,
  ) {
    final record = evidence['record'];
    if (record is! Map) return evidence;
    return <String, Object?>{
      ...evidence,
      'record': findingRecordEvidence(Map<String, Object?>.from(record)),
    };
  }

  /// Record, inside [txn], the security finding of [kind] with [evidence]
  /// that this database detected in [role], naming [aggregates]: append a
  /// `system.security_finding` event, whose aggregate is the finding's
  /// identity, unless this database already holds as authored a finding
  /// with the same identity, read inside [txn]. Returns the appended event,
  /// or null when such a finding is held. The finding commits with the
  /// detection point's outcome, in [txn].
  ///
  /// Throws [ArgumentError], appending nothing, when [kind] is not a kind
  /// the library records or [evidence] is not exactly the evidence its kind
  /// fixes.
  // Implements: EVS-DEV-security-findings/B+C+H
  // a finding carries its identity, kind, fixed evidence, the aggregates in
  //   ascending order and its detector; the library records only the listed
  //   kinds.
  // Implements: EVS-DEV-security-findings/E
  // a finding is appended only when, read inside the appending transaction,
  //   the detecting database holds as authored no finding with its identity;
  //   a received finding carrying the identity does not match.
  // Implements: EVS-DEV-security-findings/F
  // the finding is appended in the transaction of the detection point's
  //   outcome.
  Future<StoredEvent?> _recordFindingInTxn(
    Transaction txn,
    PublishCollector collector, {
    required FindingRole role,
    required FindingKind kind,
    required Map<String, Object?> evidence,
    required Iterable<String> aggregates,
  }) async {
    // Implements: EVS-DEV-security-findings/U
    // every finding's `record` evidence is the received record itself when
    //   it is free of U+0000, otherwise its base64 encoding, computed once
    //   here for every kind that carries one (identity_mismatch,
    //   event_malformed, own_event_ingested), so the finding is itself an
    //   event free of U+0000 (EVS-DEV-event-record/L) and its identity is
    //   deterministic across redeliveries of one record.
    final storableEvidence = _withStorableRecord(evidence);
    checkFindingEvidence(kind, storableEvidence);
    final findingId = securityFindingId(
      databaseId: databaseId,
      role: role,
      kind: kind,
      evidence: storableEvidence,
    );
    if (await _backend.holdsAuthoredSecurityFindingInTxn(
      txn,
      databaseId: databaseId,
      findingId: findingId,
    )) {
      return null;
    }
    return _appendReservedInTxn(
      txn,
      collector,
      entryType: kSecurityFindingEntryType,
      aggregateId: findingId,
      aggregateType: kSecurityFindingAggregateType,
      eventType: kSecurityFindingRecordedEventType,
      data: securityFindingData(
        findingId: findingId,
        kind: kind,
        evidence: storableEvidence,
        aggregates: aggregates,
        databaseId: databaseId,
        role: role,
        libraryVersion: _build().version,
      ),
      initiator: _kSecurityFindingInitiator,
      // Implements: EVS-DEV-view-convergence/E
      // every security finding is an always-stored event: a fold failure
      //   folding the finding event itself passes over and is recorded.
      // Implements: EVS-DEV-security-findings/T
      // (for a finding that is itself of kind fold_failed, the interpreter
      //   collects no failure to record.)
      mode: ApplyEventMode.alwaysStored,
    );
  }

  /// True iff [event] was originated locally on this `EventStore`'s
  /// [source].
  ///
  /// Compares originator install identity (`provenance[0].identifier`)
  /// against `source.identifier` — not `source.hopId`, because two
  /// installations of the same role class are distinct originators. A
  /// receiver uses this to discriminate locally-appended events from
  /// bridged-from-upstream events without writing provenance navigation
  /// by hand.
  ///
  /// Throws `StateError` (via `event.originatorHop`) when [event] has no
  /// provenance entries; requires every event to carry at
  /// least the originator hop.
  // install UUID; comparison is on identifier, not hopId.
  bool isLocallyOriginated(StoredEvent event) =>
      event.originatorHop.identifier == source.identifier;

  /// Delete the security-context row for [eventId] AND append one
  /// `security_context_redacted` event in the same transaction. The act
  /// of redaction is permanently auditable.
  Future<void> clearSecurityContext(
    String eventId, {
    required String reason,
    required Initiator redactedBy,
  }) async {
    await _runInTxnWithPublish<void>((txn, collector) async {
      final existing = await _securityContexts.readInTxn(txn, eventId);
      if (existing == null) {
        throw ArgumentError.value(
          eventId,
          'eventId',
          'no security context row for event',
        );
      }
      await _securityContexts.deleteInTxn(txn, eventId);
      // Emit the redaction audit event. The install UUID is the aggregate;
      // the redaction subject moves into `data.subject_event_id` so callers
      // can query "all redactions of event X" by filtering on entry_type
      // AND data.subject_event_id.
      // A caller-invoked operation, not an ingest, restore or drain
      // transaction: stays local (default mode), so a fold failure fails
      // to clearSecurityContext's caller with nothing stored.
      await _appendReservedInTxn(
        txn,
        collector,
        entryType: kSecurityContextRedactedEntryType,
        aggregateId: source.identifier,
        aggregateType: kSecurityContextAuditAggregateType,
        eventType: kSecurityContextRedactedEventType,
        data: <String, Object?>{'subject_event_id': eventId, 'reason': reason},
        initiator: redactedBy,
      );
    });
    _wakeDeliveryCycle();
  }

  /// Apply [policy] (or [SecurityRetentionPolicy.defaults]) to the
  /// security-context sidecar store. Truncates rows past `fullRetention`,
  /// deletes rows past `fullRetention + truncatedRetention`. Emits a
  /// `system.retention_policy_applied` audit event on every sweep
  /// (zero-effect sweeps included), plus per-population
  /// `security_context_compacted` / `security_context_purged` events
  /// when those sweeps are non-empty.
  Future<RetentionResult> applyRetentionPolicy({
    SecurityRetentionPolicy? policy,
    Initiator? sweepInitiator,
  }) async {
    final p = policy ?? SecurityRetentionPolicy.defaults;
    final sweepBy =
        sweepInitiator ??
        const AutomationInitiator(service: 'retention-policy-sweep');
    final now = _now();
    final compactCutoff = now.subtract(p.fullRetention);
    final purgeCutoff = compactCutoff.subtract(p.truncatedRetention);

    final result = await _runInTxnWithPublish<RetentionResult>((
      txn,
      collector,
    ) async {
      final compactCandidates = await _securityContexts
          .findUnredactedOlderThanInTxn(txn, compactCutoff);
      for (final row in compactCandidates) {
        await _securityContexts.upsertInTxn(txn, row.applyTruncation(p));
      }

      final purgeCandidates = await _securityContexts.findOlderThanInTxn(
        txn,
        purgeCutoff,
      );
      for (final row in purgeCandidates) {
        await _securityContexts.deleteInTxn(txn, row.eventId);
      }

      // The retention sweep is an operator-invoked operation, its own
      // transaction, not an ingest, restore or drain transaction: every
      // append below stays local (default mode).
      if (compactCandidates.isNotEmpty) {
        await _appendReservedInTxn(
          txn,
          collector,
          entryType: kSecurityContextCompactedEntryType,
          aggregateId: source.identifier,
          aggregateType: kSecurityContextAuditAggregateType,
          eventType: kSecurityContextCompactedEventType,
          data: <String, Object?>{
            'count': compactCandidates.length,
            'cutoff': compactCutoff.toIso8601String(),
            'policy': p.toJson(),
          },
          initiator: sweepBy,
        );
      }
      if (purgeCandidates.isNotEmpty) {
        await _appendReservedInTxn(
          txn,
          collector,
          entryType: kSecurityContextPurgedEntryType,
          aggregateId: source.identifier,
          aggregateType: kSecurityContextAuditAggregateType,
          eventType: kSecurityContextPurgedEventType,
          data: <String, Object?>{
            'count': purgeCandidates.length,
            'cutoff': purgeCutoff.toIso8601String(),
          },
          initiator: sweepBy,
        );
      }
      // Always emit the policy-applied audit event, even when both sweeps
      // were empty, so operators have a continuous retention timeline.
      await _appendReservedInTxn(
        txn,
        collector,
        entryType: kRetentionPolicyAppliedEntryType,
        aggregateId: source.identifier,
        aggregateType: kRetentionAuditAggregateType,
        eventType: kRetentionPolicyAppliedEventType,
        data: <String, Object?>{
          'policy_full_retention_seconds': p.fullRetention.inSeconds,
          'policy_truncated_retention_seconds': p.truncatedRetention.inSeconds,
          'events_truncated': compactCandidates.length,
          'events_purged': purgeCandidates.length,
          'cutoff_full': compactCutoff.toUtc().toIso8601String(),
          'cutoff_purge': purgeCutoff.toUtc().toIso8601String(),
        },
        initiator: sweepBy,
      );
      return RetentionResult(
        compactedCount: compactCandidates.length,
        purgedCount: purgeCandidates.length,
      );
    });
    _wakeDeliveryCycle();
    return result;
  }

  void _validateAppendInputs({
    required String entryType,
    required String aggregateType,
    required String eventType,
  }) {
    if (!entryTypes.isRegistered(entryType)) {
      throw ArgumentError.value(
        entryType,
        'entryType',
        'not registered in EntryTypeRegistry',
      );
    }
    if (aggregateType.isEmpty) {
      throw ArgumentError.value(
        aggregateType,
        'aggregateType',
        'must be non-empty',
      );
    }
  }

  /// Throws [ArgumentError], naming the key, when [data] holds a top-level
  /// key beginning with `$`: the default views reserve the prefix for the
  /// keys they stamp on every row.
  // Implements: EVS-PRD-materializer/H
  // an append whose data holds a top-level key beginning with `$` is
  //   refused by name before any write.
  static void _refuseReservedDataKey(Map<String, Object?> data) {
    for (final key in data.keys) {
      if (key.startsWith(r'$')) {
        throw ArgumentError.value(
          key,
          'data',
          'holds the top-level key "$key"; keys beginning with "\$" are '
              'reserved for the keys the default views stamp on every row',
        );
      }
    }
  }

  /// Throws [ArgumentError], naming the top-level field, when some string of
  /// [record] -- a key included, at any depth -- carries the character
  /// U+0000: no storage backend the library supports can hold every event
  /// the library holds unless every one is free of it.
  // Implements: EVS-DEV-event-record/L+M
  // an append carrying U+0000 in any string of the record is refused before
  //   any write, naming the top-level field.
  static void _refuseUnstorableCharacter(Map<String, Object?> record) {
    final field = recordFieldWithNulCharacter(record);
    if (field == null) return;
    throw ArgumentError.value(
      field,
      field,
      'a string of the event, a key included, carries the character '
      'U+0000; no storage backend the library supports can hold it',
    );
  }

  /// Transactional companion to [append]: appends inside the transaction
  /// of a [runTransaction] body, so the append commits atomically with the
  /// body's other work (for example a configuration change and the audit
  /// event that records it).
  ///
  /// [txn] and [collector] are the two arguments the [runTransaction] body
  /// received. The append throws [StateError] before any write when
  /// [collector] belongs to another transaction or to a run that has ended:
  /// the collector is what publishes the event and its view changes to live
  /// subscribers once the run commits, so an append through any other
  /// transaction would either publish an event its transaction rolled back
  /// or commit an event no subscriber sees.
  ///
  /// Does not wake the delivery cycle; the public [append] wakes it after
  /// the transaction commits.
  ///
  /// Validates inputs via [_validateAppendInputs] before doing any work,
  /// so direct callers do not need to pre-validate. Throws [ArgumentError],
  /// appending nothing, when [entryType] lies in the reserved entry-type
  /// namespace ([isReservedEntryType]): only the library appends reserved
  /// system events.
  ///
  /// Runs the projection interpreter inside the same transaction as the
  /// append, so all matching `ProjectionSpec`s materialize views before
  /// commit. Any spec throw rolls back the entire append.
  Future<StoredEvent?> appendInTxn(
    Transaction txn, {
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    required String? flowToken,
    required Map<String, Object?>? metadata,
    required SecurityDetails? security,
    required String? checkpointReason,
    required String? changeReason,
    required bool dedupeByContent,
    required PublishCollector collector,
  }) async {
    _refuseReservedEntryType(entryType);
    return _appendInTxn(
      txn,
      entryType: entryType,
      aggregateId: aggregateId,
      aggregateType: aggregateType,
      eventType: eventType,
      data: data,
      initiator: initiator,
      flowToken: flowToken,
      metadata: metadata,
      security: security,
      checkpointReason: checkpointReason,
      changeReason: changeReason,
      dedupeByContent: dedupeByContent,
      collector: collector,
    );
  }

  /// Throws [StateError] unless [txn] is a handle this store's
  /// [runTransaction] issued and whose body is still running: a handle of
  /// another event store, of a storage reader, or carried past its body is
  /// refused before anything is written.
  // Implements: EVS-DEV-storage-capability/G
  // a transaction handle used in an operation of an event store other than
  //   the one that issued it, or after its body returned, is refused with
  //   StateError before any write.
  void _refuseForeignHandle(Transaction txn, String operation) {
    if (!_liveHandles.contains(txn)) {
      throw StateError(
        'EventStore.$operation: the transaction handle was not issued by '
        "this event store's runTransaction, or its body has returned. Pass "
        'the handle a runTransaction body of this store received, while '
        'that body runs.',
      );
    }
  }

  /// The append every append operation shares, reserved and user entry
  /// types alike.
  // Implements: EVS-PRD-destinations/K
  // an append publishes only through the
  //   collector of the transaction run that commits it.
  Future<StoredEvent?> _appendInTxn(
    Transaction txn, {
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    required String? flowToken,
    required Map<String, Object?>? metadata,
    required SecurityDetails? security,
    required String? checkpointReason,
    required String? changeReason,
    required bool dedupeByContent,
    required PublishCollector collector,
    ApplyEventMode mode = ApplyEventMode.local,
  }) async {
    if (!collector._open || !identical(collector._transaction, txn)) {
      throw StateError(
        'EventStore.appendInTxn: the collector does not belong to this '
        'transaction run. Pass the transaction and collector a '
        'runTransaction body received, while that body runs.',
      );
    }
    _refuseForeignHandle(txn, 'appendInTxn');
    _validateAppendInputs(
      entryType: entryType,
      aggregateType: aggregateType,
      eventType: eventType,
    );
    _refuseReservedDataKey(data);
    // Implements: EVS-DEV-event-record/L+M
    // an append carrying U+0000 in any string of the record, a key
    //   included, at any depth, is refused before any write (before the
    //   sequence number and the causal record are reserved), naming the
    //   top-level field. Checked over the caller's own inputs, shaped as
    //   their top-level field ends up in the stored record (data and
    //   checkpoint_reason share the `data` field, metadata and
    //   change_reason the `metadata` field); the library's own
    //   sequence_number, causal record, hashes and timestamps are never
    //   caller-supplied strings.
    _refuseUnstorableCharacter(<String, Object?>{
      'aggregate_id': aggregateId,
      'aggregate_type': aggregateType,
      'entry_type': entryType,
      'event_type': eventType,
      'data': <String, Object?>{...data, 'checkpoint_reason': checkpointReason},
      'metadata': <String, Object?>{
        ...?metadata,
        'change_reason': changeReason,
      },
      'initiator': initiator.toJson(),
      'flow_token': flowToken,
    });

    final def = entryTypes.byId(entryType)!;
    // Implements: EVS-DEV-append-stamps-registered-version
    // substrate is
    //   the single source of truth for entry_type_version on every append;
    //   the value is read from the registry, not supplied by callers.
    final entryTypeVersion = def.registeredVersion;
    final effectiveChangeReason = changeReason ?? 'initial';

    final now = _now();

    // dedupe-by-content: compares against the most-recent event of matching
    // entry_type within the aggregate. Multiple entry types may share an
    // aggregate (e.g. system events under source.identifier); dedupe scopes
    // per entry_type so each emission stream is treated independently.
    StoredEvent? prior;
    if (dedupeByContent) {
      final aggregateHistory = await _backend.findEventsForAggregateInTxn(
        txn,
        aggregateId,
      );
      for (var i = aggregateHistory.length - 1; i >= 0; i--) {
        if (aggregateHistory[i].entryType == entryType) {
          prior = aggregateHistory[i];
          break;
        }
      }
    }
    if (dedupeByContent && prior != null) {
      final priorHash = _contentHash(
        eventType: prior.eventType,
        data: prior.data,
        changeReason: (prior.metadata['change_reason'] as String?) ?? 'initial',
      );
      final candidateHash = _contentHash(
        eventType: eventType,
        data: <String, Object?>{
          ...data,
          'checkpoint_reason': ?checkpointReason,
        },
        changeReason: effectiveChangeReason,
      );
      if (candidateHash == priorHash) return null;
    }

    final links = await _reserveChainLinksInTxn(_backend, txn, databaseId);
    final sequenceNumber = links.sequenceNumber;
    final causal = await _stampCausalInTxn(
      _backend,
      txn,
      aggregateId: aggregateId,
      declaration: def.declarationFor(eventType),
    );
    final provenance0 = _originatorEntry(
      hop: source.hopId,
      identifier: source.identifier,
      softwareVersion: source.softwareVersion,
      receivedAt: now,
      databaseId: databaseId,
      links: links,
    );
    final eventId = _uuid.v4();

    final dataMap = <String, Object?>{
      ...data,
      'checkpoint_reason': ?checkpointReason,
    };
    final metadataMap = <String, Object?>{
      ...?metadata,
      'change_reason': effectiveChangeReason,
      'provenance': <Map<String, Object?>>[provenance0.toJson()],
    };

    final recordMap = <String, Object?>{
      'event_id': eventId,
      'aggregate_id': aggregateId,
      'aggregate_type': aggregateType,
      'entry_type': entryType,
      'entry_type_version': entryTypeVersion.toJson(),
      'lib_format_version': LibVersion.dataFormat.toJson(),
      'event_type': eventType,
      'sequence_number': sequenceNumber,
      'data': dataMap,
      'metadata': metadataMap,
      'initiator': initiator.toJson(),
      'flow_token': flowToken,
      'client_timestamp': provenance0.receivedAt.toIso8601String(),
      'previous_event_hash': links.previousEventHash,
      'causal': causal.toJson(),
    };
    final eventHash = _eventHash(recordMap);
    recordMap['event_hash'] = eventHash;
    final event = StoredEvent.fromMap(recordMap, 0);

    await _backend.appendEvent(txn, event);

    if (security != null) {
      final row = EventSecurityContext(
        eventId: eventId,
        recordedAt: now,
        ipAddress: security.ipAddress,
        userAgent: security.userAgent,
        sessionId: security.sessionId,
        geoCountry: security.geoCountry,
        geoRegion: security.geoRegion,
        requestId: security.requestId,
      );
      await _securityContexts.writeInTxn(txn, row);
    }

    collector._add(event);

    // Run the projection interpreter inside the same transaction so views
    // materialize atomically with the append. Action-emitted events (via
    // ActionDispatcher → appendInTxn) MUST update views in-tx so subsequent
    // dispatches in the same flow read the new view rows.
    final applied = await _interpreter.applyEvent(
      txn: txn,
      backend: _backend,
      event: event,
      copyIds: _viewCopyIds,
      mode: mode,
    );
    if (applied.changes.isNotEmpty) {
      collector._addRowChanges(applied.changes);
    }
    // Implements: EVS-DEV-security-findings/S
    // every always-stored append records the fold_failed findings its own
    //   fold collected, in the same transaction (a reserved record of an
    //   ingest, a restore or the drain, and every security finding go
    //   through this shared append path under always-stored mode).
    if (mode == ApplyEventMode.alwaysStored && applied.failures.isNotEmpty) {
      await _recordFoldFailedFindingsInTxn(
        txn,
        collector,
        event,
        applied.failures,
      );
    }
    return event;
  }

  // Implements: EVS-PRD-hash-chain-integrity/A
  // the hash is taken over the
  //   canonical-form encoding of the event's content, so the same content
  //   always yields the same hash.
  String _contentHash({
    required String eventType,
    required Map<String, Object?> data,
    required String changeReason,
  }) {
    final input = <String, Object?>{
      'event_type': eventType,
      'data': data,
      'change_reason': changeReason,
    };
    return sha256.convert(canonicalizeBytes(input)).toString();
  }

  String _eventHash(Map<String, Object?> recordMap) =>
      _canonicalEventHash(recordMap);

  // -----------------------------------------------------------------------
  // Destination-role (ingest) write path
  // -----------------------------------------------------------------------

  /// Process-local ingest of one event, in a transaction of its own,
  /// applied to the record [incoming] writes (`incoming.toMap()`). Private
  /// to the event store's Dart library: the library admits an event only
  /// as part of a delivery ([ReceiverEndpoint.accept]), so no public entry
  /// point admits one outside a delivery (`EVS-PRD-ingest/G`). The
  /// receiver endpoint, which shares this library, and
  /// [ingestEventForTest], for the library's own tests of how ingest
  /// handles a single record, are its only callers.
  ///
  /// Refuses, before any write, an event of another data-format major
  /// ([IngestDataFormatIncompatible]), and an entry-type version above the
  /// registered major or one a view it folds into cannot promote
  /// ([IngestEntryTypeVersionAhead], [IngestEntryTypeVersionUnpromotable]).
  /// Every other anomaly is recorded as a security finding in the same
  /// transaction, and the returned outcome says whether the event was
  /// stored, found held, or kept in a finding.
  ///
  /// For an event parsed with [StoredEvent.fromMap], `incoming.toMap()` is
  /// the record it was parsed from as far as the hash reaches: parsing keeps
  /// every hashed field as the record spelled it.
  // Implements: EVS-PRD-ingest/G
  // the event store's ingest of one record is private to its Dart library;
  //   the receiver endpoint, which shares that library, is the only
  //   production caller.
  // Implements: EVS-DEV-version-compatibility/Q
  // ingest refuses another data-format major before any check of the rest
  //   of the event's record.
  Future<PerEventIngestOutcome> _ingestEvent(StoredEvent incoming) async {
    _refuseOtherDataFormatMajor(incoming.eventId, incoming.libFormatVersion);
    final record = Map<String, Object?>.from(incoming.toMap());
    return _runInTxnWithPublish((txn, collector) {
      return _ingestRecordInTxn(
        txn,
        record,
        parsed: incoming,
        batchContext: null,
        collector: collector,
      );
    });
  }

  /// Throws [IngestDataFormatIncompatible] when [version], the data-format
  /// version of the incoming event [eventId], has a major other than this
  /// build's.
  static void _refuseOtherDataFormatMajor(
    String eventId,
    DataFormatVersion version,
  ) {
    if (version.isCompatibleWith(LibVersion.dataFormat)) return;
    throw IngestDataFormatIncompatible(
      eventId: eventId,
      wireFormat: version,
      receiverFormat: LibVersion.dataFormat,
    );
  }

  /// Throws [IngestDataFormatIncompatible] when [record], an event record
  /// as a batch envelope carried it, records a data-format major other than
  /// this build's, reading nothing of the record but `lib_format_version`
  /// and `event_id`, so the refusal names the major and not a field a
  /// build of that major did not write. An integer `lib_format_version` is
  /// the version shape of a data format before 2.0 and names its major. A
  /// `lib_format_version` this reads no version from is left to
  /// [StoredEvent.fromMap], which names the field.
  // Implements: EVS-DEV-version-compatibility/Q
  // an event of another data-format major is refused by its major before
  //   any check of the rest of its record.
  static void _refuseOtherDataFormatMajorOfRecord(Map<String, Object?> record) {
    final raw = record['lib_format_version'];
    final DataFormatVersion version;
    if (raw is int && raw >= 1) {
      version = DataFormatVersion(raw, 0);
    } else {
      try {
        version = DataFormatVersion.fromJson(raw);
      } on FormatException {
        return;
      }
    }
    final eventId = record['event_id'];
    _refuseOtherDataFormatMajor(
      eventId is String ? eventId : '(no event_id)',
      version,
    );
  }

  /// Handles one received [record] inside [txn], for [_ingestEvent] and each
  /// record of a native delivery the receiver endpoint accepts: stores it
  /// as received, finds it held, or keeps it in a security finding, and
  /// records every finding it meets.
  ///
  /// [parsed] is the event [record] was written from, when the caller holds
  /// one ([_ingestEvent]); otherwise [record] is parsed here, and a record
  /// that does not parse is one the library does not store as an event.
  /// [batchContext] is non-null for a record of a delivery, and [delivery]
  /// names the delivery of a native delivery channel it arrived in.
  // Implements: EVS-PRD-ingest/G
  // every record of a delivery is admitted whatever its content or the
  //   outcome of the integrity checks, other than a record the library cannot
  //   store as an event, which is kept in full in a security finding; the
  //   rest of the delivery is admitted.
  // Implements: EVS-DEV-security-findings/F
  // every finding ingest records is appended in the ingest transaction that
  //   commits the record's outcome.
  // Implements: EVS-DEV-security-findings/Q
  // ingest records its findings under the detector role ingest; the restore
  //   operation, sharing this record handling, records them under restore.
  Future<PerEventIngestOutcome> _ingestRecordInTxn(
    Transaction txn,
    Map<String, Object?> record, {
    required StoredEvent? parsed,
    required BatchContext? batchContext,
    required PublishCollector collector,
    ProvenanceDelivery? delivery,
    FindingRole role = FindingRole.ingest,
  }) async {
    final rawEventId = record['event_id'];
    final eventId = rawEventId is String ? rawEventId : null;
    final findingIds = <String>[];
    Future<void> find(
      FindingKind kind,
      Map<String, Object?> evidence,
      List<String> aggregates,
    ) async {
      findingIds.add(
        await _recordIngestFindingInTxn(
          txn,
          collector,
          kind: kind,
          evidence: evidence,
          aggregates: aggregates,
          role: role,
        ),
      );
    }

    // Implements: EVS-DEV-chain-verification/Q
    // an event of the receiver's own identity is recognised by its
    //   originator provenance entry, never by the data it carries.
    final originator = _originatorDatabaseOfRecord(record);
    final isOwn = originator == databaseId;

    // 1. Parse, and decide whether the library can store the record as an
    //    event. The U+0000 check runs over the raw record, before any
    //    parse attempt: a parse of a record carrying it could otherwise
    //    succeed (the character does not break a record's shape) or, were
    //    a parser ever taught to refuse it too, would misclassify the
    //    record as record_malformed instead of unstorable_character.
    // Implements: EVS-DEV-security-findings/O
    // a received or restored record a string of which, a key included,
    //   carries U+0000 is stored as no event and kept in a finding of
    //   reason unstorable_character.
    StoredEvent? event;
    String? unstorable;
    if (originator == null) {
      unstorable = _kRecordMalformed;
    } else if (recordFieldWithNulCharacter(record) != null) {
      unstorable = _kUnstorableCharacter;
    } else {
      try {
        event = (parsed ?? StoredEvent.fromMap(record, 0))
          ..requireWellFormedRecord();
      } on FormatException {
        event = null;
        unstorable = _kRecordMalformed;
      }
    }
    if (event != null) {
      _refuseUnadmittedVersion(event);
      unstorable = _unstorableReason(event, originator!);
    }

    final held = eventId == null
        ? null
        : await _backend.findEventByIdInTxn(txn, eventId);
    // Implements: EVS-DEV-security-findings/B
    // a finding about a received record names the aggregate of the event
    //   the receiver holds under the record's identifier, and no aggregate
    //   when it holds none: a record kept in full is not an event it holds.
    final heldAggregates = <String>[if (held != null) held.aggregateId];

    // 2. A record the library does not store as an event.
    // Implements: EVS-DEV-security-findings/O
    // a received record the library does not store as an event (malformed,
    //   a declared reserved entry type in a shape it does not declare, or a
    //   destination audit whose identifiers are not well formed or whose
    //   database identity is not its originating database) is kept in full
    //   in an event_malformed finding naming the reason, other than a record
    //   of the receiver's own identity; no event is stored for it.
    // Implements: EVS-DEV-causal-parents/B
    // a record with no causal object of the exact shape is stored as no
    //   event and kept in a finding.
    // Implements: EVS-DEV-event-record/H
    // a record whose provenance entry lacks database_id or library_version
    //   is stored as no event and kept in a finding.
    // Implements: EVS-PRD-materializer/H
    // the ingest part: a record ingest receives whose data holds a top-level
    //   key beginning with `$` is stored as no event, and the finding names
    //   the reason. The append and registration parts are refused where
    //   they are made.
    if (unstorable != null) {
      if (isOwn) {
        await find(FindingKind.ownEventIngested, <String, Object?>{
          'event_id': eventId,
          'sealed_hash': _sealedHashOfRecord(record),
          'record': held == null ? record : null,
        }, heldAggregates);
      } else {
        await find(FindingKind.eventMalformed, <String, Object?>{
          'reason': unstorable,
          'record': record,
        }, heldAggregates);
      }
      final heldOwn = isOwn ? held : null;
      return PerEventIngestOutcome(
        eventId: eventId,
        outcome: heldOwn == null
            ? IngestOutcome.keptInFinding
            : IngestOutcome.duplicate,
        resultHash: heldOwn?.eventHash,
        findingIds: findingIds,
      );
    }
    final incoming = event!;

    // 3. Every hash the record carries, recomputed over the record as it
    //    arrived.
    // Implements: EVS-DEV-chain-verification/P
    // an event whose event_hash, or a receiver entry's arrival hash, does
    //   not recompute is stored as received with a hash_mismatch finding
    //   naming the event, the hash it carries and the hash it recomputes to.
    // Implements: EVS-PRD-ingest/D
    // each received event's hash chain is verified, and an event whose
    //   chain does not verify is admitted as received with a finding.
    final hashEvidence = hashMismatchEvidence(incoming, wireRecord: record);

    // 4. An identifier the receiver holds.
    if (held != null) {
      final heldSealed = ChainCoordinates.of(held).sealedHash;
      if (heldSealed != ChainCoordinates.of(incoming).sealedHash) {
        // Implements: EVS-DEV-security-findings/G
        // an event whose identifier the receiver holds under another sealed
        //   hash is not stored; an identity_mismatch finding carries the
        //   received record in full.
        await find(FindingKind.identityMismatch, <String, Object?>{
          'event_id': incoming.eventId,
          'held_hash': heldSealed,
          'record': record,
        }, heldAggregates);
        if (isOwn) await _findOwnEvent(find, incoming, heldAggregates);
        return PerEventIngestOutcome(
          eventId: incoming.eventId,
          outcome: IngestOutcome.keptInFinding,
          resultHash: null,
          findingIds: findingIds,
        );
      }
      // Implements: EVS-PRD-ingest/F
      // re-presenting an event already admitted stores nothing.
      await _emitDuplicateReceivedInTxn(
        txn,
        subjectEventId: incoming.eventId,
        subjectEventHashOnRecord: held.eventHash,
        batchContext: batchContext,
        collector: collector,
      );
      for (final evidence in hashEvidence) {
        await find(FindingKind.hashMismatch, evidence, heldAggregates);
      }
      if (isOwn) await _findOwnEvent(find, incoming, heldAggregates);
      return PerEventIngestOutcome(
        eventId: incoming.eventId,
        outcome: IngestOutcome.duplicate,
        resultHash: held.eventHash,
        findingIds: findingIds,
      );
    }

    // 5. A new event: reserve a fresh local sequence number, capture the
    //    originator's wire-supplied sequence number, and stamp the receiver
    //    entry.
    // Implements: EVS-DEV-security-findings/I
    // a security finding another database originated reaches this step as
    //   any event does, whatever its kind and detector, and no rule beyond
    //   ingest's own applies to it.
    final originSeq = incoming.sequenceNumber;
    final localSeq = await _backend.nextSequenceNumber(txn);
    // Implements: EVS-DEV-chain-verification/C
    // the receiver entry records the event's local sequence number and the
    //   stored hash of the event at the preceding one, read in this
    //   transaction.
    final previousTailHash = await _backend.readLatestEventHash(txn);
    // Implements: EVS-DEV-event-record/D+E
    // the receiver entry names the receiving database and the library
    //   version that stamped it.
    final receiverEntry = ProvenanceEntry(
      hop: source.hopId,
      receivedAt: _now(),
      identifier: source.identifier,
      softwareVersion: source.softwareVersion,
      arrivalHash: incoming.eventHash,
      previousIngestHash: previousTailHash,
      ingestSequenceNumber: localSeq,
      originSequenceNumber: originSeq,
      batchContext: batchContext,
      libraryVersion: _build().version,
      databaseId: databaseId,
      // Implements: EVS-DEV-delivery-receiver/H
      // the receiver entry of an event ingested from a native delivery
      //   carries the delivery: its channel and its number.
      delivery: delivery,
    );
    final updatedEvent = _appendReceiverProvenance(
      incoming,
      receiverEntry,
      localSeq: localSeq,
    );
    await _backend.appendEvent(txn, updatedEvent);
    collector._add(updatedEvent);

    // The projection interpreter runs inside the same transaction, as on
    // the local-append path. This is an always-stored event
    // (`EVS-DEV-view-convergence` Terms): a fold failure is this copy's
    // problem alone -- the copy passes over the event and stays current --
    // and is collected for the finding recorded below; the rest of the
    // delivery still commits (`EVS-PRD-ingest/G`).
    final applied = await _interpreter.applyEvent(
      txn: txn,
      backend: _backend,
      event: updatedEvent,
      copyIds: _viewCopyIds,
      mode: ApplyEventMode.alwaysStored,
    );
    if (applied.changes.isNotEmpty) collector._addRowChanges(applied.changes);

    // 6. The findings about the stored event, recorded after it so that
    //    each names its aggregate.
    final stored = <String>[updatedEvent.aggregateId];
    // Implements: EVS-DEV-security-findings/S
    // one fold_failed finding is recorded, in the storing transaction, for
    //   each copy that passed over the stored event.
    findingIds.addAll(
      await _recordFoldFailedFindingsInTxn(
        txn,
        collector,
        updatedEvent,
        applied.failures,
      ),
    );
    for (final evidence in hashEvidence) {
      await find(FindingKind.hashMismatch, evidence, stored);
    }
    // Implements: EVS-PRD-ingest/H
    // an event whose originator entry names the receiving database is
    //   stored as received when the receiver does not hold it, with a
    //   finding.
    if (isOwn) await _findOwnEvent(find, incoming, stored);
    // Implements: EVS-DEV-chain-verification/K+L
    // every event ingest stores is checked for a broken predecessor, a
    //   reused origin position and a fork inside the ingest transaction, so
    //   the events stored earlier in it count, and is stored whatever the
    //   checks find.
    for (final found in await chainStructureFindingsInTxn(
      _backend,
      txn,
      updatedEvent,
    )) {
      await find(found.kind, found.evidence, found.aggregates);
    }
    // Implements: EVS-DEV-sender-succession/K
    // storing a succession event the receiver does not hold records one
    //   succession_ahead finding for each channel it names of which the
    //   receiver holds an accepted delivery and whose named delivery is
    //   above the receiver's record of that channel.
    if (updatedEvent.entryType == kDestinationSenderSucceededEntryType) {
      await _findSuccessionAheadInTxn(txn, updatedEvent, find);
    }
    return PerEventIngestOutcome(
      eventId: updatedEvent.eventId,
      outcome: findingIds.isEmpty
          ? IngestOutcome.ingested
          : IngestOutcome.ingestedWithFinding,
      resultHash: updatedEvent.eventHash,
      findingIds: findingIds,
    );
  }

  /// Records, through [find], the `own_event_ingested` finding for
  /// [incoming], an event of this database's own identity that ingest held
  /// or stored, so the finding carries no record.
  // Implements: EVS-DEV-chain-verification/Q
  // an incoming event whose originator entry names the receiving database
  //   records one own_event_ingested finding naming the event and its sealed
  //   hash, held or not, and no event_malformed finding.
  static Future<void> _findOwnEvent(
    Future<void> Function(FindingKind, Map<String, Object?>, List<String>) find,
    StoredEvent incoming,
    List<String> aggregates,
  ) => find(FindingKind.ownEventIngested, <String, Object?>{
    'event_id': incoming.eventId,
    'sealed_hash': ChainCoordinates.of(incoming).sealedHash,
    'record': null,
  }, aggregates);

  /// Records, through [find], one `succession_ahead` finding for each
  /// channel [event]'s `predecessor_channels` names of which this database
  /// holds an accepted delivery and whose named delivery number is above
  /// this database's record of that channel, read inside [txn]. A channel
  /// this database never accepted a delivery on, and a named delivery at
  /// or below the record, name nothing.
  // Implements: EVS-DEV-sender-succession/K
  // a channel the receiver never accepted a delivery on, and a named
  //   delivery at or below the receiver's record, record nothing.
  Future<void> _findSuccessionAheadInTxn(
    Transaction txn,
    StoredEvent event,
    Future<void> Function(FindingKind, Map<String, Object?>, List<String>) find,
  ) async {
    final SenderSuccessionData succession;
    try {
      succession = SenderSuccessionData.fromJson(event.data);
    } on FormatException {
      return;
    }
    for (final predecessorChannel in succession.predecessorChannels) {
      final channel = predecessorChannel.channel;
      final record = await receiverEndpoint._recordInTxn(txn, channel);
      if (record.deliveryNumber == 0) continue;
      if (predecessorChannel.deliveryNumber <= record.deliveryNumber) {
        continue;
      }
      await find(FindingKind.successionAhead, <String, Object?>{
        'channel': channel.toJson(),
        'receiver_record': record.toJson(),
        'succession_record': DeliveryRecord(
          deliveryNumber: predecessorChannel.deliveryNumber,
          deliveryHash: predecessorChannel.deliveryHash,
        ).toJson(),
      }, const <String>[]);
    }
  }

  /// Records, inside [txn], the security finding of [kind] with [evidence]
  /// that this record handling detected under detector role [role] (ingest
  /// by default; the restore operation passes `FindingRole.restore`),
  /// naming [aggregates], unless this database holds it as authored
  /// already; returns its identity either way.
  Future<String> _recordIngestFindingInTxn(
    Transaction txn,
    PublishCollector collector, {
    required FindingKind kind,
    required Map<String, Object?> evidence,
    required List<String> aggregates,
    FindingRole role = FindingRole.ingest,
  }) async {
    await _recordFindingInTxn(
      txn,
      collector,
      role: role,
      kind: kind,
      evidence: evidence,
      aggregates: aggregates,
    );
    return securityFindingId(
      databaseId: databaseId,
      role: role,
      kind: kind,
      evidence: evidence,
    );
  }

  /// Records, inside [txn], one `fold_failed` finding under detector role
  /// [FindingRole.fold] for each of [failures], naming [event]'s aggregate:
  /// the shared recording every always-stored append uses for the fold
  /// failures its own fold collected (`EVS-DEV-security-findings/S`), and
  /// ingest and the raw internal audit fold use for the failures collected
  /// folding the event or audit they just stored. Never called for an
  /// [event] that is itself a finding of kind `fold_failed`
  /// (`EVS-DEV-security-findings/T`): the interpreter collects no
  /// failures for one, so [failures] is always empty in that case and this
  /// method is never reached with a non-empty list.
  // Implements: EVS-DEV-security-findings/S
  // one fold_failed finding is recorded, in the transaction that stores or
  //   appends the event, for each copy that passed over it.
  Future<List<String>> _recordFoldFailedFindingsInTxn(
    Transaction txn,
    PublishCollector collector,
    StoredEvent event,
    List<FoldFailureRecord> failures,
  ) async {
    final ids = <String>[];
    final aggregates = <String>[event.aggregateId];
    for (final failure in failures) {
      ids.add(
        await _recordIngestFindingInTxn(
          txn,
          collector,
          kind: FindingKind.foldFailed,
          evidence: foldFailedFindingEvidence(
            viewName: failure.viewName,
            definitionFingerprint: failure.definitionFingerprint,
            event: event,
            reason: failure.reason,
          ),
          aggregates: aggregates,
          role: FindingRole.fold,
        ),
      );
    }
    return ids;
  }

  /// The [FoldFailedFindingRecorder] the catch-up driver calls once its own
  /// catch-up transaction has rolled back unwritten on a fold failure
  /// (`EVS-DEV-view-convergence/Z`): appends the `fold_failed` finding, under
  /// detector role [FindingRole.fold], in a transaction of its own, committing
  /// before the catch-up transaction that then passes over the event
  /// (`EVS-DEV-security-findings/F`).
  Future<void> _recordCatchUpFoldFailedFinding({
    required String viewName,
    required String definitionFingerprint,
    required StoredEvent event,
    required FoldFailureReason reason,
  }) async {
    if (DeliveryTestHooks.current?.failCatchUpFoldFindingAppend?.call() ??
        false) {
      throw const InjectedFailure('catch_up_fold_finding_append');
    }
    await _runInTxnWithPublish<void>((txn, collector) async {
      await _recordFindingInTxn(
        txn,
        collector,
        role: FindingRole.fold,
        kind: FindingKind.foldFailed,
        evidence: foldFailedFindingEvidence(
          viewName: viewName,
          definitionFingerprint: definitionFingerprint,
          event: event,
          reason: reason,
        ),
        aggregates: <String>[event.aggregateId],
      );
    });
  }

  /// Throws, before the record is written, when [incoming]'s entry-type
  /// version is above the major this build registers for its entry type
  /// ([IngestEntryTypeVersionAhead]), or below it with no promoter path for
  /// a view it folds into ([IngestEntryTypeVersionUnpromotable]). An entry
  /// type this build does not register is accepted at any version: it is
  /// stored as it is and folds under its own version.
  // Implements: EVS-DEV-version-compatibility/D
  // every ingest entry point refuses a higher entry-type major, and an
  //   event below the registered version that a view it folds into has no
  //   promoter path for, by name before any write.
  void _refuseUnadmittedVersion(StoredEvent incoming) {
    _refuseOtherDataFormatMajor(incoming.eventId, incoming.libFormatVersion);
    final def = entryTypes.byId(incoming.entryType);
    if (def == null) return;
    if (incoming.entryTypeVersion.major > def.registeredVersion.major) {
      throw IngestEntryTypeVersionAhead(
        eventId: incoming.eventId,
        entryType: incoming.entryType,
        wireVersion: incoming.entryTypeVersion,
        receiverVersion: def.registeredVersion,
      );
    }
    if (!(incoming.entryTypeVersion < def.registeredVersion)) return;
    for (final spec in projections.all()) {
      if (!spec.interest.matches(incoming)) continue;
      final gap = promoters.chainGap(
        viewName: spec.viewName,
        entryType: incoming.entryType,
        fromVersion: incoming.entryTypeVersion,
        toVersion: def.registeredVersion,
      );
      if (gap != null) {
        throw IngestEntryTypeVersionUnpromotable(
          eventId: incoming.eventId,
          entryType: incoming.entryType,
          viewName: spec.viewName,
          wireVersion: incoming.entryTypeVersion,
          receiverVersion: def.registeredVersion,
          reason: gap,
        );
      }
    }
  }

  /// The reason the library does not store [incoming], a parsed record
  /// that [originator] originated, as an event, or null when it can:
  ///
  /// - `record_malformed`: its data holds a top-level key beginning with
  ///   `$`, which the views reserve; or its provenance does not place it in
  ///   an origin chain and a storage chain (an entry that is not an object,
  ///   a receiver entry without an arrival hash, a first receiver entry
  ///   without the origin position, or a later-hop entry before the last
  ///   without its local position);
  /// - `reserved_type_undeclared`: it is of a reserved entry type this
  ///   release declares, under an aggregate type or event type the library
  ///   does not declare for it;
  /// - `audit_identity_invalid`: it is a destination audit whose destination
  ///   identifier or database identity is missing, empty, not a string or
  ///   contains `|`, or whose database identity is not [originator].
  ///
  /// A reserved entry type this release does not declare, or a reserved
  /// event carrying an enumerated value it does not know, is stored.
  // Implements: EVS-DEV-destination-drain/L
  // ingest stores no event for a received event of a declared reserved
  //   entry type in an aggregate type or event type the library does not
  //   declare for it, or for a destination audit whose identifiers are not
  //   well formed or whose database identity is not its originating
  //   database; an undeclared reserved entry type or an unknown enumerated
  //   value is stored as received.
  static String? _unstorableReason(StoredEvent incoming, String originator) {
    if (incoming.data.keys.any((key) => key.startsWith(r'$'))) {
      return _kRecordMalformed;
    }
    final provenance = incoming.metadata['provenance'];
    if (provenance is! List) return _kRecordMalformed;
    for (var k = 0; k < provenance.length; k++) {
      final entry = provenance[k];
      if (entry is! Map) return _kRecordMalformed;
      if (k == 0) continue;
      if (entry['arrival_hash'] is! String) return _kRecordMalformed;
      if (k == 1 && entry['origin_sequence_number'] is! int) {
        return _kRecordMalformed;
      }
      if (k < provenance.length - 1 &&
          entry['ingest_sequence_number'] is! int) {
        return _kRecordMalformed;
      }
    }
    final shape = kReservedEventShapes[incoming.entryType];
    if (shape == null) return null;
    if (!shape.admits(incoming.aggregateType, incoming.eventType)) {
      return 'reserved_type_undeclared';
    }
    if (kDestinationAuditEntryTypes.contains(incoming.entryType) &&
        (!isWellFormedDestinationAuditData(incoming.data) ||
            incoming.data['database_id'] != originator)) {
      return 'audit_identity_invalid';
    }
    return null;
  }

  /// The database identity [record]'s originator provenance entry names,
  /// or null when its metadata carries no provenance list whose first entry
  /// is an object naming a database as a non-empty string.
  static String? _originatorDatabaseOfRecord(Map<String, Object?> record) {
    final metadata = record['metadata'];
    if (metadata is! Map) return null;
    final provenance = metadata['provenance'];
    if (provenance is! List || provenance.isEmpty) return null;
    final first = provenance.first;
    if (first is! Map) return null;
    final id = first['database_id'];
    return id is String && id.isNotEmpty ? id : null;
  }

  /// The hash [record]'s originating database sealed it under, read from
  /// the record as it arrived: its `event_hash` when its provenance holds
  /// one entry, otherwise its second entry's arrival hash; null when the
  /// record carries none as a string.
  static String? _sealedHashOfRecord(Map<String, Object?> record) {
    final metadata = record['metadata'];
    final provenance = metadata is Map ? metadata['provenance'] : null;
    if (provenance is! List || provenance.isEmpty) return null;
    final Object? hash;
    if (provenance.length == 1) {
      hash = record['event_hash'];
    } else {
      final second = provenance[1];
      hash = second is Map ? second['arrival_hash'] : null;
    }
    return hash is String ? hash : null;
  }

  // Implements: EVS-DEV-flow-token/C
  // ingest copies the incoming event verbatim (flow_token included); only metadata/sequence_number/event_hash are rewritten, so the token is preserved unchanged.
  /// Build a new [StoredEvent] with [receiverEntry] appended to
  /// `metadata.provenance`, `sequence_number` reassigned to [localSeq], and
  /// `event_hash` recomputed.
  ///
  /// Under the unified event store, the receiver overwrites the wire-supplied
  /// `sequence_number` so origin and ingest events share one monotone counter
  /// per device. The originator's wire-supplied
  /// `sequence_number` is preserved on [receiverEntry] as
  /// `originSequenceNumber`.
  StoredEvent _appendReceiverProvenance(
    StoredEvent incoming,
    ProvenanceEntry receiverEntry, {
    required int localSeq,
  }) {
    final oldProvenance = (incoming.metadata['provenance'] as List<Object?>)
        .cast<Map<String, Object?>>();
    final newProvenance = <Map<String, Object?>>[
      ...oldProvenance,
      receiverEntry.toJson(),
    ];
    final newMetadata = <String, Object?>{
      ...incoming.metadata,
      'provenance': newProvenance,
    };
    final recordMap = Map<String, Object?>.from(incoming.toMap());
    recordMap['metadata'] = newMetadata;
    recordMap['sequence_number'] = localSeq;
    recordMap.remove('event_hash'); // will be overwritten below
    final newHash = _eventHash(recordMap);
    recordMap['event_hash'] = newHash;
    return StoredEvent.fromMap(recordMap, localSeq);
  }

  /// Emit a receiver-originated `ingest.duplicate_received` audit event
  /// inside [txn], an event this database authors.
  Future<void> _emitDuplicateReceivedInTxn(
    Transaction txn, {
    required String subjectEventId,
    required String subjectEventHashOnRecord,
    required BatchContext? batchContext,
    PublishCollector? collector,
  }) async {
    final auditEvent = await _appendRawInternalEventInTxn(
      txn,
      _backend,
      databaseId: databaseId,
      hop: source.hopId,
      identifier: source.identifier,
      softwareVersion: source.softwareVersion,
      receivedAt: _now(),
      batchContext: batchContext,
      aggregateId: 'ingest-audit:${source.hopId}',
      aggregateType: kIngestAuditAggregateType,
      entryType: kIngestAuditEntryType,
      entryTypeVersion: entryTypes
          .byId(kIngestAuditEntryType)!
          .registeredVersion,
      eventType: kIngestDuplicateReceivedEventType,
      data: <String, Object?>{
        'subject_event_id': subjectEventId,
        'subject_event_hash_on_record': subjectEventHashOnRecord,
      },
      initiator: const AutomationInitiator(service: 'ingest'),
      uuid: _uuid,
      collector: collector,
    );
    await _foldRawInternalEventInTxn(txn, auditEvent, collector);
  }

  /// Folds [event], a raw internal audit this instance just appended
  /// inside [txn] (the `ingest.delivery_accepted` or `ingest.duplicate_received`
  /// audit), into every current copy exactly as an ingested event is
  /// folded, so a copy whose interest names the audit's event type stays
  /// current after the delivery that produced it. Not used for the boot's
  /// `lib_version` events, appended before this instance's projection
  /// interpreter and view copies exist.
  // Implements: EVS-DEV-view-convergence/E
  // the storing transaction of a delivery's raw audits folds them into
  //   every current copy, the same as any other stored event.
  // Implements: EVS-DEV-view-convergence/F
  // a copy this call does not set to the event's position is left
  //   entirely unchanged: this call touches only the copies the
  //   interpreter's own applyEvent decides are current.
  // Implements: EVS-DEV-security-findings/S
  // one fold_failed finding is recorded, in the same transaction, for each
  //   copy that passed over this raw internal audit.
  Future<void> _foldRawInternalEventInTxn(
    Transaction txn,
    StoredEvent event,
    PublishCollector? collector,
  ) async {
    final applied = await _interpreter.applyEvent(
      txn: txn,
      backend: _backend,
      event: event,
      copyIds: _viewCopyIds,
      mode: ApplyEventMode.alwaysStored,
    );
    if (applied.changes.isNotEmpty) collector?._addRowChanges(applied.changes);
    if (applied.failures.isEmpty) return;
    assert(
      collector != null,
      '_foldRawInternalEventInTxn: a fold failure on a raw internal audit '
      'needs a collector to record its fold_failed finding.',
    );
    await _recordFoldFailedFindingsInTxn(
      txn,
      collector!,
      event,
      applied.failures,
    );
  }
}

// ---------------------------------------------------------------------------
// File-level private helpers
// ---------------------------------------------------------------------------

/// Fixed initiator used for substrate-emitted lib_version events.
const _kLibVersionInitiator = AutomationInitiator(service: 'event_sourcing');

/// The reason an `event_malformed` finding names for a record that is not
/// an event record of this data format.
const String _kRecordMalformed = 'record_malformed';

/// The reason an `event_malformed` finding names for a record some string
/// of which, a key included, carries the character U+0000.
// Implements: EVS-DEV-security-findings/R
// the event_malformed reason unstorable_character.
const String _kUnstorableCharacter = 'unstorable_character';

/// The initiator of every security finding the library records.
const _kSecurityFindingInitiator = AutomationInitiator(
  service: 'event_sourcing',
);

/// [EventStore.verifyChains] on [store], reading the log [pageSize] events
/// at a time and awaiting [afterPage] after each page it read, for the
/// library's own tests of what the verification reads and when. In a build
/// with assertions disabled it throws [StateError] before it reads.
// Implements: EVS-PRD-storage-barrier/J
// the test-only entry point to the chain verification refuses in a build
//   with assertions disabled.
@internal
@visibleForTesting
Future<ChainVerificationVerdict> verifyChainsForTest(
  EventStore store, {
  int? from,
  int? to,
  int pageSize = kChainWalkPageSize,
  Future<void> Function()? afterPage,
}) {
  var assertionsEnabled = false;
  assert(() {
    assertionsEnabled = true;
    return true;
  }(), 'records that assertions are enabled');
  if (!assertionsEnabled) {
    throw StateError(
      'verifyChainsForTest is test-only and refuses in a build with '
      'assertions disabled',
    );
  }
  return store._verifyChains(
    from: from,
    to: to,
    pageSize: pageSize,
    afterPage: afterPage,
  );
}

/// [EventStore]'s ingest of one event outside any delivery, on [store], for
/// the library's own tests of how ingest handles a single record
/// (`EVS-PRD-ingest/G`: the library exposes no public ingest entry point
/// that admits an event outside a delivery, so this test-only seam is the
/// only way a test outside the event store's Dart library reaches it). In a
/// build with assertions disabled it throws [StateError] before it touches
/// [store].
// Implements: EVS-PRD-storage-barrier/J
// the test-only entry point to per-record ingest refuses in a build with
//   assertions disabled.
@internal
@visibleForTesting
Future<PerEventIngestOutcome> ingestEventForTest(
  EventStore store,
  StoredEvent incoming,
) {
  var assertionsEnabled = false;
  assert(() {
    assertionsEnabled = true;
    return true;
  }(), 'records that assertions are enabled');
  if (!assertionsEnabled) {
    throw StateError(
      'ingestEventForTest is test-only and refuses in a build with '
      'assertions disabled',
    );
  }
  return store._ingestEvent(incoming);
}

/// The event store's recording of a security finding inside [txn], as a
/// detection point runs it, for the library's own tests of the finding's
/// shape, identity and once-per-detector rule: the recording is private to
/// the event store's Dart library, and the detection points, which share
/// that library, are its only production callers. In a build with
/// assertions disabled it throws [StateError] before it touches [txn].
// Implements: EVS-PRD-storage-barrier/J
// the test-only entry point to the finding record refuses in a build with
//   assertions disabled, so it changes nothing the library writes there.
@internal
@visibleForTesting
Future<StoredEvent?> recordFindingInTxnForTest(
  EventStore store,
  Transaction txn,
  PublishCollector collector, {
  required FindingRole role,
  required FindingKind kind,
  required Map<String, Object?> evidence,
  required Iterable<String> aggregates,
}) async {
  var assertionsEnabled = false;
  assert(() {
    assertionsEnabled = true;
    return true;
  }(), 'records that assertions are enabled');
  if (!assertionsEnabled) {
    throw StateError(
      'recordFindingInTxnForTest is test-only and refuses in a build with '
      'assertions disabled',
    );
  }
  return store._recordFindingInTxn(
    txn,
    collector,
    role: role,
    kind: kind,
    evidence: evidence,
    aggregates: aggregates,
  );
}

/// Canonical event hash used by every raw-record-map append site; see
/// [canonicalEventHash].
String _canonicalEventHash(Map<String, Object?> recordMap) =>
    canonicalEventHash(recordMap);

/// The sequence number reserved for an event the database authors, and the
/// two links it records: its predecessor in the database's origin chain and
/// its predecessor in the database's storage chain.
typedef _ChainLinks = ({
  int sequenceNumber,
  String? previousEventHash,
  String? previousIngestHash,
});

/// Reserves the next local sequence number in [txn] and reads, in the same
/// transaction, the two links an event the database [databaseId] authors
/// at it records: the sealed hash of the latest event the database holds
/// as authored, and the stored hash of the event at the preceding local
/// sequence number. Appends nothing; the caller appends the event at the
/// reserved number.
// Implements: EVS-DEV-chain-verification/B
// the predecessor hash is the sealed hash of the event with the highest
//   local sequence number the database holds as authored, or null when it
//   holds none, read inside the append's transaction.
// Implements: EVS-PRD-hash-chain-integrity/B
// every appended event carries the hash of the event its database authored
//   immediately before it, so the database's authored events form one chain.
// Implements: EVS-DEV-chain-verification/C
// the storage link is the stored hash of the event at the preceding local
//   sequence number, read inside the storing transaction.
// Implements: EVS-DEV-chain-verification/T
// both links are keyed reads (an index on Postgres, a keyed record on
//   Sembast), so the chain adds a bounded cost to every append and ingest
//   whatever the log's size.
Future<_ChainLinks> _reserveChainLinksInTxn(
  StorageBackend backend,
  Transaction txn,
  String databaseId,
) async {
  final sequenceNumber = await backend.nextSequenceNumber(txn);
  final previousIngestHash = await backend.readLatestEventHash(txn);
  final latestAuthored = await backend.readLatestHeldAsAuthoredInTxn(
    txn,
    databaseId,
  );
  return (
    sequenceNumber: sequenceNumber,
    previousEventHash: latestAuthored == null
        ? null
        : _sealedHashOf(latestAuthored),
    previousIngestHash: previousIngestHash,
  );
}

/// The causal record of an event appended on [aggregateId] under
/// [declaration], read inside [txn]: the declared kind and eligibility, and
/// as `parents` the aggregate's latest eligible version in the database's
/// log, named by its sealed hash, or none when the database holds none.
// Implements: EVS-DEV-causal-parents/F
// kind and eligible are stamped from the appended entry type's declaration
//   for the appended event type, and parents by the stamping rule, inside
//   the append transaction.
// Implements: EVS-DEV-causal-parents/H
// parents names the aggregate's latest eligible version in the appending
//   database's log, or nothing when it holds none, read from the log inside
//   the append transaction.
Future<CausalRecord> _stampCausalInTxn(
  StorageBackend backend,
  Transaction txn, {
  required String aggregateId,
  required EventTypeDeclaration declaration,
}) async {
  final latest = await backend.readLatestEligibleVersionInTxn(txn, aggregateId);
  return CausalRecord(
    kind: declaration.kind,
    eligible: declaration.eligible,
    parents: <CausalRef>[
      if (latest != null)
        CausalRef(eventId: latest.eventId, eventHash: _sealedHashOf(latest)),
    ],
  );
}

/// The sealed hash of [stored], a copy the backend returned from the log.
/// Throws [StateError] when its provenance yields none: every copy the
/// library stores carries the entries it is read from.
// Implements: EVS-DEV-chain-verification/A
// predecessor hashes and causal parents name the sealed hash, never a
//   holder's re-stamped event_hash.
String _sealedHashOf(StoredEvent stored) {
  final sealed = ChainCoordinates.of(stored).sealedHash;
  if (sealed == null) {
    throw StateError(
      'stored event ${stored.eventId} at sequence ${stored.sequenceNumber} '
      'yields no sealed hash',
    );
  }
  return sealed;
}

/// The declaration of [eventType] by the reserved entry type [entryType],
/// read from [kSystemEntryTypes]: the library's raw internal appends run
/// before an event store, and its registry, exist. Throws [StateError] when
/// [entryType] is not a reserved entry type.
// Implements: EVS-DEV-causal-parents/F
// a raw internal append stamps kind and eligible from its reserved entry
//   type's declaration for its event type.
EventTypeDeclaration _reservedDeclaration(String entryType, String eventType) {
  for (final definition in kSystemEntryTypes) {
    if (definition.id == entryType) {
      return definition.declarationFor(eventType);
    }
  }
  throw StateError('$entryType is not a reserved system entry type');
}

/// The originator entry of an event the database [databaseId] authors at
/// [links]: attribution to [hop], [identifier] and [softwareVersion], the
/// database's identity, the library version of this build, and the
/// event's storage link.
// Implements: EVS-DEV-event-record/D+E+F
// the originator entry names the stamping database and the library version
//   this build declares, which is the compiled package version unless a
//   test installed a build declaration.
// Implements: EVS-PRD-provenance/A
// the entry the library stamps records the library's version.
// Implements: EVS-DEV-chain-verification/C
// the originator entry records the event's local sequence number and the
//   stored hash of the event before it.
ProvenanceEntry _originatorEntry({
  required String hop,
  required String identifier,
  required String softwareVersion,
  required DateTime receivedAt,
  required String databaseId,
  required _ChainLinks links,
  BatchContext? batchContext,
}) => ProvenanceEntry(
  hop: hop,
  receivedAt: receivedAt,
  identifier: identifier,
  softwareVersion: softwareVersion,
  ingestSequenceNumber: links.sequenceNumber,
  previousIngestHash: links.previousIngestHash,
  batchContext: batchContext,
  libraryVersion: EventStore._build().version,
  databaseId: databaseId,
);

/// Build and append one substrate-internal event, authored by the database
/// [databaseId], to [backend] inside [txn].
///
/// Reserves the event's sequence number and chain links, builds its
/// originator entry, assembles the record map shared by
/// [EventStore._emitDuplicateReceivedInTxn] and
/// [_appendLibVersionEventInTxn], hashes it with [_canonicalEventHash],
/// calls [StorageBackend.appendEvent], and records the event into
/// [collector] when one is given.
Future<StoredEvent> _appendRawInternalEventInTxn(
  Transaction txn,
  StorageBackend backend, {
  required String databaseId,
  required String hop,
  required String identifier,
  required String softwareVersion,
  required DateTime receivedAt,
  required String aggregateId,
  required String aggregateType,
  required String entryType,
  required EntryTypeVersion entryTypeVersion,
  required String eventType,
  required Map<String, Object?> data,
  required Initiator initiator,
  required Uuid uuid,
  BatchContext? batchContext,
  PublishCollector? collector,
}) async {
  // Every caller passes a shape it takes from the declared-shape constants,
  // so this check never fires on the library's own paths; it keeps a future
  // raw emitter from writing an undeclared shape. The emitted shapes are
  // checked on each backend by the reserved-shape assertions over the log.
  checkReservedEventShape(
    entryType: entryType,
    aggregateType: aggregateType,
    eventType: eventType,
  );
  final links = await _reserveChainLinksInTxn(backend, txn, databaseId);
  final localSeq = links.sequenceNumber;
  final causal = await _stampCausalInTxn(
    backend,
    txn,
    aggregateId: aggregateId,
    declaration: _reservedDeclaration(entryType, eventType),
  );
  final provenance0 = _originatorEntry(
    hop: hop,
    identifier: identifier,
    softwareVersion: softwareVersion,
    receivedAt: receivedAt,
    databaseId: databaseId,
    links: links,
    batchContext: batchContext,
  );
  final eventId = uuid.v4();
  final recordMap = <String, Object?>{
    'event_id': eventId,
    'aggregate_id': aggregateId,
    'aggregate_type': aggregateType,
    'entry_type': entryType,
    'entry_type_version': entryTypeVersion.toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': eventType,
    'sequence_number': localSeq,
    'data': data,
    'metadata': <String, Object?>{
      'provenance': <Map<String, Object?>>[provenance0.toJson()],
    },
    'initiator': initiator.toJson(),
    'flow_token': null,
    'client_timestamp': provenance0.receivedAt.toIso8601String(),
    'previous_event_hash': links.previousEventHash,
    'causal': causal.toJson(),
  };
  final eventHash = _canonicalEventHash(recordMap);
  recordMap['event_hash'] = eventHash;
  final event = StoredEvent.fromMap(recordMap, localSeq);
  await backend.appendEvent(txn, event);
  collector?._add(event);
  return event;
}

/// Append a substrate-emitted lib_version event to [backend] inside [txn].
///
/// Bypasses [EventStore.appendInTxn] because lib_version events are
/// appended from inside [EventStore.open]'s boot BEFORE the [EventStore]
/// instance exists, so we cannot reach the registry through it. The
/// hardcoded `entryTypeVersion` `1.0` here is the one documented exception
/// to the substrate-stamps-registeredVersion-from-the-registry rule
/// (see EVS-DEV-append-stamps-registered-version). If
/// `kLibVersionInitializedEntryType` / `kLibVersionChangedEntryType`
/// ever raise their `registeredVersion` in `kSystemEntryTypes`, this
/// constant must move in lockstep.
///
/// Delegates to [_appendRawInternalEventInTxn] for the actual
/// record-assembly and hashing.
Future<void> _appendLibVersionEventInTxn(
  Transaction txn,
  StorageBackend backend,
  String eventType,
  Map<String, Object?> data, {
  required String databaseId,
}) async {
  const uuid = Uuid();
  await _appendRawInternalEventInTxn(
    txn,
    backend,
    databaseId: databaseId,
    hop: 'event_sourcing',
    identifier: 'event_sourcing',
    softwareVersion: LibVersion.version,
    receivedAt: DateTime.now().toUtc(),
    aggregateId: kLibAggregateType,
    aggregateType: kLibAggregateType,
    entryType: eventType,
    entryTypeVersion: const EntryTypeVersion(1, 0),
    eventType: eventType,
    data: data,
    initiator: _kLibVersionInitiator,
    uuid: uuid,
  );
}

/// The security-context store an event store hands out: a separate object
/// that declares the reads alone and delegates them, so neither a downcast
/// nor a dynamic call reaches a writing member.
final class _SecurityContextReader implements SecurityContextStore {
  _SecurityContextReader(this._store);

  final SecurityContextStore _store;

  @override
  Future<EventSecurityContext?> read(String eventId) => _store.read(eventId);

  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => _store.queryAudit(
    initiator: initiator,
    flowToken: flowToken,
    ipAddress: ipAddress,
    from: from,
    to: to,
    limit: limit,
    cursor: cursor,
  );
}

/// The storage reader an [EventStore] hands out: it delegates the reads of
/// the store's backend, and nothing else.
final class _StorageReader implements StorageReader {
  _StorageReader(this._store);

  final EventStore _store;

  StorageBackend get _backend => _store._backend;

  /// The handles [transaction] has issued whose body is running.
  final Set<Transaction> _liveHandles = Set<Transaction>.identity();

  /// Returns [txn] when this reader, or its event store, issued it and its
  /// body is running; throws [StateError] otherwise.
  // Implements: EVS-DEV-storage-capability/G
  // a transaction handle used in a read of a storage reader other than the
  //   one that issued it (or its event store), or after its body returned,
  //   is refused with StateError.
  Transaction _issued(Transaction txn) {
    if (_liveHandles.contains(txn) || _store._liveHandles.contains(txn)) {
      return txn;
    }
    throw StateError(
      'StorageReader: the transaction handle was not issued by this reader '
      'or its event store, or its body has returned. Pass the handle a '
      'transaction body of this reader or of its event store received, '
      'while that body runs.',
    );
  }

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    refuseCallFromBootProgressObserver('StorageReader.transaction');
    return _backend.readOnlyTransaction<T>((txn) async {
      _liveHandles.add(txn);
      try {
        return await body(txn);
      } finally {
        _liveHandles.remove(txn);
      }
    });
  }

  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) =>
      _backend.findEventsForAggregate(aggregateId);

  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) async => _backend.findEventsForAggregateInTxn(_issued(txn), aggregateId);

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) => _backend.findAllEvents(
    afterSequence: afterSequence,
    limit: limit,
    originatorHopId: originatorHopId,
    originatorIdentifier: originatorIdentifier,
    entryType: entryType,
    clientTimestampStart: clientTimestampStart,
    clientTimestampEnd: clientTimestampEnd,
  );

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async => _backend.findAllEventsInTxn(
    _issued(txn),
    afterSequence: afterSequence,
    limit: limit,
    entryType: entryType,
    clientTimestampStart: clientTimestampStart,
    clientTimestampEnd: clientTimestampEnd,
  );

  @override
  Future<String?> readLatestEventHash(Transaction txn) async =>
      _backend.readLatestEventHash(_issued(txn));

  @override
  Future<int> readSequenceCounter() => _backend.readSequenceCounter();

  @override
  Future<StoredEvent?> findEventById(String eventId) =>
      _backend.findEventById(eventId);

  @override
  Future<StoredEvent?> findEventByIdInTxn(
    Transaction txn,
    String eventId,
  ) async => _backend.findEventByIdInTxn(_issued(txn), eventId);

  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) =>
      _backend.readEventsReverse(eventTypes: eventTypes);

  // The view-row reads below address a view by name; row storage addresses
  // rows by copy id, so each translates through the instance's copy map
  // before delegating to the backend (EVS-DEV-view-convergence). Each also
  // reads the copy's convergence state alongside its rows, in the same
  // transaction, and withholds what it cannot confirm settled
  // (EVS-DEV-converging-view-reads).

  /// The instance's [ProjectionSpec] and [ViewCopy] of [viewName], read
  /// inside [txn].
  ///
  /// The copy this instance last registered may, by the time this read
  /// runs, be marked for deletion or gone: another instance's boot or
  /// `rebuildView` legitimately marks a shared copy, and this instance's
  /// own `_viewCopyIds` follows only after its own next catch-up
  /// transaction commits. Rather than serve that copy's rows as current or
  /// throw once its record is gone, this finds the unmarked copy of the
  /// view's fingerprint in [txn]; when none is stored, it hands back a
  /// placeholder, unwritten copy at the position before the first event of
  /// the log, without writing one itself -- a storage reader's transaction
  /// runs read-only on Postgres, so it could never create the replacement
  /// there. The placeholder is empty, so the caller's currency scan reports
  /// it converging and every read built on it withholds rows
  /// (EVS-DEV-converging-view-reads/B) instead of the ones a copy being
  /// deleted has left behind; the catch-up driver, not a read, is what
  /// creates and catches up this instance's real replacement copy
  /// (EVS-DEV-view-convergence/T).
  Future<(ProjectionSpec, ViewCopy)> _specAndCopy(
    Transaction txn,
    String viewName,
  ) async {
    final spec = _store.projections.lookup(viewName);
    if (spec == null) {
      throw StateError(
        'EventStore: "$viewName" names no view this instance registered '
        'at EventStore.open.',
      );
    }
    final issued = _issued(txn);
    final fingerprint = viewFingerprint(
      spec,
      _store.entryTypes,
      _store._promoters,
    );
    final existing = await _backend.readUnmarkedViewCopyInTxn(
      issued,
      fingerprint,
    );
    final copy =
        existing ??
        ViewCopy(
          copyId: fingerprint,
          viewName: viewName,
          fingerprint: fingerprint,
          watermark: 0,
          markedForDeletion: false,
        );
    return (spec, copy);
  }

  // Implements: EVS-DEV-converging-view-reads/A
  // Implements: EVS-DEV-converging-view-reads/C
  @override
  Future<ViewRowRead> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final issued = _issued(txn);
    final (spec, copy) = await _specAndCopy(issued, viewName);
    final scan = await scanViewCurrency(
      txn: issued,
      backend: _backend,
      spec: spec,
      copy: copy,
    );
    await DeliveryTestHooks.current?.afterViewStateReadBeforeRows?.call();
    final copyId = copy.copyId;
    if (scan.state == ViewConvergenceState.converging) {
      final pending = switch (spec) {
        TableProjectionSpec() => true,
        AggregateProjectionSpec() =>
          scan.allUnsettled || scan.unsettledAggregateIds.contains(key),
      };
      if (pending) {
        return ViewRowRead(state: scan.state, row: const PendingRow());
      }
    }
    final row = await _backend.readViewRowInTxn(issued, copyId, key);
    return ViewRowRead(
      state: scan.state,
      row: row == null ? const AbsentRow() : SettledRow(row),
    );
  }

  // Implements: EVS-DEV-converging-view-reads/A
  // Implements: EVS-DEV-converging-view-reads/B
  @override
  Future<ViewRowsRead> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) => transaction(
    (txn) => findViewRowsInTxn(txn, viewName, limit: limit, offset: offset),
  );

  // Implements: EVS-DEV-converging-view-reads/A
  // Implements: EVS-DEV-converging-view-reads/C
  @override
  Future<ViewRowsByKeyRead> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  ) => transaction((txn) async {
    final issued = _issued(txn);
    final (spec, copy) = await _specAndCopy(issued, viewName);
    final scan = await scanViewCurrency(
      txn: issued,
      backend: _backend,
      spec: spec,
      copy: copy,
    );
    await DeliveryTestHooks.current?.afterViewStateReadBeforeRows?.call();
    final copyId = copy.copyId;
    final settledKeys = <String>{};
    final rows = <String, ViewRow>{};
    for (final key in keys) {
      final pending =
          scan.state == ViewConvergenceState.converging &&
          switch (spec) {
            TableProjectionSpec() => true,
            AggregateProjectionSpec() =>
              scan.allUnsettled || scan.unsettledAggregateIds.contains(key),
          };
      if (pending) {
        rows[key] = const PendingRow();
      } else {
        settledKeys.add(key);
      }
    }
    if (settledKeys.isNotEmpty) {
      final found = await _backend.readViewRowsByKeysInTxn(
        issued,
        copyId,
        settledKeys,
      );
      for (final key in settledKeys) {
        final row = found[key];
        rows[key] = row == null ? const AbsentRow() : SettledRow(row);
      }
    }
    return ViewRowsByKeyRead(state: scan.state, rows: rows);
  });

  // Implements: EVS-DEV-converging-view-reads/A
  // Implements: EVS-DEV-converging-view-reads/B
  // Implements: EVS-DEV-converging-view-reads/D
  @override
  Future<ViewRowsRead> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final issued = _issued(txn);
    final (spec, copy) = await _specAndCopy(issued, viewName);
    final scan = await scanViewCurrency(
      txn: issued,
      backend: _backend,
      spec: spec,
      copy: copy,
    );
    await DeliveryTestHooks.current?.afterViewStateReadBeforeRows?.call();
    final copyId = copy.copyId;
    if (scan.state == ViewConvergenceState.converging) {
      if (spec is TableProjectionSpec || scan.allUnsettled) {
        return ViewRowsRead(state: scan.state, rows: const []);
      }
    }
    final rows = await _backend.findViewRowsInTxn(
      issued,
      copyId,
      where: where,
      limit: limit,
      offset: offset,
    );
    if (scan.state == ViewConvergenceState.current) {
      return ViewRowsRead(state: scan.state, rows: rows);
    }
    final settled = [
      for (final row in rows)
        if (!scan.unsettledAggregateIds.contains(row['aggregateId'])) row,
    ];
    return ViewRowsRead(state: scan.state, rows: settled);
  }

  // Implements: EVS-DEV-converging-view-reads/J
  @override
  Future<List<ViewCopyStatus>> viewProgress() => transaction((txn) async {
    final issued = _issued(txn);
    final statuses = <ViewCopyStatus>[];
    for (final spec in _store.projections.all()) {
      final fingerprint = viewFingerprint(
        spec,
        _store.entryTypes,
        _store._promoters,
      );
      // A copy this instance last registered may since have been marked
      // for deletion or deleted underneath it (EVS-DEV-view-convergence/T,
      // see `_specAndCopy`); progress is reported for whichever copy the
      // driver is currently working, or as freshly converging when none
      // exists yet.
      var copy = await _backend.readUnmarkedViewCopyInTxn(issued, fingerprint);
      copy ??= ViewCopy(
        copyId: fingerprint,
        viewName: spec.viewName,
        fingerprint: fingerprint,
        watermark: 0,
        markedForDeletion: false,
      );
      final scan = await scanViewCurrency(
        txn: issued,
        backend: _backend,
        spec: spec,
        copy: copy,
      );
      // A failure while no copy existed was recorded under the
      // fingerprint (the catch-up driver's lock key when it has no known
      // copy id); a failure of a real copy's own catch-up was recorded
      // under its copy id.
      final progress =
          _store.catchUpProgressOf(copy.copyId) ??
          _store.catchUpProgressOf(fingerprint);
      statuses.add(
        ViewCopyStatus(
          viewName: spec.viewName,
          state: scan.state,
          watermark: copy.watermark,
          logHead: scan.logHead,
          lastFailure: progress?.lastFailure,
          lastFailureAt: progress?.lastFailureAt,
        ),
      );
    }
    return statuses;
  });

  @override
  Future<FifoEntry?> readFifoHead(String destinationId) =>
      _backend.readFifoHead(destinationId);

  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) => _backend.listFifoEntries(
    destinationId,
    afterSequenceInQueue: afterSequenceInQueue,
    limit: limit,
  );

  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) =>
      _backend.readFifoRow(destinationId, entryId);

  @override
  Future<bool> hasFifoWedged() => _backend.hasFifoWedged();

  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() => _backend.wedgedFifos();

  @override
  Future<int> readSchemaVersion() => _backend.readSchemaVersion();

  @override
  Future<int> readFillCursor(String destinationId) =>
      _backend.readFillCursor(destinationId);

  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) =>
      _backend.readSchedule(destinationId);

  @override
  Future<Map<String, DestinationSchedule>> listSchedules() =>
      _backend.listSchedules();

  // Implements: EVS-DEV-chain-verification/R
  // a verifier that holds only the log computes the same verdict and
  //   appends nothing.
  @override
  Future<ChainVerificationVerdict> verifyChains({int? from, int? to}) {
    refuseCallFromBootProgressObserver('StorageReader.verifyChains');
    return verifyChainsOver(_backend, from: from, to: to);
  }

  // Implements: EVS-DEV-sender-succession/G
  // the succession lineage read is derived solely from the succession
  //   events the log holds.
  @override
  Future<SuccessionLineage> successionLineageOf(String databaseId) =>
      computeSuccessionLineage(_backend, databaseId);

  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => _backend.queryAudit(
    initiator: initiator,
    flowToken: flowToken,
    ipAddress: ipAddress,
    from: from,
    to: to,
    limit: limit,
    cursor: cursor,
  );
}
