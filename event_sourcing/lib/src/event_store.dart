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
//   bootTransaction, refusals first, then the library-version event,
//   seeding, promotion, re-derivation, the generation record and the boot
//   record (_runBoot).
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
// Implements: EVS-DEV-snapshot-promotion-on-open
// _runBoot promotes lagging view rows
//   and emits view_snapshot_promoted audit events.
// Implements: EVS-DEV-entry-type-downgrade-refusal/A
// EntryTypeVersionDowngradeError
//   is thrown from open when a registered major is below the major of the
//   highest stored target version.
// Implements: EVS-DEV-entry-type-downgrade-refusal/B
// verifyNoEntryTypeDowngrade
//   runs in _runBoot before any write of the boot transaction.
// Implements: EVS-DEV-entry-type-downgrade-refusal/C
// EntryTypeVersionDowngradeError
//   carries the entryType id and the stored and registered versions, each a
//   major and a minor, for diagnostic logging.

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:event_sourcing/src/destinations/default_destination_wedges_spec.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/ingest/chain_verdict.dart';
import 'package:event_sourcing/src/ingest/ingest_errors.dart';
import 'package:event_sourcing/src/ingest/ingest_result.dart';
import 'package:event_sourcing/src/lifecycle/boot_errors.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/snapshot_promotion.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/security_details.dart';
import 'package:event_sourcing/src/security/security_retention_policy.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/boot_check.dart';
import 'package:event_sourcing/src/storage/event_hash.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/subscriptions/subscription_engine.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:provenance/provenance.dart';
import 'package:uuid/uuid.dart';

/// Fire-and-forget trigger into `SyncCycle.call()`.
typedef EventStoreSyncCycleTrigger = Future<void> Function();

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
  PublishCollector._(this._transaction);

  final Transaction _transaction;
  bool _open = true;
  final List<StoredEvent> _events = <StoredEvent>[];
  final List<AggregateFoldChange> _rowChanges = <AggregateFoldChange>[];

  @internal
  void add(StoredEvent event) {
    _checkOpen();
    _events.add(event);
  }

  @internal
  void addRowChanges(Iterable<AggregateFoldChange> changes) {
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
/// below the major of the highest target version stored for that entry type
/// in `view_target_versions`: the views hold rows folded under a newer
/// major, which this build cannot read. A higher stored minor of the same
/// major is not a downgrade. The resolution is a build whose registered
/// major is at least [fromVersion]'s major.
class EntryTypeVersionDowngradeError extends Error {
  EntryTypeVersionDowngradeError({
    required this.entryType,
    required this.fromVersion,
    required this.toVersion,
    this.recordedByOpen = false,
  });

  /// The entry type whose registered major is below its stored major.
  final String entryType;

  /// The highest target version stored for [entryType].
  final EntryTypeVersion fromVersion;

  /// The version this build registers for [entryType].
  final EntryTypeVersion toVersion;

  /// True when the higher major comes from the database's generation
  /// record (an earlier open registered it) rather than from a stored view
  /// target; [fromVersion] then carries that major with minor 0.
  final bool recordedByOpen;

  @override
  String toString() =>
      'EntryTypeVersionDowngradeError: entry type "$entryType" was '
      '${recordedByOpen ? 'registered at major ${fromVersion.major} by an '
                'earlier open of the database (its generation record)' : 'previously folded at version $fromVersion (stored in '
                'view_target_versions)'}, '
      'but this build registers version $toVersion. '
      'A build whose registered major (${toVersion.major}) is below the '
      'stored major (${fromVersion.major}) is refused. Run a build that '
      'registers major ${fromVersion.major} or higher for "$entryType".';
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
    required this.backend,
    required this.entryTypes,
    required this.source,
    required this.securityContexts,
    required this.databaseId,
    required GenerationRegistration registration,
    this.syncCycleTrigger,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    Clock? clock,
    Uuid? uuid,
  }) : _interpreter = ProjectionInterpreter(
         projections: projections ?? ProjectionRegistry(),
         promoters: promoters ?? PromoterRegistry(),
         entryTypes: entryTypes,
       ),
       _promoters = promoters ?? PromoterRegistry(),
       _registration = registration,
       _clock = clock,
       _uuid = uuid ?? const Uuid();

  final StorageBackend backend;
  final EntryTypeRegistry entryTypes;
  final Source source;
  final MutableSecurityContextStore securityContexts;
  final EventStoreSyncCycleTrigger? syncCycleTrigger;
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

  /// Opens an [EventStore] against [storage]: the single production entry
  /// point. All required collaborators ([entryTypes], [source],
  /// [securityContexts]) must be supplied; the returned store is fully
  /// configured and ready for use.
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
  /// - No registered entry type's major may be below the major stored for
  ///   it in `view_target_versions`, or recorded for it in the generation
  ///   record by an earlier boot; a lower one throws
  ///   [EntryTypeVersionDowngradeError].
  ///
  /// Only the library-version events this database appended itself count;
  /// a peer's library-version events it ingested are never read as its
  /// own. When the boot accepts, it writes, in this order: a
  /// `lib_version_initialized` event at the first open (minting the
  /// database identity, [databaseId]), or a `lib_version_changed` event
  /// when this build's package version or data format differs from the one
  /// recorded last, older ones included; the target versions of newly
  /// registered view and entry-type pairs; the promotion of views whose
  /// stored targets lag the registered versions; the re-derivation of views
  /// that are behind the log; the generation record, merged with this
  /// build's generation; and a boot record. A refused boot writes nothing.
  ///
  /// Deployment. Builds with the same data-format major and the same
  /// entry-type majors share a database in any mix -- a canary beside the
  /// serving revision, several instances, a restart, a rollback to the
  /// previous release -- and every open by a different version is recorded
  /// in the log. A view, or an entry type in a view's interest, that only
  /// some of those builds register misses the events the others store
  /// until a build that registers it opens the database again: that open
  /// re-derives it (or `rebuildView` does). That catch-up follows the entry
  /// types a view's interest names; a view whose interest names none (one
  /// that selects by aggregate type), or whose interest differs between the
  /// builds only outside its entry types, is not caught up: run
  /// `rebuildView` for it once no build lacking it, or holding the narrower
  /// interest, still serves the database. A build of another data-format
  /// major, or one that raises an entry-type major, is deployed
  /// stop-then-start: every instance of the old revision stops before the
  /// first instance of the new one opens the database, and the old
  /// revision's next open is refused afterwards. Recovery after such a
  /// deployment is a restore from a backup taken before the switch, or a
  /// roll-forward. Evolve compatibly where possible: add an optional field
  /// as a minor step, and make a real reshape a new entry type.
  ///
  /// On a backend whose transactions contend with concurrent appends, the
  /// boot first locks what every append writes, so the appends of a
  /// revision serving the same database wait for the boot to commit rather
  /// than abort it. The wait lasts for the whole boot: its reads of the
  /// library-version events and the stored view targets, its checks, and
  /// any seeding, promotion and re-derivation it performs, the last two
  /// proportional to the events and rows of the views they rewrite. A
  /// release that promotes a large view, or adds a view over a long log,
  /// pauses the serving revision's appends for as long; measure the boot on
  /// a copy of production data before such a rollout.
  // Implements: EVS-DEV-event-store-open/A+B+C+D+E+F
  // the sole production constructor; the whole boot, refusals first, runs
  //   in one storage transaction (see _runBoot).
  static Future<EventStore> open({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required Source source,
    required MutableSecurityContextStore securityContexts,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    EventStoreSyncCycleTrigger? syncCycleTrigger,
    Clock? clock,
    Uuid? uuid,
  }) async {
    final effectiveProjections = projections ?? ProjectionRegistry();
    _registerLibraryDefinitions(entryTypes, effectiveProjections);
    effectiveProjections.seal();
    final effectivePromoters = (promoters ?? PromoterRegistry())..seal();
    final (:databaseId, :registration) = await _guardedBoot(
      storage: storage,
      entryTypes: entryTypes,
      projections: effectiveProjections,
      promoters: effectivePromoters,
      recordVersion: true,
    );
    return EventStore._(
      backend: storage,
      entryTypes: entryTypes,
      source: source,
      securityContexts: securityContexts,
      databaseId: databaseId,
      registration: registration,
      projections: effectiveProjections,
      promoters: effectivePromoters,
      syncCycleTrigger: syncCycleTrigger,
      clock: clock,
      uuid: uuid,
    );
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
  /// the database holds for [open] only.
  // Implements: EVS-DEV-event-store-open/A
  // the test-only constructor: visible for testing, so the analyzer reports
  //   a call from production code; the refusals of open; no library-version
  //   event.
  @visibleForTesting
  static Future<EventStore> openForTest({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required Source source,
    required MutableSecurityContextStore securityContexts,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    EventStoreSyncCycleTrigger? syncCycleTrigger,
    Clock? clock,
    Uuid? uuid,
  }) async {
    final effectiveProjections = projections ?? ProjectionRegistry();
    _registerLibraryDefinitions(entryTypes, effectiveProjections);
    effectiveProjections.seal();
    final effectivePromoters = (promoters ?? PromoterRegistry())..seal();
    final (:databaseId, :registration) = await _guardedBoot(
      storage: storage,
      entryTypes: entryTypes,
      projections: effectiveProjections,
      promoters: effectivePromoters,
      recordVersion: false,
    );
    return EventStore._(
      backend: storage,
      entryTypes: entryTypes,
      source: source,
      securityContexts: securityContexts,
      databaseId: databaseId,
      registration: registration,
      projections: effectiveProjections,
      promoters: effectivePromoters,
      syncCycleTrigger: syncCycleTrigger,
      clock: clock,
      uuid: uuid,
    );
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
  static Future<({String databaseId, GenerationRegistration registration})>
  _guardedBoot({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required ProjectionRegistry projections,
    required PromoterRegistry promoters,
    required bool recordVersion,
  }) async {
    final build = _build();
    final descriptor = GenerationDescriptor(
      packageVersion: build.version,
      dataFormat: build.dataFormat,
      entryTypes: <String, EntryTypeVersion>{
        for (final definition in entryTypes.all())
          definition.id: definition.registeredVersion,
      },
    );
    final registration = await storage.registerGeneration(descriptor);
    try {
      final databaseId = await _runBoot(
        storage: storage,
        entryTypes: entryTypes,
        projections: projections,
        promoters: promoters,
        recordVersion: recordVersion,
        descriptor: descriptor,
        registration: registration,
      );
      await registration.completeBoot();
      return (databaseId: databaseId, registration: registration);
    } catch (_) {
      await registration.release();
      rethrow;
    }
  }

  /// The boot of [open] (with [recordVersion]) and of [openForTest]
  /// (without), in one `bootTransaction` of [storage]. Returns the database
  /// identity.
  ///
  /// Every refusal is decided before the first write: the stored shapes,
  /// the database identity, the data format (in the log, then in the
  /// generation record) and the entry-type majors (in the stored view
  /// targets, then in the generation record). Then, in order: the
  /// library-version event (when [recordVersion] and one is due),
  /// view-target seeding, snapshot promotion (each promoted pair audited by
  /// a `view_snapshot_promoted` event), the re-derivation of views behind
  /// the log, the merged generation record and [registration]'s own
  /// records, and the boot record. The whole body may run more than once (a serialization retry, or a browser database re-running it
  /// after another tab committed); each run decides again from what it
  /// reads.
  // Implements: EVS-DEV-event-store-open/B+C+D+E+F
  // one boot transaction; refusals before any write; the library-version
  //   event before seeding and promotion; the boot record on every accepted
  //   boot.
  // Implements: EVS-DEV-entry-type-downgrade-refusal/A+B
  // verifyNoEntryTypeDowngrade runs before any write of the boot
  //   transaction.
  // Implements: EVS-DEV-snapshot-promotion-on-open/A+B+C
  // lagging view rows are re-derived and a view_snapshot_promoted audit
  //   appended per pair, in the boot transaction.
  // Implements: EVS-DEV-version-compatibility/L
  // views behind the log for an entry type in their interest are
  //   re-derived in the boot transaction.
  // Implements: EVS-DEV-version-compatibility/I
  // the generation record refuses, before any write, a build it does not
  //   admit, and every accepted boot merges its generation into it.
  static Future<String> _runBoot({
    required StorageBackend storage,
    required EntryTypeRegistry entryTypes,
    required ProjectionRegistry projections,
    required PromoterRegistry promoters,
    required bool recordVersion,
    required GenerationDescriptor descriptor,
    required GenerationRegistration registration,
  }) {
    final hooks = DeliveryTestHooks.current;
    final build = _build();
    return storage.bootTransaction<String>((txn) async {
      _observeBootBodyRun(hooks);

      // -------- Decide: nothing below writes until every refusal ran.
      await _refuseEarlierFormatEvents(storage, txn);
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
      await verifyNoEntryTypeDowngrade(
        txn: txn,
        backend: storage,
        projections: projections,
        entryTypes: entryTypes,
      );
      if (record != null) {
        for (final entry in descriptor.entryTypes.entries) {
          final recordedMajor = record.entryTypeMajors[entry.key];
          if (recordedMajor != null && recordedMajor > entry.value.major) {
            throw EntryTypeVersionDowngradeError(
              entryType: entry.key,
              fromVersion: EntryTypeVersion(recordedMajor, 0),
              toVersion: entry.value,
              recordedByOpen: true,
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
          );
          versionEventAppended = true;
        }
      }
      if (versionEventAppended &&
          (hooks?.afterBootVersionEvent?.call() ?? false)) {
        throw const InjectedFailure('afterBootVersionEvent');
      }
      final seeded = await seedViewTargetVersions(
        txn: txn,
        backend: storage,
        projections: projections,
        entryTypes: entryTypes,
      );
      await promoteViewSnapshots(
        txn: txn,
        backend: storage,
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
        emitAudit:
            ({
              required String viewName,
              required String entryType,
              required EntryTypeVersion fromVersion,
              required EntryTypeVersion toVersion,
              required int rowsPromoted,
            }) async {
              await _appendViewSnapshotPromotedAuditInTxn(
                txn,
                storage,
                entryTypes,
                viewName: viewName,
                entryType: entryType,
                fromVersion: fromVersion,
                toVersion: toVersion,
                rowsPromoted: rowsPromoted,
              );
            },
      );
      await catchUpViews(
        txn: txn,
        backend: storage,
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
        seeded: seeded,
      );
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
      return databaseId;
    });
  }

  /// Throws [DatabaseResetRequiredError] when the latest event in the log
  /// is not in this data format's stored shape.
  static Future<void> _refuseEarlierFormatEvents(
    StorageBackend storage,
    Transaction txn,
  ) async {
    try {
      await for (final _ in storage.readEventsReverseInTxn(txn)) {
        break;
      }
    } on FormatException catch (e) {
      throw DatabaseResetRequiredError(
        'its events are not in this data format: ${e.message}',
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

  /// Close the backend and subscription engine, releasing all resources.
  /// Not safe to call concurrently with in-flight work.
  ///
  /// The generation registration is released last, once the store and the
  /// backend have stopped writing, so no write of this store runs after a
  /// conflicting build could register.
  Future<void> close() async {
    await _subs.close();
    await backend.close();
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
  /// Does NOT trigger the sync cycle — callers that want sync-cycle triggering
  /// must call `unawaited(syncCycleTrigger?.call())` after this returns.
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

  /// Internal helper: wraps a `backend.transaction` call with a
  /// [PublishCollector] and publishes all collected events and row changes
  /// after commit.
  // Implements: EVS-PRD-subscription/E
  // A backend may run the body more than once
  //   (Postgres re-runs it after a serialization conflict; sembast_web re-runs
  //   it after another tab commits first). Each run gets a fresh collector, and
  //   only the collector of the run that committed (the last one) publishes.
  //   Runs that overlap break that contract and are refused.
  Future<T> _runInTxnWithPublish<T>(
    Future<T> Function(Transaction txn, PublishCollector collector) body,
  ) async {
    late PublishCollector collector;
    var runInProgress = false;
    final result = await backend.transaction<T>((txn) async {
      if (runInProgress) {
        throw StateError(
          'StorageBackend.transaction started a run of the body while an '
          'earlier run was still in progress; runs must be sequential.',
        );
      }
      runInProgress = true;
      final runCollector = PublishCollector._(txn);
      collector = runCollector;
      try {
        return await body(txn, runCollector);
      } finally {
        runCollector._open = false;
        runInProgress = false;
      }
    });
    for (final event in collector.events) {
      _subs.publishEvent(event);
    }
    for (final change in collector.rowChanges) {
      _subs.publishRowChange(change);
    }
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
  /// snapshot read and forward-mode delivery. A `_replayDone` flag
  /// inside the listener routes events to the buffer during snapshot
  /// read and directly to the output controller after it.
  ///
  /// The [StreamController] is closed when the subscriber cancels,
  /// preventing infinite blocking.
  Stream<Update<T>> _subscribeAggregate<T>(
    SubscriptionFilter filter,
    AggregateMode<T> mode,
  ) {
    late StreamController<Update<T>> controller;
    StreamSubscription<AggregateFoldChange>? liveSub;

    Future<void> start() async {
      // Open ONE subscription that lasts the lifetime of this stream.
      // During the snapshot phase events go to liveBuffer; after
      // _replayDone is set they go directly to controller.
      var replayDone = false;
      var maxSequenceSeen = 0;
      final liveBuffer = <AggregateFoldChange>[];

      liveSub = _subs.rowChanges(mode.viewName).listen((change) {
        if (controller.isClosed) return;
        if (!replayDone) {
          liveBuffer.add(change);
        } else {
          final u = _changeToUpdate<T>(change, filter, mode);
          if (u != null) {
            if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
            controller.add(u);
          }
        }
      }, onDone: () => controller.close());

      // Snapshot read
      final aggregateIds = mode.aggregates;
      if (aggregateIds == null) {
        final rows = await backend.findViewRows(mode.viewName);
        for (final row in rows) {
          if (controller.isClosed) return;
          final seq = (row['sequence'] as int?) ?? 0;
          if (seq > maxSequenceSeen) maxSequenceSeen = seq;
          controller.add(Snapshot<T>(value: mode.mapper(row), sequence: seq));
        }
      } else {
        // Implements: EVS-PRD-subscription/A
        // a filtered (row-scoped)
        // materialized-state snapshot. Materialize the whole allow-list in ONE
        // bulk read instead of a BEGIN/SELECT/COMMIT per aggregate id —
        // the former per-id transaction loop was an N+1 round-trip storm
        // (~3xN Cloud SQL round-trips for a site-scoped subscriber). Each
        // requested id still emits a Snapshot, with a null value for an absent
        // row, preserving the prior per-id tombstoned/absent signal.
        final byKey = await backend.readViewRowsByKeys(
          mode.viewName,
          aggregateIds,
        );
        for (final aggId in aggregateIds) {
          if (controller.isClosed) return;
          final row = byKey[aggId];
          final seq = (row?['sequence'] as int?) ?? 0;
          if (seq > maxSequenceSeen) maxSequenceSeen = seq;
          controller.add(
            Snapshot<T>(
              value: row == null ? null : mode.mapper(row),
              sequence: seq,
            ),
          );
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
        controller.add(EndOfReplay<T>(sequence: maxSequenceSeen));
      }

      replayDone = true;
    }

    controller = StreamController<Update<T>>(
      onListen: start,
      onCancel: () async {
        await liveSub?.cancel();
        liveSub = null;
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

  DateTime _now() => (_clock ?? () => DateTime.now().toUtc())();

  /// Append a new event. Returns the persisted `StoredEvent`, or `null`
  /// when `dedupeByContent` is true and the content matches the
  /// aggregate's most recent event of [entryType].
  ///
  /// Throws [ArgumentError], appending nothing, when [entryType] is a
  /// reserved system entry type ([kReservedSystemEntryTypeIds]): only the
  /// library appends reserved system events.
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
    unawaited(syncCycleTrigger?.call());
    return event;
  }

  /// Throws [ArgumentError] when [entryType] is a reserved system entry
  /// type: the public append operations never append one.
  // Implements: EVS-DEV-destination-drain/L
  // the event store's public append operations refuse reserved system entry
  //   types.
  static void _refuseReservedEntryType(String entryType) {
    if (kReservedSystemEntryTypeIds.contains(entryType)) {
      throw ArgumentError.value(
        entryType,
        'entryType',
        'is a reserved system entry type; only the library appends reserved '
            'system events',
      );
    }
  }

  /// Throws [ArgumentError] unless [entryType] is a reserved system entry
  /// type appended in a shape the library declares for it and, for a
  /// destination audit, with data ingest admits
  /// ([isWellFormedDestinationAuditData]).
  // Implements: EVS-DEV-destination-drain/K
  // every destination audit event the library appends carries a destination
  //   identifier and the appending database's identity, each non-empty and
  //   without '|'.
  static void _checkReservedAppend({
    required String entryType,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
  }) {
    checkReservedEventShape(
      entryType: entryType,
      aggregateType: aggregateType,
      eventType: eventType,
    );
    if (kDestinationAuditEntryTypes.contains(entryType) &&
        !isWellFormedDestinationAuditData(data)) {
      throw ArgumentError.value(
        data,
        'data',
        'a destination audit event carries a destination identifier (id) '
            'and a database identity (database_id), each a non-empty string '
            "without '|'",
      );
    }
  }

  /// Append a reserved system event in its own transaction: the library's
  /// counterpart of [append] for the entry types [append] refuses.
  ///
  /// Throws [ArgumentError], appending nothing, when [entryType] is not a
  /// reserved system entry type, when [aggregateType] and [eventType] are
  /// not a shape the library declares for [entryType], or when a destination
  /// audit's [data] lacks a destination identifier or a database identity
  /// that ingest admits. Otherwise behaves as [append]: stamps the registered version, dedupes by content when
  /// [dedupeByContent] is true (returning null), publishes after the commit
  /// and triggers the sync cycle.
  @internal
  Future<StoredEvent?> appendReserved({
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    bool dedupeByContent = false,
  }) async {
    _checkReservedAppend(
      entryType: entryType,
      aggregateType: aggregateType,
      eventType: eventType,
      data: data,
    );
    final event = await _runInTxnWithPublish<StoredEvent?>(
      (txn, collector) => _appendInTxn(
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
      ),
    );
    if (event == null) return null;
    unawaited(syncCycleTrigger?.call());
    return event;
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
  @internal
  Future<StoredEvent?> appendReservedInTxn(
    Transaction txn,
    PublishCollector collector, {
    required String entryType,
    required String aggregateId,
    required String aggregateType,
    required String eventType,
    required Map<String, Object?> data,
    required Initiator initiator,
    bool dedupeByContent = false,
  }) {
    _checkReservedAppend(
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
      final existing = await securityContexts.readInTxn(txn, eventId);
      if (existing == null) {
        throw ArgumentError.value(
          eventId,
          'eventId',
          'no security context row for event',
        );
      }
      await securityContexts.deleteInTxn(txn, eventId);
      // Emit the redaction audit event. The install UUID is the aggregate;
      // the redaction subject moves into `data.subject_event_id` so callers
      // can query "all redactions of event X" by filtering on entry_type
      // AND data.subject_event_id.
      await appendReservedInTxn(
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
    unawaited(syncCycleTrigger?.call());
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
      final compactCandidates = await securityContexts
          .findUnredactedOlderThanInTxn(txn, compactCutoff);
      for (final row in compactCandidates) {
        await securityContexts.upsertInTxn(txn, row.applyTruncation(p));
      }

      final purgeCandidates = await securityContexts.findOlderThanInTxn(
        txn,
        purgeCutoff,
      );
      for (final row in purgeCandidates) {
        await securityContexts.deleteInTxn(txn, row.eventId);
      }

      if (compactCandidates.isNotEmpty) {
        await appendReservedInTxn(
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
        await appendReservedInTxn(
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
      await appendReservedInTxn(
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
    unawaited(syncCycleTrigger?.call());
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
  /// Skips `unawaited(syncCycleTrigger?.call())` — the public [append]
  /// fires that AFTER the transaction commits.
  ///
  /// Validates inputs via [_validateAppendInputs] before doing any work,
  /// so direct callers do not need to pre-validate. Throws [ArgumentError],
  /// appending nothing, when [entryType] is a reserved system entry type
  /// ([kReservedSystemEntryTypeIds]): only the library appends reserved
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
  }) async {
    if (!collector._open || !identical(collector._transaction, txn)) {
      throw StateError(
        'EventStore.appendInTxn: the collector does not belong to this '
        'transaction run. Pass the transaction and collector a '
        'runTransaction body received, while that body runs.',
      );
    }
    _validateAppendInputs(
      entryType: entryType,
      aggregateType: aggregateType,
      eventType: eventType,
    );

    final def = entryTypes.byId(entryType)!;
    // Implements: EVS-DEV-append-stamps-registered-version
    // substrate is
    //   the single source of truth for entry_type_version on every append;
    //   the value is read from the registry, not supplied by callers.
    final entryTypeVersion = def.registeredVersion;
    final effectiveChangeReason = changeReason ?? 'initial';

    final now = _now();
    final provenance0 = ProvenanceEntry(
      hop: source.hopId,
      receivedAt: now,
      identifier: source.identifier,
      softwareVersion: source.softwareVersion,
    );

    // dedupe-by-content: compares against the most-recent event of matching
    // entry_type within the aggregate. Multiple entry types may share an
    // aggregate (e.g. system events under source.identifier); dedupe scopes
    // per entry_type so each emission stream is treated independently.
    StoredEvent? prior;
    if (dedupeByContent) {
      final aggregateHistory = await backend.findEventsForAggregateInTxn(
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

    // Implements: EVS-PRD-hash-chain-integrity/B
    // every appended event carries the
    //   hash of the one before it in its chain, read inside the same
    //   transaction so the link cannot straddle a concurrent append.
    final previousHash = await backend.readLatestEventHash(txn);
    final sequenceNumber = await backend.nextSequenceNumber(txn);
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
      'previous_event_hash': previousHash,
    };
    final eventHash = _eventHash(recordMap);
    recordMap['event_hash'] = eventHash;
    final event = StoredEvent.fromMap(recordMap, 0);

    await backend.appendEvent(txn, event);

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
      await securityContexts.writeInTxn(txn, row);
    }

    collector.add(event);

    // Run the projection interpreter inside the same transaction so views
    // materialize atomically with the append. Action-emitted events (via
    // ActionDispatcher → appendInTxn) MUST update views in-tx so subsequent
    // dispatches in the same flow read the new view rows.
    final rowChanges = await _interpreter.applyEvent(
      txn: txn,
      backend: backend,
      event: event,
    );
    if (rowChanges.isNotEmpty) {
      collector.addRowChanges(rowChanges);
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

  /// Process-local ingest. Opens its own transaction and delegates to
  /// [_ingestOneInTxn] with `batchContext: null`.
  ///
  /// Accepts an [incoming] StoredEvent, refuses an incompatible data-format
  /// or entry-type version ([IngestDataFormatIncompatible],
  /// [IngestEntryTypeVersionAhead]) and a reserved system event the library
  /// does not append ([IngestReservedEventRefused]), verifies Chain 1, checks
  /// idempotency
  /// by event_id, stamps a receiver ProvenanceEntry with Chain 2 fields
  /// (`batch_context = null`), recomputes `event_hash`, and persists.
  Future<PerEventIngestOutcome> ingestEvent(StoredEvent incoming) async {
    return _runInTxnWithPublish((txn, collector) async {
      return _ingestOneInTxn(
        txn,
        incoming,
        batchContext: null,
        collector: collector,
      );
    });
  }

  /// Wire-side batch ingest. Decodes [bytes] as an `esd/batch@2` envelope,
  /// runs every subject event through [_ingestOneInTxn] inside a single
  /// transaction, and stamps each with a [BatchContext] referencing this
  /// batch. Throws [IngestDecodeFailure] for any unsupported [wireFormat] or
  /// malformed bytes; throws the refusal of any event, [ingestEvent]'s
  /// refusals included, rolling back the whole batch; throws [IngestIdentityMismatch] (rolling back the whole
  /// batch) if any subject has a hash conflict with an already-stored event.
  ///
  /// See design spec §2.5.
  Future<IngestBatchResult> ingestBatch(
    Uint8List bytes, {
    required String wireFormat,
  }) async {
    if (wireFormat != BatchEnvelope.wireFormat) {
      throw IngestDecodeFailure(
        'unsupported wireFormat: "$wireFormat"; expected "${BatchEnvelope.wireFormat}"',
      );
    }
    final envelope = BatchEnvelope.decode(bytes);
    final wireBytesHash = sha256.convert(bytes).toString();
    final outcomes = <PerEventIngestOutcome>[];

    await _runInTxnWithPublish<void>((txn, collector) async {
      // Implements: EVS-PRD-event-log/G
      // A re-run body starts from no outcomes, so
      //   the result lists the committed run's outcomes only.
      outcomes.clear();
      for (var i = 0; i < envelope.events.length; i++) {
        final eventMap = envelope.events[i];
        final StoredEvent storedEvent;
        try {
          storedEvent = StoredEvent.fromMap(
            Map<String, Object?>.from(eventMap),
            0,
          );
        } on FormatException catch (e) {
          throw IngestDecodeFailure(
            'batch ${envelope.batchId} event $i: ${e.message}',
          );
        }
        final batchContext = BatchContext(
          batchId: envelope.batchId,
          batchPosition: i,
          batchSize: envelope.events.length,
          batchWireBytesHash: wireBytesHash,
          batchWireFormat: BatchEnvelope.wireFormat,
        );
        final outcome = await _ingestOneInTxn(
          txn,
          storedEvent,
          batchContext: batchContext,
          collector: collector,
        );
        outcomes.add(outcome);
      }
    });

    return IngestBatchResult(batchId: envelope.batchId, events: outcomes);
  }

  /// Per-event ingest logic, called from both [ingestEvent] and the
  /// `ingestBatch` loop.
  ///
  /// [batchContext] is non-null when called from `ingestBatch`, null when
  /// called from [ingestEvent].
  Future<PerEventIngestOutcome> _ingestOneInTxn(
    Transaction txn,
    StoredEvent incoming, {
    required BatchContext? batchContext,
    PublishCollector? collector,
  }) async {
    // 0. Version compatibility, before any read or write: the data-format
    //    major must equal this build's, and the entry-type major must not be
    //    above the registered one. A same-major event is accepted at any
    //    minor.
    // Implements: EVS-DEV-version-compatibility/D
    // every ingest entry point (ingestEvent and each event of ingestBatch)
    //   refuses a different data-format major or a higher entry-type major
    //   before any write.
    if (!incoming.libFormatVersion.isCompatibleWith(LibVersion.dataFormat)) {
      throw IngestDataFormatIncompatible(
        eventId: incoming.eventId,
        wireFormat: incoming.libFormatVersion,
        receiverFormat: LibVersion.dataFormat,
      );
    }
    // An entry type this build does not register is accepted at any
    // version: it is stored as it is and folds under its own version.
    final def = entryTypes.byId(incoming.entryType);
    if (def != null &&
        incoming.entryTypeVersion.major > def.registeredVersion.major) {
      throw IngestEntryTypeVersionAhead(
        eventId: incoming.eventId,
        entryType: incoming.entryType,
        wireVersion: incoming.entryTypeVersion,
        receiverVersion: def.registeredVersion,
      );
    }
    // Implements: EVS-DEV-version-compatibility/D
    // an event below the registered version that a view it folds into has
    //   no promoter path for is refused by name before any write.
    if (def != null && incoming.entryTypeVersion < def.registeredVersion) {
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

    // 0b. Reserved system events: only shapes the library appends, before
    //     any read or write.
    _refuseUndeclaredReservedEvent(incoming);

    // 1. Chain 1 verify on the incoming provenance.
    final verdict = _verifyChainOn(incoming);
    if (!verdict.isValid) {
      final failure = verdict.failures.first;
      throw IngestChainBroken(
        eventId: incoming.eventId,
        hopIndex: failure.position,
        expectedHash: failure.expectedHash,
        actualHash: failure.actualHash,
      );
    }

    // 2. Idempotency check by event_id.
    final existing = await backend.findEventByIdInTxn(txn, incoming.eventId);
    if (existing != null) {
      // Event already present — compare arrival_hash for identity check.
      final existingProv = (existing.metadata['provenance'] as List<Object?>)
          .cast<Map<String, Object?>>();
      final thisHopEntry = existingProv.last;
      final storedArrivalHash = thisHopEntry['arrival_hash'] as String?;
      if (storedArrivalHash == incoming.eventHash) {
        // Duplicate — emit audit event, return duplicate outcome.
        await _emitDuplicateReceivedInTxn(
          txn,
          subjectEventId: incoming.eventId,
          subjectEventHashOnRecord: existing.eventHash,
          batchContext: batchContext,
          collector: collector,
        );
        return PerEventIngestOutcome(
          eventId: incoming.eventId,
          outcome: IngestOutcome.duplicate,
          resultHash: existing.eventHash,
        );
      } else {
        throw IngestIdentityMismatch(
          eventId: incoming.eventId,
          incomingHash: incoming.eventHash,
          storedArrivalHash: storedArrivalHash ?? '(null)',
        );
      }
    }

    // 2b. A destination audit naming this database that this database does
    //     not hold is not one it appended and still has.
    // Implements: EVS-DEV-destination-drain/L
    // ingest refuses, before any write, a reserved destination audit event
    //   that names the receiver's own database and that the receiver does not
    //   already hold.
    if (kDestinationAuditEntryTypes.contains(incoming.entryType) &&
        incoming.data['database_id'] == databaseId) {
      throw IngestReservedEventRefused(
        eventId: incoming.eventId,
        entryType: incoming.entryType,
        reason: ReservedEventRefusal.namesReceiverDatabase,
      );
    }

    // 3. New event — reserve a fresh local sequence_number, capture the
    //    originator's wire-supplied sequence_number, and stamp receiver
    //    provenance. Under the unified event store, "Chain 2 ordering"
    //    is the local sequence_number; the previous-ingest tail hash is
    //    the prior event in this destination's log.
    final originSeq = incoming.sequenceNumber;
    final localSeq = await backend.nextSequenceNumber(txn);
    final previousTailHash = await backend.readLatestEventHash(txn);
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
    );

    // 4. Build the updated event with the local sequence_number and
    //    appended receiver provenance, then recompute the event hash.
    final updatedEvent = _appendReceiverProvenance(
      incoming,
      receiverEntry,
      localSeq: localSeq,
    );

    // 5. Persist via the same path as origin appends.
    await backend.appendEvent(txn, updatedEvent);
    collector?.add(updatedEvent);

    // 6. Fire the projection interpreter symmetric with the local-append path.
    //    The interpreter runs inside the same transaction as `appendEvent`,
    //    applying all registered ProjectionSpecs whose interest filter matches
    //    the event. A throw propagates out and rolls back the entire ingest
    //    transaction (all-or-nothing batch atomicity).
    final rowChanges = await _interpreter.applyEvent(
      txn: txn,
      backend: backend,
      event: updatedEvent,
    );
    if (collector != null && rowChanges.isNotEmpty) {
      collector.addRowChanges(rowChanges);
    }

    return PerEventIngestOutcome(
      eventId: updatedEvent.eventId,
      outcome: IngestOutcome.ingested,
      resultHash: updatedEvent.eventHash,
    );
  }

  /// Throws [IngestReservedEventRefused] when [incoming] is of a reserved
  /// system entry type and is not in a shape the library appends: its
  /// aggregate type and event type must be declared for its entry type
  /// ([ReservedEventRefusal.shapeMismatch]), and a destination audit event
  /// must carry a destination identifier and a database identity, each a
  /// non-empty string without `|` ([ReservedEventRefusal.malformed]). Reads
  /// and writes nothing.
  // Implements: EVS-DEV-destination-drain/L
  // ingest refuses, with a named reason and before any write, an event of a
  //   reserved entry type whose aggregate type or event type is not one the
  //   library declares for that entry type, and an event of a reserved
  //   destination audit entry type whose destination identifier or database
  //   identity is missing, empty, not a string, or contains '|'.
  static void _refuseUndeclaredReservedEvent(StoredEvent incoming) {
    final shape = kReservedEventShapes[incoming.entryType];
    if (shape == null) return;
    if (!shape.admits(incoming.aggregateType, incoming.eventType)) {
      throw IngestReservedEventRefused(
        eventId: incoming.eventId,
        entryType: incoming.entryType,
        reason: ReservedEventRefusal.shapeMismatch,
      );
    }
    if (!kDestinationAuditEntryTypes.contains(incoming.entryType)) return;
    if (!isWellFormedDestinationAuditData(incoming.data)) {
      throw IngestReservedEventRefused(
        eventId: incoming.eventId,
        entryType: incoming.entryType,
        reason: ReservedEventRefusal.malformed,
      );
    }
  }

  // -----------------------------------------------------------------------
  // Verification APIs
  // -----------------------------------------------------------------------

  /// Walk Chain 1 on [event].metadata.provenance backward from tail to origin.
  /// Non-throwing; returns a [ChainVerdict] with `ok=true` when every
  /// `arrival_hash` matches the recomputed hash at that hop, `ok=false`
  /// otherwise with a list of [ChainFailure] instances describing each broken
  /// link. Returns `ok=true` for origin-only events (single-entry provenance —
  /// no inter-hop links to verify).
  ///
  /// See design spec §2.11.
  Future<ChainVerdict> verifyEventChain(StoredEvent event) async {
    return _verifyChainOn(event);
  }

  /// Walk Chain 2 on this destination's event log from [fromSequenceNumber]
  /// to [toSequenceNumber] (inclusive). When [toSequenceNumber] is null,
  /// walks through the current tail. Throws [ArgumentError] if
  /// `fromSequenceNumber > toSequenceNumber`. Non-throwing otherwise; returns
  /// a [ChainVerdict] with `ok=true` when every `previous_ingest_hash` equals
  /// the stored `event_hash` of the prior ingest-stamped event in the range.
  ///
  /// Under the unified event store, the "Chain 2 ordering" is the local
  /// `sequence_number` (also recorded on the receiver-hop entry as
  /// `ingest_sequence_number` for symmetry with Chain 2 fields). Events
  /// without a receiver-stamped top provenance entry — i.e. origin appends
  /// made by this device — are skipped.
  ///
  /// See design spec §2.11.
  Future<ChainVerdict> verifyIngestChain({
    int fromSequenceNumber = 0,
    int? toSequenceNumber,
  }) async {
    final allEvents = await backend.findAllEvents();
    final ingestStamped = <StoredEvent>[];
    for (final event in allEvents) {
      final ingestSeq = _ingestSeqOf(event);
      if (ingestSeq != null) {
        ingestStamped.add(event);
      }
    }
    final tailSeq = ingestStamped.isEmpty
        ? 0
        : _ingestSeqOf(ingestStamped.last)!;
    final upperBound = toSequenceNumber ?? tailSeq;
    if (fromSequenceNumber > upperBound) {
      throw ArgumentError(
        'fromSequenceNumber ($fromSequenceNumber) must be <= '
        'toSequenceNumber ($upperBound)',
      );
    }
    final failures = <ChainFailure>[];
    StoredEvent? prev;
    for (final event in ingestStamped) {
      final thisSeq = _ingestSeqOf(event)!;
      if (thisSeq < fromSequenceNumber) continue;
      if (thisSeq > upperBound) break;
      if (thisSeq <= fromSequenceNumber) {
        // Anchor at the start of the range — not verified against
        // anything before it.
        prev = event;
        continue;
      }
      final lastEntry = _lastProvenanceEntry(event)!;
      final previousIngestHash = lastEntry['previous_ingest_hash'] as String?;
      final expected = prev?.eventHash;
      if (previousIngestHash != expected) {
        failures.add(
          ChainFailure(
            position: thisSeq,
            kind: ChainFailureKind.previousIngestHashMismatch,
            expectedHash: expected ?? '(null)',
            actualHash: previousIngestHash ?? '(null)',
          ),
        );
      }
      prev = event;
    }
    return ChainVerdict(isValid: failures.isEmpty, failures: failures);
  }

  /// Extract the last provenance entry of [event] as a typed map, or `null`
  /// when provenance is absent or empty. Shared by [_ingestSeqOf] and
  /// [verifyIngestChain] to avoid duplicating the same map-shape navigation.
  Map<String, Object?>? _lastProvenanceEntry(StoredEvent event) {
    final provenanceRaw = event.metadata['provenance'];
    if (provenanceRaw is! List || provenanceRaw.isEmpty) return null;
    final last = provenanceRaw.last;
    if (last is! Map<String, Object?>) return null;
    return last;
  }

  /// Extract the `ingest_sequence_number` from the last provenance entry of
  /// [event], or `null` when the event was not ingest-stamped (i.e. an
  /// origin-only event with no receiver hop). Used by [verifyIngestChain]
  /// to identify each event's position in Chain 2.
  int? _ingestSeqOf(StoredEvent event) =>
      _lastProvenanceEntry(event)?['ingest_sequence_number'] as int?;

  /// Walk Chain 1 on [event].metadata.provenance and return a non-throwing
  /// verdict. Used by [ingestEvent] and [verifyEventChain].
  ChainVerdict _verifyChainOn(StoredEvent event) {
    final provenanceRaw = event.metadata['provenance'];
    if (provenanceRaw is! List) {
      return const ChainVerdict(
        isValid: false,
        failures: <ChainFailure>[
          ChainFailure(
            position: -1,
            kind: ChainFailureKind.provenanceMissing,
            expectedHash: '(list)',
            actualHash: '(missing or non-list)',
          ),
        ],
      );
    }
    final provenance = provenanceRaw.cast<Map<String, Object?>>();
    if (provenance.isEmpty) {
      return const ChainVerdict(
        isValid: false,
        failures: <ChainFailure>[
          ChainFailure(
            position: -1,
            kind: ChainFailureKind.provenanceMissing,
            expectedHash: '(non-empty)',
            actualHash: '(empty)',
          ),
        ],
      );
    }
    final failures = <ChainFailure>[];
    // Walk from tail back to hop 1 (skip origin at index 0).
    //
    // Each receiver hop reassigns the stored event's `sequence_number` to
    // its local counter. To recompute the hash at hop k-1,
    // substitute the seq that was on the event when hop k-1 stored it:
    //
    //   - For k == 1 (recomputing the origin's hash): use the originator's
    //     wire-supplied seq, preserved on provenance[1].origin_sequence_number.
    //   - For k > 1 (recomputing a prior receiver hop's hash): use that
    //     prior hop's reassigned local seq, recorded as
    //     provenance[k-1].ingest_sequence_number.
    for (var k = provenance.length - 1; k > 0; k--) {
      final entry = provenance[k];
      final expected = entry['arrival_hash'] as String?;
      if (expected == null) {
        failures.add(
          ChainFailure(
            position: k,
            kind: ChainFailureKind.arrivalHashMismatch,
            expectedHash: '(non-null)',
            actualHash: '(null)',
          ),
        );
        continue;
      }
      final int? seqAtHopBefore;
      if (k == 1) {
        seqAtHopBefore = entry['origin_sequence_number'] as int?;
      } else {
        seqAtHopBefore = provenance[k - 1]['ingest_sequence_number'] as int?;
      }
      final recomputed = _hashWithProvenanceSlice(
        event,
        provenance.sublist(0, k),
        sequenceNumberOverride: seqAtHopBefore,
      );
      if (recomputed != expected) {
        failures.add(
          ChainFailure(
            position: k,
            kind: ChainFailureKind.arrivalHashMismatch,
            expectedHash: expected,
            actualHash: recomputed,
          ),
        );
      }
    }
    return ChainVerdict(isValid: failures.isEmpty, failures: failures);
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

  /// Compute the hash that an event would have with its provenance replaced
  /// by [provenanceSlice] and (optionally) `sequence_number` overridden to
  /// [sequenceNumberOverride]. Used by [_verifyChainOn] to reconstruct what
  /// each intermediate hop's `event_hash` was, accounting for the receiver-
  /// side reassignment of `sequence_number`.
  String _hashWithProvenanceSlice(
    StoredEvent event,
    List<Map<String, Object?>> provenanceSlice, {
    int? sequenceNumberOverride,
  }) {
    final recordMap = Map<String, Object?>.from(event.toMap());
    final newMetadata = <String, Object?>{
      ...event.metadata,
      'provenance': provenanceSlice,
    };
    recordMap['metadata'] = newMetadata;
    if (sequenceNumberOverride != null) {
      recordMap['sequence_number'] = sequenceNumberOverride;
    }
    recordMap.remove('event_hash');
    return _eventHash(recordMap);
  }

  /// Caller-composed rejection audit. See design spec §2.7.
  ///
  /// Opens its own transaction and records one `ingest.batch_rejected` event
  /// under the `ingest-audit:{hopId}` aggregate with Chain 2 fields stamped on
  /// `provenance[0]`.  `batch_context` is null because no decoded batch is
  /// associated — the batch failed before or during decoding.
  ///
  /// Typical call site:
  /// ```dart
  /// try {
  ///   await store.ingestBatch(bytes, wireFormat: 'esd/batch@2');
  /// } on IngestIdentityMismatch catch (e) {
  ///   await store.logRejectedBatch(
  ///     bytes,
  ///     wireFormat: 'esd/batch@2',
  ///     reason: 'identityMismatch',
  ///     failedEventId: e.eventId,
  ///     errorDetail: e.toString(),
  ///   );
  /// }
  /// ```
  Future<void> logRejectedBatch(
    Uint8List bytes, {
    required String wireFormat,
    required String reason,
    String? failedEventId,
    String? errorDetail,
  }) async {
    await backend.transaction((txn) async {
      final now = _now();
      final wireBytesHash = sha256.convert(bytes).toString();
      final localSeq = await backend.nextSequenceNumber(txn);
      final previousTailHash = await backend.readLatestEventHash(txn);
      final provenance0 = ProvenanceEntry(
        hop: source.hopId,
        receivedAt: now,
        identifier: source.identifier,
        softwareVersion: source.softwareVersion,
        arrivalHash: null,
        previousIngestHash: previousTailHash,
        ingestSequenceNumber: localSeq,
        batchContext: null,
      );
      await _appendRawInternalEventInTxn(
        txn,
        backend,
        aggregateId: 'ingest-audit:${source.hopId}',
        aggregateType: kIngestAuditAggregateType,
        entryType: kIngestAuditEntryType,
        entryTypeVersion: entryTypes
            .byId(kIngestAuditEntryType)!
            .registeredVersion,
        eventType: kIngestBatchRejectedEventType,
        data: <String, Object?>{
          'wire_bytes': base64Encode(bytes),
          'wire_format': wireFormat,
          'byte_length': bytes.length,
          'wire_bytes_hash': wireBytesHash,
          'reason': reason,
          'failed_event_id': failedEventId,
          'error_detail': errorDetail,
        },
        initiator: const AutomationInitiator(service: 'ingest'),
        provenance0: provenance0,
        localSeq: localSeq,
        previousTailHash: previousTailHash,
        uuid: _uuid,
      );
    });
  }

  /// Emit a receiver-originated `ingest.duplicate_received` audit event
  /// inside [txn]. Stamped with Chain 2 fields on `provenance[0]`.
  Future<void> _emitDuplicateReceivedInTxn(
    Transaction txn, {
    required String subjectEventId,
    required String subjectEventHashOnRecord,
    required BatchContext? batchContext,
    PublishCollector? collector,
  }) async {
    final now = _now();
    // Reserve a fresh local sequence_number; under the unified store this
    // value is also the receiver-hop's ingest_sequence_number for Chain 2.
    final localSeq = await backend.nextSequenceNumber(txn);
    final previousTailHash = await backend.readLatestEventHash(txn);
    final provenance0 = ProvenanceEntry(
      hop: source.hopId,
      receivedAt: now,
      identifier: source.identifier,
      softwareVersion: source.softwareVersion,
      arrivalHash: null,
      previousIngestHash: previousTailHash,
      ingestSequenceNumber: localSeq,
      batchContext: batchContext,
    );
    await _appendRawInternalEventInTxn(
      txn,
      backend,
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
      provenance0: provenance0,
      localSeq: localSeq,
      previousTailHash: previousTailHash,
      uuid: _uuid,
      collector: collector,
    );
  }
}

// ---------------------------------------------------------------------------
// File-level private helpers
// ---------------------------------------------------------------------------

/// Fixed initiator used for substrate-emitted lib_version events.
const _kLibVersionInitiator = AutomationInitiator(service: 'event_sourcing');

/// Canonical event hash used by every raw-record-map append site; see
/// [canonicalEventHash].
String _canonicalEventHash(Map<String, Object?> recordMap) =>
    canonicalEventHash(recordMap);

/// Build and append one substrate-internal event to [backend] inside [txn].
///
/// Encapsulates the ~25-line boilerplate shared by [EventStore.logRejectedBatch],
/// [EventStore._emitDuplicateReceivedInTxn], and [_appendLibVersionEventInTxn]:
/// assemble the 14-key record map, hash it with [_canonicalEventHash], call
/// [StorageBackend.appendEvent], and optionally record the event into [collector].
///
/// [provenance0] and [localSeq] / [previousTailHash] must be reserved by the
/// caller before this function is invoked, so that the caller can incorporate
/// them into provenance entries (e.g. Chain 2 fields) before passing them here.
Future<StoredEvent> _appendRawInternalEventInTxn(
  Transaction txn,
  StorageBackend backend, {
  required String aggregateId,
  required String aggregateType,
  required String entryType,
  required EntryTypeVersion entryTypeVersion,
  required String eventType,
  required Map<String, Object?> data,
  required Initiator initiator,
  required ProvenanceEntry provenance0,
  required int localSeq,
  required String? previousTailHash,
  required Uuid uuid,
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
    'previous_event_hash': previousTailHash,
  };
  final eventHash = _canonicalEventHash(recordMap);
  recordMap['event_hash'] = eventHash;
  final event = StoredEvent.fromMap(recordMap, localSeq);
  await backend.appendEvent(txn, event);
  collector?.add(event);
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
  Map<String, Object?> data,
) async {
  const uuid = Uuid();
  final now = DateTime.now().toUtc();
  final localSeq = await backend.nextSequenceNumber(txn);
  final previousTailHash = await backend.readLatestEventHash(txn);
  final provenance0 = ProvenanceEntry(
    hop: 'event_sourcing',
    receivedAt: now,
    identifier: 'event_sourcing',
    softwareVersion: LibVersion.version,
  );
  await _appendRawInternalEventInTxn(
    txn,
    backend,
    aggregateId: kLibAggregateType,
    aggregateType: kLibAggregateType,
    entryType: eventType,
    entryTypeVersion: const EntryTypeVersion(1, 0),
    eventType: eventType,
    data: data,
    initiator: _kLibVersionInitiator,
    provenance0: provenance0,
    localSeq: localSeq,
    previousTailHash: previousTailHash,
    uuid: uuid,
  );
}

/// Append a substrate-emitted `view_snapshot_promoted` event inside [txn].
///
/// Called by [EventStore._runBoot] (via the
/// [AuditEmitter] callback wired to [promoteViewSnapshots]) once per
/// (viewName, entryType) pair that has been lifted to a new
/// `registeredVersion`. Runs inside the same backend transaction as the
/// row updates and `view_target_versions` write, so the promoted state
/// and its audit event commit atomically.
///
/// Bypasses [EventStore.appendInTxn] because this boot-time helper runs
/// before the [EventStore] instance exists. Uses [_appendRawInternalEventInTxn]
/// for record assembly and hashing.
// Implements: EVS-DEV-snapshot-promotion-on-open
// audit event emission.
Future<void> _appendViewSnapshotPromotedAuditInTxn(
  Transaction txn,
  StorageBackend backend,
  EntryTypeRegistry entryTypes, {
  required String viewName,
  required String entryType,
  required EntryTypeVersion fromVersion,
  required EntryTypeVersion toVersion,
  required int rowsPromoted,
}) async {
  const uuid = Uuid();
  final now = DateTime.now().toUtc();
  final localSeq = await backend.nextSequenceNumber(txn);
  final previousTailHash = await backend.readLatestEventHash(txn);
  final provenance0 = ProvenanceEntry(
    hop: 'event_sourcing',
    receivedAt: now,
    identifier: 'event_sourcing',
    softwareVersion: LibVersion.version,
  );
  await _appendRawInternalEventInTxn(
    txn,
    backend,
    aggregateId: kLibAggregateType,
    aggregateType: kLibAggregateType,
    entryType: kViewSnapshotPromotedEntryType,
    entryTypeVersion: entryTypes
        .byId(kViewSnapshotPromotedEntryType)!
        .registeredVersion,
    eventType: kViewSnapshotPromotedEventType,
    data: <String, Object?>{
      'viewName': viewName,
      'entryType': entryType,
      'fromVersion': fromVersion.toString(),
      'toVersion': toVersion.toString(),
      'rowsPromoted': rowsPromoted,
    },
    initiator: _kLibVersionInitiator,
    provenance0: provenance0,
    localSeq: localSeq,
    previousTailHash: previousTailHash,
    uuid: uuid,
  );
}
