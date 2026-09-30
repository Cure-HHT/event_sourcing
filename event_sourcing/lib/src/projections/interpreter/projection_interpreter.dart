// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
//
// Implements: EVS-PRD-materializer/A
// ProjectionInterpreter is the central
//   dispatch loop that drives the library's materializer: for each incoming
//   event it iterates registered specs and invokes the appropriate fold.
// Implements: EVS-PRD-materializer/B
// determinism is preserved: the same
//   event dispatched to the same specs in the same order yields the same
//   fold changes; no non-deterministic inputs (wall clock, random, I/O
//   outside the backend transaction) are introduced here.
// Implements: EVS-DEV-ingest-promotes-before-fold/A
// applies the per-view
//   promoter chain to any event of a lower major, or of the registered major
//   and a lower minor, before dispatching to the fold.
// Implements: EVS-DEV-ingest-promotes-before-fold/B
// promotion operates on
//   an in-memory event.withData(...) copy; the original StoredEvent is not
//   modified.
// Implements: EVS-DEV-ingest-promotes-before-fold/C
// two views matching the
//   same entry type receive independently-computed promoted payloads (per-spec
//   loop; promoter chain lookup is keyed by (viewName, entryType)).
// Implements: EVS-DEV-ingest-promotes-before-fold/D
// an event of the
//   registered major at an equal or higher minor bypasses the promoter chain
//   and folds unchanged.
// Implements: EVS-DEV-version-compatibility/D
// an event of a higher major than the registered one is refused before the
//   fold writes anything.
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/integrity_marks.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/fold_failure.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/view_read.dart'
    show isMarkRefreshingEvent, isSecurityFindingEvent;
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/security_finding.dart'
    show FindingKind;
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

/// How [ProjectionInterpreter.applyEvent] treats a fold failure from one
/// copy's fold step.
@internal
enum ApplyEventMode {
  /// A caller-invoked append with no always-stored obligation: a
  /// locally-dispatched action's append, a registry's public operation
  /// (e.g. `destination_registered`, an app-requested halt), a security-
  /// context redaction or retention sweep, and the boot. A fold failure
  /// propagates to the caller as its original cause, and nothing is
  /// stored, so an app-visible append failure is never silently
  /// swallowed (`EVS-PRD-ingest/G`, contrast).
  local,

  /// An always-stored event (`EVS-DEV-view-convergence` Terms): ingest of a
  /// received event, a record the library appends in the transaction of an
  /// ingest, a restore or the drain, or any security finding. A fold failure
  /// from one copy is isolated to that copy alone: the copy passes over
  /// the event -- its watermark moves to the event's position, writing no
  /// row for it -- and the failure is collected in the returned
  /// [ApplyEventResult.failures] for the caller to record as a
  /// `fold_failed` finding; the rest of the delivery, restore or record
  /// still commits (`EVS-PRD-ingest/G`, `EVS-DEV-view-convergence/E`,
  /// `EVS-DEV-security-findings/S`). A storage failure -- any throw other
  /// than [FoldFailure] -- propagates unchanged, refusing the whole
  /// transaction.
  alwaysStored,
}

/// One copy's fold failure, collected by [ProjectionInterpreter.applyEvent]
/// under [ApplyEventMode.alwaysStored] for its caller to record as a
/// `fold_failed` security finding (`EVS-DEV-security-findings/S`).
@internal
class FoldFailureRecord {
  const FoldFailureRecord({
    required this.viewName,
    required this.definitionFingerprint,
    required this.reason,
  });

  /// The name of the view whose copy passed over the event.
  final String viewName;

  /// The fingerprint of the copy's definition
  /// (`EVS-DEV-security-findings/R`, `fold_failed`'s `definition_fingerprint`).
  final String definitionFingerprint;

  /// Which of the four computation sites failed.
  final FoldFailureReason reason;
}

/// The result of [ProjectionInterpreter.applyEvent]: the row changes the
/// fold produced, and, under [ApplyEventMode.alwaysStored], the fold
/// failures it collected rather than propagated.
@internal
class ApplyEventResult {
  const ApplyEventResult({required this.changes, required this.failures});

  /// The change records from every spec that produced a change.
  final List<AggregateFoldChange> changes;

  /// One record per copy whose fold of the event failed and was passed
  /// over. Always empty under [ApplyEventMode.local].
  final List<FoldFailureRecord> failures;
}

class ProjectionInterpreter {
  ProjectionInterpreter({
    required this.projections,
    required this.promoters,
    required this.entryTypes,
  });
  final ProjectionRegistry projections;
  final PromoterRegistry promoters;
  final EntryTypeRegistry entryTypes;

  /// Apply [event] to every registered view whose copy, of [copyIds], is
  /// current in [txn], and returns the row changes the fold produced
  /// together with any fold failures it collected.
  ///
  /// The fold decides by the event's entry-type version against the
  /// registered version of its entry type: an event of a lower major, or of
  /// the registered major and a lower minor, is promoted through each
  /// matching view's promoter chain before the fold; an event of the
  /// registered major at an equal or higher minor folds unchanged; an event
  /// of a higher major throws [StateError] before anything is written.
  /// Promotion works on an in-memory copy; the original [event] is not
  /// modified. A `DefaultField` in the chain supplies its value only for a
  /// field that neither the event nor the aggregate's existing row carries
  /// under the field's name at the registered version (see
  /// `PromoterExecutor.promote`).
  ///
  /// For each of the instance's copies, [copyIds] gives its id by view
  /// name. A copy that is current in [txn] -- the log holds no event past
  /// its watermark that its definition folds -- folds [event] when its
  /// definition folds it (the view's interest matches, or [event] is a
  /// security finding) and moves its watermark to [event]'s position,
  /// whether or not it folded the event's data. A copy that is converging
  /// is left entirely unchanged: neither its rows nor its watermark move.
  ///
  /// [ApplyEventResult.changes] holds the [AggregateFoldChange] records
  /// from every spec that produced a change; null results (e.g. tombstone
  /// of non-existent row) are excluded. The caller uses this list for
  /// post-commit subscriber notification via
  /// `SubscriptionEngine.publishRowChange`.
  ///
  /// Under [ApplyEventMode.alwaysStored], the current copies' folds run
  /// inside [StorageBackend.runInSavepointInTxn] (one savepoint for all of
  /// them, and one per copy only after that one meets a fold failure): a
  /// [FoldFailure] in a copy's fold keeps no write of the failed fold,
  /// moves the copy's watermark to [event]'s position regardless -- the
  /// copy passes over the event and stays current -- and is collected into
  /// [ApplyEventResult.failures] rather than recorded here (the
  /// interpreter cannot append; its caller records the `fold_failed`
  /// finding in the same transaction, `EVS-DEV-security-findings/S`). Any
  /// other throw -- a storage failure -- propagates out of [applyEvent]
  /// unchanged, so the whole transaction is refused. Under
  /// [ApplyEventMode.local] a [FoldFailure] propagates to the caller as its
  /// original cause, and [ApplyEventResult.failures] is always empty.
  // Implements: EVS-DEV-view-convergence/E
  // a current copy folds the event when its definition folds it and moves
  //   its watermark to the event's position; when that fold meets a fold
  //   failure under always-stored mode, the copy's fold runs in a
  //   savepoint, keeps no write of the failed fold, passes over the event
  //   and stays current.
  // Implements: EVS-DEV-view-convergence/F
  // a copy this transaction does not set to the event's position -- a
  //   converging one -- is left entirely unchanged.
  // Implements: EVS-PRD-ingest/G
  // a fold failure during an always-stored fold is isolated to its copy so
  //   the rest of the delivery is still admitted; the event stays stored
  //   and the copy passes over it rather than the whole ingest rolling
  //   back.
  // Implements: EVS-PRD-materializer/I
  // a copy that passes over a stored event whose fold into it fails
  //   contributes nothing to that copy's rows for the event, and keeps
  //   folding the copy's later events and serving it.
  @internal
  Future<ApplyEventResult> applyEvent({
    required Transaction txn,
    required StorageBackend backend,
    required StoredEvent event,
    required Map<String, String> copyIds,
    ApplyEventMode mode = ApplyEventMode.local,
  }) async {
    // The entry type's registered version. An entry type the registry does
    // not hold (a library-version event appended before the registry
    // exists) folds under the event's own version: no promotion.
    final def = entryTypes.byId(event.entryType);
    final registeredVersion = def?.registeredVersion ?? event.entryTypeVersion;
    if (event.entryTypeVersion.major > registeredVersion.major) {
      throw StateError(
        'ProjectionInterpreter: event ${event.eventId} of entry type '
        '"${event.entryType}" is at version ${event.entryTypeVersion}, a '
        'higher major than the registered $registeredVersion; this build '
        'cannot fold it.',
      );
    }

    final copiesById = <String, ViewCopy>{
      for (final copy in await backend.readViewCopiesInTxn(txn))
        copy.copyId: copy,
    };

    final changes = <AggregateFoldChange>[];
    final failures = <FoldFailureRecord>[];
    final currentCopies = <_CurrentCopy>[];
    for (final spec in projections.all()) {
      final maybeCopyId = copyIds[spec.viewName];
      final copy = maybeCopyId == null ? null : copiesById[maybeCopyId];
      // A copy marked for deletion -- another instance's boot or
      // rebuildView legitimately marked the copy this instance's
      // _viewCopyIds still names, which follows only after its own next
      // catch-up transaction commits -- is treated the same as a copy
      // this instance has not yet registered: skipped entirely, neither
      // its rows nor its watermark touched. The catch-up driver, not an
      // append, is what creates this instance's replacement copy
      // (EVS-DEV-view-convergence/T).
      if (copy == null || maybeCopyId == null || copy.markedForDeletion) {
        continue;
      }
      final copyId = maybeCopyId;
      final current = await _copyIsCurrent(
        txn: txn,
        backend: backend,
        spec: spec,
        watermark: copy.watermark,
        beforeSequence: event.sequenceNumber,
      );
      if (!current) continue;

      if (mode == ApplyEventMode.local) {
        try {
          changes.addAll(
            await foldStep(
              txn: txn,
              backend: backend,
              spec: spec,
              promoters: promoters,
              event: event,
              registeredVersion: registeredVersion,
              copyId: copyId,
            ),
          );
        } on FoldFailure catch (e) {
          // A locally-dispatched action's fold failure still fails to its
          // caller with the original exception, not the FoldFailure
          // wrapper: only ingest and catch-up distinguish fold failures
          // from other throws (EVS-PRD-ingest/G, contrast).
          e.rethrowCause();
        }
        await backend.setViewCopyWatermarkInTxn(
          txn,
          copyId,
          event.sequenceNumber,
        );
        continue;
      }

      // Always-stored mode: the copy's fold is deferred to the savepoint
      // below, which covers every current copy's fold of this event.
      currentCopies.add((spec: spec, copyId: copyId, copy: copy));
    }
    if (currentCopies.isNotEmpty) {
      await _foldAlwaysStored(
        txn: txn,
        backend: backend,
        event: event,
        registeredVersion: registeredVersion,
        copies: currentCopies,
        changes: changes,
        failures: failures,
      );
    }
    return ApplyEventResult(changes: changes, failures: failures);
  }

  /// Folds [event] into each of [copies] -- the copies current in [txn]
  /// under [ApplyEventMode.alwaysStored] -- and moves each one's watermark
  /// to [event]'s position, whether or not its fold succeeded.
  ///
  /// On a backend whose savepoint undoes every write of its body
  /// ([StorageBackend.savepointRollsBackWrites]), the folds of every copy
  /// run in one savepoint ([StorageBackend.runInSavepointInTxn]). When
  /// that savepoint's body meets a fold failure (a [FoldFailure] or, on
  /// Postgres, a [RowWriteRejected]), it rolls back, keeping no write of
  /// any copy's fold, and each copy's fold is redone in a savepoint of its
  /// own; on any other backend each copy's fold runs in a savepoint of its
  /// own from the start. Either way a copy
  /// whose own fold fails keeps no write of it and passes over the event
  /// (collected into [failures], unless [event] is itself a fold_failed
  /// finding), while every other copy folds the event as usual. The
  /// [AggregateFoldChange] records of the folds that commit are added to
  /// [changes]. Any other throw -- a storage failure -- propagates.
  // Implements: EVS-DEV-view-convergence/E
  // a current copy folds the event and moves its watermark to the event's
  //   position; a copy whose fold meets a fold failure under always-stored
  //   mode keeps no write of the failed fold, passes over the event and
  //   stays current, while the event's other current copies fold it.
  Future<void> _foldAlwaysStored({
    required Transaction txn,
    required StorageBackend backend,
    required StoredEvent event,
    required EntryTypeVersion registeredVersion,
    required List<_CurrentCopy> copies,
    required List<AggregateFoldChange> changes,
    required List<FoldFailureRecord> failures,
  }) async {
    Future<List<AggregateFoldChange>> fold(_CurrentCopy c) => foldStep(
      txn: txn,
      backend: backend,
      spec: c.spec,
      promoters: promoters,
      event: event,
      registeredVersion: registeredVersion,
      copyId: c.copyId,
    );

    // One savepoint for every copy's fold, where the backend's savepoint
    // undoes every write of its body: the common case, where no fold
    // fails, costs one savepoint per event however many copies fold it.
    // A backend whose savepoint keeps the writes made before a throw runs
    // each copy's fold in a savepoint of its own from the start.
    var perCopy = !backend.savepointRollsBackWrites;
    if (!perCopy) {
      try {
        changes.addAll(
          await backend.runInSavepointInTxn(txn, () async {
            final all = <AggregateFoldChange>[];
            for (final c in copies) {
              all.addAll(await fold(c));
            }
            return all;
          }),
        );
      } on FoldFailure catch (_) {
        perCopy = true;
      } on RowWriteRejected catch (_) {
        perCopy = true;
      }
    }

    if (perCopy) {
      // Each copy's fold runs in a savepoint of its own (redone, when the
      // shared savepoint rolled back every copy's fold), so a fold failure (a row key or
      // row data function that cannot extract from the event's payload, a
      // promoter, a derived field, or, on Postgres, a row write the server
      // rejects for its value with SQLSTATE class 22, 23 or 54,
      // reclassified by the backend as RowWriteRejected) rolls back only
      // that copy's fold -- nothing else in the transaction is touched --
      // and that copy alone passes over the event, the failure collected
      // for the caller to record as a `fold_failed` finding. Every other
      // copy, and the rest of the delivery, restore or record, still folds
      // and commits (`EVS-PRD-ingest/G`). Any other throw -- a storage
      // failure -- is not caught here and propagates out of applyEvent,
      // refusing the whole transaction.
      for (final c in copies) {
        try {
          changes.addAll(await backend.runInSavepointInTxn(txn, () => fold(c)));
        } on FoldFailure catch (e) {
          // Implements: EVS-DEV-security-findings/T
          // when the event being folded is itself a finding of kind
          //   fold_failed, the copy passes over it (watermark still moves)
          //   but nothing is collected: recording a further fold_failed
          //   finding about the failed fold of a fold_failed finding would
          //   recurse without bound.
          if (!isFoldFailedFinding(event)) {
            failures.add(
              FoldFailureRecord(
                viewName: c.spec.viewName,
                definitionFingerprint: c.copy.fingerprint,
                reason: e.reason,
              ),
            );
          }
        } on RowWriteRejected catch (_) {
          // The backend classified the row write its savepoint's body
          // performed as a rejection of the row's value (`EVS-DEV-view-
          // convergence` Terms), not a storage failure: a fold failure of
          // reason rowWriteFailed, exactly as a FoldFailure above (same
          // fold_failed-of-a-fold_failed exemption, EVS-DEV-security-
          // findings/T).
          if (!isFoldFailedFinding(event)) {
            failures.add(
              FoldFailureRecord(
                viewName: c.spec.viewName,
                definitionFingerprint: c.copy.fingerprint,
                reason: FoldFailureReason.rowWriteFailed,
              ),
            );
          }
        }
      }
    }

    // Every current copy's watermark moves to the event's position, so a
    // copy that passed over the event stays current.
    for (final c in copies) {
      await backend.setViewCopyWatermarkInTxn(
        txn,
        c.copyId,
        event.sequenceNumber,
      );
    }
  }

  /// The one fold step every writer of a copy's rows shares -- an append
  /// (this class's [applyEvent]) and a catch-up transaction
  /// (`ViewCatchUpDriver`) alike: folds [event] into [spec]'s view under
  /// [registeredVersion] when [spec]'s interest matches it, and always
  /// refreshes the outstanding-finding marks a security finding changes.
  /// Calling this from both paths under the same [registeredVersion] is
  /// what makes promotion equal event-replay-with-promotion by
  /// construction (EVS-DEV-view-convergence/K).
  // Implements: EVS-DEV-view-convergence/K
  // a catch-up transaction folds each event through this step, the same
  //   one an append uses, under the instance's registered version.
  @internal
  static Future<List<AggregateFoldChange>> foldStep({
    required Transaction txn,
    required StorageBackend backend,
    required ProjectionSpec spec,
    required PromoterRegistry promoters,
    required StoredEvent event,
    required EntryTypeVersion registeredVersion,
    required String copyId,
  }) {
    final matches = spec.interest.matches(event);
    return foldIntoView(
      txn: txn,
      backend: backend,
      spec: spec,
      promoters: promoters,
      event: event,
      version: matches ? registeredVersion : null,
      copyId: copyId,
    );
  }

  /// Whether a copy at [watermark] is current in [txn]: the log holds no
  /// event, strictly before [beforeSequence] and strictly after
  /// [watermark], that [spec]'s definition folds -- its interest matches,
  /// or the event is one [isMarkRefreshingEvent] names (EVS-DEV-view-
  /// convergence Terms).
  static Future<bool> _copyIsCurrent({
    required Transaction txn,
    required StorageBackend backend,
    required ProjectionSpec spec,
    required int watermark,
    required int beforeSequence,
  }) async {
    var after = watermark;
    const chunkSize = 500;
    while (after < beforeSequence - 1) {
      final chunk = await backend.findAllEventsInTxn(
        txn,
        afterSequence: after,
        limit: chunkSize,
      );
      if (chunk.isEmpty) return true;
      for (final e in chunk) {
        if (e.sequenceNumber >= beforeSequence) return true;
        if (spec.interest.matches(e) ||
            await isMarkRefreshingEvent(txn, backend, e)) {
          return false;
        }
      }
      after = chunk.last.sequenceNumber;
      if (chunk.length < chunkSize) return true;
    }
    return true;
  }

  /// Folds [event] into the view of [spec] under [version], the version
  /// the view folds the event's entry type under, inside [txn]; with a
  /// null [version], a view that does not fold the event's data, only its
  /// outstanding-finding marks.
  ///
  /// The one fold step every fold path shares -- the interpreter, a
  /// rebuild and boot promotion -- so they derive the same rows from the
  /// same events: an event of a lower major, or of [version]'s major and a
  /// lower minor, is promoted through the view's promoter chain before the
  /// fold, each default decided against the aggregate's current row (a
  /// table view has none: it writes one row per event); an event of
  /// [version]'s major at an equal or higher minor folds unchanged; an
  /// event of a higher major throws [StateError] before anything is
  /// written.
  ///
  /// Whatever [version], the step then refreshes the `$integrity` of the
  /// view's rows whose marks the event may change (a security finding the
  /// event is, or an event that reaches a held finding), so every view
  /// folds every finding whatever its interest, in log order.
  ///
  /// Returns the change records of the rows the step changed.
  // Implements: EVS-PRD-materializer/G
  // every view folds each security finding into the marks of the rows the
  //   finding marks, whatever its interest, in log order with the events it
  //   folds.
  @internal
  static Future<List<AggregateFoldChange>> foldIntoView({
    required Transaction txn,
    required StorageBackend backend,
    required ProjectionSpec spec,
    required PromoterRegistry promoters,
    required StoredEvent event,
    required EntryTypeVersion? version,
    required String copyId,
  }) async {
    if (version != null && event.entryTypeVersion.major > version.major) {
      throw StateError(
        'ProjectionInterpreter: event ${event.eventId} of entry type '
        '"${event.entryType}" is at version ${event.entryTypeVersion}, a '
        'higher major than $version, the version view "${spec.viewName}" '
        'folds it under; this build cannot fold it.',
      );
    }
    final marks = await IntegrityMarks.forEvent(txn, backend, event);
    final changes = <AggregateFoldChange>[];
    if (version != null) {
      var eventForFold = event;
      if (event.entryTypeVersion < version) {
        final existingRow = switch (spec) {
          AggregateProjectionSpec() => await backend.readViewRowInTxn(
            txn,
            copyId,
            event.aggregateId,
          ),
          TableProjectionSpec() => null,
        };
        eventForFold = event.withData(
          guardFold(
            FoldFailureReason.promoterFailed,
            () => PromoterExecutor.promote(
              registry: promoters,
              viewName: spec.viewName,
              entryType: event.entryType,
              fromVersion: event.entryTypeVersion,
              toVersion: version,
              payload: event.data,
              existingRow: existingRow,
            ),
          ),
        );
      }
      final change = await switch (spec) {
        AggregateProjectionSpec() => AggregateFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: spec,
          event: eventForFold,
          integrity: marks.own,
          copyId: copyId,
        ),
        TableProjectionSpec() => TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: spec,
          event: eventForFold,
          integrity: marks.own,
          copyId: copyId,
        ),
      };
      if (change != null) changes.add(change);
    }
    if (marks.refresh.isNotEmpty) {
      changes.addAll(
        await _refreshMarks(
          txn: txn,
          backend: backend,
          spec: spec,
          refresh: marks.refresh,
          event: event,
          copyId: copyId,
        ),
      );
    }
    return changes;
  }

  /// Rewrites the `$integrity` of every row of the view of [spec] whose
  /// aggregate [refresh] names (for a table view, every row an event of
  /// that aggregate produced) and whose marks differ, and returns their
  /// change records, caused by [event].
  static Future<List<AggregateFoldChange>> _refreshMarks({
    required Transaction txn,
    required StorageBackend backend,
    required ProjectionSpec spec,
    required Map<String, List<String>> refresh,
    required StoredEvent event,
    required String copyId,
  }) async {
    final changes = <AggregateFoldChange>[];
    Future<void> rewrite(
      String key,
      Map<String, Object?> row,
      List<String> ids,
    ) async {
      final held = integrityFindingIdsOf(row);
      if (held != null && _sameIds(held, ids)) return;
      final next = Map<String, Object?>.unmodifiable(<String, Object?>{
        ...row,
        kIntegrityRowKey: integrityValue(ids),
      });
      await backend.upsertViewRowInTxn(txn, copyId, key, next);
      changes.add(
        AggregateFoldChange(
          viewName: spec.viewName,
          aggregateId: key,
          newValue: next,
          sequence: event.sequenceNumber,
          cause: event.eventType,
          isTombstone: false,
        ),
      );
    }

    switch (spec) {
      case AggregateProjectionSpec():
        for (final entry in refresh.entries) {
          final row = await backend.readViewRowInTxn(txn, copyId, entry.key);
          if (row != null) await rewrite(entry.key, row, entry.value);
        }
      case TableProjectionSpec():
        // The rows of a source aggregate are those its insert events
        // produced, found by the backend's own index from source aggregate
        // id to row keys (`upsertTableViewRowInTxn` /
        // `findTableRowsBySourceAggregateInTxn`) rather than a scan of the
        // whole copy.
        for (final entry in refresh.entries) {
          final ids = entry.value;
          for (final row in await backend.findTableRowsBySourceAggregateInTxn(
            txn,
            copyId,
            entry.key,
          )) {
            final key = row['aggregateId'];
            if (key is! String) continue;
            await rewrite(key, row, ids);
          }
        }
    }
    return changes;
  }

  static bool _sameIds(List<String> a, List<String> b) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
}

/// Whether [event] is itself a security finding of kind `fold_failed`
/// (`EVS-DEV-security-findings/T`): shared by the inline fold above and the
/// catch-up driver, which each exempt such an event from collecting a
/// further `fold_failed` finding about its own failed fold.
@internal
bool isFoldFailedFinding(StoredEvent event) =>
    isSecurityFindingEvent(event) &&
    event.data['kind'] == FindingKind.foldFailed.wire;

/// A copy current in an always-stored fold's transaction: its view's
/// `spec`, its `copyId` and its `copy` record.
typedef _CurrentCopy = ({ProjectionSpec spec, String copyId, ViewCopy copy});
