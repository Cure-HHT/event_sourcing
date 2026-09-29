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
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/view_read.dart'
    show isSecurityFindingEvent;
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

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
  /// current in [txn], and returns the change records the fold produced.
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
  /// Returns the list of [AggregateFoldChange] records from every spec
  /// that produced a change; null results (e.g. tombstone of non-existent
  /// row) are excluded. The caller uses this list for post-commit subscriber
  /// notification via `SubscriptionEngine.publishRowChange`.
  // Implements: EVS-DEV-view-convergence/E
  // a current copy folds the event when its definition folds it and moves
  //   its watermark to the event's position.
  // Implements: EVS-DEV-view-convergence/F
  // a copy this transaction does not set to the event's position -- a
  //   converging one -- is left entirely unchanged.
  @internal
  Future<List<AggregateFoldChange>> applyEvent({
    required Transaction txn,
    required StorageBackend backend,
    required StoredEvent event,
    required Map<String, String> copyIds,
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
    for (final spec in projections.all()) {
      final maybeCopyId = copyIds[spec.viewName];
      final copy = maybeCopyId == null ? null : copiesById[maybeCopyId];
      if (copy == null || maybeCopyId == null) continue;
      final copyId = maybeCopyId;
      final current = await _copyIsCurrent(
        txn: txn,
        backend: backend,
        spec: spec,
        watermark: copy.watermark,
        beforeSequence: event.sequenceNumber,
      );
      if (!current) continue;

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
      await backend.setViewCopyWatermarkInTxn(
        txn,
        copyId,
        event.sequenceNumber,
      );
    }
    return changes;
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
  /// or the event is a security finding (EVS-DEV-view-convergence Terms).
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
        if (spec.interest.matches(e) || isSecurityFindingEvent(e)) return false;
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
          PromoterExecutor.promote(
            registry: promoters,
            viewName: spec.viewName,
            entryType: event.entryType,
            fromVersion: event.entryTypeVersion,
            toVersion: version,
            payload: event.data,
            existingRow: existingRow,
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
