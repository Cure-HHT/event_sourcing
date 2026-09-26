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
// Implements: EVS-DEV-version-compatibility/E
// when a view's stored target for the event's entry type has the registered
//   major and a higher minor, the fold lowers it to the registered version in
//   its own transaction.
// Implements: EVS-DEV-version-compatibility/L
// every stored target of the event's entry type whose view this build
//   neither folds the event into nor registers for the entry type is marked
//   behind the log in the fold's own transaction.
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/integrity_marks.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
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

  /// Apply [event] to all matching projection specs inside [txn], and fold
  /// it into the outstanding-finding marks of every registered view,
  /// whatever its interest (see [foldIntoView]).
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
  /// For each matching view whose stored target version for the event's
  /// entry type has the registered major and a higher minor, the fold
  /// writes the registered version as the stored target, so the next open
  /// under the newer minor re-derives the rows this build folded.
  ///
  /// Every stored target of the event's entry type whose view this build
  /// neither folds the event into nor registers for that entry type (a
  /// view, or an entry type in a view's interest, that another build
  /// sharing the database registers) is marked behind the log, so the next
  /// open of a build that registers the view re-derives it.
  ///
  /// Returns the list of [AggregateFoldChange] records from every spec
  /// that produced a change; null results (e.g. tombstone of non-existent
  /// row) are excluded. The caller uses this list for post-commit subscriber
  /// notification via `SubscriptionEngine.publishRowChange`.
  @internal
  Future<List<AggregateFoldChange>> applyEvent({
    required Transaction txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    // The entry type's registered version. An entry type the registry does
    // not hold (a library-version event appended before the registry
    // exists) folds under the event's own version: no promotion, and no
    // stored target to compare.
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

    final changes = <AggregateFoldChange>[];
    for (final spec in projections.all()) {
      // Every view folds every event into its outstanding-finding marks;
      // only a view whose interest matches folds the event's data.
      if (!spec.interest.matches(event)) {
        changes.addAll(
          await foldIntoView(
            txn: txn,
            backend: backend,
            spec: spec,
            promoters: promoters,
            event: event,
            version: null,
          ),
        );
        continue;
      }

      if (def != null) {
        final stored = await backend.readViewTargetVersionInTxn(
          txn,
          spec.viewName,
          event.entryType,
        );
        if (stored != null &&
            stored.major == registeredVersion.major &&
            stored.minor > registeredVersion.minor) {
          await backend.writeViewTargetVersionInTxn(
            txn,
            spec.viewName,
            event.entryType,
            registeredVersion,
          );
        }
      }

      changes.addAll(
        await foldIntoView(
          txn: txn,
          backend: backend,
          spec: spec,
          promoters: promoters,
          event: event,
          version: registeredVersion,
        ),
      );
    }

    // A stored target of this entry type whose view this build neither
    // folds the event into nor registers for the entry type belongs to a
    // view, or a view's interest, that another build registers: mark it, so
    // the next open of a build that registers it re-derives the view.
    final stored = await backend.readViewTargetsForEntryTypeInTxn(
      txn,
      event.entryType,
    );
    for (final viewName in stored.keys) {
      final spec = projections.lookup(viewName);
      final folds = spec != null && spec.interest.matches(event);
      final registersPair =
          spec?.interest.entryTypes?.contains(event.entryType) ?? false;
      if (folds || registersPair) continue;
      await backend.markViewTargetBehindInTxn(txn, viewName, event.entryType);
    }
    return changes;
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
            spec.viewName,
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
        ),
        TableProjectionSpec() => TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: spec,
          event: eventForFold,
          integrity: marks.own,
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
      await backend.upsertViewRowInTxn(txn, spec.viewName, key, next);
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
          final row = await backend.readViewRowInTxn(
            txn,
            spec.viewName,
            entry.key,
          );
          if (row != null) await rewrite(entry.key, row, entry.value);
        }
      case TableProjectionSpec():
        // A table row records the local sequence number of the event that
        // produced it; the rows of an aggregate are those its events
        // produced.
        final producedBy = <int, List<String>>{};
        for (final entry in refresh.entries) {
          for (final e in await backend.findEventsForAggregateInTxn(
            txn,
            entry.key,
          )) {
            producedBy[e.sequenceNumber] = entry.value;
          }
        }
        for (final row in await backend.findViewRowsInTxn(txn, spec.viewName)) {
          final sequence = row['sequence'];
          final key = row['aggregateId'];
          if (sequence is! int || key is! String) continue;
          final ids = producedBy[sequence];
          if (ids != null) await rewrite(key, row, ids);
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
