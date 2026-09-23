// Substrate boot-time helpers for entry-type version evolution.
//
// Three helpers live in this file, all invoked from EventStore.open in
// fixed order:
//   1. verifyNoEntryTypeDowngrade — refuse boot if any entry type's
//      registered major is below the major of its highest stored target.
//   2. seedViewTargetVersions — ensure every (viewName, interest-matched
//      entry type) pair has a view_target_versions row; absent ones are
//      written at the current registeredVersion.
//   3. promoteViewSnapshots — for each view with a (viewName, entryType)
//      pair whose stored view_target_versions value is below the registered
//      version (a lower major, or the same major and a lower minor,
//      including a target an older minor's fold lowered), re-derive the
//      affected view rows from the log at the registered versions and
//      raise the stored target.
//
// Implements: EVS-DEV-view-target-versions-seeding/A
// seedViewTargetVersions
//   inserts a view_target_versions row for every (viewName, interest-matched
//   entryType) pair that has no existing row.
// Implements: EVS-DEV-view-target-versions-seeding/B
// existing rows are
//   skipped (not overwritten) by the `if (existing != null) continue` guard.
// Implements: EVS-DEV-view-target-versions-seeding/C
// newly-seeded rows
//   carry the registered major and minor (def.registeredVersion) as their
//   target value.
// Implements: EVS-DEV-view-target-versions-seeding/D
// the (viewName, entryType) pairs are
//   derived from each ProjectionSpec's interest filter via
//   _interestEntryTypes().
// Implements: EVS-DEV-snapshot-promotion-on-open/A
// promoteViewSnapshots
//   promotes every view row whose stored view_target_versions value is below
//   the registered version of the relevant entry type (a lower major, or the
//   same major and a lower minor).
// Implements: EVS-DEV-snapshot-promotion-on-open/B
// each affected row is
//   re-derived by folding its events again, each promoted through the
//   promoter chain to its entry type's registered version; the events in the
//   log are only read.
// Implements: EVS-DEV-snapshot-promotion-on-open/C
// exactly one audit
//   callback (emitAudit) is invoked per promoted (viewName, entryType) pair;
//   the caller wires this to a view_snapshot_promoted raw-append.
// Implements: EVS-DEV-snapshot-promotion-on-open/D
// (equivalence) a
//   re-derived row is folded through ProjectionInterpreter.foldIntoView, the
//   fold step the interpreter and rebuildView share, from the row's events
//   under the registered versions, so it is the row a replay produces.

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart'
    show EntryTypeVersionDowngradeError;
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

/// Callback invoked by [promoteViewSnapshots] once per (viewName,
/// entryType) pair that's been lifted from `fromVersion` to
/// `toVersion`. The caller (`EventStore.open`) wires this to append a
/// `view_snapshot_promoted` audit event via the substrate's
/// raw-internal-append path; boot-time helpers cannot reach
/// `EventStore.appendInTxn` because they run before the `EventStore`
/// instance exists.
typedef AuditEmitter =
    Future<void> Function({
      required String viewName,
      required String entryType,
      required EntryTypeVersion fromVersion,
      required EntryTypeVersion toVersion,
      required int rowsPromoted,
    });

/// Ensure every (registered projection viewName, entry type matched by
/// the projection's interest filter) pair has a `view_target_versions`
/// row. Absent rows are written at the entry type's current
/// `registeredVersion`. Existing rows are left untouched (they may lag
/// and drive the subsequent `promoteViewSnapshots` pass).
///
/// Runs inside the caller's transaction so the seeding and any
/// subsequent boot-time work commit atomically.
@internal
Future<void> seedViewTargetVersions({
  required Transaction txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required EntryTypeRegistry entryTypes,
}) async {
  for (final spec in projections.all()) {
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final def = entryTypes.byId(entryType);
      if (def == null) continue; // not in registry; out of scope for seeding
      final existing = await backend.readViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
      );
      if (existing != null) continue;
      await backend.writeViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
        def.registeredVersion,
      );
    }
  }
}

/// Refuse the boot if any registered entry type's major is below the major
/// of the highest stored value in `view_target_versions` across all views
/// that name it. A higher stored minor of the registered major is accepted:
/// minor steps only add fields with defaults, so a build of an older minor
/// reads rows promoted to a newer one.
///
/// Runs FIRST in the boot order (before [seedViewTargetVersions] and
/// before `promoteViewSnapshots`), so a downgrade fails fast without
/// the substrate touching any state.
Future<void> verifyNoEntryTypeDowngrade({
  required Transaction txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required EntryTypeRegistry entryTypes,
}) async {
  // For each entry type, find the maximum stored target across all
  // views that touch that entry type. Compare its major with the registry.
  final maxStored = <String, EntryTypeVersion>{};
  for (final spec in projections.all()) {
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final stored = await backend.readViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
      );
      if (stored == null) continue;
      final prior = maxStored[entryType];
      if (prior == null || stored > prior) {
        maxStored[entryType] = stored;
      }
    }
  }
  for (final entry in maxStored.entries) {
    final def = entryTypes.byId(entry.key);
    if (def == null) continue;
    if (def.registeredVersion.major < entry.value.major) {
      throw EntryTypeVersionDowngradeError(
        entryType: entry.key,
        fromVersion: entry.value,
        toVersion: def.registeredVersion,
      );
    }
  }
}

/// For each view with a (viewName, entryType) pair whose stored
/// `view_target_versions` value is below the entry type's current
/// `registeredVersion`, re-derive the view's affected rows from the log at
/// the registered versions. After each view: write the registered version
/// as the stored target of each lagging pair and invoke [emitAudit] once
/// per lagging pair so the caller can append a `view_snapshot_promoted`
/// audit event.
///
/// A row is re-derived by folding its events again through the projection
/// interpreter's fold step, each entry type under its registered version,
/// so a re-derived row is the row a replay of the log produces. The
/// affected rows of an aggregate view are those of the aggregates holding
/// an event of a lagging entry type below its registered version; every
/// other aggregate's events of that entry type fold unchanged under any
/// build of the registered major, so its row needs nothing. A table view's
/// row is keyed by what its row key extracts from an event, which need not
/// be the aggregate, so a table view with a lagging pair is refolded whole.
///
/// Runs THIRD (after [verifyNoEntryTypeDowngrade] and
/// [seedViewTargetVersions]) inside the caller's transaction.
@internal
Future<void> promoteViewSnapshots({
  required Transaction txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required AuditEmitter emitAudit,
}) async {
  for (final spec in projections.all()) {
    final lagging = <(String, EntryTypeVersion, EntryTypeVersion)>[];
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final def = entryTypes.byId(entryType);
      if (def == null) continue;
      final stored = await backend.readViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
      );
      if (stored == null) continue; // not yet seeded; skip
      if (stored >= def.registeredVersion) continue; // up to date
      lagging.add((entryType, stored, def.registeredVersion));
    }
    if (lagging.isEmpty) continue;

    final rowsByEntryType = switch (spec) {
      AggregateProjectionSpec() => await _rederiveAggregates(
        txn: txn,
        backend: backend,
        spec: spec,
        promoters: promoters,
        entryTypes: entryTypes,
        lagging: lagging,
      ),
      TableProjectionSpec() => await _refoldTable(
        txn: txn,
        backend: backend,
        spec: spec,
        promoters: promoters,
        entryTypes: entryTypes,
        lagging: lagging,
      ),
    };

    for (final (entryType, stored, registered) in lagging) {
      await backend.writeViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
        registered,
      );
      // Emit the audit via the caller-supplied callback, which appends
      // a `view_snapshot_promoted` audit event via raw-internal-append.
      await emitAudit(
        viewName: spec.viewName,
        entryType: entryType,
        fromVersion: stored,
        toVersion: registered,
        rowsPromoted: rowsByEntryType[entryType] ?? 0,
      );
    }
  }
}

/// The version the fold folds [event]'s entry type under: its registered
/// version, or the event's own for an entry type the registry does not
/// hold, as the projection interpreter decides it.
EntryTypeVersion _foldVersion(
  EntryTypeRegistry entryTypes,
  StoredEvent event,
) =>
    entryTypes.byId(event.entryType)?.registeredVersion ??
    event.entryTypeVersion;

/// Re-derives the rows of the aggregates holding an event of a lagging
/// entry type below its registered version. Returns, per lagging entry
/// type, the number of its affected aggregates that have a row afterwards.
Future<Map<String, int>> _rederiveAggregates({
  required Transaction txn,
  required StorageBackend backend,
  required AggregateProjectionSpec spec,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required List<(String, EntryTypeVersion, EntryTypeVersion)> lagging,
}) async {
  final affectedByEntryType = <String, Set<String>>{};
  final affected = <String>{};
  for (final (entryType, _, registered) in lagging) {
    final events = await backend.findAllEventsInTxn(txn, entryType: entryType);
    final ids = <String>{
      for (final e in events)
        if (e.entryTypeVersion < registered && spec.interest.matches(e))
          e.aggregateId,
    };
    affectedByEntryType[entryType] = ids;
    affected.addAll(ids);
  }

  final present = <String>{};
  for (final aggregateId in affected.toList()..sort()) {
    await backend.deleteViewRowInTxn(txn, spec.viewName, aggregateId);
    final events = await backend.findEventsForAggregateInTxn(txn, aggregateId);
    for (final event in events) {
      if (!spec.interest.matches(event)) continue;
      await ProjectionInterpreter.foldIntoView(
        txn: txn,
        backend: backend,
        spec: spec,
        promoters: promoters,
        event: event,
        version: _foldVersion(entryTypes, event),
      );
    }
    final row = await backend.readViewRowInTxn(txn, spec.viewName, aggregateId);
    if (row != null) present.add(aggregateId);
  }
  return <String, int>{
    for (final entry in affectedByEntryType.entries)
      entry.key: entry.value.where(present.contains).length,
  };
}

/// Chunk size for the streaming read of the log when a table view is
/// refolded.
const int _refoldChunkSize = 500;

/// Clears the table view of [spec] and folds every event its interest
/// matches again. Returns, for every lagging entry type, the number of
/// rows the view holds afterwards.
Future<Map<String, int>> _refoldTable({
  required Transaction txn,
  required StorageBackend backend,
  required TableProjectionSpec spec,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required List<(String, EntryTypeVersion, EntryTypeVersion)> lagging,
}) async {
  await backend.clearViewInTxn(txn, spec.viewName);
  int? lastSeq;
  while (true) {
    final chunk = await backend.findAllEventsInTxn(
      txn,
      afterSequence: lastSeq,
      limit: _refoldChunkSize,
    );
    if (chunk.isEmpty) break;
    for (final event in chunk) {
      if (!spec.interest.matches(event)) continue;
      await ProjectionInterpreter.foldIntoView(
        txn: txn,
        backend: backend,
        spec: spec,
        promoters: promoters,
        event: event,
        version: _foldVersion(entryTypes, event),
      );
    }
    if (chunk.length < _refoldChunkSize) break;
    lastSeq = chunk.last.sequenceNumber;
  }
  final rows = (await backend.findViewRowsInTxn(txn, spec.viewName)).length;
  return <String, int>{
    for (final (entryType, _, _) in lagging) entryType: rows,
  };
}

/// Returns the entry-type ids the [interest] filter names explicitly.
///
/// `SubscriptionFilter.entryTypes` is `null` for "match any user entry
/// type" and an empty set for "match nothing". Seeding only applies
/// to explicitly-named entry types (so we have something concrete to
/// seed against); a null or empty set yields no seeding rows.
Iterable<String> _interestEntryTypes(SubscriptionFilter interest) {
  final entryTypes = interest.entryTypes;
  if (entryTypes == null || entryTypes.isEmpty) return const <String>[];
  return entryTypes;
}
