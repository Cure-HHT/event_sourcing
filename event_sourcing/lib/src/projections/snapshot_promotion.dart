// Substrate boot-time helpers for entry-type version evolution.
//
// Three helpers live in this file, all invoked from EventStore.open in
// fixed order:
//   1. assertNoEntryTypeDowngrade — refuse boot if any entry type's
//      registeredVersion has decreased. (Task 10)
//   2. seedViewTargetVersions — ensure every (viewName, interest-matched
//      entry type) pair has a view_target_versions row; absent ones are
//      written at the current registeredVersion.
//   3. promoteViewSnapshots — for each (viewName, entryType) pair whose
//      stored view_target_versions value is below current
//      registeredVersion, apply the promoter chain to the affected
//      view rows (those whose history includes events of that entry
//      type) and update the row. (Task 11)
//
// See: docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md
//
// Implements: EVS-DEV-view-target-versions-seeding/A — seedViewTargetVersions
//   inserts a view_target_versions row for every (viewName, interest-matched
//   entryType) pair that has no existing row.
// Implements: EVS-DEV-view-target-versions-seeding/B — existing rows are
//   skipped (not overwritten) by the `if (existing != null) continue` guard.
// Implements: EVS-DEV-view-target-versions-seeding/C — newly-seeded rows
//   carry def.registeredVersion as their target value.
// Implements: EVS-DEV-view-target-versions-seeding/D — the (viewName,
//   entryType) pairs are derived from each ProjectionSpec's interest filter
//   via _interestEntryTypes().
// Implements: EVS-DEV-snapshot-promotion-on-open/A — promoteViewSnapshots
//   promotes every view row whose stored view_target_versions value is below
//   the current registeredVersion of the relevant entry type.
// Implements: EVS-DEV-snapshot-promotion-on-open/B — the promoter chain is
//   applied to view row data; original events in the log are not modified.
// Implements: EVS-DEV-snapshot-promotion-on-open/C — exactly one audit
//   callback (emitAudit) is invoked per promoted (viewName, entryType) pair;
//   the caller wires this to a view_snapshot_promoted raw-append.
// Implements: EVS-DEV-snapshot-promotion-on-open/D — (equivalence) the
//   promoter primitive set is restricted to shape-changers that commute with
//   the deep-merge fold; snapshot promotion at boot is provably equivalent
//   to event-replay-with-promotion.

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart'
    show EntryTypeVersionDowngradeError;
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart'
    show TransformChain;
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Callback invoked by [promoteViewSnapshots] once per (viewName,
/// entryType) pair that's been lifted from `fromVersion` to
/// `toVersion`. The caller (`EventStore.open` in Task 12) wires this
/// to append a `view_snapshot_promoted` audit event via the substrate's
/// raw-internal-append path; boot-time helpers cannot reach
/// `EventStore.appendInTxn` because they run before the `EventStore`
/// instance exists.
typedef AuditEmitter =
    Future<void> Function({
      required String viewName,
      required String entryType,
      required int fromVersion,
      required int toVersion,
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
Future<void> seedViewTargetVersions({
  required Txn txn,
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

/// Refuse the boot if any registered entry type's `registeredVersion`
/// is below the highest stored value in `view_target_versions` across
/// all views. This is a Layer 1 substrate-enforced invariant — Phase I
/// has no `DemotionSpec` mechanism.
///
/// Runs FIRST in the boot order (before [seedViewTargetVersions] and
/// before `promoteViewSnapshots`), so a downgrade fails fast without
/// the substrate touching any state.
Future<void> assertNoEntryTypeDowngrade({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required EntryTypeRegistry entryTypes,
}) async {
  // For each entry type, find the maximum stored target across all
  // views that touch that entry type. Compare against the registry.
  final maxStored = <String, int>{};
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
    if (def.registeredVersion < entry.value) {
      throw EntryTypeVersionDowngradeError(
        entryType: entry.key,
        fromVersion: entry.value,
        toVersion: def.registeredVersion,
      );
    }
  }
}

/// For each (viewName, entryType) pair where stored
/// `view_target_versions` lags the entry type's current
/// `registeredVersion`, promote affected view rows by applying the
/// registered promoter chain. After each pair's promotion: update
/// `view_target_versions` to the current `registeredVersion` and
/// invoke [emitAudit] so the caller can append a
/// `view_snapshot_promoted` audit event.
///
/// Runs THIRD (after [assertNoEntryTypeDowngrade] and
/// [seedViewTargetVersions]) inside the caller's transaction.
Future<void> promoteViewSnapshots({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required AuditEmitter emitAudit,
  required DateTime now,
}) async {
  for (final spec in projections.all()) {
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

      final chain = promoters.chain(
        viewName: spec.viewName,
        entryType: entryType,
        fromVersion: stored,
        toVersion: def.registeredVersion,
      );

      // Affected aggregate ids: those whose history includes any event
      // of this entry type. Use the extended findAllEvents filter from
      // Task 3.
      final events = await backend.findAllEventsInTxn(
        txn,
        entryType: entryType,
      );
      final affectedAggregateIds = <String>{
        for (final e in events) e.aggregateId,
      };

      var rowsPromoted = 0;
      for (final aggregateId in affectedAggregateIds) {
        final row = await backend.readViewRowInTxn(
          txn,
          spec.viewName,
          aggregateId,
        );
        if (row == null) continue; // tombstoned or never present
        final firstEventTimestampRaw = row['firstEventTimestamp'] as String?;
        final firstEventTimestamp = firstEventTimestampRaw != null
            ? DateTime.parse(firstEventTimestampRaw)
            : now;
        // Apply transform chain to the row state.
        var promotedRow = Map<String, Object?>.from(row);
        for (final pspec in chain) {
          promotedRow = TransformChain.applyAll(pspec.transforms, promotedRow);
        }
        if (spec is AggregateProjectionSpec) {
          // Re-run derivedFields over the promoted row.
          final mutable = Map<String, Object?>.from(promotedRow);
          for (final df in spec.derivedFields) {
            mutable[df.fieldName] = df.computation.resolve(
              rowState: mutable,
              firstEventTimestamp: firstEventTimestamp,
            );
          }
          promotedRow = mutable;
        }
        await backend.upsertViewRowInTxn(
          txn,
          spec.viewName,
          aggregateId,
          Map<String, Object?>.unmodifiable(promotedRow),
        );
        rowsPromoted++;
      }

      // Update view_target_versions to the new registeredVersion.
      await backend.writeViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
        def.registeredVersion,
      );

      // Emit the audit (via callback — the caller plumbs the actual
      // raw-internal-append in Task 12).
      await emitAudit(
        viewName: spec.viewName,
        entryType: entryType,
        fromVersion: stored,
        toVersion: def.registeredVersion,
        rowsPromoted: rowsPromoted,
      );
    }
  }
}

/// Returns the entry-type ids the [interest] filter names explicitly.
///
/// `SubscriptionFilter.entryTypes` is `null` for "match any user entry
/// type" and an empty list for "match nothing". Seeding only applies
/// to explicitly-named entry types (so we have something concrete to
/// seed against); a null or empty list yields no seeding rows.
List<String> _interestEntryTypes(SubscriptionFilter interest) {
  final entryTypes = interest.entryTypes;
  if (entryTypes == null || entryTypes.isEmpty) return const <String>[];
  return entryTypes;
}
