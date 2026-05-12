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

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart'
    show EntryTypeVersionDowngradeError;
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Ensure every (registered projection viewName, entry type matched by
/// the projection's interest filter) pair has a `view_target_versions`
/// row. Absent rows are written at the entry type's current
/// `registeredVersion`. Existing rows are left untouched (they may lag
/// and drive the subsequent `promoteViewSnapshots` pass).
///
/// Runs inside the caller's transaction so the seeding and any
/// subsequent boot-time work commit atomically.
// Implements: EVS-DEV-view-target-versions-seeding.
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
// Implements: EVS-DEV-entry-type-downgrade-refusal.
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
