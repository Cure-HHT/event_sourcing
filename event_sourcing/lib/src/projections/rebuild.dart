// Implements: EVS-PRD-materializer/A
// rebuildView is the library-supplied
//   helper that replaces a view's copy and lets the ordinary catch-up
//   machinery refold it from scratch; it is part of the library's
//   materializer surface.
// Implements: EVS-PRD-materializer/B
// rebuild is deterministic: replacing
//   the copy and folding the same log through the same definition, via the
//   same catch-up step every ordinary append uses, always derives the same
//   rows.
// Implements: EVS-DEV-view-convergence/U
// in one transaction, rebuildView marks the fingerprint's current unmarked
//   copy for deletion, if one is stored, and creates an empty copy of the
//   same fingerprint.
// Implements: EVS-DEV-view-convergence/V
// rebuildView returns once the new copy is current for the instance, and
//   throws ViewConvergenceTimeout, naming the view and the copy's progress,
//   once the caller-supplied deadline passes first.
// Implements: EVS-DEV-view-convergence/T
// finding no unmarked copy of the fingerprint stored, rebuildView creates
//   one, the same as any instance that finds none before folding or
//   reading the view.

part of '../event_store.dart';

/// Replaces [viewName]'s copy for [store]'s instance and returns once the
/// replacement is current, or throws once [deadline] passes first.
///
/// In one backend transaction, this resolves the view's fingerprint's
/// current unmarked copy from stored state, marks it for deletion if one
/// is stored, and creates a new, empty copy of the same fingerprint
/// (EVS-DEV-view-convergence/U): the same fingerprint, because nothing
/// about the view's definition changed, only a wish to refold it from
/// scratch. It never reads this instance's cached copy id, so a rebuild
/// that follows another instance's already-committed mark or replacement
/// of the copy proceeds against what is actually stored instead of
/// throwing. The new copy is exactly like any copy a changed registration
/// creates -- it catches up after this call through the library's
/// ordinary bounded catch-up transactions (EVS-DEV-view-convergence),
/// never inside this call's own transaction, so a rebuild of a large view
/// never holds back an append. The old copy's rows are deleted by catch-up
/// once marked for deletion, the same way a copy of a retired fingerprint
/// is deleted.
///
/// Once the replacement copy is created, this instance addresses the
/// view's rows by the new copy id: a concurrent read, through
/// [EventStore.reader], reports the new copy's convergence and rows, not
/// the old one's, from the moment this call's transaction commits.
///
/// Returns once the new copy is current for the instance (EVS-DEV-view-
/// convergence/V). Throws [ViewConvergenceTimeout], naming [viewName] and
/// the new copy's progress, once [deadline] passes with the copy still
/// converging.
///
/// A rebuild that begins after another instance's rebuild or mark of the
/// copy has already committed sees the committed state and proceeds,
/// whether that left an unmarked replacement or no unmarked copy at all.
/// This call does not itself coordinate rebuilds whose transactions
/// overlap: when one commits a replacement the other did not see, the
/// other's create is refused by the one-unmarked-copy-per-fingerprint
/// contract every backend's `createViewCopyInTxn` upholds.
Future<void> rebuildView({
  required EventStore store,
  required String viewName,
  required DateTime deadline,
}) async {
  refuseCallFromBootProgressObserver('rebuildView');
  final spec = store.projections.lookup(viewName);
  if (spec == null) {
    throw StateError(
      'rebuildView: no ProjectionSpec registered under "$viewName" in '
      'store.projections. Register the spec before calling rebuildView.',
    );
  }
  final fingerprint = viewFingerprint(spec, store.entryTypes, store._promoters);
  final backend = store._backend;
  final newCopyId = await backend.transaction<String>((txn) async {
    final currentCopy = await backend.readUnmarkedViewCopyInTxn(
      txn,
      fingerprint,
    );
    if (currentCopy != null) {
      await backend.markViewCopyForDeletionInTxn(txn, currentCopy.copyId);
    }
    return backend.createViewCopyInTxn(txn, viewName, fingerprint, 0);
  });
  // Recorded only once the transaction that marked the old copy and
  // created the new one has committed: a rolled-back attempt must not
  // leave this instance pointing at a copy no reader can find.
  store._viewCopyIds[viewName] = newCopyId;
  await waitForViewsCurrent(store, <String>{viewName}, deadline);
}
