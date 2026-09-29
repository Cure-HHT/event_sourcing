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
// in one transaction, rebuildView marks the instance's copy of the view for
//   deletion and creates an empty copy of the same fingerprint.
// Implements: EVS-DEV-view-convergence/V
// rebuildView returns once the new copy is current for the instance, and
//   throws ViewConvergenceTimeout, naming the view and the copy's progress,
//   once the caller-supplied deadline passes first.

part of '../event_store.dart';

/// Replaces [viewName]'s copy for [store]'s instance and returns once the
/// replacement is current, or throws once [deadline] passes first.
///
/// In one backend transaction, this marks the instance's existing copy of
/// the view for deletion and creates a new, empty copy of the same
/// fingerprint (EVS-DEV-view-convergence/U): the same fingerprint, because
/// nothing about the view's definition changed, only a wish to refold it
/// from scratch. The new copy is exactly like any copy a changed
/// registration creates -- it catches up after this call through the
/// library's ordinary bounded catch-up transactions (EVS-DEV-view-
/// convergence), never inside this call's own transaction, so a rebuild of
/// a large view never holds back an append. The old copy's rows are
/// deleted by catch-up once marked for deletion, the same way a copy of
/// a retired fingerprint is deleted.
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
/// One caller rebuilds one view at a time for a given [store] instance:
/// two concurrent calls for the same [viewName] both read the same
/// instance copy id before either transaction commits, and the second
/// call's create collides with the first's new, unmarked copy.
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
  final oldCopyId = store._copyIdOf(viewName);
  final newCopyId = await backend.transaction<String>((txn) async {
    await backend.markViewCopyForDeletionInTxn(txn, oldCopyId);
    return backend.createViewCopyInTxn(txn, viewName, fingerprint, 0);
  });
  // Recorded only once the transaction that marked the old copy and
  // created the new one has committed: a rolled-back attempt must not
  // leave this instance pointing at a copy no reader can find.
  store._viewCopyIds[viewName] = newCopyId;
  await waitForViewsCurrent(store, <String>{viewName}, deadline);
}
