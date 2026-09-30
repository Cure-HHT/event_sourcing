// event_sourcing/lib/src/projections/interpreter/fold_failure.dart
//
// A fold failure (`EVS-DEV-view-convergence` Terms) is a promoter, a row
// key, a row data or a derived field computation that throws while
// folding a stored event into a view copy's rows, before any write of
// that copy for the event; a storage failure, or any other throw, is not
// a fold failure. This file types the failure and its reason; what a
// storing transaction or a catch-up does with one is implemented where
// each runs.
import 'package:event_sourcing/src/storage/chain_coordinates.dart'
    show ChainCoordinates;
import 'package:event_sourcing/src/storage/stored_event.dart' show StoredEvent;
import 'package:meta/meta.dart' show internal;

/// The reason a fold failure names: the closed list
/// `EVS-DEV-security-findings/R` fixes for a `fold_failed` finding's
/// evidence.
@internal
enum FoldFailureReason {
  /// `PromoterExecutor.promote` could not promote the event's payload to
  /// the view's registered version.
  promoterFailed('promoter_failed'),

  /// A `TableProjectionSpec`'s `rowKey` extractor could not extract a key
  /// from the event.
  rowKeyFailed('row_key_failed'),

  /// A `TableProjectionSpec`'s `rowData` extractor could not extract row
  /// data from the event.
  rowDataFailed('row_data_failed'),

  /// An `AggregateProjectionSpec` derived field's computation threw.
  derivedFieldFailed('derived_field_failed'),

  /// A backend's write of the copy's row for the event was rejected as a
  /// value the row cannot hold, never as a storage failure: on
  /// `PostgresBackend`, a row write the server refuses with a SQLSTATE of
  /// class 22 (data exception), 23 (integrity constraint violation) or 54
  /// (program limit exceeded) (`EVS-DEV-view-convergence` Terms).
  rowWriteFailed('row_write_failed');

  const FoldFailureReason(this.wire);

  /// The string recorded as a `fold_failed` finding's evidence `reason`
  /// (`EVS-DEV-security-findings/R`).
  final String wire;
}

/// A promoter, row key, row data or derived field computation threw while
/// folding a stored event into one view copy, before any row write of that
/// copy for the event. [cause] and [causeStackTrace] are the original
/// throw. A caller that must preserve the exception type its own caller
/// sees -- a locally-dispatched action's append, whose fold failure
/// propagates as its cause's original type (`EVS-PRD-ingest/G`, contrast)
/// -- rethrows [cause] with [causeStackTrace] via [rethrowCause] rather
/// than this failure.
@internal
class FoldFailure implements Exception {
  FoldFailure(this.reason, this.cause, this.causeStackTrace);

  /// Which of the four computation sites failed.
  final FoldFailureReason reason;

  /// The original throw the failing computation raised.
  final Object cause;

  /// [cause]'s stack trace, for [rethrowCause] to restore.
  final StackTrace causeStackTrace;

  /// Rethrows [cause] with [causeStackTrace], so the caller sees exactly
  /// the exception a fold failure's cause was before it was wrapped.
  Never rethrowCause() => Error.throwWithStackTrace(cause, causeStackTrace);

  @override
  String toString() => 'FoldFailure(${reason.wire}: $cause)';
}

/// Runs [body], and wraps any throw other than a [FoldFailure] itself as a
/// [FoldFailure] of [reason], carrying the original throw as its cause and
/// its stack trace. Nothing runs after [body] but before the wrap, so a
/// throw here always precedes the row write the caller performs on
/// success. The four sites a fold wraps -- a promoter
/// (`PromoterExecutor.promote`), a table view's row key or row data
/// extractor, and an aggregate view's derived field computation -- each
/// call this with their own [reason]; nothing else a fold does is
/// wrapped, so a backend or other error from the row write itself passes
/// through untyped.
@internal
T guardFold<T>(FoldFailureReason reason, T Function() body) {
  try {
    return body();
  } on FoldFailure {
    rethrow;
  } catch (e, st) {
    throw FoldFailure(reason, e, st);
  }
}

/// The evidence map recorded on a `fold_failed` finding
/// (`EVS-DEV-security-findings/R`): the shared shape every always-stored
/// and catch-up fold-failure site builds, so an ingest, restore, drain or
/// catch-up recording and a catch-up's own held check (which must compute
/// the same finding id a not-yet-appended finding would get) never drift
/// apart on the evidence a given failure produces.
@internal
Map<String, Object?> foldFailedFindingEvidence({
  required String viewName,
  required String definitionFingerprint,
  required StoredEvent event,
  required FoldFailureReason reason,
}) => <String, Object?>{
  'view': viewName,
  'definition_fingerprint': definitionFingerprint,
  'event_id': event.eventId,
  'sealed_hash': ChainCoordinates.of(event).sealedHash,
  'reason': reason.wire,
};
