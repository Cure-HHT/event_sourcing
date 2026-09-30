// Implements: EVS-PRD-portability/C
// pure Dart value type; serialises
//   identically on every Dart-supported runtime.
// Implements: EVS-PRD-destinations/L
// a view copy's identity, definition
//   fingerprint, fold watermark and deletion mark are part of the
//   persisted state a StorageBackend keeps beside the views.

/// One stored copy of a registered view: the rows folded under one
/// fingerprint of the view's definition, together with the log position
/// the copy has folded through.
///
/// A copy is identified by [copyId], assigned when it is created. Builds
/// whose definition of a view agrees share the copy whose [fingerprint]
/// matches; a build whose definition differs gets a copy of its own.
/// [watermark] is the log position through which the copy has folded
/// every event its definition folds; a fresh copy's watermark is the
/// position before the first event of the log. [markedForDeletion] is set
/// once no live instance registers the copy's fingerprint any longer; a
/// marked copy is dropped once its rows are deleted.
class ViewCopy {
  const ViewCopy({
    required this.copyId,
    required this.viewName,
    required this.fingerprint,
    required this.watermark,
    required this.markedForDeletion,
  });

  /// Opaque identifier assigned at creation, stable for the copy's
  /// lifetime. Distinct copies of the same fingerprint (an old, marked
  /// copy and its replacement) never share one, so rows of the copy being
  /// deleted never mix with the new copy's rows.
  final String copyId;

  /// The name of the view this is a copy of.
  final String viewName;

  /// The digest of the view's definition (its shape, its interest, the
  /// registered entry-type versions it depends on and the promoter chains)
  /// that this copy stores rows under.
  final String fingerprint;

  /// The log position through which this copy has folded every event its
  /// definition folds.
  final int watermark;

  /// Whether this copy is marked for deletion: no live instance registers
  /// its fingerprint any longer.
  final bool markedForDeletion;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ViewCopy &&
          copyId == other.copyId &&
          viewName == other.viewName &&
          fingerprint == other.fingerprint &&
          watermark == other.watermark &&
          markedForDeletion == other.markedForDeletion;

  @override
  int get hashCode =>
      Object.hash(copyId, viewName, fingerprint, watermark, markedForDeletion);

  @override
  String toString() =>
      'ViewCopy(copyId: $copyId, viewName: $viewName, '
      'fingerprint: $fingerprint, watermark: $watermark, '
      'markedForDeletion: $markedForDeletion)';
}
