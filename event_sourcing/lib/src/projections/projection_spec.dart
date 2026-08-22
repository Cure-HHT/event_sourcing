// Implements: EVS-PRD-materializer/A
// provides the sealed ProjectionSpec type
//   (AggregateProjectionSpec, TableProjectionSpec) that is the library's
//   declarative description of a materializer rule set.
// Implements: EVS-PRD-materializer/B
// the spec is pure data; determinism of
//   materialization follows from the interpreter applying the same spec to
//   the same events in the same order.
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';

/// Declarative description of a projection: a default Layer-2
/// materialization the substrate ships for consumers who want the
/// library's default interpretation of the event stream. Applications
/// that need a different materialization can compute it on top of the
/// raw event stream via `EventStore.subscribe<T>(_, Events())` or
/// `EventStore.read(...)`; the substrate's default projection is one
/// valid view, not the unique truth. See CLAUDE.md "Epistemic layers"
/// for the framing.
sealed class ProjectionSpec {
  const ProjectionSpec();
  String get viewName;
  SubscriptionFilter get interest;
}

/// A projection that holds one row per aggregate and folds matching
/// events into that row via the library's default Layer-2 conventions:
/// generic null-as-clear key-wise merge of `event.data` into the row,
/// library-managed metadata stamping (`latestEventId`, `updatedAt`,
/// `firstEventTimestamp`, `sequence`), and derived-field computation.
///
/// The fold semantics are CONVENTIONS, not substrate facts. An
/// application processing the same events under different conventions
/// (e.g., first-write-wins, role-filtered, validation-gated) is doing
/// something equally valid; it just doesn't use this spec.
class AggregateProjectionSpec extends ProjectionSpec {
  @override
  final String viewName;
  @override
  final SubscriptionFilter interest;

  /// Event types that the AggregateProjectionSpec convention TREATS as
  /// row deletions. Not a substrate fact — the substrate could equally
  /// preserve the row with a deleted-flag, or hide-not-delete. This
  /// spec's default fold deletes the row when an event in this set is
  /// folded; applications that want different semantics for the same
  /// event types should compute their materialization outside this
  /// spec (or wait for a Layer-2 alternative primitive to ship).
  final Set<String> tombstoneEventTypes;

  final List<DerivedField> derivedFields;

  const AggregateProjectionSpec({
    required this.viewName,
    required this.interest,
    required this.tombstoneEventTypes,
    this.derivedFields = const [],
  });
}

/// A projection that maintains a flat lookup table — one row per
/// extracted key — under the library's default Layer-2 conventions:
/// `insertEventTypes` upsert a row, `removeEventTypes` delete a row.
/// The substrate could equally keep deleted rows as tombstones, or
/// merge inserts rather than overwriting; the present semantics are
/// conventions chosen for the common case.
class TableProjectionSpec extends ProjectionSpec {
  @override
  final String viewName;
  @override
  final SubscriptionFilter interest;
  final Set<String> insertEventTypes;
  final Set<String> removeEventTypes;
  final RowKeyExtractor rowKey;
  final RowDataExtractor rowData;

  const TableProjectionSpec({
    required this.viewName,
    required this.interest,
    required this.insertEventTypes,
    required this.removeEventTypes,
    required this.rowKey,
    required this.rowData,
  });
}
