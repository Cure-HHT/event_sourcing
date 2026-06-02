// example_clinical_scopes/lib/shared/clinical_types.dart
//
// Domain model shared by the client and the tests. The substrate is
// domain-neutral; the region/site/participant hierarchy lives entirely
// in this consumer package.

/// One materialised `participants` view row.
///
/// The view is an AggregateProjectionSpec, so the substrate's AggregateFold
/// stamps the aggregate id under the `'aggregateId'` key before the mapper
/// runs (a Layer-2 convention) — we read the participant id from there,
/// matching `reaction/example`'s `Note.fromRow`.
class Participant {
  const Participant({
    required this.id,
    required this.name,
    required this.siteId,
  });

  /// Participant aggregate id (equals `participant_id` in the payload).
  final String id;

  final String name;

  /// The site this participant belongs to (the one-hop containment parent).
  final String siteId;

  /// Maps a raw view-row map to a [Participant].
  static Participant fromRow(Map<String, Object?> row) => Participant(
    id: row['aggregateId']! as String,
    name: (row['name'] as String?) ?? '(unnamed)',
    siteId: (row['site_id'] as String?) ?? '(unknown)',
  );
}
