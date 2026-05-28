// reaction/example/lib/server/notes_projection.dart
//
// Declarative AggregateProjectionSpec for the `notes_today` view.
// One row per note aggregate; the row map carries the event payload
// keys directly (`title`, plus the substrate-provided `aggregate_id`).

import 'package:event_sourcing/event_sourcing.dart';

const AggregateProjectionSpec notesTodayProjection = AggregateProjectionSpec(
  viewName: 'notes_today',
  interest: SubscriptionFilter(aggregateTypes: {'note'}),
  // No tombstone event type for the demo — notes are never deleted.
  tombstoneEventTypes: <String>{},
);
