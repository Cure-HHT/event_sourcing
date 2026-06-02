// example_clinical_scopes/lib/server/clinical_projections.dart
//
// Declarative projections for the clinical scopes demo.
//
//   - `participants`            — one row per participant aggregate (the
//                                 subscribed, scope-narrowed view).
//   - `participant_site_index`  — containment index participant -> site,
//                                 walked downward by the production
//                                 ScopeDescendantExpander to narrow a
//                                 site-scoped subscription.
//   - `site_region_index`       — containment index site -> region, the
//                                 second hop for a region-scoped Overseer.

import 'package:event_sourcing/event_sourcing.dart';

/// One row per participant aggregate. Rows carry `participant_id`,
/// `name`, and `site_id` from the `participant_registered` payload (plus
/// the substrate-stamped `aggregateId`). Never tombstoned in the demo.
const AggregateProjectionSpec participantsProjection = AggregateProjectionSpec(
  viewName: 'participants',
  interest: SubscriptionFilter(aggregateTypes: {'participant'}),
  tombstoneEventTypes: <String>{},
);

/// Containment index: participant_id -> site_id. `participant_id` MUST
/// equal the participant aggregate id so the `participants` row key and
/// this index keyColumn line up.
const TableProjectionSpec participantSiteIndex = TableProjectionSpec(
  viewName: 'participant_site_index',
  interest: SubscriptionFilter(aggregateTypes: {'participant'}),
  insertEventTypes: {'participant_registered'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// Containment index: site_id -> region_id. `site_id` MUST equal the site
/// aggregate id. Provides the second containment hop region -> site that
/// an Overseer's region-scoped assignment expands through.
const TableProjectionSpec siteRegionIndex = TableProjectionSpec(
  viewName: 'site_region_index',
  interest: SubscriptionFilter(aggregateTypes: {'site'}),
  insertEventTypes: {'site_registered'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);
