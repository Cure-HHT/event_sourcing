// Re-exports SubscriptionFilter from its canonical home so that projection
// and subscribe code can import from 'projections/subscription_filter.dart'
// without duplicating the class definition. Destinations consumers that
// already import from 'destinations/subscription_filter.dart' are unaffected.
//
// Implements: EVS-PRD-materializer/A (structural) — SubscriptionFilter is the
//   mechanism by which a ProjectionSpec declares which events it materializes;
//   it is a required part of every spec and therefore part of the materializer
//   rule-set surface.
export 'package:event_sourcing/src/destinations/subscription_filter.dart'
    show SubscriptionFilter, SubscriptionPredicate;
