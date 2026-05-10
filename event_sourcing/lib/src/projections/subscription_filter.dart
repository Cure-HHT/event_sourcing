// Re-exports SubscriptionFilter from its canonical home so that projection
// and subscribe code can import from 'projections/subscription_filter.dart'
// without duplicating the class definition. Destinations consumers that
// already import from 'destinations/subscription_filter.dart' are unaffected.
export 'package:event_sourcing/src/destinations/subscription_filter.dart'
    show SubscriptionFilter, SubscriptionPredicate;
