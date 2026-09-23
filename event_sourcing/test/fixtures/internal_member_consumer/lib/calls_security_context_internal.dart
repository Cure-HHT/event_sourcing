import 'package:event_sourcing/event_sourcing.dart';

/// Deletes and rewrites an event's security context outside the event
/// store's operations.
Future<void> eraseContext(
  EventStore store,
  SembastSecurityContextStore contexts,
) => store.backend.transaction((txn) async {
  await store.securityContexts.deleteInTxn(txn, 'event-id');
  await contexts.deleteInTxn(txn, 'event-id');
});
