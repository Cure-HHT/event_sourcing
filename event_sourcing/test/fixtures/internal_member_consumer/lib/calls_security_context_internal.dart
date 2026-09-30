import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';

/// Deletes an event's security context outside the event store's
/// operations, through the abstract writing store and a concrete one.
Future<void> eraseContext(
  SembastBackend backend,
  MutableSecurityContextStore writer,
  SembastSecurityContextStore contexts,
) => backend.transaction((txn) async {
  await writer.deleteInTxn(txn, 'event-id');
  await contexts.deleteInTxn(txn, 'event-id');
});
