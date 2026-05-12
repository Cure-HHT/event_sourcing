import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Read-side contract for the security-context sidecar. Mutations are
/// package-private via `InternalSecurityContextStore` — only `EventStore`
/// writes, updates, or deletes rows so each mutation commits atomically
/// with the event-log row that describes it.
abstract class SecurityContextStore {
  Future<EventSecurityContext?> read(String eventId);

  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  });
}

/// Store-internal mutation contract used by `EventStore` to commit
/// security rows inside the same transaction as the event they describe.
/// Not exported at the library surface — application code must go
/// through `EventStore.append` / `EventStore.clearSecurityContext` /
/// `EventStore.applyRetentionPolicy`.
abstract class InternalSecurityContextStore extends SecurityContextStore {
  Future<void> writeInTxn(Txn txn, EventSecurityContext row);
  Future<EventSecurityContext?> readInTxn(Txn txn, String eventId);
  Future<void> deleteInTxn(Txn txn, String eventId);
  Future<void> upsertInTxn(Txn txn, EventSecurityContext row);
  Future<List<EventSecurityContext>> findUnredactedOlderThanInTxn(
    Txn txn,
    DateTime cutoff,
  );
  Future<List<EventSecurityContext>> findOlderThanInTxn(
    Txn txn,
    DateTime cutoff,
  );
}

/// One page of audit rows returned by `SecurityContextStore.queryAudit`.
class PagedAudit {
  const PagedAudit({required this.rows, this.nextCursor});
  final List<AuditRow> rows;
  final String? nextCursor;
}

/// One audit row: the event and its security context pair.
class AuditRow {
  const AuditRow({required this.event, required this.context});
  final StoredEvent event;
  final EventSecurityContext context;
}
