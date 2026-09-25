import 'package:event_sourcing/event_sourcing.dart';

/// Takes the drain lock directly through the backend.
Future<DrainLock> acquireDirectly(StorageBackend backend) =>
    backend.tryAcquireDrainLock(databaseId: 'db');

/// Requests the drain lock directly through the backend.
DrainLockRequest requestDirectly(StorageBackend backend) =>
    backend.requestDrainLock(
      databaseId: 'db',
      retryInterval: const Duration(seconds: 1),
    );

/// Writes a refill guard directly through the backend.
Future<void> guardDirectly(StorageBackend backend) => backend.transaction(
  (txn) => backend.writeRefillGuardTxn(
    txn,
    'dest',
    const RefillGuard(fingerprint: 'f', recoveryEventId: 'e', refillThrough: 0),
  ),
);

/// Takes over the event store's delivery-cycle trigger slot.
void takeSlot(EventStore store) {
  store.deliveryTrigger = () async {};
}

/// Waits for a Postgres backend's lock session to be registered again.
Future<void> awaitRegistered(PostgresBackend backend) =>
    backend.whenRegistered();
