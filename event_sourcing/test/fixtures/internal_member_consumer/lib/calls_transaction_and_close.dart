import 'package:event_sourcing/event_sourcing.dart';

/// Runs a transaction body, reads the queues, and closes the backend: all
/// stay public.
Future<void> transactThenClose(SembastBackend backend) async {
  await backend.transaction((txn) async => backend.readLatestEventHash(txn));
  await backend.readFifoHead('dest');
  await backend.listFifoEntries('dest');
  await backend.readFifoRow('dest', 'entry');
  await backend.hasFifoWedged();
  await backend.wedgedFifos();
  await backend.close();
}

/// The same reads on the Postgres backend.
Future<void> readPostgresQueues(PostgresBackend backend) async {
  await backend.readFifoHead('dest');
  await backend.listFifoEntries('dest');
  await backend.readFifoRow('dest', 'entry');
  await backend.hasFifoWedged();
  await backend.wedgedFifos();
  await backend.close();
}
