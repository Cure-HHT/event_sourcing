import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';

/// Reaches the raw engine transaction inside a backend transaction.
Future<void> rawSession(PostgresBackend backend) =>
    backend.transaction((txn) async {
      (txn as PostgresTxn).session;
    });
