import 'package:event_sourcing/postgres.dart';

/// Reaches the backend's raw connection pool.
Object rawPool(PostgresBackend backend) => backend.pool;
