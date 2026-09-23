import 'package:event_sourcing/event_sourcing.dart';

/// Reaches the backend's raw connection pool.
Object rawPool(PostgresBackend backend) => backend.pool;
