// Implements: EVS-DEV-storage-capability/A
// the Postgres description carries the library's schema and the
//   connection, statement-timeout, lock-session and wait settings the Postgres backend opens
//   with.
// Implements: EVS-PRD-storage-barrier/A
// the library opens a Postgres database it runs on from the description the
//   application supplies.
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/storage_description.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart' show SslMode;

/// A Postgres database the library opens with `PostgresBackend.open`: the
/// library's tables live in [schema], the pool connects to [url], and the
/// lock session to [lockUrl] (or [url]). Provisioning is a separate
/// deployment step run as the owner (`PostgresBackend.provision`).
final class PostgresStorage extends CompanionBackendStorage {
  const PostgresStorage({
    required this.url,
    required this.schema,
    this.lockUrl,
    this.sslMode = SslMode.require,
    this.queryTimeout = defaultPostgresQueryTimeout,
    this.lockQueryTimeout = const Duration(seconds: 5),
    this.lockHeartbeat = const Duration(seconds: 5),
    this.bootLockWait = const Duration(seconds: 60),
  });

  /// The pool's connection URL, as a runtime role the deployment declared.
  final String url;

  /// The schema that holds the library's tables.
  final String schema;

  /// The lock session's connection URL, as a lock role the deployment
  /// declared; the pool's URL when null.
  final String? lockUrl;

  /// The TLS mode of every connection.
  final SslMode sslMode;

  /// The timeout of every statement on the pool and of the wait for a pool
  /// connection, [defaultPostgresQueryTimeout] unless given; a statement,
  /// with every exchange the driver makes for it, ends within twice this, or
  /// its connection is closed and it fails with
  /// `PostgresStatementTimeoutException`. Positive, and longer than the
  /// longest lock wait the deployment expects (`PostgresBackend.open`).
  final Duration queryTimeout;

  /// Bounds every statement on the lock session and its connect.
  final Duration lockQueryTimeout;

  /// How often the idle lock session is probed.
  final Duration lockHeartbeat;

  /// Bounds each wait of `EventStore.open` for a boot lock.
  final Duration bootLockWait;

  @internal
  @override
  Future<(StorageBackend, MutableSecurityContextStore)> openBackend() async {
    final backend = await PostgresBackend.open(
      url: url,
      schema: schema,
      lockUrl: lockUrl,
      sslMode: sslMode,
      queryTimeout: queryTimeout,
      lockQueryTimeout: lockQueryTimeout,
      lockHeartbeat: lockHeartbeat,
      bootLockWait: bootLockWait,
    );
    return (backend, PostgresSecurityContextStore(backend: backend));
  }

  @override
  String toString() => 'PostgresStorage(schema: $schema)';
}
