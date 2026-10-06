// Implements: EVS-PRD-portability/D
// public surface of the Postgres concrete StorageBackend, one of the
//   backends whose storage the library opens itself.
// Implements: EVS-PRD-portability/B
// the Postgres backend is a public library of its own, so a build that
//   imports only package:event_sourcing/event_sourcing.dart, a web build
//   included, never compiles the Postgres driver.

/// The Postgres backend of the event-sourcing library.
///
/// Import it beside `package:event_sourcing/event_sourcing.dart` to run an
/// event store on Postgres: open one with `EventStore.open` over a
/// `PostgresStorage` description, provision a database with
/// `PostgresBackend.provision`, or construct a `PostgresBackend` and name it
/// as `ApplicationSuppliedStorage`. It is the only library of the package
/// that reaches the Postgres driver.
library;

export 'package:postgres/postgres.dart' show SslMode;

export 'src/storage/postgres/postgres_backend.dart'
    show
        PostgresBackend,
        PostgresBackendClosedException,
        PostgresIdempotencyStore,
        PostgresSecurityContextStore,
        TransactionRetryExhaustedException,
        defaultPostgresQueryTimeout;
export 'src/storage/postgres/postgres_exceptions.dart';
export 'src/storage/postgres/postgres_grants.dart'
    show postgresRuntimeRoleGrants;
export 'src/storage/postgres/postgres_schema.dart'
    show postgresMinCompatibleSchemaVersion, postgresSchemaVersion;
export 'src/storage/postgres/postgres_storage.dart' show PostgresStorage;
