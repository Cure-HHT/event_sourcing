// Implements: EVS-PRD-portability/D — public surface for the Postgres
//   concrete StorageBackend. Consumers import the library barrel and
//   pick `PostgresBackend.open` instead of `SembastBackend.new`.

export 'postgres_backend.dart';
export 'postgres_schema.dart' show postgresBackendSchemaVersion;
