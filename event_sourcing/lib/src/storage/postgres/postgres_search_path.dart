// Implements: EVS-DEV-postgres-backend/Q
// every library transaction's first statement sets the search path, for
//   that transaction only, to exactly the named schema, pg_catalog and
//   pg_temp.
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// [schema] quoted as a Postgres identifier. Throws [ArgumentError] for an
/// empty name or one containing a NUL character, which no identifier holds.
@internal
String quotePostgresIdentifier(String schema) {
  if (schema.isEmpty || schema.contains('\u0000')) {
    throw ArgumentError.value(
      schema,
      'schema',
      'a Postgres schema name is non-empty and holds no NUL character',
    );
  }
  return '"${schema.replaceAll('"', '""')}"';
}

/// The statement that opens every library transaction: it sets the search
/// path, for the transaction only, to [schema], `pg_catalog` and `pg_temp`.
///
/// `SET LOCAL` is a utility statement, so it takes no snapshot: a
/// `SERIALIZABLE` transaction's snapshot is still taken by the first query
/// after it, so a `LOCK TABLE` that follows it still waits before the
/// snapshot is taken. The name `search_path` is a setting, resolved through
/// no search path, and [schema] reaches the statement quoted as an
/// identifier.
@internal
String postgresSearchPathStatement(String schema) =>
    'SET LOCAL search_path TO ${quotePostgresIdentifier(schema)}, '
    'pg_catalog, pg_temp';

/// Runs [body] in a transaction of [executor] whose first statement is
/// [postgresSearchPathStatement] for [schema]. Every statement the library
/// runs on Postgres runs through this, or through a transaction that opens
/// with the same statement.
@internal
Future<T> runLibraryTransaction<T>(
  SessionExecutor executor,
  String schema,
  Future<T> Function(TxSession tx) body, {
  TransactionSettings? settings,
}) {
  final pin = postgresSearchPathStatement(schema);
  return executor.runTx<T>((tx) async {
    await tx.execute(pin);
    return body(tx);
  }, settings: settings);
}

/// Runs [body] in a library transaction on [connection], one connection the
/// caller holds, issuing the transaction's control statements (`BEGIN`,
/// `COMMIT`, `ROLLBACK`) as ordinary statements, so the connection's query
/// timeout bounds each of them as it bounds every statement of [body]. With
/// [serializable] the transaction runs at `SERIALIZABLE`.
///
/// An error [body] throws rolls the transaction back and is rethrown; a
/// rollback that fails is reported to [onRollbackFailure], since the
/// connection is then in no known state, and the error [body] threw is
/// still the one rethrown.
@internal
Future<T> runLibraryTransactionOnConnection<T>(
  Connection connection,
  String schema,
  Future<T> Function(Session session) body, {
  bool serializable = false,
  void Function(Object error)? onRollbackFailure,
}) async {
  final pin = postgresSearchPathStatement(schema);
  await connection.execute(
    serializable ? 'BEGIN ISOLATION LEVEL SERIALIZABLE' : 'BEGIN',
  );
  try {
    await connection.execute(pin);
    final result = await body(connection);
    await connection.execute('COMMIT');
    return result;
  } catch (_) {
    try {
      await connection.execute('ROLLBACK');
    } on Object catch (rollbackError) {
      onRollbackFailure?.call(rollbackError);
    }
    rethrow;
  }
}
