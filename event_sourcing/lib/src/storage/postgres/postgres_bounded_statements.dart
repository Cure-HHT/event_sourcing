// Implements: EVS-DEV-postgres-backend/J
// every statement on the lock session, with every exchange the driver makes
//   for it, is bounded by the query timeout plus the connect timeout.
// Implements: EVS-DEV-postgres-backend/U+V+W
// every library statement on a pool connection, with every exchange the
//   driver makes for it, ends within the statement timeout plus the connect
//   timeout; past that bound the connection is closed, so the pool does not
//   hand it out again, and the statement fails with
//   PostgresStatementTimeoutException, which the library classifies as
//   transient.
import 'dart:async';

import 'package:event_sourcing/src/storage/postgres/postgres_exceptions.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

// The driver (`postgres` 3.5) bounds a statement's result by the session's
// query timeout, its transaction's COMMIT and ROLLBACK by the query timeout
// plus the connect timeout, and a connect by the connect timeout. It waits
// without any timeout for the reply to the portal close it sends after a
// statement inside a transaction has returned its result, and for the reply
// to the BEGIN that `runTx` sends. Neither the query timeout nor the connect
// timeout reaches those waits, so a connection that stops answering in
// either window (a network black hole: the peer is silent, the socket is not
// reset) holds the caller until the operating system abandons the socket, or
// forever. The wrappers below exist to bound those exchanges, and only those:
// a statement is bounded as a whole, so its portal close is inside the bound,
// and a transaction's BEGIN is bounded on its own; COMMIT, ROLLBACK and the
// connect are left to the driver. They can be removed once the driver bounds
// the portal close and the BEGIN itself.

/// The bound on [timeout] (or [queryTimeout] when null) and every exchange
/// the driver makes for one statement: the limit the driver itself puts on
/// a statement's result, the statement's timeout plus the connect timeout.
Duration _statementBound(
  Duration? timeout,
  Duration queryTimeout,
  Duration connectTimeout,
) => (timeout ?? queryTimeout) + connectTimeout;

/// Runs [op] on [connection], closing [connection] by force once [bound]
/// passes, which ends whatever exchange is waiting. A run past [bound], or a
/// timeout the driver raises for the statement, throws
/// [PostgresStatementTimeoutException] naming [where]; any other error [op]
/// throws is rethrown.
Future<T> _runBounded<T>(
  Connection connection,
  Duration bound,
  String where,
  Future<T> Function() op,
) async {
  var expired = false;
  final timer = Timer(bound, () {
    expired = true;
    unawaited(connection.close(force: true));
  });
  T? result;
  Object? error;
  StackTrace? stackTrace;
  try {
    result = await op();
  } on Object catch (e, st) {
    error = e;
    stackTrace = st;
  } finally {
    timer.cancel();
  }
  if (expired || error is TimeoutException) {
    if (error is PostgresStatementTimeoutException) {
      Error.throwWithStackTrace(error, stackTrace!);
    }
    throw PostgresStatementTimeoutException(where: where, bound: bound);
  }
  if (error != null) Error.throwWithStackTrace(error, stackTrace!);
  return result as T;
}

/// [_session], a session of [_connection] (or [_connection] itself), with
/// every statement bounded as a whole: its timeout (the session's query
/// timeout when the caller gives none) plus the connect timeout. Once the
/// bound passes [_connection] is closed by force and the statement throws
/// [PostgresStatementTimeoutException]. It runs no prepared statement: a
/// prepared statement's runs and its disposal would escape the bound.
@internal
class BoundedStatements implements Session {
  /// Bounds the statements of [_session], closing [_connection] on expiry.
  /// [where] names the connection in the timeout's message.
  BoundedStatements(
    this._session,
    this._connection, {
    required this.queryTimeout,
    required this.connectTimeout,
    required this.where,
  });

  final Session _session;
  final Connection _connection;

  /// The timeout of a statement that gives none.
  final Duration queryTimeout;

  /// Added to a statement's timeout to give its bound.
  final Duration connectTimeout;

  /// Names the connection in a timeout's message.
  final String where;

  @override
  bool get isOpen => _session.isOpen;

  @override
  Future<void> get closed => _session.closed;

  @override
  Future<Statement> prepare(Object query) => Future<Statement>.error(
    UnsupportedError('the library runs no prepared statement on $where'),
  );

  @override
  Future<Result> execute(
    Object query, {
    Object? parameters,
    bool ignoreRows = false,
    QueryMode? queryMode,
    Duration? timeout,
  }) => _runBounded(
    _connection,
    _statementBound(timeout, queryTimeout, connectTimeout),
    where,
    () => _session.execute(
      query,
      parameters: parameters,
      ignoreRows: ignoreRows,
      queryMode: queryMode,
      timeout: timeout,
    ),
  );
}

/// A transaction of a pool connection with every statement bounded as
/// [BoundedStatements] bounds it. Its rollback is the driver's, which the
/// driver bounds.
final class _BoundedTxStatements extends BoundedStatements
    implements TxSession {
  _BoundedTxStatements(
    TxSession super._session,
    super._connection, {
    required super.queryTimeout,
    required super.connectTimeout,
    required super.where,
  });

  @override
  Future<void> rollback() => (_session as TxSession).rollback();
}

/// A pool connection whose statements, and the statements of whatever
/// session or transaction it opens, are bounded as [BoundedStatements]
/// bounds them. A transaction's BEGIN is bounded too, by the query timeout
/// plus the connect timeout.
final class _BoundedPoolConnection extends BoundedStatements
    implements Connection {
  _BoundedPoolConnection(
    Connection connection, {
    required super.queryTimeout,
    required super.connectTimeout,
  }) : super(connection, connection, where: 'a pool connection');

  @override
  ConnectionInfo get info => _connection.info;

  @override
  Channels get channels => throw UnsupportedError(
    'the library listens on no channel of a pool connection',
  );

  @override
  Future<void> close({bool force = false}) => _connection.close(force: force);

  @internal
  @override
  Future<R> run<R>(
    Future<R> Function(Session session) fn, {
    SessionSettings? settings,
  }) => _connection.run(
    (session) => fn(
      BoundedStatements(
        session,
        _connection,
        queryTimeout: settings?.queryTimeout ?? queryTimeout,
        connectTimeout: connectTimeout,
        where: where,
      ),
    ),
    settings: settings,
  );

  @internal
  @override
  Future<R> runTx<R>(
    Future<R> Function(TxSession session) fn, {
    TransactionSettings? settings,
  }) async {
    // The driver sends the transaction's BEGIN before it calls [fn] and
    // waits for the reply without a timeout, so the wait for [fn] to start
    // is bounded here.
    final bound = _statementBound(
      null,
      settings?.queryTimeout ?? queryTimeout,
      connectTimeout,
    );
    var begun = false;
    var expired = false;
    final timer = Timer(bound, () {
      if (begun) return;
      expired = true;
      unawaited(_connection.close(force: true));
    });
    try {
      return await _connection.runTx((tx) {
        begun = true;
        timer.cancel();
        if (expired) {
          throw PostgresStatementTimeoutException(where: where, bound: bound);
        }
        return fn(
          _BoundedTxStatements(
            tx,
            _connection,
            queryTimeout: settings?.queryTimeout ?? queryTimeout,
            connectTimeout: connectTimeout,
            where: where,
          ),
        );
      }, settings: settings);
    } on Object {
      if (expired && !begun) {
        throw PostgresStatementTimeoutException(where: where, bound: bound);
      }
      rethrow;
    } finally {
      timer.cancel();
    }
  }
}

/// [_pool] with every statement on its connections bounded as
/// [BoundedStatements] bounds it, through whichever of its entry points
/// reaches the connection. A connection whose bound passed is closed, so
/// the pool disposes of it instead of handing it out again. It runs no
/// prepared statement.
@internal
final class BoundedPool implements Pool<void> {
  /// Bounds [_pool]'s statements by [queryTimeout] plus [connectTimeout].
  BoundedPool(
    this._pool, {
    required this.queryTimeout,
    required this.connectTimeout,
  });

  final Pool<void> _pool;

  /// The timeout of a statement that gives none.
  final Duration queryTimeout;

  /// Added to a statement's timeout to give its bound.
  final Duration connectTimeout;

  @override
  bool get isOpen => _pool.isOpen;

  @override
  Future<void> get closed => _pool.closed;

  @override
  Future<void> close({bool force = false}) => _pool.close(force: force);

  @override
  Future<Statement> prepare(Object query) => Future<Statement>.error(
    UnsupportedError('the library runs no prepared statement on its pool'),
  );

  @internal
  @override
  Future<R> withConnection<R>(
    Future<R> Function(Connection connection) fn, {
    ConnectionSettings? settings,
    void locality,
  }) => _pool.withConnection(
    (connection) => fn(
      _BoundedPoolConnection(
        connection,
        queryTimeout: settings?.queryTimeout ?? queryTimeout,
        connectTimeout: settings?.connectTimeout ?? connectTimeout,
      ),
    ),
    settings: settings,
  );

  @override
  Future<Result> execute(
    Object query, {
    Object? parameters,
    bool ignoreRows = false,
    QueryMode? queryMode,
    Duration? timeout,
  }) => withConnection(
    (connection) => connection.execute(
      query,
      parameters: parameters,
      ignoreRows: ignoreRows,
      queryMode: queryMode,
      timeout: timeout,
    ),
  );

  @internal
  @override
  Future<R> run<R>(
    Future<R> Function(Session session) fn, {
    SessionSettings? settings,
    void locality,
  }) => withConnection((connection) => connection.run(fn, settings: settings));

  @internal
  @override
  Future<R> runTx<R>(
    Future<R> Function(TxSession session) fn, {
    TransactionSettings? settings,
    void locality,
  }) =>
      withConnection((connection) => connection.runTx(fn, settings: settings));
}
