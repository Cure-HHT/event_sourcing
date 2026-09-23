// Implements: EVS-DEV-postgres-backend/J
// the dedicated lock session: verified at open to be one server session
//   reaching the pool's database and schema, configured with keepalives, no
//   idle-session timeout and bounded connect and query timeouts, and used by
//   one library operation at a time.
import 'dart:async';
import 'dart:convert';
import 'dart:math' show Random;

import 'package:crypto/crypto.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_exceptions.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// The scope of the library's advisory locks on a Postgres server: one
/// database and one schema. Two library databases in two schemas of one
/// Postgres database never see each other's locks.
@internal
final class PostgresScope {
  const PostgresScope({required this.database, required this.schema});

  /// `current_database()`.
  final String database;

  /// `current_schema()`, or null when no schema on the search path exists.
  final String? schema;

  @override
  bool operator ==(Object other) =>
      other is PostgresScope &&
      other.database == database &&
      other.schema == schema;

  @override
  int get hashCode => Object.hash(database, schema);

  @override
  String toString() => '$database/${schema ?? '(no schema)'}';

  /// Reads the scope [session] reaches.
  static Future<PostgresScope> read(Session session) async {
    final r = await session.execute(
      'SELECT current_database(), current_schema()',
    );
    return PostgresScope(
      database: r.first[0]! as String,
      schema: r.first[1] as String?,
    );
  }
}

/// The advisory-lock key of [component] under [prefix] in [scope]: the
/// first eight bytes of a SHA-256 over the three, read as a signed 64-bit
/// integer. Postgres splits it into `pg_locks.classid` (high 32 bits) and
/// `objid` (low 32 bits).
@internal
int postgresAdvisoryKey(
  String prefix,
  PostgresScope scope, [
  String component = '',
]) {
  final digest = sha256.convert(
    utf8.encode(
      jsonEncode(<Object?>[prefix, scope.database, scope.schema, component]),
    ),
  );
  var key = 0;
  for (var i = 0; i < 8; i++) {
    key = (key << 8) | digest.bytes[i];
  }
  return key;
}

/// SQL giving every granted advisory lock of the current database as one
/// signed 64-bit key, the form [postgresAdvisoryKey] returns.
@internal
const String heldAdvisoryKeysSql = '''
SELECT (l.classid::bigint << 32) | l.objid::bigint
FROM pg_locks l
WHERE l.locktype = 'advisory'
  AND l.objsubid = 1
  AND l.granted
  AND l.database = (SELECT oid FROM pg_database
                    WHERE datname = current_database())
''';

/// Verifies that [session] reaches the same Postgres server as [pool]. A
/// connection of [pool] takes a transaction-level advisory lock on a random
/// key, and the lock session must see that lock, held by that connection's
/// server process, in its own `pg_locks`. Another server -- another
/// instance whose database and schema have the same names, or a standby --
/// does not list it, so its advisory locks would be invisible to the
/// instances that use the pool's server. A mismatch throws
/// [LockSessionConfigurationException].
@internal
Future<void> verifyLockSessionServer(
  SessionExecutor pool,
  PostgresLockSession session,
) async {
  final random = Random.secure();
  var key = 0;
  for (var i = 0; i < 8; i++) {
    key = (key << 8) | random.nextInt(256);
  }
  await pool.runTx<void>((tx) async {
    await tx.execute(
      Sql.named('SELECT pg_advisory_xact_lock(@k)'),
      parameters: <String, Object?>{'k': key},
    );
    final pid = (await tx.execute('SELECT pg_backend_pid()')).first[0]! as int;
    final holders = await session.run(
      (c) => c.execute(
        Sql.named('''
          SELECT l.pid FROM pg_locks l
          WHERE l.locktype = 'advisory'
            AND l.objsubid = 1
            AND l.granted
            AND ((l.classid::bigint << 32) | l.objid::bigint) = @k
            AND l.database = (SELECT oid FROM pg_database
                              WHERE datname = current_database())
        '''),
        parameters: <String, Object?>{'k': key},
      ),
    );
    if (!holders.any((row) => row[0] == pid)) {
      throw LockSessionConfigurationException(
        'the lock connection reaches another Postgres server than the pool: '
        "a lock the pool's server session $pid holds is not visible to the "
        'lock session (pid ${session.identity.pid})',
      );
    }
  });
}

/// The identity of one server session: its process id and start time. A
/// later session that reuses the process id has another start time.
@internal
typedef PostgresSessionIdentity = ({int pid, DateTime backendStart});

/// The seconds of idle time before the server sends its first keepalive
/// probe on the lock session.
@internal
const int lockSessionKeepaliveIdleSeconds = 10;

/// The seconds between the server's keepalive probes on the lock session.
@internal
const int lockSessionKeepaliveIntervalSeconds = 5;

/// The unanswered keepalive probes after which the server ends the lock
/// session.
@internal
const int lockSessionKeepaliveCount = 3;

/// One dedicated Postgres connection, the lock session, on which the
/// library holds its advisory locks.
///
/// Every library statement or transaction on it goes through [run], which
/// runs one operation at a time: the driver starts a statement's timeout
/// clock before the statement reaches the connection, and a timed-out
/// statement's cancel request cancels whatever the session runs, so no
/// library statement waits on the connection behind another. A connection
/// failure or a timeout of any operation declares the session lost.
@internal
final class PostgresLockSession {
  PostgresLockSession._(
    this._connection, {
    required this.scope,
    required this.identity,
    required this.connectTimeout,
  });

  final Connection _connection;

  /// The database and schema the session reaches.
  final PostgresScope scope;

  /// The server session's process id and start time.
  final PostgresSessionIdentity identity;

  /// Bounds the connection's close.
  final Duration connectTimeout;

  Future<void> _tail = Future<void>.value();
  bool _busy = false;
  bool _lost = false;
  final Completer<void> _lostCompleter = Completer<void>();
  void Function(PostgresLockSession session, Object error)? _onLost;

  /// True while an operation runs.
  bool get isBusy => _busy;

  /// True once the session was declared lost.
  bool get isLost => _lost;

  /// Completes when the session is declared lost.
  Future<void> get lost => _lostCompleter.future;

  /// Called once when the session is declared lost.
  set onLost(void Function(PostgresLockSession session, Object error) f) =>
      _onLost = f;

  /// Opens the lock session to [endpoint] and checks it.
  ///
  /// The check sets a random session setting and reads it back, with the
  /// server process id, in three separate statements; every one must show
  /// the same process id and the setting, or the connection is not one
  /// server session. It then compares the database and schema the session
  /// reaches with [expectedScope]. Any mismatch closes the connection and
  /// throws [LockSessionConfigurationException].
  static Future<PostgresLockSession> open({
    required Endpoint endpoint,
    required SslMode sslMode,
    required Duration queryTimeout,
    required PostgresScope expectedScope,
  }) async {
    final hooks = DeliveryTestHooks.current;
    final settings = ConnectionSettings(
      sslMode: sslMode,
      connectTimeout: queryTimeout,
      queryTimeout: queryTimeout,
      applicationName: 'event_sourcing.lock_session',
    );
    final connection = await Connection.open(endpoint, settings: settings);
    try {
      await connection.execute(
        'SET tcp_keepalives_idle = $lockSessionKeepaliveIdleSeconds',
      );
      await connection.execute(
        'SET tcp_keepalives_interval = $lockSessionKeepaliveIntervalSeconds',
      );
      await connection.execute(
        'SET tcp_keepalives_count = $lockSessionKeepaliveCount',
      );
      await connection.execute('SET idle_session_timeout = 0');
      await connection.execute('SET idle_in_transaction_session_timeout = 0');

      final token = _randomToken();
      await connection.execute(
        Sql.named("SELECT set_config('event_sourcing.lock_token', @t, false)"),
        parameters: <String, Object?>{'t': token},
      );
      Connection? second;
      if (hooks?.splitLockSessionStatements ?? false) {
        second = await Connection.open(endpoint, settings: settings);
      }
      final observed = <(int, String?)>[];
      try {
        for (var i = 0; i < 3; i++) {
          final via = (second != null && i.isOdd) ? second : connection;
          final r = await via.execute(
            'SELECT pg_backend_pid(), '
            "current_setting('event_sourcing.lock_token', true)",
          );
          observed.add((r.first[0]! as int, r.first[1] as String?));
        }
      } finally {
        await second?.close();
      }
      final pid = observed.first.$1;
      if (observed.any((o) => o.$1 != pid || o.$2 != token)) {
        throw LockSessionConfigurationException(
          'three statements on the lock connection reached server sessions '
          '${observed.map((o) => o.$1).join(', ')}, not one session carrying '
          'the setting the first statement made',
        );
      }
      final scope = await PostgresScope.read(connection);
      if (scope != expectedScope) {
        throw LockSessionConfigurationException(
          'the lock connection reaches $scope, but the pool reaches '
          '$expectedScope',
        );
      }
      final started = await connection.execute(
        'SELECT backend_start FROM pg_stat_activity '
        'WHERE pid = pg_backend_pid()',
      );
      final backendStart = (started.first[0]! as DateTime).toUtc();
      return PostgresLockSession._(
        connection,
        scope: scope,
        identity: (pid: pid, backendStart: backendStart),
        connectTimeout: queryTimeout,
      );
    } catch (_) {
      await _closeQuietly(connection, queryTimeout);
      rethrow;
    }
  }

  /// Runs [op] on the session once every earlier operation has finished.
  ///
  /// A connection failure or a timeout declares the session lost and is
  /// rethrown; a statement the server refused (a SQL error) is rethrown
  /// without declaring it.
  Future<T> run<T>(Future<T> Function(Connection connection) op) {
    final result = _tail.then((_) async {
      if (_lost) {
        throw PgException('the lock session was declared lost');
      }
      _busy = true;
      try {
        return await op(_connection);
      } catch (e) {
        if (isConnectionFailure(e)) declareLost(e);
        rethrow;
      } finally {
        _busy = false;
      }
    });
    _tail = result.then<void>((_) {}, onError: (Object _) {});
    return result;
  }

  /// Declares the session lost: completes [lost] and calls the loss
  /// callback, once.
  void declareLost(Object error) {
    if (_lost) return;
    _lost = true;
    _lostCompleter.complete();
    _onLost?.call(this, error);
  }

  /// Closes the connection, bounded by [connectTimeout]; a failure to close
  /// is logged and not relied on.
  Future<void> close() => _closeQuietly(_connection, connectTimeout);

  /// The connection, to set aside unclosed when a close is treated as
  /// failed.
  @internal
  Connection get connection => _connection;

  /// True when [error] means the session may be gone or out of step: a
  /// timeout, a cancelled statement, a closed or broken connection (a
  /// driver error that carries no server error), or a server error of the
  /// connection-exception or operator-intervention classes. Any other
  /// server error is a refused statement, and an error the library raises
  /// itself (a refusal, an injected failure) says nothing about the session.
  static bool isConnectionFailure(Object error) {
    if (error is TimeoutException) return true;
    if (error is ServerException) {
      final code = error.code ?? '';
      return code.startsWith('08') || code.startsWith('57');
    }
    return error is PgException;
  }

  static Future<void> _closeQuietly(
    Connection connection,
    Duration timeout,
  ) async {
    try {
      await connection.close().timeout(timeout);
    } on Object catch (e) {
      libraryLog(
        'postgres_lock_session',
        'closing a lock connection failed; not relied on',
        level: LibraryLogLevel.warning,
        error: e,
      );
    }
  }

  static String _randomToken() {
    final random = Random.secure();
    return List<String>.generate(
      16,
      (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
    ).join();
  }
}
