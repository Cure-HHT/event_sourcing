// Implements: EVS-DEV-destination-drain-lock/A+B+C
// Postgres: a session advisory lock on the backend's verified lock session,
//   keyed by the database, the schema and the database identity; every
//   acquisition raises the drain epoch in a transaction on the lock session
//   that confirms the key and the identity, then verifies through the pool
//   that the lock session holds the key; a step that fails after the key
//   was taken gives the key up before the failure surfaces; the check a
//   queue-changing transaction runs reads the epoch under a share lock.
// Implements: EVS-DEV-postgres-backend/J
// the drain lock lives on the lock session, and its heartbeat is the
//   session's probe, run through the session's one-operation queue.
import 'dart:async';

import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_generation_guard.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_lock_session.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// Prefix of the drain-lock key.
const String _drainPrefix = 'event_sourcing.drainer';

/// `backend_state` key of the drain epoch.
@internal
const String drainEpochKey = 'drain_epoch';

/// The drain-lock key of the library database whose identity is
/// [databaseId] in [scope].
@internal
int postgresDrainKey(PostgresScope scope, String databaseId) =>
    postgresAdvisoryKey(_drainPrefix, scope, databaseId);

/// SQL counting the grants of advisory key `@k` in the current database to
/// the server process `@pid`.
const String _keyHeldByPidSql = '''
SELECT count(*) FROM pg_locks l
WHERE l.locktype = 'advisory'
  AND l.objsubid = 1
  AND l.granted
  AND ((l.classid::bigint << 32) | l.objid::bigint) = @k
  AND l.pid = @pid
  AND l.database = (SELECT oid FROM pg_database
                    WHERE datname = current_database())
''';

/// SQL counting the grants of advisory key `@k` in the current database to
/// the session that runs it.
const String _keyHeldHereSql = '''
SELECT count(*) FROM pg_locks l
WHERE l.locktype = 'advisory'
  AND l.objsubid = 1
  AND l.granted
  AND ((l.classid::bigint << 32) | l.objid::bigint) = @k
  AND l.pid = pg_backend_pid()
  AND l.database = (SELECT oid FROM pg_database
                    WHERE datname = current_database())
''';

/// Acquires the drain lock of the database whose identity is [databaseId]
/// on [guard]'s lock session. [runTransaction] runs a transaction of the
/// backend, for [PostgresDrainLock.assertHeld].
@internal
Future<PostgresDrainLock> acquirePostgresDrainLock({
  required PostgresGenerationGuard guard,
  required SessionExecutor pool,
  required String databaseId,
  required Future<void> Function(Future<void> Function(Transaction txn) body)
  runTransaction,
}) async {
  final hooks = DeliveryTestHooks.current;
  if (hooks?.failLockAcquisition?.call() ?? false) {
    throw const InjectedFailure('the drain lock acquisition');
  }
  if (guard.status == GenerationStatus.fenced) {
    throw const GenerationFencedException(
      'the backend is fenced; it acquires no drain lock',
    );
  }
  final session = guard.currentSession;
  if (session.isLost) {
    throw PgException(
      'the lock session is lost and not yet replaced; the drain lock is '
      'requested again once it is',
    );
  }
  final key = postgresDrainKey(guard.scope, databaseId);
  final pid = session.identity.pid;
  var obtained = false;
  try {
    await session.run((c) async {
      if (hooks?.holdDrainKeyOutsideLibrary?.call() ?? false) {
        await c.execute(
          Sql.named('SELECT pg_advisory_lock(@k)'),
          parameters: <String, Object?>{'k': key},
        );
      }
      final own = await c.execute(
        Sql.named(_keyHeldByPidSql),
        parameters: <String, Object?>{'k': key, 'pid': pid},
      );
      if ((own.first[0]! as int) > 0) {
        throw const DrainLockConfigurationException(
          'the lock session already holds the drain key, and no drain lock '
          'of this backend accounts for it: something other than the library '
          'took it on this session',
        );
      }
      final got = await c.execute(
        Sql.named('SELECT pg_try_advisory_lock(@k)'),
        parameters: <String, Object?>{'k': key},
      );
      if (got.first[0] != true) {
        throw const DrainLockUnavailableException(
          'another session holds the drain lock of this database',
        );
      }
      obtained = true;
    });
    await hooks?.afterLockAcquireBeforeEpochBump?.call();
    final epoch = await session.run(
      (c) => c.runTx<int>(
        (tx) async {
          final held = await tx.execute(
            Sql.named(_keyHeldHereSql),
            parameters: <String, Object?>{'k': key},
          );
          if ((held.first[0]! as int) != 1) {
            throw const DrainLockConfigurationException(
              'the lock session does not hold the drain key it took',
            );
          }
          final stored = await tx.execute(
            Sql.named('SELECT value FROM backend_state WHERE key = @k'),
            parameters: <String, Object?>{'k': 'database_id'},
          );
          final storedId = stored.isEmpty ? null : stored.first[0];
          if (storedId != databaseId) {
            throw DrainLockConfigurationException(
              'the database the lock session reaches has identity $storedId, '
              'not $databaseId',
            );
          }
          final bumped = await tx.execute(
            Sql.named('''
              INSERT INTO backend_state (key, value)
              VALUES (@k, to_jsonb(1))
              ON CONFLICT (key) DO UPDATE
                SET value = to_jsonb((backend_state.value #>> '{}')::bigint + 1)
              RETURNING (value #>> '{}')::bigint
            '''),
            parameters: <String, Object?>{'k': drainEpochKey},
          );
          if (hooks?.failEpochBumpWithSerializationFailure?.call() ?? false) {
            await tx.execute(
              r"DO $$ BEGIN RAISE EXCEPTION 'injected serialization failure' "
              r"USING ERRCODE = '40001'; END $$",
            );
          }
          if (hooks?.stallEpochBumpPastQueryTimeout?.call() ?? false) {
            final seconds =
                (guard.lockQueryTimeout + const Duration(seconds: 1))
                    .inMilliseconds /
                1000;
            await tx.execute('SELECT pg_sleep($seconds)');
          }
          await hooks?.insideEpochBumpBeforeCommit?.call();
          return bumped.first[0]! as int;
        },
        settings: TransactionSettings(
          isolationLevel: IsolationLevel.serializable,
        ),
      ),
    );
    final bool verified;
    if (hooks?.failDrainLockVerification?.call() ?? false) {
      verified = false;
    } else {
      final seen = await pool.run(
        (s) => s.execute(
          Sql.named(_keyHeldByPidSql),
          parameters: <String, Object?>{'k': key, 'pid': pid},
        ),
      );
      verified = (seen.first[0]! as int) == 1;
    }
    if (!verified) {
      throw DrainLockConfigurationException(
        'the pool does not see the drain key held by the lock session (pid '
        '$pid): the lock session and the pool do not reach one server',
      );
    }
    return PostgresDrainLock._(
      guard: guard,
      session: session,
      key: key,
      epoch: epoch,
      runTransaction: runTransaction,
    );
  } catch (_) {
    if (obtained) await _giveUp(session, key);
    rethrow;
  }
}

/// Gives the drain key up on [session]: `pg_advisory_unlock` under the
/// session's query timeout, or, when that fails or the session is already
/// lost, declares the session lost, so the backend closes it and its
/// replacement ends the old server session, which frees the key.
Future<void> _giveUp(PostgresLockSession session, int key) async {
  if (session.isLost) return;
  try {
    await session.run(
      (c) => c.execute(
        Sql.named('SELECT pg_advisory_unlock(@k)'),
        parameters: <String, Object?>{'k': key},
      ),
    );
  } on Object catch (e) {
    session.declareLost(e);
  }
}

/// A drain lock held on a Postgres backend's lock session.
@internal
final class PostgresDrainLock implements DrainLock {
  PostgresDrainLock._({
    required PostgresGenerationGuard guard,
    required PostgresLockSession session,
    required int key,
    required this.epoch,
    required Future<void> Function(Future<void> Function(Transaction txn) body)
    runTransaction,
  }) : _guard = guard,
       _session = session,
       _key = key,
       _runTransaction = runTransaction {
    unawaited(
      Future.any(<Future<void>>[
        session.lost,
        guard.fenced,
      ]).then((_) => _detectLoss()),
    );
  }

  final PostgresGenerationGuard _guard;
  final PostgresLockSession _session;
  final int _key;
  final Future<void> Function(Future<void> Function(Transaction txn) body)
  _runTransaction;
  bool _released = false;
  bool _lossDetected = false;
  final Completer<void> _lost = Completer<void>();

  /// Called once the lock is released, by the backend that granted it.
  void Function()? onReleased;

  @override
  final int epoch;

  @override
  bool get isReleased => _released;

  void _detectLoss() {
    if (_released) return;
    _lossDetected = true;
    if (!_lost.isCompleted) _lost.complete();
  }

  @override
  Future<void> assertHeldInTxn(Transaction txn) async {
    if (_released) {
      throw const DrainLockLostException(
        DrainLockLossReason.released,
        'the drain lock was released',
      );
    }
    if (_lossDetected || _session.isLost) {
      _detectLoss();
      throw const DrainLockLostException(
        DrainLockLossReason.lossDetected,
        'the lock session that held the drain lock was lost',
      );
    }
    // The share lock holds the stored epoch until this transaction ends: a
    // new holder's raise waits for it, and a raise committed after this
    // transaction's snapshot makes the lock fail with a serialization
    // failure, whose re-run reads the new epoch.
    final rows = await (txn as PostgresTxn).session.execute(
      Sql.named(
        "SELECT (value #>> '{}')::bigint FROM backend_state "
        'WHERE key = @k FOR SHARE',
      ),
      parameters: <String, Object?>{'k': drainEpochKey},
    );
    final stored = rows.isEmpty ? null : rows.first[0] as int?;
    if (stored != epoch) {
      throw DrainLockLostException(
        DrainLockLossReason.epochChanged,
        'the database stores drain epoch $stored; this holder acquired '
        '$epoch',
      );
    }
  }

  @override
  Future<void> assertHeld() => _runTransaction(assertHeldInTxn);

  @override
  Future<void> heartbeat() async {
    if (_released || _lossDetected) return;
    if (DeliveryTestHooks.current?.failNextHeartbeat?.call() ?? false) {
      const failure = InjectedFailure('the drain lock heartbeat');
      _session.declareLost(failure);
      _detectLoss();
      throw failure;
    }
    try {
      await _guard.probeSession(_session);
    } on Object {
      if (_released) return;
      _detectLoss();
      rethrow;
    }
  }

  @override
  Future<void> release() async {
    if (_released) return;
    _released = true;
    onReleased?.call();
    if (_session.isLost || _guard.status == GenerationStatus.fenced) return;
    await _giveUp(_session, _key);
  }

  @override
  Future<void> get lost => _lost.future;
}
