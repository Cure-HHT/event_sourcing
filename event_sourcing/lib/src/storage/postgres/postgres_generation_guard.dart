// Implements: EVS-DEV-version-compatibility/F
// before any write of an open, a live component that conflicts with the
//   opening generation refuses it; otherwise the generation's components
//   are registered until the event store closes, and released when the open
//   fails after registering.
// Implements: EVS-DEV-version-compatibility/G
// generation inspection, registration and the boot transaction of every
//   instance, and every provisioning, run under one exclusive boot lock per
//   database.
// Implements: EVS-DEV-version-compatibility/H
// on Postgres the guard covers every process and session on the database,
//   through the verified lock session.
// Implements: EVS-DEV-version-compatibility/I
// a lost registration checks the live registrations, the stored schema
//   pair and the record before it registers again, and a backend whose
//   generation is no longer admitted is fenced.
// Implements: EVS-DEV-postgres-backend/J
// a lost lock session is closed; before anything is registered again, the
//   old server session is ended if it still holds a library lock.
import 'dart:async';

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_exceptions.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_lock_session.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// Prefix of the exclusive boot-lock key.
const String _bootPrefix = 'event_sourcing.boot';

/// Prefix of the generation component keys.
const String _generationPrefix = 'event_sourcing.generation';

/// `backend_state` key prefix of the records that map a component key back
/// to its component.
const String _componentRecordPrefix = 'generation_component_';

/// `backend_state` keys of the stored schema pair.
@internal
const String schemaVersionKey = 'schema_version';

/// See [schemaVersionKey].
@internal
const String minCompatibleSchemaVersionKey = 'min_compatible_schema_version';

/// `backend_state` key of the database's generation record.
@internal
const String dataGenerationKey = 'data_generation';

/// One component of a registration: its kind, id, major and lock key.
typedef _Component = ({String kind, String id, int value, int key});

/// The stored schema pair, each null when absent.
@internal
typedef StoredSchemaPair = ({int? version, int? minCompatible});

/// Reads the stored schema pair through [session]; both are null when the
/// schema holds no `backend_state` table.
@internal
Future<StoredSchemaPair> readStoredSchemaPair(Session session) async {
  final table = await session.execute(
    "SELECT to_regclass('backend_state') IS NOT NULL",
  );
  if (table.first[0] != true) return (version: null, minCompatible: null);
  final rows = await session.execute(
    Sql.named(
      'SELECT key, value::numeric::int FROM backend_state '
      'WHERE key IN (@v, @m)',
    ),
    parameters: <String, Object?>{
      'v': schemaVersionKey,
      'm': minCompatibleSchemaVersionKey,
    },
  );
  int? version;
  int? minCompatible;
  for (final row in rows) {
    if (row[0] == schemaVersionKey) version = row[1] as int?;
    if (row[0] == minCompatibleSchemaVersionKey) minCompatible = row[1] as int?;
  }
  return (version: version, minCompatible: minCompatible);
}

/// Throws [PostgresSchemaIncompatibleException] unless [stored] is a schema
/// a build of schema version [buildVersion] opens: present, at or above
/// [buildVersion], with a minimum at or below it.
@internal
void refuseUnsupportedSchema(StoredSchemaPair stored, int buildVersion) {
  final String reason;
  if (stored.version == null) {
    reason = 'the database has no provisioned schema';
  } else if (stored.version! < buildVersion) {
    reason =
        'the database schema is at version ${stored.version}, below this '
        "build's";
  } else if ((stored.minCompatible ?? stored.version!) > buildVersion) {
    reason =
        'the database schema requires at least schema version '
        '${stored.minCompatible ?? stored.version}';
  } else {
    return;
  }
  throw PostgresSchemaIncompatibleException(
    reason: reason,
    storedSchemaVersion: stored.version,
    storedMinCompatibleSchemaVersion: stored.minCompatible,
    buildSchemaVersion: buildVersion,
  );
}

/// The live components of the database [session] reaches, in [scope]:
/// every component record whose key some session holds.
@internal
Future<List<({String kind, String id, int value})>> readLiveComponents(
  Session session,
  PostgresScope scope,
) async {
  final table = await session.execute(
    "SELECT to_regclass('backend_state') IS NOT NULL",
  );
  if (table.first[0] != true) return const [];
  final records = await session.execute(
    Sql.named('SELECT value FROM backend_state WHERE key LIKE @p'),
    parameters: <String, Object?>{'p': '$_componentRecordPrefix%'},
  );
  final held = (await session.execute(
    heldAdvisoryKeysSql,
  )).map((r) => r[0]! as int).toSet();
  final live = <({String kind, String id, int value})>[];
  for (final row in records) {
    final value = row[0];
    if (value is! Map) continue;
    final kind = value['kind'];
    final id = value['id'];
    final major = value['value'];
    if (kind is! String || id is! String || major is! int) continue;
    final component = generationComponent(kind: kind, id: id, value: major);
    if (held.contains(
      postgresAdvisoryKey(_generationPrefix, scope, component),
    )) {
      live.add((kind: kind, id: id, value: major));
    }
  }
  return live;
}

/// Takes the exclusive boot lock of [scope] on [connection], retrying every
/// 100 ms for at most [wait]; an overrun throws
/// [GenerationGuardConfigurationException].
@internal
Future<int> takePostgresBootLock(
  Connection connection,
  PostgresScope scope,
  Duration wait,
) async {
  final key = postgresAdvisoryKey(_bootPrefix, scope);
  final giveUpAt = DateTime.now().add(wait);
  var logged = false;
  while (true) {
    final r = await connection.execute(
      Sql.named('SELECT pg_try_advisory_lock(@k)'),
      parameters: <String, Object?>{'k': key},
    );
    if (r.first[0] == true) return key;
    if (!logged) {
      libraryLog(
        'generation_guard',
        'waiting for the boot lock of $scope: another instance is booting '
            'or provisioning the database',
        level: LibraryLogLevel.fine,
      );
      logged = true;
    }
    if (!DateTime.now().isBefore(giveUpAt)) {
      throw GenerationGuardConfigurationException(
        'the boot lock of $scope was held for longer than $wait by another '
        "instance's boot or by a provisioning; the wait must exceed the "
        'longest boot the deployment expects',
      );
    }
    await Future<void>.delayed(const Duration(milliseconds: 100));
  }
}

/// Releases the boot lock [key] on [connection].
@internal
Future<void> releasePostgresBootLock(Connection connection, int key) async {
  await connection.execute(
    Sql.named('SELECT pg_advisory_unlock(@k)'),
    parameters: <String, Object?>{'k': key},
  );
}

/// Reads the database's generation record through [session], or null when
/// no boot has recorded one.
Future<GenerationRecord?> _readRecord(Session session) async {
  final rows = await session.execute(
    Sql.named('SELECT value FROM backend_state WHERE key = @g'),
    parameters: <String, Object?>{'g': dataGenerationKey},
  );
  return rows.isEmpty ? null : GenerationRecord.fromJson(rows.first[0]);
}

/// The components of [live] that conflict with [descriptor]: the same kind
/// and id with another major.
List<String> _conflictsOf(
  List<({String kind, String id, int value})> live,
  GenerationDescriptor descriptor,
) => <String>[
  for (final component in live)
    if (switch (component.kind) {
      'data_format' => component.value != descriptor.dataFormat.major,
      'entry_type' =>
        descriptor.entryTypes[component.id] != null &&
            descriptor.entryTypes[component.id]!.major != component.value,
      _ => false,
    })
      generationComponent(
        kind: component.kind,
        id: component.id,
        value: component.value,
      ),
];

/// The database's generation record does not admit a generation the
/// backend serves.
final class _RecordRefusal implements Exception {
  const _RecordRefusal(this.reason);

  final String reason;

  @override
  String toString() => reason;
}

/// Mutual exclusion for asynchronous code: each holder runs once every
/// earlier holder has released.
final class _AsyncMutex {
  Future<void> _tail = Future<void>.value();

  /// Waits for every earlier holder, then returns the release function,
  /// which is idempotent. A [timeout] that runs out first throws
  /// [TimeoutException]; the turn this call queued then passes on as soon
  /// as the earlier holder releases, so later holders are not blocked.
  Future<void Function()> acquire({Duration? timeout}) async {
    final previous = _tail;
    final done = Completer<void>();
    _tail = done.future;
    if (timeout == null) {
      await previous;
    } else {
      try {
        await previous.timeout(timeout);
      } on TimeoutException {
        unawaited(
          previous.whenComplete(() {
            if (!done.isCompleted) done.complete();
          }),
        );
        rethrow;
      }
    }
    return () {
      if (!done.isCompleted) done.complete();
    };
  }

  /// Runs [body] holding the mutex.
  Future<T> protect<T>(Future<T> Function() body) async {
    final release = await acquire();
    try {
      return await body();
    } finally {
      release();
    }
  }
}

/// The incompatible-generation guard of one `PostgresBackend`: its lock
/// session, the registrations of its open event stores (the active set),
/// the probe that watches the session, and the replacement that follows a
/// loss.
@internal
final class PostgresGenerationGuard {
  PostgresGenerationGuard({
    required this.lockEndpoint,
    required this.sslMode,
    required this.lockQueryTimeout,
    required this.lockHeartbeat,
    required this.bootLockWait,
    required this.scope,
    required this.schemaVersion,
    required Pool<void> pool,
    required PostgresLockSession session,
  }) : _pool = pool,
       _session = session {
    session.onLost = _onSessionLost;
  }

  final Endpoint lockEndpoint;
  final SslMode sslMode;
  final Duration lockQueryTimeout;
  final Duration lockHeartbeat;
  final Duration bootLockWait;
  final PostgresScope scope;

  /// The schema version of the build (its `schema:<n>` component).
  final int schemaVersion;
  final Pool<void> _pool;

  PostgresLockSession _session;
  int _epoch = 0;
  GenerationStatus _status = GenerationStatus.registered;
  String? _fenceReason;
  final List<PostgresGenerationRegistration> _active =
      <PostgresGenerationRegistration>[];

  /// The registration whose boot is in progress, from its registration
  /// until its boot completes or it is released. A replacement of the lock
  /// session registers it with the active set.
  PostgresGenerationRegistration? _booting;

  /// Orders the opens on this backend: held from a registration until its
  /// boot completes or it is released. Session-level advisory locks are
  /// re-entrant, so the boot lock alone does not order two opens that share
  /// one lock session.
  final _AsyncMutex _bootTurn = _AsyncMutex();

  /// Orders every change to the registrations: a registration, a completed
  /// boot, a release, and a replacement of the lock session.
  final _AsyncMutex _changes = _AsyncMutex();

  Timer? _timer;
  bool _closing = false;
  Future<void>? _replacing;
  PostgresSessionIdentity? _oldIdentity;
  final List<Connection> _setAside = <Connection>[];
  Completer<void> _sessionLost = Completer<void>();
  final Completer<void> _fenced = Completer<void>();
  Completer<void> _registeredAgain = Completer<void>();

  /// The state of the backend's registrations.
  GenerationStatus get status => _status;

  /// The generations of the event stores open on the backend.
  List<GenerationDescriptor> get activeDescriptors => <GenerationDescriptor>[
    for (final r in _active) r.descriptor,
  ];

  /// The current lock session's server identity.
  PostgresSessionIdentity get sessionIdentity => _session.identity;

  /// Completes when the current lock session is declared lost.
  Future<void> get sessionLost => _sessionLost.future;

  /// Completes when the backend is fenced.
  Future<void> get fenced => _fenced.future;

  /// The current lock session. While a lost session is being replaced it is
  /// the lost one.
  PostgresLockSession get currentSession => _session;

  /// Completes once every generation is registered on a live lock session:
  /// at once while the status is registered, and otherwise when a
  /// replacement session becomes current. Completes with
  /// [GenerationFencedException] when the backend is fenced, never
  /// successfully after that.
  Future<void> whenRegistered() {
    if (_status == GenerationStatus.fenced) {
      return Future<void>.error(
        GenerationFencedException(_fenceReason ?? 'the backend is fenced'),
      );
    }
    if (_status == GenerationStatus.registered && !_session.isLost) {
      return Future<void>.value();
    }
    if (_registeredAgain.isCompleted) _registeredAgain = Completer<void>();
    return _registeredAgain.future;
  }

  /// Probes [session] as the heartbeat does, through its one-operation
  /// queue: a failure or a timeout declares it lost and is rethrown.
  Future<void> probeSession(PostgresLockSession session) async {
    try {
      await session.run((c) => c.execute('SELECT 1'));
    } on Object catch (e) {
      session.declareLost(e);
      rethrow;
    }
  }

  /// Runs [op] on the current lock session. While a lost session is being
  /// replaced the current session is the lost one, and [op] fails: a
  /// replacement session becomes current only once every generation is
  /// registered on it again.
  Future<T> runOnSession<T>(Future<T> Function(Connection c) op) =>
      _session.run(op);

  /// Starts the probe.
  void start() {
    final factory = DeliveryTestHooks.current?.timerFactory;
    void onTick(Timer _) => _tick();
    _timer = factory != null
        ? factory(lockHeartbeat, onTick)
        : Timer.periodic(lockHeartbeat, onTick);
  }

  /// Registers [descriptor]: waits for any other open on this backend to
  /// finish its boot, takes the boot lock, re-checks the schema, inspects
  /// the live components and refuses a conflict, then takes a shared lock
  /// per component and returns holding the boot lock.
  Future<PostgresGenerationRegistration> register(
    GenerationDescriptor descriptor,
  ) async {
    _refuseWhenFenced();
    final void Function() endTurn;
    try {
      endTurn = await _bootTurn.acquire(timeout: bootLockWait);
    } on TimeoutException {
      throw GenerationGuardConfigurationException(
        'another open on this backend held its boot for longer than '
        '$bootLockWait; the wait must exceed the longest boot the '
        'deployment expects',
      );
    }
    try {
      if (_session.isLost) await _replace();
      return await _changes.protect(() async {
        _refuseWhenFenced();
        final session = _session;
        if (session.isLost) {
          throw const GenerationGuardConfigurationException(
            'the lock session is lost and could not be replaced',
          );
        }
        final registration = PostgresGenerationRegistration._(
          this,
          descriptor,
          _componentsOf(descriptor),
          endTurn,
        );
        await session.run(
          (c) => _registerAll(
            session,
            c,
            <PostgresGenerationRegistration>[registration],
            booting: registration,
            checkRecord: false,
          ),
        );
        registration._epoch = _epoch;
        _booting = registration;
        return registration;
      });
    } catch (_) {
      endTurn();
      rethrow;
    }
  }

  List<_Component> _componentsOf(GenerationDescriptor descriptor) {
    final out = <_Component>[];
    void add(String kind, String id, int value) {
      final component = generationComponent(kind: kind, id: id, value: value);
      out.add((
        kind: kind,
        id: id,
        value: value,
        key: postgresAdvisoryKey(_generationPrefix, scope, component),
      ));
    }

    add('data_format', '', descriptor.dataFormat.major);
    final ids = descriptor.entryTypes.keys.toList()..sort();
    for (final id in ids) {
      add('entry_type', id, descriptor.entryTypes[id]!.major);
    }
    add('schema', '', schemaVersion);
    return out;
  }

  /// The protocol on [c], a connection of [session], for [registrations]:
  /// takes the boot lock; re-checks the stored schema pair; inspects the
  /// live components and refuses a conflict; with [checkRecord], refuses a
  /// registration other than [booting] that the generation record does not
  /// admit; and only then takes a shared lock per component. The boot lock
  /// stays held for [booting] and is released otherwise. A refusal or a
  /// failure gives up everything taken, so a refused registration holds no
  /// lock.
  Future<void> _registerAll(
    PostgresLockSession session,
    Connection c,
    List<PostgresGenerationRegistration> registrations, {
    required PostgresGenerationRegistration? booting,
    required bool checkRecord,
  }) async {
    final hooks = DeliveryTestHooks.current;
    final bootKey = await takePostgresBootLock(c, scope, bootLockWait);
    var bootHeld = true;
    final taken = <int>[];
    try {
      refuseUnsupportedSchema(await readStoredSchemaPair(c), schemaVersion);
      final live = await readLiveComponents(c, scope);
      for (final r in registrations) {
        final conflicts = _conflictsOf(live, r.descriptor);
        if (conflicts.isNotEmpty) {
          throw IncompatibleGenerationException(
            conflictingComponents: (conflicts.toSet().toList()..sort()),
            descriptor: r.descriptor,
          );
        }
      }
      if (checkRecord) {
        final record = await _readRecord(c);
        for (final r in registrations) {
          // A boot in progress is decided by its own boot transaction.
          if (record == null || identical(r, booting)) continue;
          final refused = record.refusedComponent(r.descriptor);
          if (refused != null) {
            throw _RecordRefusal(
              'the database records $refused, which does not admit the '
              'generation this instance serves',
            );
          }
        }
      }
      final inside = hooks?.insideBootLock;
      if (inside != null) await inside();
      final held = <PostgresGenerationRegistration, List<int>>{};
      for (final r in registrations) {
        final keys = <int>[];
        for (final component in r._components) {
          final granted = await c.execute(
            Sql.named('SELECT pg_try_advisory_lock_shared(@k)'),
            parameters: <String, Object?>{'k': component.key},
          );
          if (granted.first[0] != true) {
            throw StateError(
              'the shared lock of ${component.kind}:${component.id}:'
              '${component.value} was not granted',
            );
          }
          taken.add(component.key);
          keys.add(component.key);
          if (hooks?.failGenerationRegistration?.call() ?? false) {
            throw const InjectedFailure('failGenerationRegistration');
          }
        }
        held[r] = keys;
      }
      if (booting == null) {
        await releasePostgresBootLock(c, bootKey);
        bootHeld = false;
      }
      for (final entry in held.entries) {
        entry.key._held
          ..clear()
          ..addAll(entry.value);
      }
      booting?._bootKey = bootKey;
    } catch (e) {
      try {
        for (final key in taken) {
          await c.execute(
            Sql.named('SELECT pg_advisory_unlock_shared(@k)'),
            parameters: <String, Object?>{'k': key},
          );
        }
        if (bootHeld) await releasePostgresBootLock(c, bootKey);
      } catch (unlockError) {
        session.declareLost(unlockError);
      }
      rethrow;
    }
  }

  Future<void> _completeBoot(PostgresGenerationRegistration r) async {
    try {
      await _changes.protect(() async {
        if (identical(_booting, r)) _booting = null;
        final bootKey = r._bootKey;
        r._bootKey = null;
        // A fenced backend commits nothing, so no store is handed out.
        _refuseWhenFenced();
        if (!_active.contains(r)) _active.add(r);
        if (bootKey != null && r._epoch == _epoch && !_session.isLost) {
          final session = _session;
          try {
            await session.run((c) => releasePostgresBootLock(c, bootKey));
          } on Object catch (e) {
            libraryLog(
              'generation_guard',
              'releasing the boot lock failed; the lock session is replaced',
              level: LibraryLogLevel.warning,
              error: e,
            );
            // The boot lock may still be held; ending the session frees it.
            session.declareLost(e);
          }
        }
        // Otherwise the lock session was lost during the boot and is being
        // replaced: the replacement registers the active set, which now
        // holds this registration.
      });
    } finally {
      r._endTurn();
    }
  }

  Future<void> _release(PostgresGenerationRegistration r) async {
    try {
      await _changes.protect(() async {
        _active.remove(r);
        if (identical(_booting, r)) _booting = null;
        final held = List<int>.of(r._held);
        final bootKey = r._bootKey;
        r._held.clear();
        r._bootKey = null;
        if (_status == GenerationStatus.fenced ||
            r._epoch != _epoch ||
            _session.isLost ||
            _closing) {
          return;
        }
        if (held.isEmpty && bootKey == null) return;
        final session = _session;
        try {
          await session.run((c) async {
            for (final key in held) {
              await c.execute(
                Sql.named('SELECT pg_advisory_unlock_shared(@k)'),
                parameters: <String, Object?>{'k': key},
              );
            }
            if (bootKey != null) await releasePostgresBootLock(c, bootKey);
          });
        } on Object catch (e) {
          libraryLog(
            'generation_guard',
            'releasing a generation registration failed; the lock session is '
                'replaced',
            level: LibraryLogLevel.warning,
            error: e,
          );
          session.declareLost(e);
        }
      });
    } finally {
      r._endTurn();
    }
  }

  /// Checks, at the start of a transaction, that the database still
  /// admits every active generation and that the stored schema pair is one
  /// this build supports; otherwise fences the backend and throws
  /// [GenerationFencedException]. Neither can change back: the record and
  /// the stored pair only rise.
  Future<void> fence(TxSession tx) async {
    _refuseWhenFenced();
    final rows = await tx.execute(
      Sql.named(
        'SELECT key, value FROM backend_state WHERE key IN (@g, @v, @m)',
      ),
      parameters: <String, Object?>{
        'g': dataGenerationKey,
        'v': schemaVersionKey,
        'm': minCompatibleSchemaVersionKey,
      },
    );
    GenerationRecord? record;
    int? version;
    int? minCompatible;
    for (final row in rows) {
      final key = row[0]! as String;
      final value = row[1];
      if (key == dataGenerationKey) record = GenerationRecord.fromJson(value);
      if (key == schemaVersionKey) version = (value as num?)?.toInt();
      if (key == minCompatibleSchemaVersionKey) {
        minCompatible = (value as num?)?.toInt();
      }
    }
    try {
      refuseUnsupportedSchema((
        version: version,
        minCompatible: minCompatible,
      ), schemaVersion);
    } on PostgresSchemaIncompatibleException catch (e) {
      final reason =
          'the database schema is not one this build supports: ${e.reason}';
      _fence(reason);
      throw GenerationFencedException(reason);
    }
    if (record == null) return;
    for (final r in _active) {
      final refused = record.refusedComponent(r.descriptor);
      if (refused != null) {
        final reason =
            'the database records $refused, which does not admit the '
            'generation this instance serves '
            '(${r.descriptor.components.join(', ')})';
        _fence(reason);
        throw GenerationFencedException(reason);
      }
    }
  }

  void _refuseWhenFenced() {
    if (_status == GenerationStatus.fenced) {
      throw GenerationFencedException(_fenceReason ?? 'the backend is fenced');
    }
  }

  void _tick() {
    if (_closing || _status == GenerationStatus.fenced) return;
    if (_session.isLost) {
      unawaited(_replace());
      return;
    }
    if (_session.isBusy) return;
    unawaited(_probe());
  }

  Future<void> _probe() async {
    final hooks = DeliveryTestHooks.current;
    final session = _session;
    try {
      await session.run((c) async {
        if (hooks?.failNextLockHeartbeat?.call() ?? false) {
          throw const InjectedFailure('failNextLockHeartbeat');
        }
        if (hooks?.stallLockHeartbeatPastQueryTimeout?.call() ?? false) {
          final seconds =
              (lockQueryTimeout + const Duration(seconds: 1)).inMilliseconds /
              1000;
          await c.execute('SELECT pg_sleep($seconds)');
          return;
        }
        await c.execute('SELECT 1');
      });
    } on Object catch (e) {
      // Any probe failure, a cancelled statement on a live session
      // included, declares the session lost.
      session.declareLost(e);
    }
  }

  void _onSessionLost(PostgresLockSession session, Object error) {
    if (_closing || !identical(session, _session)) return;
    if (_status == GenerationStatus.fenced) return;
    libraryLog(
      'generation_guard',
      'the lock session (pid ${session.identity.pid}) is lost; it is closed '
          'and replaced',
      level: LibraryLogLevel.warning,
      error: error,
    );
    _status = GenerationStatus.lost;
    _oldIdentity = session.identity;
    for (final r in <PostgresGenerationRegistration>[..._active, ?_booting]) {
      r._lost = true;
    }
    if (!_sessionLost.isCompleted) _sessionLost.complete();
    if (DeliveryTestHooks.current?.failLostSessionClose?.call() ?? false) {
      _setAside.add(session.connection);
    } else {
      unawaited(session.close());
    }
    // Replaced now, and retried at every probe tick until it succeeds.
    unawaited(_replace());
  }

  /// Replaces a lost session: opens and checks a new one, ends the old
  /// server session while it still holds a library lock, and registers
  /// every active generation, and the one whose boot is in progress, on it
  /// again after checking the live registrations, the stored schema pair
  /// and the generation record. Only then does the new session become
  /// current. A refusal fences the backend, holding no lock; any other
  /// failure is retried at every probe tick.
  Future<void> _replace() {
    final running = _replacing;
    if (running != null) return running;
    final attempt = _changes
        .protect(_replaceOnce)
        .whenComplete(() => _replacing = null);
    _replacing = attempt;
    return attempt;
  }

  Future<void> _replaceOnce() async {
    if (_closing || _status == GenerationStatus.fenced) return;
    if (!_session.isLost) return;
    final PostgresLockSession next;
    try {
      next = await PostgresLockSession.open(
        endpoint: lockEndpoint,
        sslMode: sslMode,
        queryTimeout: lockQueryTimeout,
        expectedScope: scope,
      );
    } on Object catch (e) {
      libraryLog(
        'generation_guard',
        'opening a replacement lock session failed; retried at the next '
            'probe',
        level: LibraryLogLevel.warning,
        error: e,
      );
      return;
    }
    var adopted = false;
    try {
      await verifyLockSessionServer(_pool, next);
      final old = _oldIdentity;
      if (old != null) {
        final ended = await _endOldSession(next, old);
        if (!ended) {
          libraryLog(
            'generation_guard',
            'the lost lock session (pid ${old.pid}) still holds a library '
                'lock and could not be ended: the lock role must be allowed to '
                'end its own sessions (EVS-DEV-postgres-backend/J); retried at '
                'the next probe',
            level: LibraryLogLevel.severe,
          );
          return;
        }
        _oldIdentity = null;
      }
      final booting = _booting;
      final registrations = <PostgresGenerationRegistration>[
        ..._active,
        ?booting,
      ];
      await next.run(
        (c) => _registerAll(
          next,
          c,
          registrations,
          booting: booting,
          checkRecord: true,
        ),
      );
      next.onLost = _onSessionLost;
      _session = next;
      adopted = true;
      _epoch++;
      for (final r in registrations) {
        r
          .._epoch = _epoch
          .._lost = false;
      }
      _sessionLost = Completer<void>();
      _status = GenerationStatus.registered;
      if (!_registeredAgain.isCompleted) _registeredAgain.complete();
      libraryLog(
        'generation_guard',
        'the lock session is replaced (pid ${next.identity.pid}) and every '
            'open generation is registered again',
      );
    } on IncompatibleGenerationException catch (e) {
      _fence('a conflicting build is registered: $e');
    } on PostgresSchemaIncompatibleException catch (e) {
      _fence('the database schema is not one this build supports: $e');
    } on _RecordRefusal catch (e) {
      _fence(e.reason);
    } on Object catch (e) {
      libraryLog(
        'generation_guard',
        'registering again on the replacement lock session failed; retried '
            'at the next probe',
        level: LibraryLogLevel.warning,
        error: e,
      );
    } finally {
      if (!adopted) await next.close();
    }
  }

  /// Ends the old server session [old] while it holds an advisory lock of
  /// this database, polling every 100 ms for at most the lock query
  /// timeout until it holds none. False when the termination was refused
  /// or the poll timed out.
  Future<bool> _endOldSession(
    PostgresLockSession next,
    PostgresSessionIdentity old,
  ) async {
    Future<int> heldByOld() => next.run((c) async {
      final r = await c.execute(
        Sql.named('''
          SELECT count(*) FROM pg_locks l
          JOIN pg_stat_activity a ON a.pid = l.pid
          WHERE l.locktype = 'advisory'
            AND l.pid = @pid
            AND a.backend_start = @started
            AND l.database = (SELECT oid FROM pg_database
                              WHERE datname = current_database())
        '''),
        parameters: <String, Object?>{
          'pid': old.pid,
          'started': old.backendStart,
        },
      );
      return r.first[0]! as int;
    });
    if (await heldByOld() == 0) return true;
    final refused =
        DeliveryTestHooks.current?.failOldSessionTermination?.call() ?? false;
    if (refused) return false;
    final terminated = await next.run((c) async {
      final r = await c.execute(
        Sql.named('SELECT pg_terminate_backend(@pid)'),
        parameters: <String, Object?>{'pid': old.pid},
      );
      return r.first[0] == true;
    });
    if (!terminated) return false;
    final giveUpAt = DateTime.now().add(lockQueryTimeout);
    while (DateTime.now().isBefore(giveUpAt)) {
      if (await heldByOld() == 0) return true;
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    return false;
  }

  /// Fences the backend: every later transaction throws
  /// [GenerationFencedException]. A fenced backend gives up every lock it
  /// holds, by ending its lock session, so it never stands in the way of
  /// the generation that fenced it.
  void _fence(String reason) {
    if (_status == GenerationStatus.fenced) return;
    _status = GenerationStatus.fenced;
    _fenceReason = reason;
    if (!_fenced.isCompleted) _fenced.complete();
    if (!_registeredAgain.isCompleted) {
      _registeredAgain.completeError(GenerationFencedException(reason));
      // Nobody may be waiting; the error is delivered to whoever is.
      unawaited(_registeredAgain.future.then((_) {}, onError: (Object _) {}));
    }
    libraryLog(
      'generation_guard',
      'the backend is fenced: $reason; every transaction is refused, every '
          'lock is given up, and the instance must be stopped',
      level: LibraryLogLevel.severe,
    );
    final session = _session;
    if (!session.isLost && !_closing) unawaited(_dropLocks(session));
  }

  Future<void> _dropLocks(PostgresLockSession session) async {
    try {
      await session.run((c) => c.execute('SELECT pg_advisory_unlock_all()'));
    } on Object catch (e) {
      libraryLog(
        'generation_guard',
        'giving up the locks of a fenced backend failed; its lock session is '
            'closed',
        level: LibraryLogLevel.warning,
        error: e,
      );
    }
    await session.close();
  }

  /// Stops the probe and closes the lock session.
  Future<void> close() async {
    _closing = true;
    _timer?.cancel();
    final replacing = _replacing;
    if (replacing != null) {
      try {
        await replacing;
      } on Object catch (_) {
        // Closing: the replacement's outcome no longer matters.
      }
    }
    await _session.close();
    for (final c in _setAside) {
      try {
        await c.close().timeout(lockQueryTimeout);
      } on Object catch (_) {
        // Set aside because closing failed once; not relied on.
      }
    }
  }
}

/// A generation registered with a [PostgresGenerationGuard].
@internal
final class PostgresGenerationRegistration extends GenerationRegistration {
  PostgresGenerationRegistration._(
    this._guard,
    this.descriptor,
    this._components,
    this._endTurn,
  );

  final PostgresGenerationGuard _guard;

  /// The registered generation.
  final GenerationDescriptor descriptor;
  final List<_Component> _components;

  /// Lets the next open on the backend proceed; idempotent.
  final void Function() _endTurn;
  final List<int> _held = <int>[];
  int? _bootKey;
  int _epoch = 0;
  bool _lost = false;
  bool _released = false;

  @override
  bool get isLost => _lost || _epoch != _guard._epoch;

  @override
  @internal
  Future<void> recordInTxn(Transaction txn) async {
    final pgTxn = txn as PostgresTxn;
    final session = pgTxn.session;
    pgTxn.wroteBackendState = true;
    for (final c in _components) {
      final hex = c.key.toUnsigned(64).toRadixString(16).padLeft(16, '0');
      await session.execute(
        Sql.named('''
          INSERT INTO backend_state (key, value) VALUES (@k, @v:jsonb)
          ON CONFLICT (key) DO NOTHING
        '''),
        parameters: <String, Object?>{
          'k': '$_componentRecordPrefix$hex',
          'v': <String, Object?>{'kind': c.kind, 'id': c.id, 'value': c.value},
        },
      );
    }
  }

  @override
  Future<void> completeBoot() => _guard._completeBoot(this);

  @override
  Future<void> release() async {
    if (_released) return;
    _released = true;
    await _guard._release(this);
  }
}
