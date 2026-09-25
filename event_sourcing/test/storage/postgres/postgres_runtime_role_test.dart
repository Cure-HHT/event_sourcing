// The Postgres runtime role: a schema owned and provisioned by one role
// (the owner), and a backend opened as another (the runtime role) that
// holds exactly `postgresRuntimeRoleGrants` and USAGE on the schema, and
// neither owns nor can create the tables. The schema is set up as the
// documentation describes: created for the owner, CREATE revoked from
// PUBLIC, USAGE granted to the runtime role, provisioned by the owner, then
// the table grants. Every library operation other than provisioning runs as
// the runtime role: the storage and idempotency conformance harnesses and
// the queue, operator-halt, delivery-cycle, drain-wedge, reconfigure,
// destination-wedges-view and version scenarios run here again as that
// role. The boot scenarios are not rerun (their fixture writes an earlier
// data format's tables as the administrative role); the boot's statements
// are those the version scenarios and every event-store open here run. A
// lock session opened as a third role holding only the `backend_state`
// privileges carries a boot, a delivery cycle and a stall replacement. The
// owner-only operations fail for the runtime role, and each granted
// privilege is shown to be needed. Gated on PG_TEST_URL, whose role must be
// able to create roles. The roles are cluster-global, so their names and
// the schema's carry this process's id; they are created afresh and dropped
// when the file ends.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:io' show pid;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresLibraryTables;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/delivery_cycle_conformance.dart';
import '../../test_support/destination_wedges_view_conformance.dart';
import '../../test_support/drain_wedge_conformance.dart';
import '../../test_support/fake_destination.dart';
import '../../test_support/operator_halt_conformance.dart';
import '../../test_support/queue_registry_conformance.dart';
import '../../test_support/queue_test_support.dart';
import '../../test_support/reconfigure_conformance.dart';
import '../../test_support/version_compatibility_conformance.dart';
import '../idempotency_store_conformance.dart';
import '../storage_backend_conformance.dart';
import 'test_postgres_url.dart';

final String _schema = 'evs_runtime_role_$pid';
final String _owner = 'evs_schema_owner_$pid';
final String _runtime = 'evs_runtime_$pid';

/// A role that holds only what a lock session needs: USAGE on the schema
/// and the runtime role's privileges on `backend_state`.
final String _lock = 'evs_lock_$pid';

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

String _asRole(String url, String role) =>
    Uri.parse(url).replace(userInfo: '$role:evs').toString();

/// The owner and runtime roles and the schema they share.
class _Roles {
  _Roles(this.url);

  /// The administrative URL (PG_TEST_URL), whose role creates the roles.
  final String url;

  String get ownerUrl => _asRole(url, _owner);
  String get runtimeUrl => _asRole(url, _runtime);
  String get lockUrl => _asRole(url, _lock);

  Future<void> _dropAll(Connection admin) async {
    await admin.execute('DROP SCHEMA IF EXISTS $_schema CASCADE');
    for (final role in <String>[_lock, _runtime, _owner]) {
      final exists = await admin.execute(
        Sql.named('SELECT 1 FROM pg_roles WHERE rolname = @r'),
        parameters: <String, Object?>{'r': role},
      );
      if (exists.isEmpty) continue;
      await admin.execute('DROP OWNED BY $role');
      await admin.execute('DROP ROLE $role');
    }
  }

  /// Creates both roles afresh; each connects with the schema alone on its
  /// search path.
  Future<void> create() async {
    final admin = await _connect(url);
    try {
      await _dropAll(admin);
      for (final role in <String>[_owner, _runtime, _lock]) {
        await admin.execute(
          "CREATE ROLE $role LOGIN PASSWORD 'evs' "
          'NOSUPERUSER NOCREATEDB NOCREATEROLE',
        );
        await admin.execute('ALTER ROLE $role SET search_path = $_schema');
      }
    } finally {
      await admin.close();
    }
  }

  Future<void> drop() async {
    final admin = await _connect(url);
    try {
      await _dropAll(admin);
    } finally {
      await admin.close();
    }
  }

  /// Recreates the schema as the owner's, revokes CREATE on it from
  /// PUBLIC, grants the runtime and lock roles USAGE on it, provisions it
  /// as the owner (unless [provision] is false) and grants the runtime role
  /// [grants] and the lock role the `backend_state` privileges.
  Future<void> reset({
    bool provision = true,
    Map<String, Set<String>> grants = postgresRuntimeRoleGrants,
  }) async {
    final admin = await _connect(url);
    try {
      await admin.execute('DROP SCHEMA IF EXISTS $_schema CASCADE');
      await admin.execute('CREATE SCHEMA $_schema AUTHORIZATION $_owner');
      await admin.execute('REVOKE CREATE ON SCHEMA $_schema FROM PUBLIC');
      await admin.execute('GRANT USAGE ON SCHEMA $_schema TO $_runtime');
      await admin.execute('GRANT USAGE ON SCHEMA $_schema TO $_lock');
    } finally {
      await admin.close();
    }
    if (!provision) return;
    await PostgresBackend.provision(ownerUrl, sslMode: SslMode.disable);
    await asOwner((c) async {
      for (final MapEntry(key: table, value: privileges) in grants.entries) {
        if (privileges.isEmpty) continue;
        await c.execute(
          'GRANT ${privileges.join(', ')} ON $table TO $_runtime',
        );
      }
      await c.execute(
        'GRANT ${postgresRuntimeRoleGrants['backend_state']!.join(', ')} '
        'ON backend_state TO $_lock',
      );
    });
  }

  Future<void> revoke(String table, String privilege) =>
      asOwner((c) => c.execute('REVOKE $privilege ON $table FROM $_runtime'));

  Future<T> asOwner<T>(Future<T> Function(Connection c) body) async {
    final c = await _connect(ownerUrl);
    try {
      return await body(c);
    } finally {
      await c.close();
    }
  }

  Future<T> asRuntime<T>(Future<T> Function(Connection c) body) async {
    final c = await _connect(runtimeUrl);
    try {
      return await body(c);
    } finally {
      await c.close();
    }
  }

  /// The tables in the schema.
  Future<Set<String>> tables() async {
    final admin = await _connect(url);
    try {
      final r = await admin.execute(
        Sql.named(
          'SELECT table_name FROM information_schema.tables '
          'WHERE table_schema = @s',
        ),
        parameters: <String, Object?>{'s': _schema},
      );
      return <String>{for (final row in r) row[0]! as String};
    } finally {
      await admin.close();
    }
  }

  Future<PostgresBackend> openRuntimeBackend({
    Duration lockQueryTimeout = const Duration(seconds: 5),
    bool separateLockRole = false,
  }) => PostgresBackend.open(
    url: runtimeUrl,
    lockUrl: separateLockRole ? lockUrl : null,
    sslMode: SslMode.disable,
    lockQueryTimeout: lockQueryTimeout,
  );
}

/// A database for the shared scenarios whose backends run as the runtime
/// role over a schema the owner provisioned.
class _RuntimeDatabase implements QueueTestDatabase, VersionTestDatabase {
  _RuntimeDatabase(this._roles);

  final _Roles _roles;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await _roles.openRuntimeBackend();
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<Set<String>> backendStateKeys() async {
    final conn = await _connect(_roles.url);
    try {
      final rows = await conn.execute('SELECT key FROM $_schema.backend_state');
      return <String>{for (final r in rows) r[0]! as String};
    } finally {
      await conn.close();
    }
  }

  @override
  Future<void> stop(EventStore store) => store.close();

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
  }
}

const Initiator _init = AutomationInitiator(service: 'runtime-role');
const String _noteType = 'runtime_note';
const String _noteView = 'runtime_notes';
const Source _source = Source(
  hopId: 'runtime-hop',
  identifier: 'runtime-install',
  softwareVersion: 'test@1.0.0',
);

/// A process over the runtime role's backend: an event store with one
/// aggregate view over [_noteType], a destination registry and the
/// idempotency store.
class _World {
  _World._(this.backend, this.store);

  final PostgresBackend backend;
  final EventStore store;
  late final DestinationRegistry registry = DestinationRegistry(
    eventStore: store,
  );
  late final PostgresIdempotencyStore idempotency =
      PostgresIdempotencyStore.forBackend(backend);

  /// The clock the event store stamps security contexts with.
  static DateTime now = DateTime.utc(2026, 3, 1);

  static Future<_World> open(PostgresBackend backend) async {
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
      ),
    );
    final projections = ProjectionRegistry()
      ..register(
        const AggregateProjectionSpec(
          viewName: _noteView,
          interest: SubscriptionFilter(entryTypes: <String>{_noteType}),
          tombstoneEventTypes: <String>{},
        ),
      );
    final store = await EventStore.open(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: PostgresSecurityContextStore(backend: backend),
      projections: projections,
      clock: () => now,
    );
    return _World._(backend, store);
  }

  Future<StoredEvent> note(String id, {bool withSecurity = false}) async =>
      (await store.append(
        entryType: _noteType,
        aggregateId: id,
        aggregateType: 'note',
        eventType: 'noted',
        data: <String, Object?>{'id': id},
        initiator: const UserInitiator('u'),
        security: withSecurity
            ? const SecurityDetails(ipAddress: '10.1.2.3', userAgent: 'test')
            : null,
      ))!;

  Future<void> activate(Destination d) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  }

  Future<void> fill(Destination d) => fillForTest(
    d,
    backend: backend,
    source: _source,
    clock: () => DateTime.utc(2027, 1, 1),
  );

  Future<void> close() async {
    await store.close();
    await backend.close();
  }
}

/// One named case of the grants matrix: [arrange] runs with every granted
/// privilege, [act] after one privilege is revoked.
class _Case {
  const _Case(this.arrange, this.act);

  final Future<Object?> Function(_Roles roles) arrange;
  final Future<void> Function(_Roles roles, Object? arranged) act;
}

Future<Object?> _nothing(_Roles roles) async => null;

Future<Object?> _world(_Roles roles) async =>
    _World.open(await roles.openRuntimeBackend());

/// A world with destination `x` activated and two notes filled into its
/// queue.
Future<Object?> _filledWorld(_Roles roles) async {
  final w = await _World.open(await roles.openRuntimeBackend());
  final d = FakeDestination(id: 'x');
  await w.activate(d);
  await w.note('q1');
  await w.note('q2');
  await w.fill(d);
  await w.fill(d);
  return w;
}

/// The cases of the grants matrix, by name.
final Map<String, _Case> _cases = <String, _Case>{
  'open the backend': _Case(_nothing, (roles, _) async {
    final backend = await roles.openRuntimeBackend();
    await backend.close();
  }),
  'boot an event store': _Case(_nothing, (roles, _) async {
    final backend = await roles.openRuntimeBackend();
    try {
      await (await _World.open(backend)).close();
    } finally {
      await backend.close();
    }
  }),
  'append an event with a security context': _Case(
    _world,
    (roles, w) => (w! as _World).note('a1', withSecurity: true),
  ),
  'read a view': _Case(_world, (roles, w) async {
    await (w! as _World).backend.findViewRows(_noteView);
  }),
  'rebuild a view': _Case(_world, (roles, w) async {
    final world = w! as _World;
    await world.note('r1');
    await rebuildView(
      store: world.store,
      viewName: _noteView,
      targetVersionByEntryType: const <String, EntryTypeVersion>{
        _noteType: EntryTypeVersion(1, 0),
      },
    );
  }),
  'read a security context': _Case(_world, (roles, w) async {
    final world = w! as _World;
    final event = await world.note('s1', withSecurity: true);
    await PostgresSecurityContextStore(
      backend: world.backend,
    ).read(event.eventId);
  }),
  'redact a security context': _Case(
    (roles) async {
      final w = await _World.open(await roles.openRuntimeBackend());
      return (w, await w.note('s2', withSecurity: true));
    },
    (roles, arranged) async {
      final (w, event) = arranged! as (_World, StoredEvent);
      await w.store.clearSecurityContext(
        event.eventId,
        reason: 'test',
        redactedBy: _init,
      );
    },
  ),
  'apply the retention policy': _Case(
    (roles) async {
      _World.now = DateTime.utc(2026, 3, 1);
      final w = await _World.open(await roles.openRuntimeBackend());
      await w.note('old', withSecurity: true);
      return w;
    },
    (roles, w) async {
      _World.now = DateTime.utc(2026, 6, 1);
      try {
        await (w! as _World).store.applyRetentionPolicy(
          policy: const SecurityRetentionPolicy(
            fullRetention: Duration(days: 30),
          ),
        );
      } finally {
        _World.now = DateTime.utc(2026, 3, 1);
      }
    },
  ),
  'fill a destination': _Case((roles) async {
    final w = await _World.open(await roles.openRuntimeBackend());
    await w.activate(FakeDestination(id: 'x'));
    await w.note('f1');
    return w;
  }, (roles, w) => (w! as _World).fill(FakeDestination(id: 'x'))),
  'read a queue head': _Case(
    _world,
    (roles, w) async => (w! as _World).backend.readFifoHead('x'),
  ),
  'deliver a queue item': _Case(
    _filledWorld,
    (roles, w) => drainForTest(
      FakeDestination(id: 'x', script: <SendResult>[const SendOk()]),
      registry: (w! as _World).registry,
    ),
  ),
  'recover a wedged head': _Case(
    (roles) async {
      final w = (await _filledWorld(roles))! as _World;
      final head = await wedgeHeadForTest(w.registry, 'x');
      return (w, head);
    },
    (roles, arranged) async {
      final (w, head) = arranged! as (_World, String);
      await w.registry.tombstoneAndRefill('x', head, initiator: _init);
    },
  ),
  'cancel a halt request': _Case((roles) async {
    final w = await _World.open(await roles.openRuntimeBackend());
    await w.activate(FakeDestination(id: 'x'));
    await w.registry.requestHalt(
      'x',
      initiator: _init,
      purpose: HaltPurpose.pause,
    );
    return w;
  }, (roles, w) => (w! as _World).registry.cancelHalt('x', initiator: _init)),
  'look up an idempotency entry': _Case(
    _world,
    (roles, w) async => (w! as _World).idempotency.lookup('a', 'p', 'k'),
  ),
  'record an idempotency entry': _Case(
    _world,
    (roles, w) => (w! as _World).idempotency.record(
      actionName: 'a',
      principalId: 'p',
      key: 'k',
      resultJson: const <String, Object?>{'ok': true},
      emittedEventIds: const <String>[],
      expiresAt: DateTime.utc(2100),
    ),
  ),
  'sweep expired idempotency entries': _Case(
    _world,
    (roles, w) async =>
        (w! as _World).idempotency.sweepExpired(before: DateTime.utc(2100)),
  ),
};

/// The case each granted privilege is needed by: with only that privilege
/// revoked, the case fails with a permission error naming its table.
const Map<(String, String), String> _neededBy = <(String, String), String>{
  ('events', 'SELECT'): 'boot an event store',
  ('events', 'INSERT'): 'boot an event store',
  ('view_rows', 'SELECT'): 'read a view',
  ('view_rows', 'INSERT'): 'append an event with a security context',
  ('view_rows', 'UPDATE'): 'append an event with a security context',
  ('view_rows', 'DELETE'): 'rebuild a view',
  ('view_target_versions', 'SELECT'): 'boot an event store',
  ('view_target_versions', 'INSERT'): 'boot an event store',
  ('view_target_versions', 'UPDATE'): 'boot an event store',
  ('view_target_versions', 'DELETE'): 'rebuild a view',
  ('fifo_entries', 'SELECT'): 'read a queue head',
  ('fifo_entries', 'INSERT'): 'fill a destination',
  ('fifo_entries', 'UPDATE'): 'deliver a queue item',
  ('fifo_entries', 'DELETE'): 'recover a wedged head',
  ('backend_state', 'SELECT'): 'open the backend',
  ('backend_state', 'INSERT'): 'boot an event store',
  ('backend_state', 'UPDATE'): 'boot an event store',
  ('backend_state', 'DELETE'): 'cancel a halt request',
  ('security_context', 'SELECT'): 'read a security context',
  ('security_context', 'INSERT'): 'append an event with a security context',
  ('security_context', 'UPDATE'): 'apply the retention policy',
  ('security_context', 'DELETE'): 'redact a security context',
  ('idempotency', 'SELECT'): 'look up an idempotency entry',
  ('idempotency', 'INSERT'): 'record an idempotency entry',
  ('idempotency', 'UPDATE'): 'record an idempotency entry',
  ('idempotency', 'DELETE'): 'sweep expired idempotency entries',
};

/// Whether [error], or a log line the library wrote, reports a permission
/// denied on [table].
bool _deniedOn(String table, Object? error, List<LibraryLogRecord> log) {
  final needle = 'permission denied for table $table';
  bool names(Object? e) {
    if (e == null) return false;
    if (e is ServerException && e.code == '42501') {
      return e.message.contains(needle);
    }
    return e.toString().contains(needle);
  }

  return names(error) || log.any((r) => names(r.error) || names(r.message));
}

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  final roles = _Roles(url);
  setUpAll(roles.create);
  tearDownAll(roles.drop);

  Future<_RuntimeDatabase> freshDatabase() async {
    await roles.reset();
    return _RuntimeDatabase(roles);
  }

  // The whole storage contract, as the runtime role.
  runStorageBackendConformanceTests(
    () async {
      await roles.reset();
      return roles.openRuntimeBackend();
    },
    reopen: (_) => roles.openRuntimeBackend(),
    backendLabel: 'postgres, runtime role',
    securityStoreOf: (backend) =>
        PostgresSecurityContextStore(backend: backend as PostgresBackend),
  );

  final idempotencyBackends = <PostgresBackend>[];
  tearDown(() async {
    for (final backend in idempotencyBackends) {
      await backend.close();
    }
    idempotencyBackends.clear();
  });
  runIdempotencyStoreConformanceTests(() async {
    await roles.reset();
    final backend = await roles.openRuntimeBackend();
    idempotencyBackends.add(backend);
    return PostgresIdempotencyStore.forBackend(backend);
  }, label: 'postgres, runtime role');

  // Recovery, deletion (sent, wedged and pending items), registry
  // operations and their rollbacks.
  runQueueRegistryScenarios(freshDatabase, label: 'postgres, runtime role');

  // Wedge, halt and recovery under a delivery cycle's drain lock.
  runOperatorHaltScenarios(freshDatabase, label: 'postgres, runtime role');

  // Standby, takeover and fencing of the drain lock.
  runDeliveryCycleScenarios(
    freshDatabase,
    label: 'postgres, runtime role',
    giveUpInjection: (fail) => (
      failAfterExclusionObtained: null,
      failEpochBumpWithSerializationFailure: fail,
    ),
  );

  // Permanent-refusal and exhausted-budget wedges with their events.
  runDrainWedgeScenarios(
    freshDatabase,
    label: 'postgres, runtime role',
    retriesConflictingTransactions: true,
    processesShareDatabaseHandle: false,
  );

  // The reconfigure halt, the refill guard and the start-date refusals.
  runReconfigureScenarios(freshDatabase, label: 'postgres, runtime role');

  // The default destination-wedges view, its rebuild, the delivery status
  // and ingest of peers' and forged reserved events.
  runDestinationWedgesViewScenarios(
    freshDatabase,
    label: 'postgres, runtime role',
  );

  // Snapshot promotion, view catch-up, the generation record and the
  // version refusals.
  runVersionCompatibilityScenarios(
    freshDatabase,
    backendLabel: 'postgres, runtime role',
  );

  group('runtime role', () {
    // Verifies: EVS-DEV-postgres-backend/K
    // the runtime role cannot provision the schema: on an unprovisioned
    //   schema, provisioning as the runtime role fails and creates nothing.
    test(
      'provisioning as the runtime role fails and creates nothing',
      () async {
        await roles.reset(provision: false);
        await expectLater(
          PostgresBackend.provision(roles.runtimeUrl, sslMode: SslMode.disable),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '42501'),
          ),
        );
        expect(await roles.tables(), isEmpty);
      },
    );

    // Verifies: EVS-DEV-postgres-backend/K
    // the runtime role neither owns nor can create the tables.
    test('the runtime role cannot create a table and owns none', () async {
      await roles.reset();
      await expectLater(
        roles.asRuntime((c) => c.execute('CREATE TABLE extra (x int)')),
        throwsA(isA<ServerException>().having((e) => e.code, 'code', '42501')),
      );
      final owners = await roles.asOwner(
        (c) => c.execute(
          Sql.named(
            'SELECT DISTINCT tableowner FROM pg_tables WHERE schemaname = @s',
          ),
          parameters: <String, Object?>{'s': _schema},
        ),
      );
      expect(
        <String>{for (final r in owners) r[0]! as String},
        <String>{_owner},
      );
      expect(await roles.tables(), postgresLibraryTables.toSet());
    });

    // Verifies: EVS-DEV-destination-drain/S
    // the runtime role cannot disable or drop the queue table's guard,
    //   remove its status check or replace its function, and the guard
    //   still refuses an illegal change and an illegal insert afterwards.
    // Verifies: EVS-DEV-postgres-backend/K
    // the owner-only operations on the guard fail for the runtime role.
    test('the runtime role cannot remove the guard, which still holds', () async {
      await roles.reset();
      for (final statement in <String>[
        'ALTER TABLE fifo_entries DISABLE TRIGGER fifo_entries_guard',
        'ALTER TABLE fifo_entries DISABLE TRIGGER ALL',
        'DROP TRIGGER fifo_entries_guard ON fifo_entries',
        'DROP TRIGGER fifo_entries_truncate_guard ON fifo_entries',
        'ALTER TABLE fifo_entries DROP CONSTRAINT fifo_entries_final_status_check',
        'DROP FUNCTION fifo_entries_guard()',
        r'CREATE OR REPLACE FUNCTION fifo_entries_guard() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN RETURN NEW; END $f$',
        'TRUNCATE fifo_entries',
      ]) {
        await expectLater(
          roles.asRuntime((c) => c.execute(statement)),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '42501'),
          ),
          reason: statement,
        );
      }
      await roles.asRuntime((c) async {
        await expectLater(
          c.execute(
            'INSERT INTO fifo_entries (destination_id, sequence_in_queue, '
            'entry_id, event_ids, event_id_first_seq, event_id_last_seq, '
            'wire_format, enqueued_at, attempts, final_status) '
            "VALUES ('d', 1, 'w', '[\"ev\"]'::jsonb, 1, 1, 'fake-v1', now(), "
            "'[]'::jsonb, 'wedged')",
          ),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '23514'),
          ),
        );
        await c.execute(
          'INSERT INTO fifo_entries (destination_id, sequence_in_queue, '
          'entry_id, event_ids, event_id_first_seq, event_id_last_seq, '
          'wire_format, enqueued_at, attempts) '
          "VALUES ('d', 2, 'p', '[\"ev\"]'::jsonb, 1, 1, 'fake-v1', now(), "
          "'[]'::jsonb)",
        );
        await expectLater(
          c.execute(
            "UPDATE fifo_entries SET final_status = 'tombstoned' "
            "WHERE entry_id = 'p'",
          ),
          throwsA(
            isA<ServerException>().having((e) => e.code, 'code', '23514'),
          ),
        );
        final r = await c.execute('SELECT final_status FROM fifo_entries');
        expect(<Object?>[for (final row in r) row[0]], <Object?>[null]);
      });
    });

    // Verifies: EVS-DEV-postgres-backend/K
    // redaction, the retention sweep (compaction and purge) and a view
    //   rebuild run as the runtime role.
    test('redaction, retention and a view rebuild run as the runtime '
        'role', () async {
      await roles.reset();
      _World.now = DateTime.utc(2025, 1, 1);
      final w = await _World.open(await roles.openRuntimeBackend());
      addTearDown(() async {
        _World.now = DateTime.utc(2026, 3, 1);
        await w.close();
      });
      final purged = await w.note('purged', withSecurity: true);
      _World.now = DateTime.utc(2026, 5, 1);
      final compacted = await w.note('compacted', withSecurity: true);
      final redacted = await w.note('redacted', withSecurity: true);
      await w.store.clearSecurityContext(
        redacted.eventId,
        reason: 'test',
        redactedBy: _init,
      );
      _World.now = DateTime.utc(2026, 6, 15);
      final result = await w.store.applyRetentionPolicy(
        policy: const SecurityRetentionPolicy(
          fullRetention: Duration(days: 30),
          truncatedRetention: Duration(days: 300),
        ),
      );
      expect(result.purgedCount, 1);
      expect(result.compactedCount, greaterThanOrEqualTo(1));
      final security = PostgresSecurityContextStore(backend: w.backend);
      expect(await security.read(purged.eventId), isNull);
      expect(await security.read(redacted.eventId), isNull);
      expect(await security.read(compacted.eventId), isNotNull);
      final before = await w.backend.findViewRows(_noteView);
      expect(before, hasLength(3));
      await rebuildView(
        store: w.store,
        viewName: _noteView,
        targetVersionByEntryType: const <String, EntryTypeVersion>{
          _noteType: EntryTypeVersion(1, 0),
        },
      );
      expect(await w.backend.findViewRows(_noteView), before);
    });

    // Verifies: EVS-DEV-postgres-backend/K
    // an open through the generation guard runs as the runtime role: it
    //   records the component records and the data generation, takes the
    //   generation locks on its lock session, and a later transaction
    //   passes the fence read.
    test('an open through the generation guard runs as the runtime '
        'role', () async {
      await roles.reset();
      final w = await _World.open(await roles.openRuntimeBackend());
      addTearDown(w.close);
      expect(w.backend.generationStatus, GenerationStatus.registered);
      final keys = await _RuntimeDatabase(roles).backendStateKeys();
      expect(keys, contains('data_generation'));
      expect(
        keys.where((k) => k.startsWith('generation_component_')),
        isNotEmpty,
      );
      final pid = (await w.backend.lockSessionForTest()).pid;
      final held = await roles.asOwner(
        (c) => c.execute(
          Sql.named(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
            'AND granted AND pid = @p',
          ),
          parameters: <String, Object?>{'p': pid},
        ),
      );
      expect(held.first[0]! as int, greaterThan(0));
      await w.note('after-open');
    });

    for (final separateLockRole in <bool>[false, true]) {
      // Verifies: EVS-DEV-postgres-backend/J+K
      // a probe that outlasts the query timeout declares the lock session
      //   lost, and the replacement, as the runtime role or as a lock role
      //   holding only the backend_state privileges, reads the lock and
      //   activity catalogs and ends its own old server session.
      test('a stalled probe is replaced and the old session ended by the '
          '${separateLockRole ? 'lock' : 'runtime'} role', () async {
        await roles.reset();
        final timers = _Timers();
        var stall = false;
        final backend = await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            timerFactory: timers.create,
            stallLockHeartbeatPastQueryTimeout: () => stall,
            failLostSessionClose: () => true,
          ),
          () => roles.openRuntimeBackend(
            lockQueryTimeout: const Duration(seconds: 1),
            separateLockRole: separateLockRole,
          ),
        );
        final w = await _World.open(backend);
        addTearDown(w.close);
        final oldPid = (await backend.lockSessionForTest()).pid;
        stall = true;
        timers.fireAll();
        await _until(() async {
          if (backend.generationStatus != GenerationStatus.registered) {
            return false;
          }
          try {
            return (await backend.lockSessionForTest()).pid != oldPid;
          } on Object {
            return false;
          }
        });
        stall = false;
        await _until(
          () async =>
              (await roles.asOwner(
                (c) => c.execute(
                  Sql.named(
                    'SELECT count(*) FROM pg_stat_activity WHERE pid = @p',
                  ),
                  parameters: <String, Object?>{'p': oldPid},
                ),
              )).first[0] ==
              0,
        );
        expect((await backend.lockSessionForTest()).pid, isNot(oldPid));
        await w.note('after-replacement');
      });
    }

    // Verifies: EVS-DEV-postgres-backend/K
    // a lock session opened as a role that holds only USAGE on the schema
    //   and the runtime role's privileges on backend_state carries an
    //   event store's boot and a delivery cycle's drain lock.
    test('a lock role holding only the backend_state privileges carries '
        'the boot and the drain lock', () async {
      await roles.reset();
      final w = await _World.open(
        await roles.openRuntimeBackend(separateLockRole: true),
      );
      addTearDown(w.close);
      final lockPid = (await w.backend.lockSessionForTest()).pid;
      final user = await roles.asOwner(
        (c) => c.execute(
          Sql.named('SELECT usename FROM pg_stat_activity WHERE pid = @p'),
          parameters: <String, Object?>{'p': lockPid},
        ),
      );
      expect(user.first[0], _lock);
      final d = FakeDestination(id: 'x');
      await w.activate(d);
      final cycle = await SyncCycle.start(
        registry: w.registry,
        cadence: const Duration(milliseconds: 50),
      );
      addTearDown(cycle.close);
      expect(cycle.state, SyncCycleState.running);
      await w.note('delivered');
      await _until(() => d.sent.isNotEmpty);
      await cycle.close();
    });

    // Verifies: EVS-DEV-postgres-backend/K
    // every privilege in the documented set is needed: for each one, a role
    //   granted the set without it fails a named case with a permission
    //   error on its table; a privilege no case needs is reported and fails.
    test('every granted privilege is needed by a named case', () async {
      final unmapped = <String>[
        for (final MapEntry(key: table, value: privileges)
            in postgresRuntimeRoleGrants.entries)
          for (final privilege in privileges)
            if (!_neededBy.containsKey((table, privilege))) '$table $privilege',
      ];
      expect(unmapped, isEmpty, reason: 'granted but needed by no case');
      final extra = <String>[
        for (final (table, privilege) in _neededBy.keys)
          if (!(postgresRuntimeRoleGrants[table]?.contains(privilege) ?? false))
            '$table $privilege',
      ];
      expect(extra, isEmpty, reason: 'mapped but not granted');

      final unneeded = <String>[];
      for (final MapEntry(key: (table, privilege), value: name)
          in _neededBy.entries) {
        final testCase = _cases[name]!;
        await roles.reset();
        final opened = <Object?>[];
        Object? arranged;
        Object? error;
        final log = <LibraryLogRecord>[];
        try {
          arranged = await testCase.arrange(roles);
          opened.add(arranged);
          await roles.revoke(table, privilege);
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(onLog: log.add),
            () => testCase.act(roles, arranged),
          );
        } on Object catch (e) {
          error = e;
        } finally {
          for (final o in opened) {
            final world = switch (o) {
              final _World w => w,
              (final _World w, _) => w,
              _ => null,
            };
            if (world != null) {
              try {
                await world.close();
              } on Object catch (_) {
                // A world whose store failed part-way closes what it can.
              }
            }
          }
        }
        if (!_deniedOn(table, error, log)) {
          unneeded.add(
            '$table $privilege (case "$name": ${error ?? 'succeeded'})',
          );
        }
      }
      expect(unneeded, isEmpty, reason: 'granted privileges no case needs');
    });
  });
}

/// A timer the test fires by hand.
class _ManualTimer implements Timer {
  _ManualTimer(this._callback);
  final void Function(Timer) _callback;
  bool _active = true;
  int _ticks = 0;

  void fire() {
    if (!_active) return;
    _ticks++;
    _callback(this);
  }

  @override
  void cancel() => _active = false;

  @override
  bool get isActive => _active;

  @override
  int get tick => _ticks;
}

/// The timers the backend created in its zone.
class _Timers {
  final List<_ManualTimer> all = <_ManualTimer>[];

  Timer create(Duration _, void Function(Timer) callback) {
    final timer = _ManualTimer(Zone.current.bindUnaryCallback(callback));
    all.add(timer);
    return timer;
  }

  void fireAll() {
    for (final t in all) {
      t.fire();
    }
  }
}

Future<void> _until(
  FutureOr<bool> Function() condition, {
  Duration within = const Duration(seconds: 15),
}) async {
  final deadline = DateTime.now().add(within);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition not reached');
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
}
