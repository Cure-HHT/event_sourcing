// test/permissions/converging_permission_views_test.dart
// Verifies: EVS-DEV-converging-view-reads/H
// a submission the policy would decide from a converging role-assignment,
//   permission-grant, or containment view is refused with the typed,
//   transient ViewConvergingRefusal naming the view; the dispatcher appends
//   no event, and the same submission succeeds once the view is current.
//   PermissionSeedApplier.apply and bootstrapRoleAssignments never decide
//   from a converging read either: a read taken converging after their
//   wait already reported the view current is discarded and they wait
//   again, so a role-permission-grants or user-role-scopes view that
//   converges in that narrow window still yields no duplicate event.
// Verifies: EVS-DEV-converging-view-reads/I
// bootstrapRoleAssignments waits until the view it reads is current before
//   reading it, succeeding once the copy catches up within the caller's
//   deadline, and throws ViewConvergenceTimeout naming the view and its
//   copy's progress once the deadline passes first. PermissionSeedApplier
//   .apply does the same for role_permission_grants.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../actions/fixtures/test_actions.dart'
    show HelloAction, ScopedPatientEditAction;
import 'test_support/policy_harness.dart' show patientSiteIndexSpec;

/// Fails every catch-up step so a copy this test has rewound behind the
/// log stays converging for as long as the hook is installed
/// (EVS-DEV-view-convergence/Q backs the driver off, so a store opened
/// under this hook never catches up on its own).
void _pauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure(
      'paused for a converging-permission-views test',
    );

var _dbCounter = 0;

/// One in-memory Sembast database shared by a paused [EventStore] (used to
/// exercise the converging path) and, once a test wants the copy to catch
/// up, a second, unpaused [EventStore] opened on the same backend with the
/// same registrations. Views are stored per definition
/// (EVS-DEV-view-convergence), so the two instances compute the same copy
/// id and the second instance's driver catches up the copy the first
/// instance reads.
Future<sembast.Database> _openDb() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'converging-permission-views-$_dbCounter.db',
  );
}

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Action denial',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'greeting',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Greeting',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'role_permission_grant',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Role-permission grant',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'User-role-scope assignment',
      ),
    );
}

ProjectionRegistry _projections() => ProjectionRegistry()
  ..register(rolePermissionGrantsSpec)
  ..register(userRoleScopesSpec);

Future<EventStore> _openWith(SembastBackend backend) => EventStore.openForTest(
  storage: backend,
  entryTypes: _entryTypes(),
  source: const Source(
    hopId: 'test-server',
    identifier: 'test-instance-1',
    softwareVersion: 'event_sourcing_test@0.0.0',
  ),
  securityContexts: SembastSecurityContextStore(backend: backend),
  projections: _projections(),
);

Future<EventStore> _openStore(
  SembastBackend backend, {
  DeliveryTestHooks? hooks,
}) {
  return hooks == null
      ? _openWith(backend)
      : runWithDeliveryTestHooks(hooks, () => _openWith(backend));
}

Future<EventStore> _openWithScoped(SembastBackend backend) =>
    EventStore.openForTest(
      storage: backend,
      entryTypes: _entryTypesScoped(),
      source: const Source(
        hopId: 'test-server',
        identifier: 'test-instance-1',
        softwareVersion: 'event_sourcing_test@0.0.0',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: _projectionsScoped(),
    );

Future<EventStore> _openStoreScoped(
  SembastBackend backend, {
  DeliveryTestHooks? hooks,
}) {
  return hooks == null
      ? _openWithScoped(backend)
      : runWithDeliveryTestHooks(hooks, () => _openWithScoped(backend));
}

Future<void> _grant(
  EventStore store, {
  required String role,
  required String perm,
}) => store.append(
  entryType: 'role_permission_grant',
  aggregateType: 'role_permission_grant',
  aggregateId: '$role:$perm',
  eventType: 'permission_granted',
  data: PermissionGrantedPayload(role: role, permissionName: perm).toJson(),
  initiator: const AutomationInitiator(service: 'test'),
);

Future<void> _assign(
  EventStore store, {
  required String userId,
  required String role,
  ScopeValue scope = const TotalWildcardScope(),
}) => store.append(
  entryType: 'user_role_scope',
  aggregateType: 'user_role_scope',
  aggregateId: computeRoleAssignmentAggregateId(
    userId: userId,
    role: role,
    scope: scope,
  ),
  eventType: 'role_assigned',
  data: RoleAssignedPayload(userId: userId, role: role, scope: scope).toJson(),
  initiator: const AutomationInitiator(service: 'test'),
);

Future<void> _rewindWatermark(
  EventStore store,
  SembastBackend backend,
  String viewName,
  int watermark,
) async {
  final copyId = store.copyIdOf(viewName);
  await backend.transaction(
    (txn) => backend.setViewCopyWatermarkInTxn(txn, copyId, watermark),
  );
}

/// Starts a transaction on [backend] and holds it open until [release]
/// completes, returning only once the transaction has begun. `SembastBackend`
/// serializes its transactions (`StorageBackend.transaction`), so every
/// later `backend.transaction` call -- including the ones a store's own
/// operations make -- queues up behind this one in the order it was made:
/// releasing it then runs those queued transactions in that fixed order,
/// deterministically, with no production test hook and no timing race.
Future<void> _holdTransaction(
  SembastBackend backend,
  Future<void> release,
) async {
  final started = Completer<void>();
  unawaited(
    backend.transaction((txn) async {
      started.complete();
      await release;
    }),
  );
  await started.future;
}

Future<void> _waitUntilCurrent(EventStore store, String viewName) async {
  for (var i = 0; i < 200; i++) {
    final progress = await store.reader.viewProgress();
    final view = progress.singleWhere((p) => p.viewName == viewName);
    if (view.state == ViewConvergenceState.current) return;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw StateError('$viewName did not converge in time');
}

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'u1',
    roles: const {'admin'},
    activeRole: 'admin',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.parse('2026-04-22T12:00:00Z'),
);

TableBackedAuthorizationPolicy _policy(EventStore store) =>
    TableBackedAuthorizationPolicy(
      reader: store.reader,
      scopeClassRegistry: ScopeClassRegistry(
        classes: const [],
        projectionLookup: (_) => null,
      ),
    );

ActionDispatcher _dispatcher(EventStore store) => ActionDispatcher(
  registry: ActionRegistry()..register(HelloAction()),
  authorization: _policy(store),
  events: store,
  idempotency: InMemoryIdempotencyStore(),
);

/// Declares the columns `patient_site_index` produces, for the scope-class
/// registry's composition-time column validation.
class _PatientSiteIndexDescriptor implements ScopeProjectionDescriptor {
  const _PatientSiteIndexDescriptor();
  @override
  Set<String> get columns => const {'patient_id', 'site_id'};
}

/// Entry types for the scoped-permission tests: the unscoped set plus
/// `patient_site_assignment`, whose events feed `patient_site_index`.
EntryTypeRegistry _entryTypesScoped() => _entryTypes()
  ..register(
    const EntryTypeDefinition(
      id: 'patient_site_assignment',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Patient-to-site assignment',
    ),
  );

ProjectionRegistry _projectionsScoped() =>
    _projections()..register(patientSiteIndexSpec);

Future<void> _setPatientSite(
  EventStore store, {
  required String patientId,
  required String siteId,
}) => store.append(
  entryType: 'patient_site_assignment',
  aggregateType: 'patient_site_assignment',
  aggregateId: patientId,
  eventType: 'patient_site_set',
  data: <String, Object?>{'patient_id': patientId, 'site_id': siteId},
  initiator: const AutomationInitiator(service: 'test'),
);

/// site top-level; patient contained in site via `patient_site_index`,
/// matching [_setPatientSite]'s payload shape.
final _scopedRegistry = ScopeClassRegistry(
  classes: const [
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'patient',
      containedIn: ContainmentReference(
        parentClass: 'site',
        projection: 'patient_site_index',
        keyColumn: 'patient_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: (name) =>
      name == 'patient_site_index' ? const _PatientSiteIndexDescriptor() : null,
);

TableBackedAuthorizationPolicy _scopedPolicy(EventStore store) =>
    TableBackedAuthorizationPolicy(
      reader: store.reader,
      scopeClassRegistry: _scopedRegistry,
    );

ActionDispatcher _scopedDispatcher(EventStore store) => ActionDispatcher(
  registry: ActionRegistry()..register(const ScopedPatientEditAction()),
  authorization: _scopedPolicy(store),
  events: store,
  idempotency: InMemoryIdempotencyStore(),
);

void main() {
  group('converging permission views: authorization refuses transiently', () {
    test('refuses with ViewConvergingRefusal naming the view, appends no '
        'event, and leaves the log unchanged', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'test.hello');
      await _assign(store, userId: 'u1', role: 'admin');
      await _rewindWatermark(store, backend, 'user_role_scopes', 0);

      final beforeCount = (await store.reader.findAllEvents()).length;

      await expectLater(
        () => _dispatcher(store).dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        ),
        throwsA(
          isA<ViewConvergingRefusal>().having(
            (e) => e.viewName,
            'viewName',
            'user_role_scopes',
          ),
        ),
      );

      final afterCount = (await store.reader.findAllEvents()).length;
      expect(afterCount, beforeCount);
    });

    test('the same submission succeeds once the view is current', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'test.hello');
      await _assign(store, userId: 'u1', role: 'admin');
      await _rewindWatermark(store, backend, 'user_role_scopes', 0);

      await expectLater(
        () => _dispatcher(store).dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        ),
        throwsA(isA<ViewConvergingRefusal>()),
      );

      // A second, unpaused instance over the same backend and the same
      // registrations computes the same fingerprinted copy id and drives
      // it current (EVS-DEV-view-convergence: views are stored per
      // definition).
      final catchingUp = await _openStore(backend);
      addTearDown(catchingUp.close);
      await _waitUntilCurrent(catchingUp, 'user_role_scopes');

      final result = await _dispatcher(store).dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());
      final events = await store.reader.findAllEvents();
      expect(events.where((e) => e.eventType == 'hello.said'), hasLength(1));
    });
  });

  group('converging permission views: bootstrap waits with a deadline', () {
    test('bootstrapRoleAssignments waits and succeeds once the copy '
        'catches up within the deadline', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      // A decoy assignment folds into the copy, then the test rewinds the
      // watermark behind it so the copy is converging for `store`.
      await _assign(store, userId: 'decoy', role: 'observer');
      await _rewindWatermark(store, backend, 'user_role_scopes', 0);

      const seed = RoleAssignmentSeed(
        entries: [
          RoleAssignmentSeedEntry(
            userId: 'u1',
            role: 'admin',
            scope: TotalWildcardScope(),
          ),
        ],
      );
      final resultFuture = bootstrapRoleAssignments(
        eventStore: store,
        seed: seed,
        timeout: const Duration(seconds: 5),
      );

      // Drive the shared copy current from a second, unpaused instance.
      final catchingUp = await _openStore(backend);
      addTearDown(catchingUp.close);
      await _waitUntilCurrent(catchingUp, 'user_role_scopes');

      // entriesInViewNotInSeed proves the read that computed it happened
      // after the copy caught up: the decoy row was withheld while
      // converging, so a read taken before the wait would report it absent.
      final result = await resultFuture;
      expect(result.entriesEmitted, 1);
      expect(
        result.entriesInViewNotInSeed,
        contains(
          computeRoleAssignmentAggregateId(
            userId: 'decoy',
            role: 'observer',
            scope: const TotalWildcardScope(),
          ),
        ),
      );
    });

    test('bootstrapRoleAssignments throws ViewConvergenceTimeout naming the '
        'view and its progress once the deadline passes first', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _assign(store, userId: 'decoy', role: 'observer');
      await _rewindWatermark(store, backend, 'user_role_scopes', 0);

      const seed = RoleAssignmentSeed(
        entries: [
          RoleAssignmentSeedEntry(
            userId: 'u1',
            role: 'admin',
            scope: TotalWildcardScope(),
          ),
        ],
      );

      await expectLater(
        () => bootstrapRoleAssignments(
          eventStore: store,
          seed: seed,
          timeout: Duration.zero,
        ),
        throwsA(
          isA<ViewConvergenceTimeout>()
              .having(
                (e) => e.converging.map((s) => s.viewName),
                'converging view names',
                contains('user_role_scopes'),
              )
              .having(
                (e) => e.converging
                    .singleWhere((s) => s.viewName == 'user_role_scopes')
                    .watermark,
                'watermark',
                0,
              )
              .having(
                (e) => e.converging
                    .singleWhere((s) => s.viewName == 'user_role_scopes')
                    .logHead,
                'logHead',
                greaterThan(0),
              ),
        ),
      );
    });
  });

  group('converging permission views: containment and grant reads refuse '
      'transiently', () {
    test('a converging containment view the resolver reads while matching '
        'a scoped permission refuses with ViewConvergingRefusal naming it, '
        'and appends no event', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStoreScoped(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'patient.edit');
      // A site-scoped assignment (not a wildcard) so the match walks the
      // requested patient scope up to site via the containment resolver.
      await _assign(
        store,
        userId: 'u1',
        role: 'admin',
        scope: const BoundScope(class_: 'site', value: 'site-A'),
      );
      await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');
      // Only the containment view is rewound: user_role_scopes and
      // role_permission_grants must stay current so the read reaches the
      // resolver.
      await _rewindWatermark(store, backend, 'patient_site_index', 0);

      final beforeCount = (await store.reader.findAllEvents()).length;

      await expectLater(
        () => _scopedDispatcher(store).dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-1'},
          ),
          _ctx(),
        ),
        throwsA(
          isA<ViewConvergingRefusal>().having(
            (e) => e.viewName,
            'viewName',
            'patient_site_index',
          ),
        ),
      );

      final afterCount = (await store.reader.findAllEvents()).length;
      expect(afterCount, beforeCount);
    });

    test('the same submission succeeds once the containment view is '
        'current', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStoreScoped(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'patient.edit');
      await _assign(
        store,
        userId: 'u1',
        role: 'admin',
        scope: const BoundScope(class_: 'site', value: 'site-A'),
      );
      await _setPatientSite(store, patientId: 'p-1', siteId: 'site-A');
      await _rewindWatermark(store, backend, 'patient_site_index', 0);

      await expectLater(
        () => _scopedDispatcher(store).dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-1'},
          ),
          _ctx(),
        ),
        throwsA(isA<ViewConvergingRefusal>()),
      );

      final catchingUp = await _openStoreScoped(backend);
      addTearDown(catchingUp.close);
      await _waitUntilCurrent(catchingUp, 'patient_site_index');

      final result = await _scopedDispatcher(store).dispatch(
        const ActionSubmission(
          actionName: 'patient_edit',
          rawInput: <String, Object?>{'patient_id': 'p-1'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());
    });

    test('a converging role_permission_grants view refuses with '
        'ViewConvergingRefusal naming it, and appends no event', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'test.hello');
      await _assign(store, userId: 'u1', role: 'admin');
      await _rewindWatermark(store, backend, 'role_permission_grants', 0);

      final beforeCount = (await store.reader.findAllEvents()).length;

      await expectLater(
        () => _dispatcher(store).dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        ),
        throwsA(
          isA<ViewConvergingRefusal>().having(
            (e) => e.viewName,
            'viewName',
            'role_permission_grants',
          ),
        ),
      );

      final afterCount = (await store.reader.findAllEvents()).length;
      expect(afterCount, beforeCount);
    });

    test('PermissionSeedApplier.apply throws ViewConvergenceTimeout naming '
        'role_permission_grants once the deadline passes first', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'observer', perm: 'test.decoy');
      await _rewindWatermark(store, backend, 'role_permission_grants', 0);

      final applier = PermissionSeedApplier(
        eventStore: store,
        seedInitiator: const AutomationInitiator(service: 'test'),
      );
      const seed = PermissionSeed(
        roles: {'admin'},
        grants: {
          'admin': {'test.hello'},
        },
      );

      await expectLater(
        () => applier.apply(seed, {
          const Permission('test.hello'),
        }, timeout: Duration.zero),
        throwsA(
          isA<ViewConvergenceTimeout>().having(
            (e) => e.converging.map((s) => s.viewName),
            'converging view names',
            contains('role_permission_grants'),
          ),
        ),
      );
    });
  });

  group('converging permission views: seed and bootstrap decide only from '
      'a current read', () {
    test('PermissionSeedApplier.apply does not re-grant an already-present '
        'permission when role_permission_grants converges between its wait '
        'and its read', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _grant(store, role: 'admin', perm: 'test.hello');

      final applier = PermissionSeedApplier(
        eventStore: store,
        seedInitiator: const AutomationInitiator(service: 'test'),
      );
      const seed = PermissionSeed(
        roles: {'admin'},
        grants: {
          'admin': {'test.hello'},
        },
      );

      // Sequence, via the backend's serialized transaction queue: (1) a
      // held transaction; (2) apply()'s waitForViewsCurrent poll, which
      // sees the copy current and returns; (3) a rewind that makes the
      // copy converging again; (4) apply()'s findViewRows, enqueued only
      // once apply() resumes past its wait -- after (3). Releasing (1)
      // runs (2), (3) and (4) in that fixed order.
      final release = Completer<void>();
      await _holdTransaction(backend, release.future);
      final applyFuture = applier.apply(seed, {const Permission('test.hello')});
      final rewindFuture = _rewindWatermark(
        store,
        backend,
        'role_permission_grants',
        0,
      );
      release.complete();
      await rewindFuture;

      // Let apply()'s findViewRows (item 4) run and observe the converging
      // state before a second, unpaused instance drives the copy back to
      // current for the deadline-bounded retry.
      await Future<void>.delayed(Duration.zero);
      final catchingUp = await _openStore(backend);
      addTearDown(catchingUp.close);
      await _waitUntilCurrent(catchingUp, 'role_permission_grants');

      final result = await applyFuture;

      expect(result.grantsEmitted, 0);
      final events = await store.reader.findAllEvents();
      expect(
        events.where((e) => e.eventType == 'permission_granted'),
        hasLength(1),
      );
    });

    test('bootstrapRoleAssignments does not re-assign an already-present '
        'role when user_role_scopes converges between its wait and its '
        'read', () async {
      final db = await _openDb();
      final backend = SembastBackend(database: db);
      final store = await _openStore(
        backend,
        hooks: const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
      );
      addTearDown(store.close);

      await _assign(store, userId: 'u1', role: 'admin');

      const seed = RoleAssignmentSeed(
        entries: [
          RoleAssignmentSeedEntry(
            userId: 'u1',
            role: 'admin',
            scope: TotalWildcardScope(),
          ),
        ],
      );

      final release = Completer<void>();
      await _holdTransaction(backend, release.future);
      final bootstrapFuture = bootstrapRoleAssignments(
        eventStore: store,
        seed: seed,
      );
      final rewindFuture = _rewindWatermark(
        store,
        backend,
        'user_role_scopes',
        0,
      );
      release.complete();
      await rewindFuture;

      await Future<void>.delayed(Duration.zero);
      final catchingUp = await _openStore(backend);
      addTearDown(catchingUp.close);
      await _waitUntilCurrent(catchingUp, 'user_role_scopes');

      final result = await bootstrapFuture;

      expect(result.entriesEmitted, 0);
      final events = await store.reader.findAllEvents();
      expect(events.where((e) => e.eventType == 'role_assigned'), hasLength(1));
    });
  });
}
