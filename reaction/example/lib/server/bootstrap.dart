// reaction/example/lib/server/bootstrap.dart
//
// Composes the substrate (in-memory sembast) + ReactionHandlers +
// per-user role-aware auth + a `/admin/revoke` demo endpoint into a
// single shelf Router, and returns a dispose() callback.
//
// The example exercises the substrate's full permission model:
//
//   - Three roles (viewer, editor, admin) seeded with different grants
//   - One scope class (`workspace`) with two values (`west`, `east`)
//   - Four seeded users with different assignments — see _seedUsers
//   - Three actions: `submit_note` (workspace-scoped),
//     `assign_role` and `unassign_role` (admin, unscoped)
//   - One unscoped data view: `notes_today` (rows carry their workspace)
//
// State is ephemeral — every server restart begins fresh.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_example/server/notes_projection.dart';
import 'package:reaction_example/server/role_admin_actions.dart';
import 'package:reaction_example/server/role_aware_auth_validator.dart';
import 'package:reaction_example/server/submit_note_action.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

/// Bundle returned by [bootstrap]: the composed top-level shelf router
/// (ready for `shelf_io.serve`) and a `dispose` callback that tears
/// down the substrate + AuthorizationWatcher on graceful shutdown.
class BootstrapResult {
  BootstrapResult({required this.router, required this.dispose});

  final Router router;
  final Future<void> Function() dispose;
}

/// Single source of truth for the demo's seeded users + their starting
/// role/scope assignments. Surfaced as a typed list so tests and the
/// /admin/revoke handler can introspect it without duplicating the seed
/// definition.
class _SeededUser {
  const _SeededUser({
    required this.userId,
    required this.role,
    required this.scope,
  });
  final String userId;
  final String role;
  final ScopeValue scope;
}

const List<_SeededUser> _seedUsers = <_SeededUser>[
  // alice / bob are workspace-bound editors — alice can submit notes in
  // 'west' only, bob in 'east' only. Demonstrates BoundScope grants.
  _SeededUser(
    userId: 'alice',
    role: 'editor',
    scope: BoundScope(class_: 'workspace', value: 'west'),
  ),
  _SeededUser(
    userId: 'bob',
    role: 'editor',
    scope: BoundScope(class_: 'workspace', value: 'east'),
  ),
  // carol is admin with TotalWildcardScope — covers assign_role
  // (unscoped) AND submit_note in any workspace.
  _SeededUser(userId: 'carol', role: 'admin', scope: TotalWildcardScope()),
  // dave is a viewer (read-only). Has view:notes_today but no
  // submit_note; the UI hides the submit form for him.
  _SeededUser(userId: 'dave', role: 'viewer', scope: TotalWildcardScope()),
];

/// Wire up an ephemeral in-memory substrate, register the demo actions,
/// projections, scope classes, role grants, and user-role-scope
/// assignments, then compose the reaction HTTP+WS pipeline plus the
/// `/admin/revoke` demo route.
Future<BootstrapResult> bootstrap() async {
  // --- Storage: in-memory sembast (ephemeral). ---
  final db = await newDatabaseFactoryMemory().openDatabase(
    'reaction-example.db',
  );
  final backend = SembastBackend(database: db);

  // --- Entry types ---
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes
    ..register(
      const EntryTypeDefinition(id: 'note', registeredVersion: 1, name: 'Note'),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'role_permission_grant',
        registeredVersion: 1,
        name: 'Role-Permission Grant',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: 1,
        name: 'User-Role-Scope Assignment',
      ),
    )
    // ActionDispatcher emits this on denial.
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: 1,
        name: 'Action Denial',
      ),
    );

  // --- Projections ---
  final projections = ProjectionRegistry()
    ..register(rolePermissionGrantsSpec)
    ..register(userRoleScopesSpec)
    ..register(notesTodayProjection);

  // --- Security context store ---
  final securityContexts = SembastSecurityContextStore(backend: backend);

  // --- EventStore ---
  final eventStore = await EventStore.open(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'reaction-example',
      identifier: 'reaction-example-install-1',
      softwareVersion: 'reaction_example@0.1.0',
    ),
    securityContexts: securityContexts,
    projections: projections,
  );

  // --- Scope classes: one top-level class `workspace`. ---
  // No containment: the demo doesn't model parent-of-workspace, so a
  // bare ScopeClassSpec is the minimum viable registration. The class
  // appears in BoundScope.class_ on role_assigned/submit_note dispatch.
  final scopeClassRegistry = ScopeClassRegistry(
    classes: const <ScopeClassSpec>[ScopeClassSpec(name: 'workspace')],
    projectionLookup: (_) => null,
  );
  final policy = TableBackedAuthorizationPolicy(
    backend: backend,
    scopeClassRegistry: scopeClassRegistry,
    transactionProvider: <T>(fn) => backend.transaction<T>(fn),
  );

  // --- Action dispatcher ---
  final dispatcher = bootstrapAuditedActions(
    events: eventStore,
    authorization: policy,
    idempotency: InMemoryIdempotencyStore(),
    actions: <Action<Object?, Object?>>[
      SubmitNoteAction(),
      const AssignRoleAction(),
      const UnassignRoleAction(),
    ],
  );

  // --- Seed role-permission grants ---
  //
  // viewer  -> {view:notes_today}
  // editor  -> {view:notes_today, submit_note}
  // admin   -> {view:notes_today, submit_note, assign_role,
  //             unassign_role, view:user_role_scopes,
  //             view:role_permission_grants}
  //
  // The admin role gets `view:user_role_scopes` so the admin-panel UI
  // can subscribe to the seeded assignments view (default
  // ViewPermissionNamer is `view:<viewName>`); seed accordingly.
  const Map<String, List<String>> rolePerms = <String, List<String>>{
    'viewer': <String>['view:notes_today'],
    'editor': <String>['view:notes_today', 'submit_note'],
    'admin': <String>[
      'view:notes_today',
      'submit_note',
      'assign_role',
      'unassign_role',
      'view:user_role_scopes',
      'view:role_permission_grants',
    ],
  };
  for (final entry in rolePerms.entries) {
    final role = entry.key;
    for (final perm in entry.value) {
      await eventStore.append(
        entryType: 'role_permission_grant',
        aggregateType: 'role_permission_grant',
        aggregateId: '$role:$perm',
        eventType: 'permission_granted',
        data: PermissionGrantedPayload(
          role: role,
          permissionName: perm,
        ).toJson(),
        initiator: const AutomationInitiator(service: 'reaction-example-seed'),
      );
    }
  }

  // --- Seed user-role-scope assignments ---
  for (final user in _seedUsers) {
    await eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: computeRoleAssignmentAggregateId(
        userId: user.userId,
        role: user.role,
        scope: user.scope,
      ),
      eventType: 'role_assigned',
      data: RoleAssignedPayload(
        userId: user.userId,
        role: user.role,
        scope: user.scope,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction-example-seed'),
    );
  }

  // --- Reaction handlers ---
  //
  // RoleAwareTrustingValidator reads `user_role_scopes` at authenticate-
  // time and picks the highest-privileged role the user holds: alice
  // and bob both surface as `editor`, carol as `admin`, dave as
  // `viewer`. Unknown bearers (any non-empty string with no role
  // assignment) come back as AnonymousPrincipal — substrate then denies
  // every action against them, demonstrating the closed-under-events
  // trust model end-to-end.
  final validator = RoleAwareTrustingValidator(backend: backend);
  final reactionHandlers = ReactionHandlers(
    eventStore: eventStore,
    dispatcher: dispatcher,
    policy: policy,
  );

  final httpRouter = Router()
    ..get('/me', reactionHandlers.me)
    ..post('/actions', reactionHandlers.actions)
    ..get('/permissions/snapshot', reactionHandlers.permissions);

  final httpPipeline = const Pipeline()
      .addMiddleware(authMiddleware(validator))
      .addHandler(httpRouter.call);

  // /admin/revoke is a no-auth admin escape hatch: it appends a
  // role_unassigned event for the supplied user, AuthorizationWatcher closes
  // their WS with 4003, the client flips to Expired. Production
  // deployments wouldn't expose this — the in-app `unassign_role`
  // action (admin-gated) is the primary mechanism. Useful for testing
  // the force-logout path without first acquiring an admin session.
  Future<Response> revokeHandler(Request request) async {
    final userId = request.url.queryParameters['user'];
    if (userId == null || userId.isEmpty) {
      return Response(400, body: 'missing ?user=<userId>');
    }
    final user = _seedUsers.firstWhere(
      (u) => u.userId == userId,
      orElse: () => const _SeededUser(
        userId: '',
        role: 'editor',
        scope: TotalWildcardScope(),
      ),
    );
    if (user.userId.isEmpty) {
      return Response(404, body: 'unknown user $userId\n');
    }
    await eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: computeRoleAssignmentAggregateId(
        userId: user.userId,
        role: user.role,
        scope: user.scope,
      ),
      eventType: 'role_unassigned',
      data: RoleUnassignedPayload(
        userId: user.userId,
        role: user.role,
        scope: user.scope,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction-example-admin'),
    );
    return Response.ok('revoked ${user.role} for ${user.userId}\n');
  }

  final topRouter = Router()
    ..get('/subscriptions', reactionHandlers.subscriptions(validator))
    ..post('/admin/revoke', revokeHandler)
    ..mount('/', httpPipeline);

  Future<void> dispose() async {
    await reactionHandlers.dispose();
    await eventStore.close();
  }

  return BootstrapResult(router: topRouter, dispose: dispose);
}
