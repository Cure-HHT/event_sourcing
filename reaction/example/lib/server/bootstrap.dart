// reaction/example/lib/server/bootstrap.dart
//
// Composes the substrate (in-memory sembast) + ReactionHandlers + a
// thin `/admin/revoke` demo endpoint into a single shelf Router and
// returns a dispose() callback.
//
// Mirrors `reaction/test/e2e/test_support/reaction_remote_test_harness.dart`
// but stripped to one ProjectionSpec, one Action, and one extra admin
// route. State is ephemeral — every server restart begins fresh.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_example/server/notes_projection.dart';
import 'package:reaction_example/server/submit_note_action.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

/// Bundle returned by [bootstrap]: the composed top-level shelf router
/// (ready for `shelf_io.serve`) and a `dispose` callback that tears
/// down the substrate + AuthzWatcher on graceful shutdown.
class BootstrapResult {
  BootstrapResult({required this.router, required this.dispose});

  final Router router;
  final Future<void> Function() dispose;
}

/// Wire up an ephemeral in-memory substrate, register the demo action
/// and projection, seed the `editor` role's grants, and compose the
/// reaction HTTP + WS pipeline plus the demo `/admin/revoke` route.
Future<BootstrapResult> bootstrap() async {
  // --- Storage: in-memory sembast (ephemeral). ---
  final db = await newDatabaseFactoryMemory().openDatabase(
    'reaction-example.db',
  );
  final backend = SembastBackend(database: db);

  // --- Entry types ---
  final entryTypes = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    entryTypes.register(defn);
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
    // Required by AuthzWatcher (force-logout path) and by the demo's
    // /admin/revoke endpoint.
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
    // The TableBackedAuthorizationPolicy now reads user_role_scopes to
    // verify (userId, activeRole) membership on every authorize call
    // (closed-under-events trust model): the Principal's activeRole
    // claim from TrustingAuthValidator is not honoured until a
    // `role_assigned` event is in the log for that user.
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

  // --- Permissions ---
  // The demo's two permissions (`submit_note`, `view:notes_today`) are
  // both unscoped, so an empty ScopeClassRegistry is sufficient.
  final scopeClassRegistry = ScopeClassRegistry(
    classes: const <ScopeClassSpec>[],
    projectionLookup: (_) => null,
  );
  final policy = TableBackedAuthorizationPolicy(
    backend: backend,
    scopeClassRegistry: scopeClassRegistry,
    txnProvider: <T>(fn) => backend.transaction<T>(fn),
  );

  // --- Action dispatcher ---
  final dispatcher = bootstrapAuditedActions(
    events: eventStore,
    authorization: policy,
    idempotency: InMemoryIdempotencyStore(),
    actions: <Action<Object?, Object?>>[SubmitNoteAction()],
  );

  // --- Seed role grants: editor -> {submit_note, view:notes_today} ---
  for (final perm in const <String>['submit_note', 'view:notes_today']) {
    await eventStore.append(
      entryType: 'role_permission_grant',
      aggregateType: 'role_permission_grant',
      aggregateId: 'editor:$perm',
      eventType: 'permission_granted',
      data: PermissionGrantedPayload(
        role: 'editor',
        permissionName: perm,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction-example-seed'),
    );
  }

  // --- Seed user-role-scope assignments for the demo's known users ---
  //
  // The substrate verifies (userId, activeRole) membership against
  // user_role_scopes for every authorize call — without these
  // role_assigned events, alice/bob/carol log in but every action is
  // denied with `Denied: submit_note`. We seed a TotalWildcardScope
  // assignment for each: it covers the demo's unscoped permissions
  // without over-granting any scoped one (the demo has none).
  //
  // Trying to log in as any OTHER username (e.g. `dave`) succeeds at
  // the auth-validator boundary (TrustingAuthValidator accepts any
  // non-empty credential) but then the substrate refuses to honour the
  // claimed `editor` role — every dispatch denies. That demonstrates
  // the closed-under-events trust model: identity is auth's job, but
  // role-membership comes from the event log.
  for (final userId in const <String>['alice', 'bob', 'carol']) {
    await eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: roleAssignmentAggregateId(
        userId: userId,
        role: 'editor',
        scope: const TotalWildcardScope(),
      ),
      eventType: 'role_assigned',
      data: RoleAssignedPayload(
        userId: userId,
        role: 'editor',
        scope: const TotalWildcardScope(),
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction-example-seed'),
    );
  }

  // --- Reaction handlers ---
  final validator = TrustingAuthValidator(defaultActiveRole: 'editor');
  final reactionHandlers = ReactionHandlers(
    eventStore: eventStore,
    dispatcher: dispatcher,
    policy: policy,
  );

  // HTTP routes are gated by Bearer-token auth middleware. The WS
  // upgrade path (`/subscriptions`) is NOT — credentials arrive
  // in-band via the first WS AuthMsg.
  final httpRouter = Router()
    ..get('/me', reactionHandlers.me)
    ..post('/actions', reactionHandlers.actions)
    ..get('/permissions/snapshot', reactionHandlers.permissions);

  final httpPipeline = const Pipeline()
      .addMiddleware(authMiddleware(validator))
      .addHandler(httpRouter.call);

  // /admin/revoke appends a role_unassigned event so the AuthzWatcher
  // closes the user's WS with 4003 → client RemoteAuthSession flips to
  // Expired → UI routes to the expired screen.
  //
  // DEMO ONLY: in production, this would be gated behind admin auth.
  Future<Response> revokeHandler(Request request) async {
    final userId = request.url.queryParameters['user'];
    if (userId == null || userId.isEmpty) {
      return Response(400, body: 'missing ?user=<userId>');
    }
    await eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: roleAssignmentAggregateId(
        userId: userId,
        role: 'editor',
        scope: const TotalWildcardScope(),
      ),
      eventType: 'role_unassigned',
      data: RoleUnassignedPayload(
        userId: userId,
        role: 'editor',
        scope: const TotalWildcardScope(),
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction-example-admin'),
    );
    return Response.ok('revoked role for $userId\n');
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
