// example_clinical_scopes/lib/server/clinical_bootstrap.dart
//
// Composition root for the clinical-scopes demo. Mirrors
// `reaction/example/lib/server/bootstrap.dart` but with a region ->
// site -> participant hierarchy and Investigator / Overseer / Admin
// roles. Wires the PRODUCTION ScopeDescendantExpander by registering a
// multi-hop ScopeClassRegistry with real containment indexes and passing
// it (plus a ViewScopeRegistry binding `participants`) to ReactionHandlers.
//
// The demo exercises the substrate's hierarchy-scoped READ path:
//
//   - Three scope classes: region -> site -> participant (containment).
//   - Two containment indexes: site_region_index, participant_site_index.
//   - Three roles: Investigator (site-bound), Overseer (region-bound),
//     Admin (TotalWildcardScope) — all granted view:participants.
//   - Four seeded users — see _seedUsers.
//
// State is ephemeral — every server restart begins fresh.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:example_clinical_scopes/server/clinical_projections.dart';
import 'package:example_clinical_scopes/server/role_aware_auth_validator.dart';
import 'package:reaction/reaction.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

/// Bundle returned by [bootstrap]: the composed top-level shelf router
/// (ready for `shelf_io.serve`) and a `dispose` callback that tears down
/// the substrate + AuthorizationWatcher on graceful shutdown.
class BootstrapResult {
  BootstrapResult({required this.router, required this.dispose});

  final Router router;
  final Future<void> Function() dispose;
}

/// Describes a containment index's columns so [ScopeClassRegistry] can
/// validate the keyColumn / parentColumn references resolve. Both demo
/// indexes use `WholePayload`, so the columns are the payload keys.
class _IndexDescriptor implements ScopeProjectionDescriptor {
  const _IndexDescriptor(this.columns);

  @override
  final Set<String> columns;
}

/// A seeded demo user and their starting role/scope assignment.
class _SeededUser {
  const _SeededUser({
    required this.userId,
    required this.role,
    required this.scopes,
  });
  final String userId;
  final String role;

  /// One user may hold several assignments under the same role (e.g. an
  /// Investigator covering more than one site).
  final List<ScopeValue> scopes;
}

const List<_SeededUser> _seedUsers = <_SeededUser>[
  // Investigator assigned to two sites in region-West (A + B). Should see
  // every participant at site-A and site-B (one-hop expansion), but NOT
  // P-9 at site-C in region-East.
  _SeededUser(
    userId: 'dr-investigator',
    role: 'Investigator',
    scopes: <ScopeValue>[
      BoundScope(class_: 'site', value: 'site-A'),
      BoundScope(class_: 'site', value: 'site-B'),
    ],
  ),
  // Overseer assigned to a whole region (West). Should see the same
  // participants via a TWO-hop expansion region -> site -> participant,
  // again excluding P-9 (region-East).
  _SeededUser(
    userId: 'dr-overseer',
    role: 'Overseer',
    scopes: <ScopeValue>[BoundScope(class_: 'region', value: 'region-West')],
  ),
  // Admin with a TotalWildcardScope — sees every participant including
  // P-9.
  _SeededUser(
    userId: 'dr-admin',
    role: 'Admin',
    scopes: <ScopeValue>[TotalWildcardScope()],
  ),
  // NOTE: 'dr-unassigned' is deliberately absent from _seedUsers. It is a
  // test/UI credential with NO role_assigned event in the log, so the
  // RoleAwareTrustingValidator surfaces it as an AnonymousPrincipal and the
  // substrate denies it. Do NOT add it here with an empty scope — that would
  // change the demo's "unassigned sees nothing / is denied" behaviour.
];

/// One seeded site: aggregate id == site_id.
class _Site {
  const _Site({required this.id, required this.regionId, required this.name});
  final String id;
  final String regionId;
  final String name;
}

const List<_Site> _sites = <_Site>[
  _Site(id: 'site-A', regionId: 'region-West', name: 'Site A'),
  _Site(id: 'site-B', regionId: 'region-West', name: 'Site B'),
  _Site(id: 'site-C', regionId: 'region-East', name: 'Site C'),
];

/// One seeded participant: aggregate id == participant_id.
class _Participant {
  const _Participant({
    required this.id,
    required this.siteId,
    required this.name,
  });
  final String id;
  final String siteId;
  final String name;
}

const List<_Participant> _participants = <_Participant>[
  _Participant(id: 'P-1', siteId: 'site-A', name: 'Ann'),
  _Participant(id: 'P-2', siteId: 'site-A', name: 'Bob'),
  _Participant(id: 'P-3', siteId: 'site-B', name: 'Cara'),
  _Participant(id: 'P-9', siteId: 'site-C', name: 'Dan'),
];

/// Wire up an ephemeral in-memory substrate with the region/site/
/// participant hierarchy, seed sites + participants + role grants + role
/// assignments, then compose the reaction HTTP+WS pipeline.
Future<BootstrapResult> bootstrap() async {
  // --- Storage: in-memory sembast (ephemeral). ---
  final db = await newDatabaseFactoryMemory().openDatabase(
    'clinical-scopes.db',
  );
  final backend = SembastBackend(database: db);

  // --- Entry types ---
  final entryTypes = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    entryTypes.register(definition);
  }
  entryTypes
    ..register(
      const EntryTypeDefinition(
        id: 'participant',
        registeredVersion: 1,
        name: 'Participant',
        isMaterialized: true,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'site',
        registeredVersion: 1,
        name: 'Site',
        isMaterialized: true,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'role_permission_grant',
        registeredVersion: 1,
        name: 'Role-Permission Grant',
        isMaterialized: false,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: 1,
        name: 'User-Role-Scope Assignment',
        isMaterialized: false,
      ),
    )
    // ActionDispatcher emits this on denial.
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: 1,
        name: 'Action Denial',
        isMaterialized: false,
      ),
    );

  // --- Projections ---
  final projections = ProjectionRegistry()
    ..register(rolePermissionGrantsSpec)
    ..register(userRoleScopesSpec)
    ..register(participantsProjection)
    ..register(participantSiteIndex)
    ..register(siteRegionIndex);

  // --- Security context store ---
  final securityContexts = SembastSecurityContextStore(backend: backend);

  // --- EventStore (production .open — appends a lib_version event). ---
  final eventStore = await EventStore.open(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'clinical-scopes-example',
      identifier: 'clinical-scopes-install-1',
      softwareVersion: 'example_clinical_scopes@0.1.0',
    ),
    securityContexts: securityContexts,
    projections: projections,
  );

  // --- Scope classes: region -> site -> participant containment chain.
  //     Each containment reference is validated against the columns of
  //     its index projection (WholePayload of the *_registered event). ---
  final scopeClassRegistry = ScopeClassRegistry(
    classes: const <ScopeClassSpec>[
      ScopeClassSpec(name: 'region'),
      ScopeClassSpec(
        name: 'site',
        containedIn: ContainmentReference(
          parentClass: 'region',
          projection: 'site_region_index',
          keyColumn: 'site_id',
          parentColumn: 'region_id',
        ),
      ),
      ScopeClassSpec(
        name: 'participant',
        containedIn: ContainmentReference(
          parentClass: 'site',
          projection: 'participant_site_index',
          keyColumn: 'participant_id',
          parentColumn: 'site_id',
        ),
      ),
    ],
    projectionLookup: (name) => switch (name) {
      'site_region_index' => const _IndexDescriptor({
        'site_id',
        'region_id',
        'name',
      }),
      'participant_site_index' => const _IndexDescriptor({
        'participant_id',
        'site_id',
        'name',
      }),
      _ => null,
    },
  );

  final policy = TableBackedAuthorizationPolicy(
    backend: backend,
    scopeClassRegistry: scopeClassRegistry,
    transactionProvider: <T>(fn) => backend.transaction<T>(fn),
  );

  // --- Action dispatcher (participants are read-only; no domain actions). ---
  final dispatcher = bootstrapAuditedActions(
    events: eventStore,
    authorization: policy,
    idempotency: InMemoryIdempotencyStore(),
    actions: const <Action<Object?, Object?>>[],
  );

  // --- Seed sites (site_registered; aggregate id == site_id). ---
  for (final site in _sites) {
    await eventStore.append(
      entryType: 'site',
      aggregateType: 'site',
      aggregateId: site.id,
      eventType: 'site_registered',
      data: <String, Object?>{
        'site_id': site.id,
        'region_id': site.regionId,
        'name': site.name,
      },
      initiator: const AutomationInitiator(service: 'clinical-scopes-seed'),
    );
  }

  // --- Seed participants (participant_registered; id == participant_id). ---
  for (final p in _participants) {
    await eventStore.append(
      entryType: 'participant',
      aggregateType: 'participant',
      aggregateId: p.id,
      eventType: 'participant_registered',
      data: <String, Object?>{
        'participant_id': p.id,
        'site_id': p.siteId,
        'name': p.name,
      },
      initiator: const AutomationInitiator(service: 'clinical-scopes-seed'),
    );
  }

  // --- Seed role-permission grants. Every role just needs read access to
  //     the participants view; the default ViewPermissionNamer is
  //     `view:<viewName>`, so the permission name is `view:participants`. ---
  const Map<String, List<String>> rolePerms = <String, List<String>>{
    'Investigator': <String>['view:participants'],
    'Overseer': <String>['view:participants'],
    'Admin': <String>['view:participants'],
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
        initiator: const AutomationInitiator(service: 'clinical-scopes-seed'),
      );
    }
  }

  // --- Seed user-role-scope assignments (one event per assignment). ---
  for (final user in _seedUsers) {
    for (final scope in user.scopes) {
      await eventStore.append(
        entryType: 'user_role_scope',
        aggregateType: 'user_role_scope',
        aggregateId: computeRoleAssignmentAggregateId(
          userId: user.userId,
          role: user.role,
          scope: scope,
        ),
        eventType: 'role_assigned',
        data: RoleAssignedPayload(
          userId: user.userId,
          role: user.role,
          scope: scope,
        ).toJson(),
        initiator: const AutomationInitiator(service: 'clinical-scopes-seed'),
      );
    }
  }

  // --- View-scope binding: the participants view is scoped by the
  //     participant class; a participant scope value IS the participant
  //     aggregate id (1:1). Site- and region-scoped principals flow
  //     through the descendant expander wired below. ---
  final viewScopes = ViewScopeRegistry()
    ..register(
      viewName: 'participants',
      scopeClass: 'participant',
      aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
    );

  // --- Reaction handlers. Passing scopeClassRegistry activates the
  //     production ScopeDescendantExpander on the subscription read path. ---
  final validator = RoleAwareTrustingValidator(backend: backend);
  final reactionHandlers = ReactionHandlers(
    eventStore: eventStore,
    dispatcher: dispatcher,
    policy: policy,
    viewScopeRegistry: viewScopes,
    scopeClassRegistry: scopeClassRegistry,
  );

  final httpRouter = Router()
    ..get('/me', reactionHandlers.me)
    ..post('/actions', reactionHandlers.actions)
    ..get('/permissions/snapshot', reactionHandlers.permissions);

  final httpPipeline = const Pipeline()
      .addMiddleware(authMiddleware(validator))
      .addHandler(httpRouter.call);

  final topRouter = Router()
    ..get('/subscriptions', reactionHandlers.subscriptions(validator))
    ..mount('/', httpPipeline);

  Future<void> dispose() async {
    await reactionHandlers.dispose();
    await eventStore.close();
  }

  return BootstrapResult(router: topRouter, dispose: dispose);
}
