// IMPLEMENTS REQUIREMENTS:
//   into a single DemoServerComponents facade the demo server reads through.
//   user directory) before the dispatcher accepts any request.
//   verdict and the FailSafe policy's errors so the inspector can show why
//   every dispatch denies.

import 'package:action_permissions_demo/server/action_catalog.dart';
import 'package:action_permissions_demo/server/log_destination.dart';
import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:action_permissions_demo/server/user_directory_materializer.dart';
import 'package:action_permissions_demo/server/user_directory_seed_applier.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

/// Collaborators a running demo server reads through after bootstrap.
@immutable
class DemoServerComponents {
  const DemoServerComponents({
    required this.dispatcher,
    required this.eventStore,
    required this.destinations,
    required this.deliveryDestination,
    required this.directory,
    required this.policy,
    required this.idempotencyStore,
    required this.policyErrors,
  });

  final ActionDispatcher dispatcher;
  final EventStore eventStore;

  /// The destination registry of the server's database. The server starts
  /// its delivery cycle over it; at most one delivery cycle drains a
  /// database, and a server whose cycle cannot take the drain lock stands
  /// by until it can.
  final DestinationRegistry destinations;

  /// The demo destination every instance registers: it delivers batches to
  /// the server log. Every process that may drain registers the same
  /// destinations, because delivery uses the destinations registered in the
  /// process that drains.
  final LogDestination deliveryDestination;
  final UserDirectory directory;
  final AuthorizationPolicy policy;

  /// The store the dispatcher records idempotency outcomes into.
  /// Inspector code (`collectIdempotencyEntries`) calls
  /// `IdempotencyStore.listEntries()` — every backend implements it, so
  /// the inspector pane renders cache contents regardless of which
  /// concrete store is wired in (DemoIdempotencyStore,
  /// PostgresIdempotencyStore, etc.).
  final IdempotencyStore idempotencyStore;

  /// Empty when the YAML seed validated cleanly. Non-empty when the
  /// policy is the FailSafe variant — every dispatch will deny with
  /// `DenyReason.notGranted` and the inspector surfaces these errors so
  /// operators can see why.
  final List<String> policyErrors;
}

/// The permission the operator routes under `/demo/delivery/` require. It is
/// granted in the permissions seed like every other demo permission, so the
/// grant is an event in the log, and the routes ask the authorization policy
/// for it.
const Permission deliveryOperatePermission = Permission('delivery.operate');

/// The start date the demo destination is activated with. Every instance
/// sets the same date when the database has none, so two instances booting
/// together agree on it.
final DateTime logDestinationStartDate = DateTime.utc(2026, 1, 1);

/// Bootstrap a fresh demo server over a caller-supplied [backend] and
/// [idempotencyStore]. The caller decides which concrete persistence
/// layer to use (Sembast in-memory / on-disk, Postgres, etc.) and owns
/// the lifecycle of both — bootstrap neither opens nor closes them.
///
/// [installIdentifier] is the per-installation unique identity stamped onto
/// `metadata.provenance[0]` of every appended event (see
/// `Source.identifier`). Production callers persist a UUIDv4 across boots;
/// tests can pass any UUID-shaped string.
///
/// [deliveryLog] receives one line per batch the demo destination delivers
/// (the server passes its standard output). [onBootProgress] observes the
/// boot of the event store (see `EventStore.open`): it must only record.
/// [entryTypeVersions] replaces the registered version of the named demo
/// entry types; a test uses it to play a build that raises a major.
/// [deliveryDestination] replaces the demo destination (a test passes one
/// whose sends it controls); [deliveryLog] is then unused.
Future<DemoServerComponents> bootstrapDemoServer({
  required StorageBackend backend,
  required IdempotencyStore idempotencyStore,
  required String permissionsYaml,
  required String usersYaml,
  required String installIdentifier,
  void Function(String line)? deliveryLog,
  void Function(BootProgress progress)? onBootProgress,
  @visibleForTesting
  Map<String, EntryTypeVersion> entryTypeVersions =
      const <String, EntryTypeVersion>{},
  @visibleForTesting LogDestination? deliveryDestination,
}) async {
  // 1. Build the action registry up front so we can pass its declared
  //    permissions to the seed validator. The directory the
  //    ProvisionUserAction reads/writes is the same one the materializer
  //    populates from user_provisioned events.
  final directory = UserDirectory();
  final directoryMaterializer = UserDirectoryMaterializer(directory: directory);
  final registry = buildDemoActionRegistry(directory: directory);

  // 2. Bootstrap the append-only datastore. The role_permission_grants and
  //    user_role_scopes views are driven by their respective
  //    TableProjectionSpecs via the ProjectionRegistry. Every entry type the
  //    demo writes must be registered up front; missing registrations fail
  //    at append.
  final demoProjections = ProjectionRegistry()
    ..register(rolePermissionGrantsSpec)
    ..register(userRoleScopesSpec);

  // 2a. ScopeClassRegistry: the demo declares a single top-level scope
  //     class ('site'). No containment hierarchy is needed (the demo's
  //     two workspaces are flat). Apps that need parent/child scope
  //     hierarchies wire a richer registry here; the policy reads it at
  //     authorize time via ContainmentResolver.
  final scopeClassRegistry = ScopeClassRegistry(
    classes: const <ScopeClassSpec>[ScopeClassSpec(name: 'site')],
    projectionLookup: (_) => null,
  );
  final destination =
      deliveryDestination ?? LogDestination(sink: deliveryLog ?? (_) {});
  final datastore = await bootstrapEventStore(
    backend: backend,
    source: Source(
      hopId: 'app-server',
      identifier: installIdentifier,
      softwareVersion: '0.1.0+1',
    ),
    entryTypes: <EntryTypeDefinition>[
      for (final d in _demoEntryTypes)
        entryTypeVersions.containsKey(d.id)
            ? EntryTypeDefinition(
                id: d.id,
                registeredVersion: entryTypeVersions[d.id]!,
                name: d.name,
              )
            : d,
    ],
    destinations: <Destination>[destination],
    projections: demoProjections,
    onBootProgress: onBootProgress,
  );
  final eventStore = datastore.eventStore;

  // 2c. Activate the demo destination once: the first instance to boot on a
  //     database records the start date, and every later boot finds it.
  final schedule = await datastore.destinations.scheduleOf(logDestinationId);
  if (schedule.startDate == null) {
    await datastore.destinations.setStartDate(
      logDestinationId,
      logDestinationStartDate,
      initiator: const AutomationInitiator(service: 'demo_server_bootstrap'),
    );
  }

  // 2b. Wire the in-memory UserDirectory to the substrate's reactive stream.
  //     The subscribe<StoredEvent> call delivers a Delta for every
  //     user_provisioned event appended after this point. Seed-time
  //     population is handled synchronously by UserDirectorySeedApplier
  //     (see step 4 below), so no timing window exists between the listener
  //     attach and the first seed append.
  //
  //     The substrate's appendInTxn runs the projection interpreter inside
  //     the dispatch transaction (see EventStore.appendInTxn), so the
  //     user_role_scopes view materializes atomically with the
  //     ProvisionUserAction's user_provisioned event. ProvisionUserAction
  //     emits the role_assigned event itself.
  eventStore
      .subscribe<StoredEvent>(
        const SubscriptionFilter(
          entryTypes: <String>{'user_provisioned'},
          eventTypes: <String>{'user_provisioned'},
        ),
        const Events(),
      )
      .listen((update) {
        // Events()-mode subscriptions never emit Snapshot/EndOfReplay/Tombstone;
        // we only act on Delta. Any future variants are intentionally ignored.
        if (update is Delta<StoredEvent>) {
          directoryMaterializer.applyDirect(update.value.data);
        }
      });

  // 3. Apply the role-permission matrix YAML seed. Returns either
  //    PolicyReady(policy) or PolicyFailSafe(errors); on FailSafe the
  //    returned policy denies everything and the errors flow back to the
  //    caller via DemoServerComponents.policyErrors. The scopeClassRegistry
  //    is forwarded so the seed validator can verify every scoped
  //    permission references a registered scopeClass.
  final policyBootstrap = await bootstrapActionPermissions(
    eventStore: eventStore,
    declaredPermissions: <Permission>{
      ...registry.allDeclaredPermissions,
      deliveryOperatePermission,
    },
    scopeClassRegistry: scopeClassRegistry,
    yamlSource: permissionsYaml,
  );

  // 4. Apply the user-directory YAML seed. The applier diffs YAML against
  //    the in-memory directory; for each missing entry it calls `emit`
  //    (a sync callback typed `void Function(...)`) and `applyDirect`.
  //    `eventStore.append` is async, so we collect emissions in `pending`
  //    and await each append sequentially after `applyYaml` returns. This
  //    keeps seed-write ordering deterministic without changing the
  //    applier's API.
  final pending = <Map<String, Object?>>[];
  final dirSeedApplier = UserDirectorySeedApplier(
    directory: directory,
    materializer: directoryMaterializer,
    emit: pending.add,
  );
  dirSeedApplier.applyYaml(usersYaml);
  for (final payload in pending) {
    await eventStore.append(
      entryType: 'user_provisioned',
      aggregateType: 'user_directory',
      aggregateId: payload['userId']! as String,
      eventType: 'user_provisioned',
      data: Map<String, Object?>.from(payload),
      initiator: const AutomationInitiator(service: 'user_directory_seed'),
    );
  }

  // 4a. Seed user-role-scope assignments. The substrate verifies
  //     user-role membership via `user_role_scopes` for every permission
  //     check (scoped and unscoped). Every directory entry needs
  //     a `role_assigned` event, regardless of whether the user holds any
  //     site-scoped permissions:
  //       * users with `activeSite != null` get a BoundScope assignment
  //         (covers their site-scoped permissions);
  //       * users with `activeSite == null` (e.g. Admin in the demo seed)
  //         get a `TotalWildcardScope` assignment, which the substrate
  //         treats as "role-membership without a site binding" —
  //         sufficient for unscoped permissions like `users.provision`,
  //         and does not over-grant any scoped permission they don't hold
  //         at the role level.
  final roleAssignments = <RoleAssignmentSeedEntry>[
    for (final entry in directory.listEntries())
      RoleAssignmentSeedEntry(
        userId: entry.userId,
        role: entry.role,
        scope: entry.activeSite != null
            ? BoundScope(class_: 'site', value: entry.activeSite!)
            : const TotalWildcardScope(),
      ),
  ];
  await bootstrapRoleAssignments(
    eventStore: eventStore,
    seed: RoleAssignmentSeed(entries: roleAssignments),
  );

  // 5. Dispatcher wired through the caller-supplied idempotency store.
  final dispatcher = bootstrapAuditedActions(
    events: eventStore,
    authorization: policyBootstrap.policy,
    idempotency: idempotencyStore,
    actions: registry.all,
  );

  return DemoServerComponents(
    dispatcher: dispatcher,
    eventStore: eventStore,
    destinations: datastore.destinations,
    deliveryDestination: destination,
    directory: directory,
    policy: policyBootstrap.policy,
    idempotencyStore: idempotencyStore,
    policyErrors: policyBootstrap.errors,
  );
}

/// All entry types the demo writes through the EventStore. Every entry
/// type the actions emit (or that the dispatcher emits as denial events,
/// or that the seed appliers emit) must appear here so the EntryTypeRegistry
/// accepts the append.
const List<EntryTypeDefinition> _demoEntryTypes = <EntryTypeDefinition>[
  // Action-emitted entry types.
  EntryTypeDefinition(
    id: 'help_request',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Help Request',
  ),
  EntryTypeDefinition(
    id: 'demo_note',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Demo Note',
  ),
  EntryTypeDefinition(
    id: 'green_button_press',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Green Button Press',
  ),
  EntryTypeDefinition(
    id: 'blue_button_press',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Blue Button Press',
  ),
  EntryTypeDefinition(
    id: 'red_alarm',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Red Alarm',
  ),
  EntryTypeDefinition(
    id: 'user_provisioned',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'User Provisioned',
  ),
  // Permissions module emits these via PermissionSeedApplier on bootstrap.
  // The role_permission_grants view is projected by rolePermissionGrantsSpec
  // (TableProjectionSpec) registered in the ProjectionRegistry.
  EntryTypeDefinition(
    id: 'role_permission_grant',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Role-Permission Grant',
  ),
  // Permissions module emits role_assigned events here via
  // bootstrapRoleAssignments; the user_role_scopes view (projected by
  // userRoleScopesSpec) drives the policy's scope-coverage check.
  EntryTypeDefinition(
    id: 'user_role_scope',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'User-Role-Scope Assignment',
  ),
  // The dispatcher emits one of these for every denial stage.
  EntryTypeDefinition(
    id: 'action_denial',
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Action Denial',
  ),
];
