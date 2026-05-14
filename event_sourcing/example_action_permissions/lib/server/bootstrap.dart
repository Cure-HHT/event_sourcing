// IMPLEMENTS REQUIREMENTS:
//   into a single DemoServerComponents facade the demo server reads through.
//   user directory) before the dispatcher accepts any request.
//   verdict and the FailSafe policy's errors so the inspector can show why
//   every dispatch denies.

import 'package:action_permissions_demo/server/action_catalog.dart';
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
    required this.directory,
    required this.policy,
    required this.idempotencyStore,
    required this.policyErrors,
  });

  final ActionDispatcher dispatcher;
  final EventStore eventStore;
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

/// Bootstrap a fresh demo server over a caller-supplied [backend] and
/// [idempotencyStore]. The caller decides which concrete persistence
/// layer to use (Sembast in-memory / on-disk, Postgres, etc.) and owns
/// the lifecycle of both — bootstrap neither opens nor closes them.
///
/// [installIdentifier] is the per-installation unique identity stamped onto
/// `metadata.provenance[0]` of every appended event (see
/// `Source.identifier`). Production callers persist a UUIDv4 across boots;
/// tests can pass any UUID-shaped string.
Future<DemoServerComponents> bootstrapDemoServer({
  required StorageBackend backend,
  required IdempotencyStore idempotencyStore,
  required String permissionsYaml,
  required String usersYaml,
  required String installIdentifier,
}) async {
  // 1. Build the action registry up front so we can pass its declared
  //    permissions to the seed validator. The directory the
  //    ProvisionUserAction reads/writes is the same one the materializer
  //    populates from user_provisioned events.
  final directory = UserDirectory();
  final directoryMaterializer = UserDirectoryMaterializer(directory: directory);
  final registry = buildDemoActionRegistry(directory: directory);

  // 2. Bootstrap the append-only datastore. The role_permission_grants view
  //    is driven by rolePermissionGrantsSpec (TableProjectionSpec) via the
  //    ProjectionRegistry. Every entry type the demo writes must be
  //    registered up front; missing registrations fail at append.
  final demoProjections = ProjectionRegistry()
    ..register(rolePermissionGrantsSpec);
  final datastore = await bootstrapAppendOnlyDatastore(
    backend: backend,
    source: Source(
      hopId: 'portal-server',
      identifier: installIdentifier,
      softwareVersion: '0.1.0+1',
    ),
    entryTypes: _demoEntryTypes,
    destinations: const <Destination>[],
    projections: demoProjections,
  );
  final eventStore = datastore.eventStore;

  // 2b. Wire the in-memory UserDirectory to the substrate's reactive stream.
  //     The subscribe<StoredEvent> call delivers a Delta for every
  //     user_provisioned event appended after this point. Seed-time
  //     population is handled synchronously by UserDirectorySeedApplier
  //     (see step 4 below), so no timing window exists between the listener
  //     attach and the first seed append.
  eventStore
      .subscribe<StoredEvent>(
        const SubscriptionFilter(
          entryTypes: <String>['user_provisioned'],
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
  //    caller via DemoServerComponents.policyErrors.
  final policyBootstrap = await bootstrapActionPermissions(
    eventStore: eventStore,
    declaredPermissions: registry.allDeclaredPermissions,
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
    registeredVersion: 1,
    name: 'Help Request',
  ),
  EntryTypeDefinition(id: 'demo_note', registeredVersion: 1, name: 'Demo Note'),
  EntryTypeDefinition(
    id: 'green_button_press',
    registeredVersion: 1,
    name: 'Green Button Press',
  ),
  EntryTypeDefinition(
    id: 'blue_button_press',
    registeredVersion: 1,
    name: 'Blue Button Press',
  ),
  EntryTypeDefinition(id: 'red_alarm', registeredVersion: 1, name: 'Red Alarm'),
  EntryTypeDefinition(
    id: 'user_provisioned',
    registeredVersion: 1,
    name: 'User Provisioned',
  ),
  // Permissions module emits these via EventSeedApplier on bootstrap.
  // The role_permission_grants view is projected by rolePermissionGrantsSpec
  // (TableProjectionSpec) registered in the ProjectionRegistry.
  EntryTypeDefinition(
    id: 'role_permission_grant',
    registeredVersion: 1,
    name: 'Role-Permission Grant',
  ),
  // The dispatcher emits one of these for every denial stage.
  EntryTypeDefinition(
    id: 'action_denial',
    registeredVersion: 1,
    name: 'Action Denial',
  ),
];
