// Verifies: EVS-PRD-cross-process-event-transport/E
// end-to-end proof
//   that ReactionHandlers wires the PRODUCTION ScopeDescendantExpander
//   into the subscription handler. A principal assigned an ancestor-class
//   BoundScope (site-A) subscribing to a descendant-class view
//   (participants) receives ONLY the descendant rows contained in that
//   ancestor (P-1, P-2 at site-A) and NOT rows under a sibling ancestor
//   (P-9 at site-C). The expansion is computed by querying a REAL
//   in-memory containment index (participant_site_index), so this test
//   exercises the production read-path expander, not a stub.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/reaction_handlers.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:web_socket_channel/web_socket_channel.dart';

/// Containment index descriptor for the participant->site relationship.
/// Mirrors the columns the participant_site_index TableProjectionSpec
/// materializes (WholePayload of participant_registered).
class _ParticipantSiteDescriptor implements ScopeProjectionDescriptor {
  @override
  Set<String> get columns => {'participant_id', 'site_id'};
}

void main() {
  late Database db;
  late SembastBackend backend;
  late EventStore store;
  late ReactionHandlers handlers;

  setUp(() async {
    // --- Storage ---
    db = await newDatabaseFactoryMemory().openDatabase(
      'reaction-containment-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    backend = SembastBackend(database: db);

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
      // One row per participant aggregate (the subscribed view).
      ..register(
        const AggregateProjectionSpec(
          viewName: 'participants',
          interest: SubscriptionFilter(aggregateTypes: {'participant'}),
          tombstoneEventTypes: {},
        ),
      )
      // Containment index: participant_id -> site_id. Walked downward by
      // the production ScopeDescendantExpander when narrowing a
      // site-scoped subscription to the descendant participant rows.
      ..register(
        const TableProjectionSpec(
          viewName: 'participant_site_index',
          interest: SubscriptionFilter(aggregateTypes: {'participant'}),
          insertEventTypes: {'participant_registered'},
          removeEventTypes: {},
          rowKey: AggregateIdKey(),
          rowData: WholePayload(),
        ),
      );

    // --- Security context store ---
    final securityContexts = SembastSecurityContextStore(backend: backend);

    // --- EventStore ---
    store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'reaction-test',
        identifier: 'reaction-containment-instance-1',
        softwareVersion: 'reaction_test@0.0.0',
      ),
      securityContexts: securityContexts,
      projections: projections,
      promoters: PromoterRegistry(),
    );

    // --- Seed participant_registered events. The participant_id in the
    //     payload MUST equal the aggregateId so the participants row key
    //     (aggregate id) and the index keyColumn (participant_id) line up.
    Future<void> registerParticipant({
      required String id,
      required String siteId,
      required String name,
    }) async {
      await store.append(
        entryType: 'participant',
        aggregateType: 'participant',
        aggregateId: id,
        eventType: 'participant_registered',
        data: <String, Object?>{
          'participant_id': id,
          'site_id': siteId,
          'name': name,
        },
        initiator: const AnonymousInitiator(ipAddress: null),
      );
    }

    await registerParticipant(id: 'P-1', siteId: 'site-A', name: 'Ann');
    await registerParticipant(id: 'P-2', siteId: 'site-A', name: 'Bob');
    await registerParticipant(id: 'P-9', siteId: 'site-C', name: 'Cara');

    // --- Seed a site-A role assignment for user 'dr' under role
    //     'investigator'. The policy reads this from the event log
    //     (closed-under-events) to derive the principal's scope
    //     assignments.
    await store.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: computeRoleAssignmentAggregateId(
        userId: 'dr',
        role: 'investigator',
        scope: const BoundScope(class_: 'site', value: 'site-A'),
      ),
      eventType: 'role_assigned',
      data: const RoleAssignedPayload(
        userId: 'dr',
        role: 'investigator',
        scope: BoundScope(class_: 'site', value: 'site-A'),
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction_test_seed'),
    );

    // --- Grant investigator role the view:participants permission. ---
    await store.append(
      entryType: 'role_permission_grant',
      aggregateType: 'role_permission_grant',
      aggregateId: 'investigator:view:participants',
      eventType: 'permission_granted',
      data: const PermissionGrantedPayload(
        role: 'investigator',
        permissionName: 'view:participants',
      ).toJson(),
      initiator: const AutomationInitiator(service: 'reaction_test_seed'),
    );

    // --- Scope-class registry: participant contained in site via the
    //     participant_site_index containment projection. ---
    final registry = ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'site'),
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
      projectionLookup: (_) => _ParticipantSiteDescriptor(),
    );

    // --- Write-path policy (reads the real projection rows). ---
    final policy = TableBackedAuthorizationPolicy(
      backend: backend,
      scopeClassRegistry: registry,
      transactionProvider: <T>(fn) => backend.transaction<T>(fn),
    );

    // --- View-scope binding: the participants view is scoped by the
    //     participant class; a participant scope value IS the participant
    //     aggregate id (1:1). ---
    final viewScopes = ViewScopeRegistry()
      ..register(
        viewName: 'participants',
        scopeClass: 'participant',
        aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
      );

    // --- Action dispatcher (empty registry; actions not exercised). ---
    final dispatcher = ActionDispatcher(
      registry: ActionRegistry(),
      authorization: policy,
      events: store,
      idempotency: InMemoryIdempotencyStore(),
    );

    handlers = ReactionHandlers(
      eventStore: store,
      dispatcher: dispatcher,
      policy: policy,
      viewScopeRegistry: viewScopes,
      scopeClassRegistry: registry,
    );
  });

  tearDown(() async {
    await handlers.dispose();
    await store.close();
  });

  test(
    "site-A principal subscribing to 'participants' receives only "
    'descendant participants P-1 and P-2 (P-9 at site-C absent) — '
    'production ScopeDescendantExpander wired through ReactionHandlers',
    () async {
      // Serve the real WS subscription handler on a loopback port.
      final validator = TrustingAuthValidator(
        defaultActiveRole: 'investigator',
      );
      final server = await shelf_io.serve(
        handlers.subscriptions(validator),
        'localhost',
        0,
      );
      addTearDown(() async => server.close(force: true));

      final client = WebSocketChannel.connect(
        Uri.parse('ws://localhost:${server.port}/'),
      );
      await client.ready;
      addTearDown(() async => client.sink.close());

      // Collect snapshot rows until end_of_replay.
      final snapshots = <Map<String, Object?>>[];
      final endOfReplay = Completer<void>();
      final sub = client.stream.listen((raw) {
        final json = jsonDecode(raw as String) as Map<String, Object?>;
        switch (json['type']) {
          case 'snapshot':
            final value = json['value'];
            if (value is Map) {
              snapshots.add(Map<String, Object?>.from(value));
            }
          case 'end_of_replay':
            if (!endOfReplay.isCompleted) endOfReplay.complete();
        }
      });
      addTearDown(sub.cancel);

      // Authenticate as 'dr'.
      client.sink.add(jsonEncode({'type': 'auth', 'credential': 'dr'}));
      // Let the server process auth before subscribing.
      await Future<void>.delayed(const Duration(milliseconds: 50));
      client.sink.add(
        jsonEncode({
          'type': 'subscribe',
          'subscriptionId': 'sub-1',
          'viewName': 'participants',
        }),
      );

      await endOfReplay.future.timeout(
        const Duration(seconds: 5),
        onTimeout: () => fail(
          'did not receive end_of_replay within timeout; '
          'snapshots so far: $snapshots',
        ),
      );

      final ids = snapshots
          .map((r) => r['participant_id'])
          .whereType<String>()
          .toSet();

      expect(
        ids,
        {'P-1', 'P-2'},
        reason:
            'site-A assignment must expand (via the real containment '
            'index) to exactly the participants at site-A; P-9 (site-C) '
            'must be excluded.',
      );
      expect(ids.contains('P-9'), isFalse);
    },
  );
}
