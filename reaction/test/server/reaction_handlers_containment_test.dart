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
// Verifies: EVS-DEV-converging-view-reads/H
// a scoped subscription made while the containment view the expander
//   reads is converging receives an ErrorMsg with code view_converging
//   naming the view, and no rows, through the real ReactionHandlers /
//   subscription-handler / wire path; the same subscription succeeds
//   with the descendant rows once the view is current.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/reaction_handlers.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' hide Transaction;
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:web_socket_channel/web_socket_channel.dart';

/// Containment index descriptor for the participant->site relationship.
/// Mirrors the columns the participant_site_index TableProjectionSpec
/// materializes (WholePayload of participant_registered).
class _ParticipantSiteDescriptor implements ScopeProjectionDescriptor {
  @override
  Set<String> get columns => {'participant_id', 'site_id'};
}

/// A [SembastBackend] that, while [forceConverging] is set, reports
/// [convergingCopyId]'s stored watermark as rewound to the position before
/// the log so every read of that copy scans the whole log past it and
/// finds folding events, deciding converging exactly as a genuinely
/// behind copy would. Overrides only [readViewCopiesInTxn] (a public,
/// non-`@internal` `StorageBackend` member; app-supplied backends compose
/// this way per the substrate's storage contract) and delegates
/// everything else, including the real fold and the real stored
/// watermark, to the inherited implementation — so once
/// [forceConverging] is cleared, the copy reports however far the real
/// (uninterrupted) fold has actually reached.
class _ForceConvergingBackend extends SembastBackend {
  _ForceConvergingBackend({required super.database});

  bool forceConverging = false;
  String? convergingCopyId;

  @override
  Future<List<ViewCopy>> readViewCopiesInTxn(Transaction txn) async {
    final copies = await super.readViewCopiesInTxn(txn);
    final targetId = convergingCopyId;
    if (!forceConverging || targetId == null) return copies;
    return [
      for (final c in copies)
        if (c.copyId == targetId)
          ViewCopy(
            copyId: c.copyId,
            viewName: c.viewName,
            fingerprint: c.fingerprint,
            watermark: 0,
            markedForDeletion: c.markedForDeletion,
          )
        else
          c,
    ];
  }
}

EntryTypeRegistry _entryTypes() => EntryTypeRegistry()
  ..register(
    const EntryTypeDefinition(
      id: 'participant',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Participant',
    ),
  )
  ..register(
    const EntryTypeDefinition(
      id: 'role_permission_grant',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Role-Permission Grant',
    ),
  )
  ..register(
    const EntryTypeDefinition(
      id: 'user_role_scope',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'User-Role-Scope Assignment',
    ),
  )
  ..register(
    const EntryTypeDefinition(
      id: 'action_denial',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Action Denial',
    ),
  );

ProjectionRegistry _projections() => ProjectionRegistry()
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

final _scopeClassRegistry = ScopeClassRegistry(
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

Future<EventStore> _openStore(SembastBackend backend) => EventStore.openForTest(
  storage: backend,
  entryTypes: _entryTypes(),
  source: const Source(
    hopId: 'reaction-test',
    identifier: 'reaction-containment-instance-1',
    softwareVersion: 'reaction_test@0.0.0',
  ),
  securityContexts: SembastSecurityContextStore(backend: backend),
  projections: _projections(),
  promoters: PromoterRegistry(),
);

Future<void> _registerParticipant(
  EventStore store, {
  required String id,
  required String siteId,
  required String name,
}) => store.append(
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

/// Seeds the site-A role assignment for user 'dr' under 'investigator' and
/// grants that role `view:participants`, so 'dr' is a valid subscriber
/// whose row-level narrowing exercises the containment expander.
Future<void> _seedDrAtSiteA(EventStore store) async {
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
}

ViewScopeRegistry _viewScopes() => ViewScopeRegistry()
  ..register(
    viewName: 'participants',
    scopeClass: 'participant',
    aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
  );

ReactionHandlers _buildHandlers(EventStore store) {
  final policy = TableBackedAuthorizationPolicy(
    reader: store.reader,
    scopeClassRegistry: _scopeClassRegistry,
  );
  final dispatcher = ActionDispatcher(
    registry: ActionRegistry(),
    authorization: policy,
    events: store,
    idempotency: InMemoryIdempotencyStore(),
  );
  return ReactionHandlers(
    eventStore: store,
    dispatcher: dispatcher,
    policy: policy,
    viewScopeRegistry: _viewScopes(),
    scopeClassRegistry: _scopeClassRegistry,
  );
}

var _dbCounter = 0;
Future<sembast.Database> _openDb() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'reaction-containment-$_dbCounter.db',
  );
}

/// Connects a client to [handlers]' subscriptions endpoint, authenticates
/// as 'dr' and subscribes to 'participants'. Returns the connected client
/// and the list of decoded server messages it accumulates.
Future<(WebSocketChannel, List<Map<String, Object?>>)> _subscribeAsDr(
  ReactionHandlers handlers,
) async {
  final validator = TrustingAuthValidator(defaultActiveRole: 'investigator');
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

  final messages = <Map<String, Object?>>[];
  final sub = client.stream.listen(
    (raw) => messages.add(jsonDecode(raw as String) as Map<String, Object?>),
  );
  addTearDown(sub.cancel);

  client.sink.add(jsonEncode({'type': 'auth', 'credential': 'dr'}));
  await Future<void>.delayed(const Duration(milliseconds: 50));
  client.sink.add(
    jsonEncode({
      'type': 'subscribe',
      'subscriptionId': 'sub-1',
      'viewName': 'participants',
    }),
  );

  return (client, messages);
}

Future<void> _waitFor(
  List<Map<String, Object?>> messages,
  bool Function(List<Map<String, Object?>>) done, {
  required String describing,
}) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!done(messages)) {
    if (DateTime.now().isAfter(deadline)) {
      fail('did not observe $describing in time; messages so far: $messages');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  late sembast.Database db;
  late SembastBackend backend;
  late EventStore store;
  late ReactionHandlers handlers;

  setUp(() async {
    db = await _openDb();
    backend = SembastBackend(database: db);
    store = await _openStore(backend);

    await _registerParticipant(store, id: 'P-1', siteId: 'site-A', name: 'Ann');
    await _registerParticipant(store, id: 'P-2', siteId: 'site-A', name: 'Bob');
    await _registerParticipant(
      store,
      id: 'P-9',
      siteId: 'site-C',
      name: 'Cara',
    );
    await _seedDrAtSiteA(store);

    handlers = _buildHandlers(store);
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
      final (_, messages) = await _subscribeAsDr(handlers);

      await _waitFor(
        messages,
        (m) => m.any((e) => e['type'] == 'end_of_replay'),
        describing: 'end_of_replay',
      );

      final ids = messages
          .where((m) => m['type'] == 'snapshot')
          .map((m) => m['value'])
          .whereType<Map<String, Object?>>()
          .map((v) => v['participant_id'])
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

  group('converging containment view', () {
    test('a scoped subscription applying through an ancestor while the '
        'containment view converges receives view_converging naming it and '
        'no rows; the same subscription succeeds with the descendants once '
        'the view reports current', () async {
      final convergingDb = await _openDb();
      final convergingBackend = _ForceConvergingBackend(database: convergingDb);
      final convergingStore = await _openStore(convergingBackend);
      addTearDown(convergingStore.close);

      await _registerParticipant(
        convergingStore,
        id: 'P-1',
        siteId: 'site-A',
        name: 'Ann',
      );
      await _registerParticipant(
        convergingStore,
        id: 'P-2',
        siteId: 'site-A',
        name: 'Bob',
      );
      await _registerParticipant(
        convergingStore,
        id: 'P-9',
        siteId: 'site-C',
        name: 'Cara',
      );
      await _seedDrAtSiteA(convergingStore);

      final convergingHandlers = _buildHandlers(convergingStore);
      addTearDown(convergingHandlers.dispose);

      // Only the containment view reports converging; participants,
      // user_role_scopes and role_permission_grants are all genuinely
      // current, so the refusal is attributable to the containment
      // read the expander makes.
      convergingBackend
        ..convergingCopyId = convergingStore.copyIdOf('participant_site_index')
        ..forceConverging = true;

      final (client, messages) = await _subscribeAsDr(convergingHandlers);
      await _waitFor(
        messages,
        (m) => m.any((e) => e['type'] == 'error'),
        describing: 'error',
      );

      expect(
        messages.any((m) => m['type'] == 'snapshot'),
        isFalse,
        reason:
            'a converging containment view must not narrow the '
            'subscription from an unsettled read',
      );
      final errorMsg = messages.singleWhere((m) => m['type'] == 'error');
      expect(errorMsg['code'], 'view_converging');
      expect(errorMsg['message'], 'participant_site_index');
      await client.sink.close();

      // The view reports current again: the same subscription now
      // receives exactly the descendant rows.
      convergingBackend.forceConverging = false;
      final (_, retryMessages) = await _subscribeAsDr(convergingHandlers);
      await _waitFor(
        retryMessages,
        (m) => m.any((e) => e['type'] == 'end_of_replay'),
        describing: 'end_of_replay after convergence',
      );

      final ids = retryMessages
          .where((m) => m['type'] == 'snapshot')
          .map((m) => m['value'])
          .whereType<Map<String, Object?>>()
          .map((v) => v['participant_id'])
          .whereType<String>()
          .toSet();
      expect(ids, {'P-1', 'P-2'});
    });
  });
}
