// reaction/test/e2e/test_support/reaction_remote_test_harness.dart
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_router/shelf_router.dart';

import '../../local/test_support/reaction_test_harness.dart';

/// Fully-wired in-memory substrate + reaction handlers mounted on a
/// real shelf server + RemoteScope. Mirrors `ReactionTestHarness` for
/// the cross-process side: tests interact with `harness.scope.*`
/// exactly as production widget code would.
class ReactionRemoteTestHarness {
  ReactionRemoteTestHarness._({
    required this.substrate,
    required this.reaction,
    required this.httpServer,
    required this.scope,
  });

  final ReactionTestHarness substrate;
  final ReactionHandlers reaction;
  final HttpServer httpServer;
  final RemoteScope scope;

  static Future<ReactionRemoteTestHarness> open({
    String defaultActiveRole = 'install',
  }) async {
    final substrate = await ReactionTestHarness.open();
    final validator = TrustingAuthValidator(
      defaultActiveRole: defaultActiveRole,
    );

    final reaction = ReactionHandlers(
      eventStore: substrate.eventStore,
      dispatcher: substrate.dispatcher,
      policy: substrate.dispatcher.authorization,
    );

    // HTTP routes are gated by Bearer-token auth middleware. The WS
    // upgrade path (`/subscriptions`) is NOT — credentials arrive
    // in-band via the first WS AuthMessage (see RemoteConnection._connect
    // and the `subscriptions` handler's docstring) because Flutter web
    // cannot attach an Authorization header during a WS upgrade.
    final httpRouter = Router()
      ..get('/me', reaction.me)
      ..post('/actions', reaction.actions)
      ..get('/permissions/snapshot', reaction.permissions);

    final httpPipeline = const Pipeline()
        .addMiddleware(authMiddleware(validator))
        .addHandler(httpRouter.call);

    final topRouter = Router()
      ..get('/subscriptions', reaction.subscriptions(validator))
      ..mount('/', httpPipeline);

    final httpServer = await shelf_io.serve(topRouter.call, '127.0.0.1', 0);

    final scope = RemoteScope(
      baseUrl: Uri.parse('http://127.0.0.1:${httpServer.port}'),
    );

    return ReactionRemoteTestHarness._(
      substrate: substrate,
      reaction: reaction,
      httpServer: httpServer,
      scope: scope,
    );
  }

  Future<void> close() async {
    await scope.dispose();
    await reaction.dispose(); // stops AuthorizationWatcher
    await httpServer.close(force: true);
    await substrate.close();
  }

  /// Seed a `role_permission_grants` row by appending a
  /// `permission_granted` event. Used to authorize action dispatch
  /// (`Permission('say_hello')`) or view subscription
  /// (`Permission('view:notes_today')`) in e2e tests, where the
  /// harness's [TrustingAuthValidator] surfaces every credential as
  /// the configured `defaultActiveRole` (`'install'`) but
  /// [TableBackedAuthorizationPolicy] still requires a matching grant
  /// row.
  Future<void> grantPermission({
    required String role,
    required String permission,
  }) async {
    await substrate.eventStore.append(
      entryType: 'role_permission_grant',
      aggregateType: 'role_permission_grant',
      aggregateId: '$role:$permission',
      eventType: 'permission_granted',
      data: PermissionGrantedPayload(
        role: role,
        permissionName: permission,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'test'),
    );
  }

  /// Seed a `user_role_scopes` row by appending a `role_assigned`
  /// event. The substrate verifies (userId, activeRole) membership
  /// against this view on every authorize call (closed-under-events
  /// trust model): the Principal's `activeRole` claim from
  /// [TrustingAuthValidator] is not honoured until this row exists.
  /// Defaults to [TotalWildcardScope] so unscoped actions authorize
  /// without requiring callers to think about scope semantics.
  Future<void> assignRole({
    required String userId,
    required String role,
    ScopeValue scope = const TotalWildcardScope(),
  }) async {
    await substrate.seedRoleAssigned(userId: userId, role: role, scope: scope);
  }
}
