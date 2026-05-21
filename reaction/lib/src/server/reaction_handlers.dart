import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/action_route.dart';
import 'package:reaction/src/server/authz_watcher.dart';
import 'package:reaction/src/server/me_route.dart';
import 'package:reaction/src/server/permission_route.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_web_socket/shelf_web_socket.dart';

/// Composition bundle for the reaction server-side adapters.
///
/// Holds the four substrate handles + the view-scope registry and
/// exposes the four shelf request handlers as properties. The
/// consumer composes them into their own shelf router however they
/// like — there is no "reaction server" class that owns its own
/// router or `HttpServer`.
///
/// Example consumer composition (the expected first consumers are
/// `portal_server` and `diary_server` in `hht_diary`, which already
/// run their own shelf pipelines with their own auth middleware):
///
/// ```dart
/// final reaction = ReactionHandlers(
///   eventStore: store,
///   dispatcher: dispatcher,
///   policy: policy,
///   viewScopeRegistry: portalViewScopes,
/// );
///
/// final router = Router()
///   ..get('/api/v1/portal/me',                   reaction.me)
///   ..post('/api/v1/portal/actions',             reaction.actions)
///   ..get('/api/v1/portal/permissions/snapshot', reaction.permissions)
///   ..get('/api/v1/portal/subscriptions',
///         reaction.subscriptionsWithValidator(validator));
///
/// final pipeline = const Pipeline()
///     .addMiddleware(consumerAuthMiddleware)
///     .addHandler(router.call);
/// await shelf_io.serve(pipeline, '0.0.0.0', 8080);
/// ```
class ReactionHandlers {
  ReactionHandlers({
    required this.eventStore,
    required this.dispatcher,
    required this.policy,
    ViewScopeRegistry? viewScopeRegistry,
    ViewPermissionNamer? viewPermissionNamer,
  }) : viewScopeRegistry = viewScopeRegistry ?? ViewScopeRegistry(),
       connectionRegistry = WsConnectionRegistry(),
       _viewPermissionNamer =
           viewPermissionNamer ?? defaultViewPermissionNamer {
    _authzWatcher = AuthzWatcher(
      eventStore: eventStore,
      connectionRegistry: connectionRegistry,
      policy: policy,
    );
    unawaited(_authzWatcher.start());
  }

  final EventStore eventStore;
  final ActionDispatcher dispatcher;
  final AuthorizationPolicy policy;
  final ViewScopeRegistry viewScopeRegistry;
  final WsConnectionRegistry connectionRegistry;
  final ViewPermissionNamer _viewPermissionNamer;
  late final AuthzWatcher _authzWatcher;

  /// Default view-permission name resolver: `view:<viewName>`.
  static String? defaultViewPermissionNamer(String viewName) =>
      'view:$viewName';

  /// Opt-in: also watch a containment projection's event source and
  /// emit `stale_data` to all connected users when it changes.
  /// Default behavior is NOT to watch any containment projection.
  void watchContainment(String aggregateType) =>
      _authzWatcher.watchContainment(aggregateType);

  /// GET handler: returns the authenticated Principal as JSON.
  Handler get me => meRouteHandler();

  /// POST handler: dispatches an ActionSubmission and returns the
  /// DispatchResult. Same auth requirement as [me].
  Handler get actions => actionRouteHandler(dispatcher: dispatcher);

  /// GET handler: returns the EffectiveAuthorization for the
  /// authenticated Principal. Same auth requirement as [me].
  Handler get permissions => permissionRouteHandler(policy: policy);

  /// WS upgrade handler: opens a per-connection state machine that
  /// authenticates via first WS message (the lib's
  /// PrincipalAuthValidator interface), then accepts subscribe /
  /// unsubscribe messages and relays substrate `Update<T>` envelopes.
  /// The connection registers with [connectionRegistry] on auth_ok
  /// so the AuthzWatcher can find it on permission events.
  ///
  /// The validator is supplied per-call (rather than at construction)
  /// because the WS upgrade path cannot carry an Authorization header
  /// from Flutter web — credentials arrive in the first WS message.
  /// Consumers typically wrap the same auth logic used in their HTTP
  /// auth middleware in a `PrincipalAuthValidator` and pass it here.
  Handler subscriptionsWithValidator(PrincipalAuthValidator validator) =>
      webSocketHandler((channel, _) {
        runSubscriptionHandler(
          channel: channel,
          validator: validator,
          eventStore: eventStore,
          policy: policy,
          viewScopes: viewScopeRegistry,
          viewPermissionNamer: _viewPermissionNamer,
          connectionRegistry: connectionRegistry,
        );
      });

  /// Stop the AuthzWatcher subscription. Call on graceful shutdown.
  Future<void> dispose() => _authzWatcher.stop();
}
