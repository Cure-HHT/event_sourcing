// Implements: EVS-PRD-library-charter
// Implements: EVS-PRD-auth-session
// Implements: EVS-PRD-action-submitter
// Implements: EVS-PRD-view-subscriber
// Implements: EVS-PRD-permission-source
/// Substrate-agnostic action submission, view subscription, permission
/// snapshots, and credential lifecycle for apps built on `event_sourcing`.
///
/// See `spec/prd-reaction.md` (in the parent repo) for the architectural
/// spec.
///
/// ## What this package provides
///
/// Five transport-agnostic interfaces:
///
/// - [AuthSession] — credential lifecycle; surfaces [AuthStatus]
///   (sealed: [Authenticated], [NotAuthenticated], [Expired]).
/// - [ActionSubmitter] — submit actions to the substrate's dispatch
///   pipeline.
/// - [ViewSource] — subscribe to a registered view's row-level updates
///   (the substrate's `Update<T>` stream: Snapshot × N → EndOfReplay →
///   Delta/Tombstone × ∞).
/// - [PermissionSource] — per-Principal view of the substrate's
///   [EffectiveAuthorization] (active role + role permissions + scope
///   assignments).
/// - [PrincipalAuthValidator] — server-side credential validation seam
///   (consumed by [ReactionHandlers] on the server side).
///
/// Two state types:
///
/// - [ActionState] — sealed widget-side submission state machine
///   (Idle/Submitting/Success/Denied/Failed). Used by `ActionBuilder`
///   in `reaction_widgets`.
/// - [IdempotencyKeyGenerator] — UUID v4 by default
///   ([Uuid4IdempotencyKeyGenerator]).
///
/// Four in-process Local implementations:
///
/// - [LocalAuthSession] — holds a [Principal] directly.
/// - [LocalActionSubmitter] — wraps `ActionDispatcher.dispatch`.
/// - [LocalViewSource] — wraps `EventStore.subscribe<T>`.
/// - [LocalPermissionSource] — subscribes to the substrate's
///   `role_permission_grants` and `user_role_scopes` views and routes
///   through `AuthorizationPolicy.effectivePermissionsFor` to derive
///   the [EffectiveAuthorization] for the active principal.
///
/// Four cross-process Remote implementations + a connection scope,
/// wiring the same four interfaces over HTTP + WebSocket against a
/// [ReactionHandlers]-hosted server:
///
/// - [RemoteScope] — per-connection composition root that owns the
///   shared HTTP client + multiplexed WS connection.
/// - [RemoteAuthSession], [RemoteActionSubmitter], [RemoteViewSource],
///   [RemotePermissionSource] — the four interfaces wired to that
///   scope.
///
/// Server-side adapters for hosting the wire protocol from any shelf
/// pipeline:
///
/// - [ReactionHandlers] — composition bundle exposing `me` /
///   `actions` / `permissions` / `subscriptions` shelf handlers
///   against a substrate (`EventStore` + `ActionDispatcher` +
///   `AuthorizationPolicy`). Optional [ViewPermissionNamer] customizes
///   the view-level authorization name.
/// - [authMiddleware] + [principalFromContext] — bearer-token auth
///   middleware backed by any [PrincipalAuthValidator].
/// - [ViewScopeRegistry] / [ViewScopeBinding] — declarative bindings
///   from `(viewName, filter)` to a row-level [Permission] namer.
/// - [WsConnectionRegistry] — book-keeping for live WS connections so
///   the server can force-logout principals on credential revocation.
/// - [TrustingAuthValidator] — test-only `PrincipalAuthValidator`
///   that accepts any bearer token and surfaces the requested role.
///   Production deployments supply their own validator (Firebase,
///   Auth0, JWT, etc.).
///
/// Flutter widgets live in a separate `reaction_widgets` package.
library;

// Interfaces
export 'src/interfaces/action_submitter.dart'
    show ActionSubmitter, TransportException;
export 'src/interfaces/auth_session.dart'
    show Authenticated, AuthSession, AuthStatus, Expired, NotAuthenticated;
export 'src/interfaces/permission_source.dart' show PermissionSource;
export 'src/interfaces/principal_auth_validator.dart'
    show AuthenticationDenied, PrincipalAuthValidator;
export 'src/interfaces/view_source.dart' show ViewSource;

// State
export 'src/state/action_state.dart'
    show ActionState, Denied, Failed, Idle, Submitting, Success;
export 'src/state/idempotency_key_generator.dart'
    show IdempotencyKeyGenerator, Uuid4IdempotencyKeyGenerator;

// Scope
export 'src/scope/connection_status.dart'
    show ConnectionStatus, Connected, Reconnecting, Disconnected;

// Local impls
export 'src/local/local_action_submitter.dart' show LocalActionSubmitter;
export 'src/local/local_auth_session.dart' show LocalAuthSession;
export 'src/local/local_permission_source.dart' show LocalPermissionSource;
export 'src/local/local_view_source.dart' show LocalViewSource;

// Remote impls
export 'src/remote/remote_action_submitter.dart' show RemoteActionSubmitter;
export 'src/remote/remote_auth_session.dart' show RemoteAuthSession;
export 'src/remote/remote_permission_source.dart' show RemotePermissionSource;
export 'src/remote/remote_scope.dart' show RemoteScope;
export 'src/remote/remote_view_source.dart' show RemoteViewSource;

// Server-side adapters
export 'src/server/auth_middleware.dart'
    show authMiddleware, principalFromContext;
export 'src/server/reaction_handlers.dart'
    show ReactionHandlers, ViewPermissionNamer;
export 'src/server/validators/trusting_auth_validator.dart'
    show TrustingAuthValidator;
export 'src/server/view_scope_registry.dart'
    show ViewScopeBinding, ViewScopeRegistry;
export 'src/server/ws_connection_registry.dart' show WsConnectionRegistry;

// AuthzWatcher is package-private; consumers interact via
// ReactionHandlers.watchContainment(...).
