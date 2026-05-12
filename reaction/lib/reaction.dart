/// Substrate-agnostic action submission, view subscription, permission
/// snapshots, and credential lifecycle for apps built on `event_sourcing`.
///
/// See `spec/prd-reaction.md` (in the parent repo) for the architectural
/// spec.
///
/// ## What this package provides (Plan B-local — in-process only)
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
///   `RolePermissionGrants` projection.
/// - [PrincipalAuthValidator] — server-side credential validation seam
///   (consumed by Plan C's reaction server module).
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
/// - [LocalPermissionSource] — wraps the `RoleMatrixReader` +
///   `PermissionSnapshot` machinery.
///
/// Remote impls + wire protocol land in Plan B-remote; the pure-Dart
/// shelf server lands in Plan C; Flutter widgets land in Plan D.
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

// Local impls
export 'src/local/local_action_submitter.dart' show LocalActionSubmitter;
export 'src/local/local_auth_session.dart' show LocalAuthSession;
export 'src/local/local_permission_source.dart' show LocalPermissionSource;
export 'src/local/local_view_source.dart' show LocalViewSource;
