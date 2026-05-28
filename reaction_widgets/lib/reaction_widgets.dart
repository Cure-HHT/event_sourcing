/// Headless Flutter widget primitives for apps built on the `reaction`
/// package. See `spec/prd-reaction.md` (EVS-PRD-reaction-widget-contract).
///
/// ## What this package provides
///
/// - [ReActionScope] — InheritedWidget that threads a `ReactionScope`
///   (the four reaction interfaces + connection status) down the
///   widget tree. Mount once near the app root.
/// - [ActionBuilder] — rebuilds on `ActionState` transitions
///   (Idle/Submitting/Success/Denied/Failed) for a single
///   `ActionSubmitter` flow.
/// - [ViewBuilder] / [ViewListener] — declarative (builds rows) and
///   imperative (side-effects on row changes) consumers of a
///   `ViewSource` subscription. [ViewState] is the sealed
///   build-time state (Loading/Ready/Stale).
/// - [PermissionGate] — conditionally builds children based on a
///   `PermissionSource` snapshot for the active principal.
/// - [ReActionErrorListener] — surfaces transport / subscription
///   errors out of the scope as imperative callbacks.
/// - [FakeReaction] + [pumpReactionWidget] — in-memory test harness
///   for widget tests that does not require a real substrate.
library;

// Scope
export 'src/scope/reaction_scope_widget.dart' show ReActionScope;

// Action
export 'src/action/action_builder.dart' show ActionBuilder, ActionBuilderFn;

// View
export 'src/view/view_state.dart' show ViewState, Loading, Ready, Stale;
export 'src/view/view_builder.dart' show ViewBuilder, ViewBuilderFn;
export 'src/view/view_listener.dart' show ViewListener;

// Permission
export 'src/permission/permission_gate.dart' show PermissionGate;

// Error
export 'src/error/reaction_error_listener.dart'
    show ReActionErrorListener, ReActionErrorCallback;

// Testing
export 'src/testing/fake_reaction.dart' show FakeReaction, pumpReactionWidget;
