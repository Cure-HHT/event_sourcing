/// Headless Flutter widget primitives for apps built on the `reaction`
/// package. See `spec/prd-reaction.md` (EVS-PRD-reaction-widget-contract).
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
