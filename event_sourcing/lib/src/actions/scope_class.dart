// Implements: EVS-PRD-action-dispatch/B (ScopeClass is the session-context precondition checked during the authorize stage)
// Implements: EVS-PRD-permissions-as-events/B (scope preconditions constrain which principals may exercise a permission per projection-derived session state)

/// The session-context precondition a permission attaches to.
///
/// - [global]: no precondition; the principal's session state doesn't
///   restrict whether the permission may be exercised.
/// - [site]: the principal must have a non-null `activeSite`.
/// - [self]: the principal must be authenticated (non-null `userId`).
///
/// Adding a value here is a deliberate code-plus-REQ change.
enum ScopeClass { global, site, self }
