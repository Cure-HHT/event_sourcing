# Changelog

## 0.1.0-dev (unreleased)

- `FakeReaction.emitViewError(viewName, error, [stackTrace])` delivers an
  error on the error channel of a view's active subscribers, so widget
  tests can drive a `ViewConvergingRefusal`, a `SubscriptionDenied` or any
  other subscription error.
