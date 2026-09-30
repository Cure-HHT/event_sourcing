# Changelog

## 0.1.0-dev (unreleased)

- Breaking: `ViewState` gains three sealed variants, so every exhaustive
  switch over it needs three more arms. `ViewBuilder` surfaces every error
  of its subscription as one of them and leaves none uncaught:
  - `Converging(viewName)` for a `ViewConvergingRefusal`: the view source
    re-issues the subscription itself, and the first update of the
    recovered replay moves the state to `Loading` (or `Ready` in progressive
    mode), its `EndOfReplay` to `Ready`.
  - `Rejected(denial)` for a `SubscriptionDenied`.
  - `Errored(error, stackTrace)` for any other error.

  `Rejected` and `Errored` are terminal: `ViewBuilder` cancels the
  subscription, and later updates or `ConnectionStatus` changes leave the
  state in place. The `semanticIdentifier` value token gains
  `converging`, `rejected` and `errored`.
- `ViewListener` gains an optional `onError` callback receiving each
  subscription error with its stack trace. Without one, a
  `ViewConvergingRefusal` is dropped and any other error is reported to
  `FlutterError.reportError`.
