# Changelog

## 0.1.0-dev (unreleased)

- A subscription refused with `view_converging` no longer terminates: the
  refusal still surfaces once on the subscription's stream as a typed,
  transient `ViewConvergingRefusal`, and `RemoteConnection` re-issues the
  same subscribe on a capped backoff, with no bound on the number of
  attempts, until the server serves it or the caller unsubscribes or the
  connection is disposed — the caller recovers from its single `watch()`
  call with no new request. `RemotePermissionSource`'s snapshot retry on a
  503 `view_converging` response loses its fixed attempt bound the same
  way, for the same reason.
- `RemoteActionSubmitter` gains an opt-in, off-by-default automatic
  retry of a `view_converging` refusal: `maxConvergingRetries` bounds
  the number of re-sends, each waiting a bounded backoff, re-sending
  the same `ActionSubmission` unchanged (its idempotency key never
  changes across attempts), and surfacing each refusal on the new
  `convergingStream` while it waits. With the option left at its
  default (`0`), `submit()` still delivers the typed, transient
  `ViewConvergingRefusal` to its caller immediately, with no retry.
- `UpdateCodec` gains wire shapes for the substrate's `Pending<T>` variant
  (`type: "pending"`, `aggregateId`, `sequence` always 0) and for
  `EndOfReplay<T>`'s new `state` field (`type: "end_of_replay"`, `state`:
  `"current"` or `"converging"`), preserving both fields end to end for a
  remote consumer exactly as an in-process subscriber sees them.
  `RemoteViewSource` maps both across the consumer-supplied mapper.
- A scoped subscription whose containment view is still converging refuses
  with the typed `WireErrorCode.viewConverging` wire error, naming the
  view, instead of narrowing the subscription's aggregate set or
  surfacing as `internal_error`. `ScopeDescendantExpander`'s row reads go
  through the substrate's `currentViewRows` adapter, the same currency
  check `ContainmentResolver` uses for the write path.
- The subscription-handler `ErrorMsg` for a `view_converging` refusal
  carries the subscriptionId it refuses, so `RemoteConnection` routes it
  to that subscription's stream and the client surfaces it there instead
  of silently dropping an unaddressed error frame.
- Action dispatch and the permission-snapshot route answer a
  `ViewConvergingRefusal` with a 503 and a `{"error": "view_converging",
  "view": "<name>"}` body naming the view (`Retry-After` set).
  `RemoteActionSubmitter` decodes that body with
  `decodeViewConvergingBody` and throws the typed, transient
  `ViewConvergingRefusal` rather than a generic `TransportException`.
  `RemotePermissionSource` decodes it the same way and, on both the
  Authenticated-transition fetch and an explicit `refresh()`, schedules
  a bounded-backoff retry (honouring `Retry-After` when the server
  sends one) instead of leaving the snapshot stale; `refresh()` still
  throws the refusal to its awaiting caller. Its `converging` getter
  and `convergingStream` expose the typed refusal while a retry is
  pending, clearing on the next successful fetch.
- `AuthorizationWatcher`'s revoke fan-out (`permission_revoked`,
  `role_unassigned`) fails closed on every error reading a connected
  user's role, not only the permission policy's typed
  `ViewConvergingRefusal`: the user is force-logged-out rather than left
  connected either way, and the fan-out continues past that user rather
  than aborting. An error other than the typed refusal is logged at
  `severe` through the `logging` package, since it is not the expected
  transient case. No error from the unawaited fan-out escapes uncaught.
