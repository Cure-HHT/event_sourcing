# Changelog

## 0.1.0-dev (unreleased)

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
  `RemoteActionSubmitter` and `RemotePermissionSource` decode that body
  with `decodeViewConvergingBody` and throw the typed, transient
  `ViewConvergingRefusal` rather than a generic `TransportException`.
- `AuthorizationWatcher`'s revoke fan-out (`permission_revoked`,
  `role_unassigned`) fails closed: a connected user whose role membership
  cannot be confirmed while the permission policy's view converges is
  force-logged-out rather than left connected, and the fan-out continues
  past that user rather than aborting on the transient refusal.
