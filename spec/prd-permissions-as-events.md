# EVS-PRD-permissions-as-events: Permissions as Events

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library treats authorization data as part of the audit log: permission grants, role assignments, and policy decisions are themselves events. The library's authorization machinery reads the projections of those events to make decisions; it does not consult any authority outside the log at decision time. The audit story for "what did the system do?" and the audit story for "what was X allowed to do?" rest on the same log and the same materializer.

This PRD pins the trust model. The action-dispatch flow that consumes this trust model is specified separately in EVS-PRD-action-dispatch.

## Assertions

A. Permission grants, role assignments, and policy decisions SHALL be events recorded in the same log as application state changes.

B. The library SHALL evaluate authorization decisions solely from its own event-derived projections.

C. The permission state at any point in the log SHALL be reconstructable from the log alone.

D. The `AuthorizationPolicy` implementation evaluating decisions SHALL live in library code; applications SHALL NOT supply alternative policy implementations at composition time. Apps declare the permissions an action requires via `Action.permissions` and seed grants/assignments via events (`permission_granted`, `role_assigned`); they do NOT supply Allow/Deny logic. Alternative policy mechanisms (e.g., ABAC, rule engines, attribute-based scope evaluators) SHALL be introduced through library extension under the Append-Only Primitives discipline (a new shipped policy type with frozen semantics), NOT through app-side replacement.

## Rationale

**Why are permissions events rather than rows in an IAM table?** A permission system that lives outside the event log can drift from the audit story without leaving evidence: a row updated, a role re-scoped, an entry deleted, all without the kind of immutable record the rest of the system produces. Putting permission grants and role assignments into the log subjects them to the same append-only, hash-chained, auditable treatment as every other state change.

**Why evaluate decisions from projections rather than from external authorities?** External authorities (OIDC providers, IAM services, key vaults) are authoritative about *who someone is*, not about *what they're allowed to do in this application*. Mixing those concerns at decision time creates two problems: the audit trail no longer fully explains decisions (some authority was consulted at runtime), and identity-provider outages become application outages. Translating identity assertions into events at ingest, then evaluating decisions from projections, keeps the audit story complete and the runtime self-contained.

**Why reconstructability from the log alone?** A regulator reviewing the audit asks "did X have permission to do Y at time T?" If permissions live outside the log, the answer requires consulting an external system whose own state at time T may not be reconstructable. By guaranteeing that the log alone is sufficient, the library makes "permission as of time T" computable from the same evidence base as "action as of time T".

**Where do external identity providers fit?** At the ingest boundary. An identity assertion from an external system enters the log as an event ("X authenticated via provider P at time T"); from there it participates in projections like any other event. External systems become event sources, not decision-time consultants.

**Why is policy itself substrate code, not an app-supplied callback (assertion D)?** Closed-under-events (assertion C) requires that every Allow/Deny outcome be reproducible from `(events, lib_version)` alone. An app-supplied policy callback would make the Allow/Deny outcome depend on application code at the point of decision: the same log replayed against the same library version could produce different outcomes if the app's policy code had evolved. The closed-under-events guarantee would no longer hold for action outcomes. By pinning the policy mechanism in library code, both the *inputs* to the decision (the projections it reads) and the *decision function itself* are part of the substrate's epistemic surface — and any change to either is visible as a `lib_version_changed` event. Apps still customize what's permitted: they declare `Action.permissions` per action, register `ScopeClassSpec`s, and seed `permission_granted` / `role_assigned` events. They do not, however, write Allow/Deny logic.

**Why is the alternative-mechanism path "library extension under Append-Only Primitives"?** If a deployment genuinely needs a policy mechanism the role/permission/scope model can't express (e.g., attribute-based access control over arbitrary event-derived facts), the right move is to ship a new policy type *in library code*, named, with frozen semantics. That preserves the closed-under-events guarantee — outcomes still depend only on `(events, lib_version)` — and the new type's existence is itself part of the substrate's versioned surface. App-side replacement would break the guarantee; lib-side extension reinforces it.

*End* *Permissions as Events* | **Hash**: 5617d92d

## EVS-DEV-bootstrap-action-permissions: YAML-seeded role/permission bootstrap

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-permissions-as-events

### Assertions

A. `bootstrapActionPermissions` SHALL load a declarative role/permission seed (from `yamlPath` or `yamlSource`) and SHALL append a `permission_granted` event into the substrate log for every (role, permission) pair declared in the seed that is not already present in the `role_permission_grants` projection.

B. On seed parse failure or validation failure (unknown permission name, scope-class reference not registered, etc.), `bootstrapActionPermissions` SHALL return a `PolicyFailSafe` whose `.policy` is a `FailSafeAuthorizationPolicy` that denies every `isPermitted` call with `DenyReason.notGranted` and returns `EffectiveAuthorization.empty` from every `effectivePermissionsFor` call.

C. On success, `bootstrapActionPermissions` SHALL return a `PolicyReady` whose `.policy` is a `TableBackedAuthorizationPolicy` reading directly from the substrate's `role_permission_grants` and `user_role_scopes` projections (i.e., evaluating decisions solely from the event-derived projections per `EVS-PRD-permissions-as-events/B`).

D. Seed application SHALL be idempotent: re-running `bootstrapActionPermissions` against an `EventStore` whose log already contains all grants declared in the seed SHALL emit zero new `permission_granted` events. (The closed-under-events guarantee in `EVS-PRD-permissions-as-events/C` requires that the event log alone is sufficient; idempotent seeding ensures bootstrap composes safely into normal startup paths.)

### Changelog

- 2026-05-24 | fe9d9a46 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-05-24 | - | - | Michael Lewis (<michael.lewis.c@gmail.com>) | Initial authoring; locks in shipped bootstrapActionPermissions surface

*End* *YAML-seeded role/permission bootstrap* | **Hash**: fe9d9a46
