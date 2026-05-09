# EVS-prd-permissions-as-events: Permissions as Events

**Level**: prd | **Status**: Draft | **Refines**: EVS-prd-library-charter

## Purpose

The library treats authorization data as part of the audit log: permission grants, role assignments, and policy decisions are themselves events. The library's authorization machinery reads the projections of those events to make decisions; it does not consult any authority outside the log at decision time. The audit story for "what did the system do?" and the audit story for "what was X allowed to do?" rest on the same log and the same materializer.

This PRD pins the trust model. The action-dispatch flow that consumes this trust model is specified separately in EVS-prd-action-dispatch.

## Assertions

A. Permission grants, role assignments, and policy decisions SHALL be events recorded in the same log as application state changes.

B. The library SHALL evaluate authorization decisions solely from its own event-derived projections.

C. The permission state at any point in the log SHALL be reconstructable from the log alone.

## Rationale

**Why are permissions events rather than rows in an IAM table?** A permission system that lives outside the event log can drift from the audit story without leaving evidence: a row updated, a role re-scoped, an entry deleted, all without the kind of immutable record the rest of the system produces. Putting permission grants and role assignments into the log subjects them to the same append-only, hash-chained, auditable treatment as every other state change.

**Why evaluate decisions from projections rather than from external authorities?** External authorities (OIDC providers, IAM services, key vaults) are authoritative about *who someone is*, not about *what they're allowed to do in this application*. Mixing those concerns at decision time creates two problems: the audit trail no longer fully explains decisions (some authority was consulted at runtime), and identity-provider outages become application outages. Translating identity assertions into events at ingest, then evaluating decisions from projections, keeps the audit story complete and the runtime self-contained.

**Why reconstructability from the log alone?** A regulator reviewing the audit asks "did X have permission to do Y at time T?" If permissions live outside the log, the answer requires consulting an external system whose own state at time T may not be reconstructable. By guaranteeing that the log alone is sufficient, the library makes "permission as of time T" computable from the same evidence base as "action as of time T".

**Where do external identity providers fit?** At the ingest boundary. An identity assertion from an external system enters the log as an event ("X authenticated via provider P at time T"); from there it participates in projections like any other event. External systems become event sources, not decision-time consultants.

*End* *Permissions as Events* | **Hash**: 00000000
