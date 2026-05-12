# EVS-PRD-action-dispatch: Action Dispatch

**Level**: prd | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

Application-level operations enter the library through the action-dispatch flow. A principal submits an action describing what it wants to do; the library parses it, validates its shape, authorizes it against the closed-under-events trust model (EVS-PRD-permissions-as-events), executes the application logic, and records the outcome in the event log. Whether the action succeeds or is rejected, the audit log records the decision.

The dispatch flow is the only path by which consumer-initiated state changes reach the log. Direct event-append APIs are not exposed for consumer use; everything that ends up in the log goes through the same auditable pipeline.

## Assertions

A. The library SHALL accept actions submitted by principals and process each through the dispatch flow.

B. The dispatch flow SHALL include parsing, validation, authorization, execution, and recording stages, applied in that order.

C. Every dispatched action SHALL produce a recorded outcome in the event log: success events for actions that pass all stages, or a denial event describing the stage at which the action failed and why.

D. The dispatch flow SHALL be idempotent on the action identifier: a retry with the same identifier and matching content SHALL produce the same outcome as the original attempt.

E. Submission of an action whose identifier matches a prior action with different content SHALL produce a denial event.

F. All consumer-initiated events recorded in the local log SHALL be produced by the dispatch flow.

## Rationale

**Why a single dispatch flow rather than separate APIs?** Consumers that have to compose parse → validate → authorize → execute → record themselves end up reimplementing the same orchestration in every application, with subtly different failure semantics each time. Concentrating the orchestration in one library flow gives the audit story a single shape and makes the failure modes a closed set.

**Why the structural assertion F?** Assertion A says dispatch is the path for consumer-submitted actions; F says it is the *only* path consumer-initiated events take to the local log. Without F, a consumer could in principle reach the log via a side-door (a direct append API, a back-channel from another module) and the audit story would weaken accordingly. F is verified by a structural / scan test: the codebase's public surface and call graph admit no log-write path other than dispatch (events arriving via ingest are handled separately and bear upstream identity, not consumer identity). Both A and F belong because A is observable per-action and F is observable across the codebase.

**Why these specific stages?** Each captures a distinct class of failure that the audit needs to distinguish. Parse failures mean the action could not be understood; validation failures mean the action was understood but malformed; authorization failures mean the principal was not allowed; execution failures mean the application logic rejected the action even though it was authorized. Conflating these would make audit answers like "why was this denied?" ambiguous.

**Why record denials, not just successes?** An audit that only records what the system accepted leaves "what the system rejected, and why" unrepresented. A regulator reviewing access patterns cannot see whether a principal repeatedly attempted unauthorized actions, because no record exists. Denial events make the rejection trail part of the same evidence base as the success trail.

**Why idempotency by identifier?** Consumers retry. Network glitches, app restarts, and tier-to-tier replays all produce duplicate submissions of the same logical action. Without dispatcher-level idempotency, every consumer would have to deduplicate themselves before submitting; with it, retries are safe by default and the audit log records each logical action once even when it is submitted many times.

**Why is mismatched content under a duplicate identifier a denial?** Two cases need distinct handling. A retry with identical content is the consumer's intended duplicate suppression — return the cached outcome. A submission with the same identifier but different content is either a consumer bug or an attack; silently overwriting or silently returning the cached outcome would absorb the discrepancy, leaving no audit trail. A denial event records the conflict explicitly so the audit captures what was attempted, not just what was accepted.

*End* *Action Dispatch* | **Hash**: 3b0a0ef4
