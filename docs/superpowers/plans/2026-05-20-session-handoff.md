# Session handoff — Plan B-remote+C ready for implementation

**Date:** 2026-05-20
**Branch:** `CUR-1317-plan-b-remote`
**Purpose:** A self-contained prompt for resuming Plan B-remote+C work in a fresh session.

---

## Prompt to start the next session

> Resume the `CUR-1317-plan-b-remote` branch at
> `/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing`.
> We're implementing Plan B-remote+C: cross-process wire layer for
> the reaction package (Remote* client impls + JSON wire codecs +
> server-side shelf handlers).
>
> The full design + impl plan is committed on the branch; CUR-1331
> (scope-aware permissions) is merged on main and provides the
> primitives this work consults. Start with **Task 1 of the impl
> plan at `docs/superpowers/plans/2026-05-13-reaction-remote-impl.md`**
> (a quick verification sweep), then proceed task-by-task using
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans.
>
> Read these first, in order, before touching code:
>
> 1. `docs/superpowers/plans/2026-05-20-session-handoff.md`
>    (this file) — current state + load-bearing decisions from
>    earlier sessions.
> 2. `spec/reaction-remote.md` — design spec; module layout,
>    wire protocol, server-side composition, mid-session
>    permission-change handling, decisions and rejected
>    alternatives.
> 3. `docs/superpowers/plans/2026-05-13-reaction-remote-impl.md`
>    — task-by-task implementation plan. Task 1 is the API
>    drift-sweep verification; Tasks 2–37 are the actual work.
> 4. `docs/event-sourcing-guide.md` (chapter "Provisional:
>    cross-process client/server deployments") — narrative
>    framing for the design.
> 5. `spec/scoped-permissions.md` — CUR-1331's substrate-side
>    permission primitives this plan consults.

---

## State at the end of this session

### What's committed on `CUR-1317-plan-b-remote`

```text
e39a53d Plan B-remote+C: handle mid-session permission changes (force-logout + stale-data)
28d787d Plan B-remote+C: drop ReactionServer; ship ReactionHandlers; drop per-Principal permission cache
eb9be48 Plan B-remote+C implementation plan (gated on CUR-1331 impl)
c85b623 Move reaction-remote design into spec/ per in-place lifecycle; align to CUR-1331
50769f4 Plan B-remote+C design spec — reaction remote impls, server, wire
b6906a9 [main] CUR-1331 scope-aware permissions
```

### What's open in the working tree (uncommitted)

- This handoff file + final drift-sweep edits to the impl plan.
  Committed at the end of this session.

The guide chapter (`docs/event-sourcing-guide.md`, including the
new section on mid-session permission changes) was committed in
`e39a53d`. Mirror copy at `~/event-sourcing-guide.md`.

### What's design-complete

- **Spec** (`spec/reaction-remote.md`, ~1300 lines): wire protocol,
  client lifecycle (RemoteScope + 4 Remote* impls + shared
  RemoteConnection with reconnect), server-side composition
  (ReactionHandlers config bundle, NOT a ReactionServer class,
  composing into consumer-supplied shelf pipelines), per-subscription
  authorization (Approach B: view-level deny + row-level narrowing
  via CUR-1331's ContainmentResolver), mid-session permission-change
  handling (AuthzWatcher + force-logout via 4003 close-frame +
  stale_data envelope on UX-only changes), testing strategy.
- **Impl plan** (`docs/superpowers/plans/2026-05-13-reaction-remote-impl.md`,
  ~5000 lines): 37 tasks structured Phase 0–5, each with failing
  test, impl, verify, commit steps. API names verified against
  CUR-1331 + CUR-1330 delivered surface (drift-corrected this
  session).
- **Guide chapter** (in `docs/event-sourcing-guide.md`): provisional
  chapter explaining the design narratively, including the
  mid-session permission-change subsection.

### What's still open as design decisions

1. **Drift remains in spec/plan from earlier API names that may have
   shifted in CUR-1330/1331.** Task 1 in the impl plan is an
   explicit drift-sweep — verify before starting Task 2. Specifically:
   confirm `DispatchResult` variants (`DispatchSuccess`,
   `DispatchUnknownAction`, `DispatchParseDenied`,
   `DispatchValidationDenied`, `DispatchAuthorizationDenied`,
   `DispatchExecutionFailed`, `DispatchIdempotencyHit`),
   `UserPrincipal(userId, roles, activeRole)` shape, `EventStore.append`
   signature (`data:` not `payload:`, `initiator:` not `principal:`,
   `aggregateType:` required), `ActionDispatcher.authorization` (not
   `.policy`), and that `PermissionSnapshot` + `EffectiveAuthorization`
   both still exist.
2. **`spec/reaction-remote.md` Open Questions section** has 4 remaining
   items (Principal wire encoding canonicalization, HTTP timeout
   defaults, reconnect backoff parameters, concurrent subscribes
   during reconnect) — all small impl-time refinements; resolve as
   encountered.

### Load-bearing decisions from this session (don't re-litigate)

The full rationale lives in the spec's "Decisions and alternatives
rejected" section. The shortlist:

1. **No `ReactionServer` class.** Ship `ReactionHandlers` (config
   bundle) + middleware + validator interface. Consumers compose
   handlers into their own shelf pipelines. Matches portal_server +
   diary_server's existing patterns; supports the collapse-not-add
   migration story (portal's 30+ bespoke routes shrink to ~5
   reaction handlers).

2. **No per-Principal permission cache on the WS connection.** Each
   `subscribe` message calls policy methods fresh. Substrate queries
   are sub-millisecond; cache complexity (race + cleanup) wasn't
   worth it.

3. **Mid-session permission changes: force-logout on revocation +
   stale_data signal on expansion/containment.** One server-wide
   `AuthzWatcher` substrate subscription. Close affected WS with new
   `4003 permissions_changed` on `role_unassigned` /
   `permission_revoked` (security-narrowing intent). Send `stale_data`
   envelope on `role_assigned` / `permission_granted` (UX-only).
   Containment-projection changes default OFF (opt-in via
   `ReactionHandlers.watchContainment(...)`).

4. **Auth validators are consumer-supplied.** Lib ships
   `PrincipalAuthValidator` interface + `TrustingAuthValidator`
   (dev/test only). Firebase/Auth0/linking-code validators live in
   `portal_server` / `diary_server` / `cure-hht/portal` repos.

5. **Per-subscription authz: Approach B (two-tier).** View-level deny
   via `policy.isPermitted(principal, viewPermission, null)`;
   row-level narrowing via expanding `effectivePermissionsFor`
   scopeAssignments through CUR-1331's `ContainmentResolver`.

### Implementation sequencing

Tasks run roughly sequentially within phases; tasks across phases
have natural dependencies. Subagent-driven flow:

- **Phase 0 (Tasks 1–2)**: drift sweep + pubspec deps. Quick.
- **Phase 1 (Tasks 3–10)**: wire codecs. Each task ~30 min.
  Highly parallelizable across subagents (no inter-task deps
  beyond shared `envelope.dart` from Task 3).
- **Phase 2 (Tasks 11–18)**: server-side primitives. Some
  dependencies (Task 17 needs Task 12 + 13; Task 18 needs all of
  11–17b). Task 17b is the new AuthzWatcher + WsConnectionRegistry
  task between original Task 17 and Task 18.
- **Phase 3 (Tasks 19–25)**: client primitives. Internal
  dependencies but largely independent from server.
- **Phase 4 (Tasks 26–33)**: test harness + e2e suite. Depends on
  Phases 2 and 3.
- **Phase 5 (Tasks 34–37)**: barrel exports, CLAUDE.md trust
  boundaries update, roadmap update, final verification.

Aim ~3–5 task commits per session for review-rate-of-progress.

### Things to verify in the drift sweep (Task 1)

These are the specific API shapes the plan assumes from CUR-1331 +
CUR-1330. Confirm each before writing code (greps in the
`event_sourcing/lib/` tree):

```text
class AuthorizationPolicy             abstract; Future<Decision>
                                        isPermitted(Principal,
                                                    Permission,
                                                    ScopeValue?,
                                                    {Txn? txn})
                                       Future<EffectiveAuthorization>
                                        effectivePermissionsFor(Principal,
                                                                {Txn? txn})

class EffectiveAuthorization          activeRole, rolePermissions,
                                       scopeAssignments

sealed class Principal                UserPrincipal(userId, roles, activeRole),
                                       AnonymousPrincipal(ipAddress?)
                                       — UserPrincipal NOT const

sealed class ScopeValue               BoundScope(class_, value),
                                       ValueWildcardScope(class_),
                                       TotalWildcardScope()
                                       — already has toJson()/fromJson

sealed class DispatchResult<TResult>  DispatchSuccess, DispatchUnknownAction,
                                       DispatchParseDenied,
                                       DispatchValidationDenied,
                                       DispatchAuthorizationDenied,
                                       DispatchExecutionFailed,
                                       DispatchIdempotencyHit

class ContainmentResolver             resolve({required Txn, required BoundScope,
                                                required String target})
                                       -> Future<BoundScope?>

class EventStore.append               aggregateType:, aggregateId:,
                                       entryType:, eventType:,
                                       data:, initiator:, ...
                                       (NOT payload:/principal:)

class ActionDispatcher                .authorization field (NOT .policy)
                                       .dispatch(ActionSubmission, ActionContext)

class PermissionSnapshot              still exists; role + grants + issuedAt;
                                       has toJson()/fromJson factory
                                       (LocalPermissionSource still uses it)

StorageBackend.findViewRowsInTxn      (Txn, viewName, {where, limit, offset})
                                       where = Map<String, Object?>? for
                                       column-equality filtering
```

Verify with `grep -rn "<symbol>" event_sourcing/lib/`. Any mismatch
needs a plan edit before continuing.

### Useful refs

- Memory: `~/.claude/projects/-home-metagamer-cure-hht-event-sourcing/memory/project_reaction_remote_design.md` — design summary for future sessions
- Memory: `feedback_auth_validators_are_consumer_supplied.md` — why no JWT validators in lib
- Memory: `project_trust_boundaries.md` — updated this session to add the wire-auth flow as the fourth enumerated trust input
- Roadmap: `docs/superpowers/specs/2026-05-11-roadmap.md` — Plan B-remote+C entry under "## Plan B-remote+C" (added by Task 36 of the impl plan when executed)

### When done

Phase 5 includes (Task 34) updating the barrel exports, (Task 35)
adding the fourth trust-boundary entry to CLAUDE.md, (Task 36)
updating the roadmap. The guide chapter loses its "Provisional"
marker when impl lands.

After all 37 tasks: open a PR from `CUR-1317-plan-b-remote` to
`main`. The PR title format the org's branch-protection rule
requires is `[CUR-1317] Plan B-remote+C: <subject>`.
