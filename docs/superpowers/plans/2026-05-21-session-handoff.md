# Session handoff — Plan B-remote+C through Task 20

**Date:** 2026-05-21
**Branch:** `CUR-1317-plan-b-remote`
**Purpose:** Self-contained prompt + status doc for resuming Plan B-remote+C
implementation in a fresh session. Picks up after Task 20.

---

## Prompt to start the next session

> Resume the `CUR-1317-plan-b-remote` branch at
> `/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing`.
> Phase 1 (8 wire codecs) and Phase 2 (server primitives + ReactionHandlers)
> are complete. Phase 3 is in progress: Task 19 (RemoteConnection skeleton)
> and Task 20 (WS lifecycle + subscription routing) are committed.
>
> The next task is **Task 21: RemoteAuthSession** at
> `docs/superpowers/plans/2026-05-13-reaction-remote-impl.md` line ~3872.
> Use superpowers:subagent-driven-development (the standard flow this
> session used).
>
> Read these first, in order, before touching code:
>
> 1. `docs/superpowers/plans/2026-05-21-session-handoff.md` (this file) —
>    current state + lessons learned + per-task drift map for Phase 3-5.
> 2. `docs/superpowers/plans/2026-05-20-session-handoff.md` — the prior
>    session's handoff (still useful for design decisions).
> 3. `spec/reaction-remote.md` — design spec (updated this session with
>    substrate-true wire shapes for Update<T> and DispatchResult, plus
>    "Structured error encoding" Future-work note).
> 4. `docs/superpowers/plans/2026-05-13-reaction-remote-impl.md` —
>    impl plan. Tasks 6, 8, 14 have been swept this session for
>    substrate-true API; Tasks 17, 17b, 18 have substantial corrections
>    that the implementer adopted but the plan text isn't yet swept for
>    (see "Plan drift NOT yet swept" below).
> 5. `docs/event-sourcing-guide.md` "Provisional: cross-process
>    client/server deployments" chapter — narrative framing.

---

## State at end of session

### What's committed on `CUR-1317-plan-b-remote`

Run `git log --oneline CUR-1317-plan-b-remote ^main` to see the full list.
The session added ~25 commits beyond the prior handoff's `35ab189`:

```text
ef61fba reaction remote: RemoteConnection WS lifecycle + subscription routing  (Task 20)
08bce75 reaction remote: RemoteConnection skeleton (HTTP + credential)        (Task 19)
582e94d reaction server: ReactionHandlers config bundle                       (Task 18)
a02d4be reaction server: AuthzWatcher                                         (Task 17b)
c8ac719 reaction server: WS subscription handler (state machine + per-sub authz) (Task 17)
4c87312 reaction server: GET /permissions/snapshot route handler              (Task 16)
ec126f9 reaction server: GET /me route handler                                (Task 15)
92e45f2 reaction remote: sweep plan for non-generic ActionDispatcher.dispatch (sweep)
6f958d6 reaction server: POST /actions handler                                (Task 14)
02917ee reaction server: auth middleware (Bearer -> Principal)                (Task 13)
35629ee reaction server: ViewScopeRegistry                                    (Task 12)
5815df9 reaction server: TrustingAuthValidator                                (Task 11)
37f8a35 reaction wire: subscription control-plane messages                    (Task 10)
e40138d reaction wire: EffectiveAuthorization JSON codec                      (Task 9)
66fd591 reaction remote: sweep spec+plan for DispatchResult drift             (sweep)
ecd2f20 reaction wire: DispatchResult JSON codec (7 variants)                 (Task 8)
470bfac reaction wire: ActionSubmission JSON codec                            (Task 7)
3bed4a4 reaction remote: sweep spec+plan for Update<T> drift                  (sweep)
6b8ae07 reaction wire: Update<T> JSON codec (4 variants)                      (Task 6)
952d423 reaction wire: SubscriptionFilter JSON codec                          (Task 5)
d0a3e1c event_sourcing: normalize SubscriptionFilter (Set + ==/hashCode)      (substrate prep)
d6f7aa3 reaction wire: Principal JSON codec                                   (Task 4)
8a97114 reaction wire: envelope discriminator helpers                         (Task 3)
d1bad2d reaction: add HTTP / WS / shelf deps for Plan B-remote+C              (Task 2)
```

Test counts at session end:

- `event_sourcing` package: 861 + 106 = 967 tests pass
- `reaction` package: ~110 tests pass + ~6 intentionally skipped (Phase 4 deferrals on
  authz_watcher_test.dart and reaction_handlers_test.dart)

### What's in the working tree (uncommitted)

This handoff file. Commit at the end of this session.

---

## Tasks completed this session

| Task | Topic | Commit | Notes |
|---|---|---|---|
| 1 | Drift-sweep verification | (no commit) | All CUR-1331+1330 API shapes verified |
| 2 | Pubspec deps | `d1bad2d` | web_socket_channel pinned to ^3.0.0 (^3.1.0 not on pub) |
| 3 | Wire envelope helpers | `8a97114` | |
| 4 | Principal codec | `d6f7aa3` | |
| — | **Substrate prep** | `d0a3e1c` | SubscriptionFilter normalization (see "Lessons" below) |
| 5 | SubscriptionFilter codec | `952d423` | Encodes includeSystemEvents only when true; predicate dropped |
| 6 | Update<T> codec | `6b8ae07` | Substrate-true: value (not aggregateId+row); Delta has cause |
| — | Update<T> drift sweep | `3bed4a4` | spec + plan |
| 7 | ActionSubmission codec | `470bfac` | |
| 8 | DispatchResult codec | `ecd2f20` | Substrate-true: 7 simpler variants; error as toString() (lossy) |
| — | DispatchResult drift sweep | `66fd591` | spec + plan + Tasks 14, 28 |
| 9 | EffectiveAuthorization codec | `e40138d` | Uses substrate's ScopeValue.toJson/fromJson directly |
| 10 | Subscription messages | `37f8a35` | Sealed ClientMessage/ServerMessage + 3 enums |
| 11 | TrustingAuthValidator | `5815df9` | roles: {defaultActiveRole} (substrate invariant) |
| 12 | ViewScopeRegistry | `35629ee` | |
| 13 | AuthMiddleware | `02917ee` | |
| 14 | POST /actions | `6f958d6` | dispatch is non-generic on the substrate |
| — | dispatch sweep | `92e45f2` | plan only |
| 15 | GET /me | `ec126f9` | |
| 16 | GET /permissions/snapshot | `4c87312` | stub policy needs {Txn? txn} + const Allow() |
| 17 | Subscription handler | `c8ac719` | Many substrate corrections (see "Substrate corrections" below) |
| 17b | AuthzWatcher + Registry | `a02d4be` | Watches StoredEvent (not Map) |
| 18 | ReactionHandlers | `582e94d` | Phase 2 closeout |
| 19 | RemoteConnection skeleton | `08bce75` | HTTP + credential storage |
| 20 | RemoteConnection WS lifecycle | `ef61fba` | Subagent socket-errored; recovered cleanly via manual commit |

---

## Lessons & decisions captured this session

### Substrate normalization (SubscriptionFilter)

Commit `d0a3e1c` performed a small substrate sweep:

- `SubscriptionFilter.entryTypes` changed from `List<String>?` to `Set<String>?`
  (matches the other two allow-lists; allow-list semantics are set-like).
- Added structural `==`/`hashCode` with set-equality on the three allow-lists,
  value-equality on `includeSystemEvents`, **identity equality on `predicate`**
  (closures have no defensible structural equality).
- 11 test files + 2 production files + 6 example files updated.
- `spec/reaction-remote.md` got a "Cross-process predicates" Future-work note —
  predicates can't be serialized; if a future consumer needs cross-process
  predicates, the named-predicate-registry path is the recommended design.

### Substrate-true API drift discovered & corrected

Multiple substrate types diverged from the plan's verbatim text. Each was
caught at implementer-dispatch time and corrected; subagents either applied
the correction inline or surfaced as `BLOCKED` / `DONE_WITH_CONCERNS`. The
correction list:

1. **`Update<T>` shape** — Plan said Snapshot/Delta carry `aggregateId + row`.
   Actual: Snapshot `value: T?`, Delta `value: T, cause: String`, Tombstone
   `aggregateId, sequence`, EndOfReplay `sequence`. Snapshot.value nullable.
   AggregateMode rows carry aggregateId as a column by convention.

2. **`DispatchResult<TResult>` shape** — Plan had `appendedEvents: List<StoredEvent>`,
   rich `AuthorizationDenied(reason, permission, scope, detail)`, `cachedAt: DateTime`.
   Actual much simpler: `emittedEventIds: List<String>`, `AuthorizationDenied(permission)`,
   `IdempotencyHit(cachedResult, priorEmittedEventIds)`. Error variants carry
   `Object error` (encoded as `error.toString()` — lossy; Future-work note added).

3. **`ActionDispatcher.dispatch`** — Plan said `dispatch<TResult>(...) -> Future<DispatchResult<TResult>>`.
   Actual is non-generic: `dispatch(submission, ctx) -> Future<DispatchResult<Object?>>`.

4. **`AuthorizationPolicy` overrides require `{Txn? txn}`** on BOTH `isPermitted`
   and `effectivePermissionsFor`. Plan's stubs missed it consistently — Tasks 16, 17, 17b
   all needed correction.

5. **`AuthorizationDecision.allow()` / `.deny()` factories don't exist** — it's a sealed
   type with `const Allow()` and `Deny({required this.permission, required this.reason})`.
   Tasks 16, 17 stubs corrected.

6. **`Events` not `EventsMode<T>`** — `Events extends SubscriptionMode<StoredEvent>` (no
   type parameter). Used in Task 17b's AuthzWatcher: `subscribe<StoredEvent>(filter, const Events())`
   yields `Stream<Update<StoredEvent>>`. The plan's `EventsMode<Map<String, Object?>>()`
   doesn't compile.

7. **`SubscriptionMode<T>` not `SubscribeMode<T>`** — typo in the plan's stub signature.

8. **`UserPrincipal` requires `roles` non-empty and `roles.contains(activeRole)`** —
   stubs constructing `UserPrincipal(userId, activeRole)` without `roles` violate
   the substrate's runtime invariant. Fix: `roles: {activeRole}`.

9. **`StoredEvent.eventType` docstring is stale** — claims `'finalized' | 'checkpoint' | 'tombstone'`,
   but production code stamps action-specific names like `'role_assigned'` directly into
   `eventType`. The AuthzWatcher dispatches on `event.eventType` based on real usage, not
   the docstring. **Worth a separate substrate-cleanup PR.**

### `Permission.==` keys on name only

A surface noted in Task 9: `Permission.==` ignores `scopeClass` (only `name`).
Codecs still preserve both fields on the wire; tests must drill into per-field
assertions on `permission.name` and `permission.scopeClass` rather than relying
on object equality. The `EffectiveAuthorizationCodec` test docs this.

### Combined reviewer per task (per [[combined-subagent-reviews]] memory)

Each task this session got a single combined-reviewer dispatch covering spec
compliance + code quality, not two separate dispatches. This halved the
reviewer overhead vs the skill's default two-stage split. The fix-loop
discipline was preserved (none of this session's reviews surfaced Critical /
Important issues requiring a re-review).

### Subagent reliability

Task 20's subagent socket-errored after ~23 minutes. The work was on-disk
uncommitted; recovery was a manual `dart format` + re-stage + commit. Pattern
to watch for in subsequent sessions: if a subagent doesn't return cleanly,
check `git status` for uncommitted work before re-dispatching.

---

## Plan drift NOT yet swept

The plan doc (`docs/superpowers/plans/2026-05-13-reaction-remote-impl.md`)
still has stale text for the following tasks. The committed code is correct
(implementers applied corrections); the plan text wasn't swept:

- **Task 16 (`/permissions/snapshot`)** — stub policy in plan misses `{Txn? txn}`
  and uses `AuthorizationDecision.allow()` (doesn't exist).
- **Task 17 (subscription handler)** — `SubscribeMode<T>`, `AuthorizationDecision.allow()/deny()`,
  missing `{Txn? txn}` on stub policy, missing `permissionViewName` removal.
- **Task 17b (AuthzWatcher)** — `EventsMode<T>` (should be `const Events()`),
  `Update<Map<String, Object?>>` (should be `Update<StoredEvent>`),
  `payload['eventType']` (should be `event.eventType`), `payload['user_id']`
  (should be `event.data['user_id']`).
- **Task 18 (ReactionHandlers)** — minor: no big drift; mostly correct verbatim.

A "follow-up plan sweep" commit could refresh these — analogous to the Update<T>
sweep at `3bed4a4` and the DispatchResult sweep at `66fd591`. Reasonable
follow-on work; not blocking for resuming Task 21.

### Plan drift to verify on Tasks 21-37 (substrate-true checklist)

When dispatching each remaining task, verify these substrate shapes upfront:

```text
- AuthSession interface         reaction/lib/src/interfaces/auth_session.dart
- AuthStatus sealed type        NotAuthenticated / Authenticated(principal) / Expired
                                  (verify field names; some plans say `principal:`,
                                   others positional — check the interface)
- ActionSubmitter interface     reaction/lib/src/interfaces/action_submitter.dart
- TransportException            shipped where? reaction.dart re-export?
- PermissionSource interface    reaction/lib/src/interfaces/permission_source.dart
- LocalAuthSession              reference impl (for matching shape)
- LocalActionSubmitter          reference impl
- LocalViewSource               reference impl
- LocalPermissionSource         reference impl
```

For Phase 4 (e2e harness + tests, Tasks 26-33):

```text
- SembastEventStoreHarness      event_sourcing/test/permissions/test_support/sembast_event_store_harness.dart
- PolicyHarness                 event_sourcing/test/permissions/test_support/policy_harness.dart
- shelf_io.serve                package:shelf/shelf_io.dart
- WebSocketChannel.connect      production WS-factory call
```

---

## What's remaining (Tasks 21-37)

### Phase 3 — Client primitives (5 tasks)

- **Task 21:** RemoteAuthSession — HTTP GET /me on setCredential; AuthStatus transitions; WS close-frame integration via `onAuthRejected()` / `onWireUnauthorized()`.
- **Task 22:** RemoteActionSubmitter — JSON POST /actions; TransportException on no-auth.
- **Task 23:** RemoteViewSource — `openSubscription` on RemoteConnection + consumer-supplied row mapper.
- **Task 24:** RemotePermissionSource — HTTP GET /permissions/snapshot + WS sub on role_permission_grants for live updates.
- **Task 25:** RemoteScope — composition class; constructs the four `Remote*` impls over one shared RemoteConnection.

### Phase 4 — Test harness + e2e (8 tasks)

- **Task 26:** ReactionRemoteTestHarness — substrate setup, validator, ReactionHandlers, shelf_io.serve, RemoteScope. Mirrors `event_sourcing/test/permissions/test_support/sembast_event_store_harness.dart` pattern.
- **Task 27:** auth_test.dart e2e
- **Task 28:** action_test.dart e2e (includes DispatchResult variant round-trips — already plan-swept for substrate-true shape)
- **Task 29:** view_test.dart e2e (includes Update<T> envelope assertions — already plan-swept)
- **Task 30:** permission_test.dart e2e
- **Task 31:** reconnect_test.dart e2e (also exercises Task 20's reconnect path)
- **Task 32:** authz_test.dart e2e (the AuthzWatcher coverage that authz_watcher_test.dart skip-stubs deferred to Phase 4)
- **Task 33:** edge_cases_test.dart e2e

### Phase 5 — Exports + CLAUDE.md + roadmap (4 tasks)

- **Task 34:** Barrel exports in `reaction/lib/reaction.dart` — ReactionHandlers, TrustingAuthValidator, ViewScopeRegistry, ViewPermissionNamer, RemoteScope, RemoteConnection, etc.
- **Task 35:** Add 4th trust-boundary entry to CLAUDE.md (wire-auth flow).
- **Task 36:** Update `docs/superpowers/specs/2026-05-11-roadmap.md` with Plan B-remote+C entry.
- **Task 37:** Drop the "Provisional" marker on the guide chapter; final verification (full `flutter test` across both packages).

---

## When done

Phase 5 includes (Task 35) the CLAUDE.md trust-boundaries update, (Task 36)
the roadmap entry, and (Task 37) the final verification + guide-chapter
de-provisionalization.

After all 37 tasks: open a PR from `CUR-1317-plan-b-remote` to `main`. PR
title format the org's branch-protection rule requires:

```text
[CUR-1317] Plan B-remote+C: <subject>
```

---

## Useful refs

- Memory: `~/.claude/projects/-home-metagamer-cure-hht-event-sourcing/memory/`
  - `project_reaction_remote_design.md` — design summary (now references this session's progress)
  - `feedback_combined_subagent_reviews.md` — combined-review pattern (used this session)
  - `feedback_greenfield_fix_root_not_workaround.md` — drove the SubscriptionFilter substrate prep
  - `feedback_auth_validators_are_consumer_supplied.md` — explains why TrustingAuthValidator is the only validator shipped
- Roadmap: `docs/superpowers/specs/2026-05-11-roadmap.md`
- Spec: `spec/reaction-remote.md` (now includes "Cross-process predicates" and "Structured error encoding" Future-work notes)
- Prior handoff: `docs/superpowers/plans/2026-05-20-session-handoff.md`
