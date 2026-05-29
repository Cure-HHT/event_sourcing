# Scoped Permissions

**Phase**: I (substrate primitive; precedes portal cutover in CUR-1170)
**Status**: Implemented — normative requirement blocks (`EVS-PRD-scoped-permissions` plus supporting `EVS-DEV-*`) are present in this file (see below) and active in the requirement graph.
**Last updated**: 2026-05-13
**Linear**: CUR-1331

> **Lifecycle note.** This file was authored as a design document and grew its normative `EVS-{TYPE}-{component}` requirement blocks in place as the design stabilized — the brainstorm → stabilize → migrate lifecycle in `spec/README.md` is short-circuited here so the design lives in `spec/` from the start (avoiding information loss in a migration step). Those blocks are now present (see the `## EVS-PRD-scoped-permissions` / `## EVS-DEV-*` sections), so elspais treats this file as normative.

## Scope

This document designs the substrate's scope-aware permission mechanism. The v1 substrate ships role-to-permission grants only — coarse-grained, no scope value binding. The portal already runs site-scoped Study Coordinator and patient-scoped roles in production (today, via Postgres RLS). Porting the portal onto the substrate without scoping would silently downgrade production functionality. This design supplies the mechanism.

In scope:

- A generalized scope-class registration mechanism (apps declare their scope classes; substrate doesn't bake `site` or `patient` into its vocabulary).
- A grant data model that binds scope values to user-role assignments via new substrate event types.
- The match algorithm: equality, value-wildcard, class-wildcard, and hierarchy/containment via app-supplied projections.
- Action API extension so actions supply scope values from parsed input at dispatch time.
- `AuthorizationPolicy` signature reshape.
- Dispatch consistency stance: authorize and execute share a single storage-transaction snapshot.
- Migration of existing in-lib types (`ScopeClass` enum, `Permission.scope`, `Principal.activeSite`) — pre-ship, so cleanest cut rather than promoter chain.
- Forward-compatibility check against future bundling / CRUD primitive work.

Out of scope (deferred to follow-on tickets):

- Implementation. This spec is the design; impl lands in a separate ticket once design stabilizes.
- Permission bundling / `PermissionGroup` primitive (see Future Work).
- Substrate-defined CRUD Action templates per `ProjectionSpec` (see Future Work).
- Range comparison (numeric/date) on scope values (see Future Work).
- Explicit deny-grants that override allows (see Future Work).
- Multi-source-aware grant visibility (Phase II; this v1 design is single-source-per-aggregate-type per the existing commitment).
- Portal/hht_diary-side migration off Postgres RLS (CUR-1170, downstream of this).

## Architectural framing

This is a **Layer 2 convention extension** under the Append-Only Primitives discipline. From the charter:

- Permissions/scoping data live in events in the same log; the substrate's projection interpreter reads its own outputs.
- The `AuthorizationPolicy` mechanism remains substrate code (closed-under-events trust model). Alternatives are library extensions, not app-supplied policy logic.
- Once a primitive ships under a name with given semantics, those semantics are frozen; alternative behavior is a new primitive, not a re-interpretation.

This design is **pre-ship** (Phase I has not released). The existing `permission_granted` / `permission_revoked` event types and their payload shapes — kick-started during the `hht_diary` extraction — are reshaped freely. After Phase I ships, the shapes pinned here become frozen under AOP.

The scope-class mechanism is **domain-neutral**: the substrate ships the registration and matching machinery; apps register their own scope classes (`site`, `patient`, `project`, etc.). The substrate has no built-in knowledge of clinical-trial concepts. This is a corollary of the lib's domain-neutral commitment and replaces the legacy hardcoded `ScopeClass{global, site, self}` enum.

## Overview diagram

```text
+----------------------------------------------------------------+
|                       Composition root                         |
|                                                                |
|  ProjectionRegistry  ScopeClassRegistry  ActionRegistry        |
|         |                  |                  |                |
|  user_role_scopes      site, patient,     EditPatient,         |
|  role_permission_      project, ... ;     EnrollPatient,       |
|  grants;               each with optional Action; ...          |
|  patient_site_index;   ContainmentRef                          |
|  ...                                                           |
+----------------------------------------------------------------+
                                |
                                v
+----------------------------------------------------------------+
|                 ActionDispatcher (single tx)                   |
|                                                                |
|  begin storage tx (snapshot N)                                 |
|    parse(raw) -> input                                         |
|    validate(input)                                             |
|    for each perm in action.permissions:                        |
|      scope = perm.scopeClass == null                           |
|              ? null                                            |
|              : action.scopeFor(perm, input)                    |
|      decision = policy.isPermitted(principal, perm, scope)     |
|      if not Allow: emit authorization_denied; abort            |
|    result = execute(input, ctx)                                |
|    append result.events at N+1..N+k                            |
|  commit                                                        |
+----------------------------------------------------------------+
                                |
                                v
+----------------------------------------------------------------+
|              TableBackedAuthorizationPolicy                    |
|                                                                |
|  isPermitted(P, perm, scope):                                  |
|    1. user_role_scopes where user_id == P.userId               |
|       AND role == P.activeRole -> assignments                  |
|       (membership gate; empty -> Deny(notGranted))             |
|    2. role_permission_grants where role == P.activeRole        |
|       AND permission_name == perm.name -> exist?               |
|       (empty -> Deny(notGranted))                              |
|    3. if perm.scopeClass == null -> Allow (unscoped grant      |
|       suffices once membership is verified)                    |
|    4. for each assignment from step 1, run match (equality /   |
|       wildcard / containment via ContainmentResolver)          |
|    5. first match -> Allow ; no match -> Deny(notGranted)      |
+----------------------------------------------------------------+
```

## Design

### Section 1 — Scope class registration

A scope class is an app-registered named dimension along which permissions can be scoped. Apps register a `ScopeClassSpec` at composition time, parallel to `ProjectionSpec`:

```text
ScopeClassSpec(
  name: 'site',
  // top-level: no containment
)

ScopeClassSpec(
  name: 'patient',
  containedIn: ContainmentRef(
    parentClass:  'site',
    projection:   'patient_site_index',  // a TableProjectionSpec
    keyColumn:    'patient_id',          // how the projection is keyed
    parentColumn: 'site_id',             // column carrying the parent value
  ),
)
```

`ContainmentRef` points at a `TableProjectionSpec` the app already maintains for its own state. At evaluation time, to test whether assignment `(site, A)` covers requested scope `(patient, P-42)`, the substrate:

1. Sees that `patient.containedIn.parentClass == site`.
2. Reads `patient_site_index` row where `patient_id = P-42`; reads its `site_id` column → `A`.
3. Compares against the assignment's value: `A == A` → match.

Containment chains. Substrate walks the class graph upward until either match or top-of-graph. Composition-time validation refuses cycles.

**Properties:**

- Hierarchy lives in **data** (an event-derived projection), not in app-supplied code. Same epistemic-layer story as the rest of the substrate: Layer-1 facts (the projection's rows) drive a Layer-2 convention (the containment match).
- Containment evaluation is substrate code, not app-callback code — preserves the no-app-supplied-policy-logic commitment.
- The projection is a regular `TableProjectionSpec` the app likely already maintains for its UI. Substrate just *reads* it.
- Fail-closed on missing rows: if `patient_site_index` has no row for `P-42` at evaluation time, containment lookup misses, scope match fails, and the principal is denied. This handles projection-catch-up lag, deliberately-unmapped values, and misconfigured `ContainmentRef`s identically. The client retries; the audit captures the denial.

**Composition-time validation** (substrate refuses to construct on violation):

- `ContainmentRef.projection` must be a registered `ProjectionSpec` of `TableProjectionSpec` kind.
- `keyColumn` and `parentColumn` must be columns the projection emits.
- `parentClass` must be a registered `ScopeClassSpec`.
- No cycles in the class-containment graph.
- Every `Permission.scopeClass` (non-null) references a registered `ScopeClassSpec`.

**Runtime denials** (NOT errors; expected behavior, audit-logged):

- Principal's `activeRole` has no `user_role_scopes` row → deny.
- No assignment matches at any containment level → deny.
- Containment lookup misses (projection lag or unmapped value) → deny.
- `Action.scopeFor` returns `null` for a scoped permission → deny with `scopeUnresolvable`.

### Section 2 — Grant data model

Two layers of grant data in the log:

**Layer A — Role to permission templates** (refined existing model)

The existing `permission_granted` and `permission_revoked` event types remain. Their payloads drop the legacy `scope` field (the old `ScopeClass` enum); the permission's scope class is registered on the `Permission` definition itself, not per-grant.

```text
permission_granted   { role: String, permission_name: String }
permission_revoked   { role: String, permission_name: String }
  - aggregate_type: 'role_permission_grant'
  - aggregate_id:   '{role}:{permission_name}'  (unchanged format)
```

Projection `role_permission_grants` (existing) becomes a thin role-to-permissions lookup.

**Layer B — User to role assignments with scope value** (new)

```text
role_assigned       { user_id, role, scope: <ScopeValue JSON> }
role_unassigned     { user_id, role, scope: <ScopeValue JSON> }
  - aggregate_type: 'user_role_scope'
  - aggregate_id:   derived from the payload (see Aggregate id below)
```

`<ScopeValue JSON>` is one of three distinct shapes, modeling a sealed `ScopeValue` type. Each shape's JSON discriminator is the presence/absence of the keys:

```text
Bound scope (specific value of a specific class):
  { "class": "site", "value": "A" }

Value-wildcard (any value of a specific class):
  { "class": "site", "wildcard_value": true }

Total wildcard (any class, any value):
  { "wildcard_class": true }
```

The wildcard shapes are distinct *types*, not sentinel strings inside a normal `value` field. This protects against the corner case where a legitimate value happens to be the literal `'*'` — there is no sentinel to clash with.

**JSON parse contract.** The three key sets are mutually exclusive by construction: `{"wildcard_class": true}` (no other keys) is total-wildcard; `{"class": <s>, "wildcard_value": true}` (no `"value"` key) is value-wildcard; `{"class": <s>, "value": <s>}` (no `"wildcard_value"` key) is bound. `ScopeValue.fromJson` SHALL reject any object whose keys do not match exactly one of these three shapes with a `FormatException`.

**Aggregate id** uniquely identifies one (user, role, scope) tuple so that the projection's insert/remove discipline works. To avoid encoding ambiguity from colon-bearing values (e.g., a path-like `scope_value` or any future `user_id` shape that contains a `:`), the aggregate id is the canonical-JSON serialization of the tuple:

```text
aggregate_id = canonical_json({
  "user_id": <string>,
  "role":    <string>,
  "scope":   <ScopeValue JSON, one of the three shapes above>,
})
```

Canonical JSON (JCS, RFC 8785) is already a substrate dependency. The aggregate id is constructed-only; no parser needs to reverse it. The projection's row key (`AggregateIdKey()`) treats it as opaque. This is collision-free across any (user_id, role, scope) tuple regardless of segment content.

Granting two sites to a user emits two `role_assigned` events with distinct aggregate ids. Revoking one targets that aggregate id specifically.

**Projection `user_role_scopes`** (new):

```text
viewName:          'user_role_scopes'
interest:          eventTypes={role_assigned, role_unassigned},
                   aggregateTypes={user_role_scope}
insertEventTypes:  {role_assigned}
removeEventTypes:  {role_unassigned}
rowKey:            AggregateIdKey()
rowData:           WholePayload()
```

**Match algorithm** (used by `TableBackedAuthorizationPolicy.isPermitted`):

```text
isPermitted(principal U, permission P, requestedScope (Cr, Vr)):

  step 1 - role-level grant exists?
    rolePermissionGrants WHERE role == U.activeRole
                          AND permission_name == P.name
    if empty: return Deny(notGranted)

  step 2 - unscoped permission shortcut:
    if P.scopeClass == null:
      assert requestedScope == null
      return Allow

  step 3 - scoped permission: find assignments:
    assignments = userRoleScopes WHERE user_id == U.userId
                                  AND role == U.activeRole
    if empty: return Deny(notGranted)

  step 4 - match (first-match-wins):
    for each assignment A in assignments:
      case A is TotalWildcardScope:
        return Allow
      case A is ValueWildcardScope(class: Ca):
        if Ca == Cr: return Allow
        if Ca is ancestor of Cr in the class graph:
          # any value of Ca covers all descendants
          return Allow
        continue
      case A is BoundScope(class: Ca, value: Va):
        if Ca == Cr AND Va == Vr:
          return Allow
        if Ca is ancestor of Cr in the class graph:
          ancestorValue = ContainmentResolver.resolve(Cr, Vr, target: Ca)
          if ancestorValue == Va:
            return Allow
        continue
    return Deny(notGranted)
```

`ContainmentResolver.resolve(Cr, Vr, target: Ca)` walks the chain `Cr -> parentOf(Cr) -> ...` looking up each hop in the appropriate `TableProjection`. Returns the value at class `Ca`, or `null` if any hop misses (which is treated as "doesn't match this assignment" — continues to the next).

**YAML seed grammar** is unchanged. Today's seed already carries only role-to-permission names (`event_sourcing/example_action_permissions/tool/permissions.yaml`); the legacy `ScopeClass` enum lived in code on the `Permission` definition, not in the YAML. Under the new model, the `Permission` definition carries `scopeClass: String?` instead. The YAML keeps the existing shape:

```text
roles:
  - StudyCoordinator
  - Supervisor
grants:
  StudyCoordinator:
    - patient.view
    - patient.edit
    - site.read
  Supervisor:
    - patient.view
```

User-role assignments are **not** in the YAML seed — they are runtime user-management events that the application emits (the portal, hht_diary). The substrate exposes the event types; the application chooses when to emit them.

A `BootstrapRoleAssignments` seed applier mirrors the existing `bootstrap_action_permissions.dart` pattern for test fixtures (declarative list of `(user_id, role, scope)` tuples that the substrate folds into the projection without the full event pipeline).

### Section 3 — Action API and dispatch flow

**`Permission`** (reshaped):

```text
class Permission {
  const Permission(this.name, {this.scopeClass});
  final String name;
  final String? scopeClass;   // null = unscoped
                              // otherwise = registered ScopeClassSpec.name
}
```

The legacy `scope: ScopeClass` field is removed with the enum.

**`Action`** (one new method):

```text
abstract class Action<TInput, TResult> {
  // ... existing parseInput, validate, execute, permissions ...

  /// Per dispatch, supply the scope value for each scoped permission.
  /// Pure: no I/O. Returns null for unscoped permissions (default impl).
  /// For scoped permissions, the returned ScopeValue's scopeClass MUST
  /// equal the permission's declared scopeClass; mismatch is denial.
  ScopeValue? scopeFor(Permission perm, TInput input) => null;
}
```

Default returns `null` so existing/unscoped actions don't need to override. Scoped actions provide an impl. Purity contract matches `parseInput` / `validate`.

**`ScopeValue`** (sealed, three variants matching the JSON shapes):

```text
sealed class ScopeValue
final class BoundScope         extends ScopeValue { String class_; String value; }
final class ValueWildcardScope extends ScopeValue { String class_; }
final class TotalWildcardScope extends ScopeValue { /* no fields */ }
```

The `Action.scopeFor` method typically returns `BoundScope` from input; the wildcard variants are primarily for role-assignment *storage*, not action-side scope-binding. `TotalWildcardScope` MUST NOT be returned from `scopeFor`: it carries no `class_` field, so the dispatcher's class-check cannot apply. If `scopeFor` returns `TotalWildcardScope`, the dispatcher denies with `scopeUnresolvable` (treated as a programmer bug, surfaced at runtime). `ValueWildcardScope` is technically returnable when an action genuinely operates on all values of a class (e.g., a cross-site report-generation action), but the action author should document the intent per call site.

**`AuthorizationPolicy`** (extended signature):

```text
abstract class AuthorizationPolicy {
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,    // non-null iff permission.scopeClass != null
  );

  /// Materials for client-side UI gating and app-side scope-aware queries.
  Future<EffectiveAuthorization> effectivePermissionsFor(Principal principal);
}

class EffectiveAuthorization {
  final String activeRole;
  final Set<Permission> rolePermissions;
  final List<ScopeAssignment> scopeAssignments;
}

class ScopeAssignment {
  final ScopeValue scope;       // sealed-type variants as above
}
```

`effectivePermissionsFor` replaces the legacy `permissionsFor`. The legacy method returned a session-precondition-filtered permission set, which no longer has a coherent meaning under the new model. The new method returns raw materials: the permission set the active role carries, and the user's scope assignments under that role. Clients and apps compose these with their own data projections to build "items I can act on" lists; per-decision UI gating still uses `isPermitted`.

**Dispatcher authorize stage:**

```text
For each perm in action.permissions:
  if perm.scopeClass == null:
      decision = policy.isPermitted(principal, perm, null)
  else:
      scope = action.scopeFor(perm, parsedInput)
      if scope == null:
          decision = Deny(scopeUnresolvable,
                          detail: 'action did not supply scope value
                                   for scoped permission')
      else if scope.class_ != perm.scopeClass:
          decision = Deny(scopeUnresolvable,
                          detail: 'class mismatch: action returned
                                   {actualClass}, permission requires
                                   {expectedClass}')
      else:
          decision = policy.isPermitted(principal, perm, scope)
  if decision is not Allow:
      emit authorization_denied(perm, scope, decision.reason)
      short-circuit
```

The scope value the dispatcher passed (when present) is stamped on the `authorization_denied` denial event payload — preserves "what was requested, not just that it was refused" for audit.

**Consistency stance: transactional authorize + execute.**

The dispatcher wraps both stages in a single storage transaction with a fixed read-consistent snapshot:

```text
begin storage tx (snapshot at sequence N):
  parse, validate
  authorize  (reads role_permission_grants, user_role_scopes,
              containment projections; all at snapshot N)
  execute    (computes events; any reads see snapshot N)
  append events at N+1..N+k  (atomic with the snapshot)
commit
```

This closes the narrow race where an ingest-arriving revocation could land between authorize's projection read and execute's event append. Under the transaction, authorize and execute see the same projection state, and the appended events are committed atomically against that state. Future dispatches see the post-revoke state.

The decision this pins: a revocation arriving between two dispatches takes effect on the *second* one; a revocation arriving during a single dispatch's transaction takes effect *after* it commits. Concretely on a single-source v1 deployment without ingest, the race window doesn't exist at all; the transaction stance is the correct posture regardless and prepares for ingest-active deployments.

**Implementation pre-req — transactional view-row scan.** The match algorithm enumerates all `user_role_scopes` rows for a given `(userId, role)` pair inside the dispatch transaction. The current `StorageBackend` surface (`event_sourcing/lib/src/storage/storage_backend.dart`) provides `readViewRowInTxn` (single-row, in-txn) and `findViewRows` (multi-row, non-transactional); the transactional-snapshot guarantee requires a transactional multi-row read. The impl ticket adds `findViewRowsInTxn(Txn, viewName, {filter})` (or equivalent — name TBD by impl) to the abstract `StorageBackend` interface, with implementations in each backend (sembast first). This is a substrate-interface addition, not an app surface; follows the same abstract-backend-agnostic contract as existing `*InTxn` methods.

**Denial reasons** (refined):

```text
DenyReason:
  notGranted         -- the principal's active role doesn't carry the
                       permission, OR no scope assignment under that
                       role covers the requested scope (including the
                       fail-closed containment-miss case).
  scopeUnresolvable  -- action.scopeFor returned null for a scoped
                       permission, OR returned the wrong scope class.
                       This is a programmer-bug surface; the audit log
                       captures it so it surfaces loudly.

(REMOVED: sessionPreconditionMissing -- the precondition concept is
         superseded; "no covering assignment" is now just notGranted.)
```

### Section 4 — Migration and cleanup

This design is pre-ship; per the libify greenfield posture, we prefer cleanest design over backwards-compat shims. No promoters are required; the existing event-type names are reused with reshaped payloads, and the substrate emits one `lib_version_initialized` carrying the new shapes.

**Deletions:**

```text
event_sourcing/lib/src/actions/scope_class.dart                  DELETE
  - the ScopeClass{global, site, self} enum

event_sourcing/lib/src/actions/permission.dart                   MODIFY
  - drop 'scope' field; add nullable 'scopeClass: String?'

event_sourcing/lib/src/actions/principal.dart                    MODIFY
  - drop UserPrincipal.activeSite (legacy hht_diary debt)

event_sourcing/lib/src/permissions/permission_granted_payload.dart  MODIFY
  - drop 'scope' field

event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart
  - remove _scopePreconditionMet                                 MODIFY
  - rewrite isPermitted around the new match algorithm (Section 2)
  - implement effectivePermissionsFor

event_sourcing/lib/src/actions/authorization_decision.dart       MODIFY
  - drop DenyReason.sessionPreconditionMissing
  - add DenyReason.scopeUnresolvable
```

**Additions:**

```text
event_sourcing/lib/src/permissions/scope_class_spec.dart         NEW
  - ScopeClassSpec, ContainmentRef
  - ScopeClassRegistry (composition-time validation)

event_sourcing/lib/src/actions/scope_value.dart                  NEW
  - sealed ScopeValue + BoundScope / ValueWildcardScope /
    TotalWildcardScope; JSON ser/de

event_sourcing/lib/src/permissions/role_assigned_payload.dart    NEW
event_sourcing/lib/src/permissions/role_unassigned_payload.dart  NEW

event_sourcing/lib/src/permissions/user_role_scopes_spec.dart    NEW

event_sourcing/lib/src/permissions/scope_assignment.dart         NEW
event_sourcing/lib/src/permissions/effective_authorization.dart  NEW

event_sourcing/lib/src/permissions/role_assignment_seed.dart     NEW
event_sourcing/lib/src/permissions/bootstrap_role_assignments.dart  NEW

event_sourcing/lib/src/permissions/containment_resolver.dart     NEW
```

**Updates** (sketch; not exhaustive):

```text
- ActionDispatcher authorize stage      rewire per Section 3
- ActionDispatcher                      wrap authorize+execute in one tx
- yaml_seed_loader.dart                 drop 'scope' field
- in_memory_role_matrix_reader.dart     drop scope handling
- materialized_view_role_matrix_reader.dart  same
- snapshot_role_matrix_reader.dart      same
- permission_seed.dart                  drop scope from seed shape
- seed_validator.dart                   add scope-class-registry validation
- bootstrap_action_permissions.dart     unchanged in shape; payload change
- fail_safe_authorization_policy.dart   replace permissionsFor with
                                         effectivePermissionsFor returning
                                         an empty EffectiveAuthorization
- example/ and example_action_permissions/  add a scoped action sample
```

**Test impact:** the 13 files under `event_sourcing/test/permissions/` need updates. The largest rewrite is `table_backed_authorization_policy_test.dart` for the new match algorithm. New test files cover `ScopeClassSpec` validation, `ContainmentResolver`, hierarchy expansion, all wildcard cases, multi-assignment union within active role, fail-closed-on-missing-containment, and `scopeUnresolvable` denials.

**Spec / PRD work:**

- This design doc lives at `spec/scoped-permissions.md` from the start (see the lifecycle note at the top of this file). Normative `EVS-PRD-scoped-permissions` and supporting `EVS-DEV-*` requirement blocks land in-place against this file as impl stabilizes; no migration step.
- `spec/prd-permissions-as-events.md` likely picks up additional assertions about scope-class registration and event-derived containment evaluation.
- `spec/prd-action-dispatch.md` may need refinement of assertion B (the authorize stage acquires scope-binding sub-steps) and of the consistency / transaction stance.
- DEV-level requirements (`EVS-DEV-*`) authored alongside impl per CLAUDE.md.

**Downstream impact** (out of scope; for context):

`hht_diary` (only consumer today) will need to:

- Register `ScopeClassSpec`s for `site` and `patient`.
- Register the `patient_site_index` table projection (it already exists in some form).
- Implement `scopeFor` on every scoped Action.
- Emit `role_assigned` events on user-management operations (replaces today's RLS-driven assignments at portal cutover).
- Drop `activeSite` from Principal construction.

This work is portal-cutover-side (CUR-1170 is blocked-by). Not in scope for CUR-1331 or its impl follow-up.

### Section 5 — Open questions and future work

**Open (pre-impl decisions still to make):**

1. **Containment-ref column-mapping typing.** Today's `TableProjection` rows have a row-key plus arbitrary columns. `ContainmentRef` carries column-name strings. The projection-row contract could be more typed for scope-class projections (e.g., a `ScopeIndexProjection` subtype with declared `keyType` / `parentValueType`). Cleaner type discipline; more boilerplate. Tend yes for future tightening; not load-bearing for v1.

2. **Action validation for scoped-permission completeness.** A scoped `Action` whose `scopeFor` returns `null` for some input shape silently denies with `scopeUnresolvable`. Registration-time validation cannot detect this (return value depends on input). Recommended posture: runtime denial as designed, with a **DEV-level test obligation per scoped action** that exercises `scopeFor` against realistic and edge-case inputs.

**Future work (deliberately deferred):**

- **F1 — Range / numeric scope classes.** Equality + wildcard + containment is v1. Range comparison (numeric, lexical, date) ships as a future primitive under AOP discipline if a real use case arises. None today.

- **F2 — Explicit deny-grants.** "Supervisor cannot edit, even at sites they are assigned to." Today's model is union of allows; a deny-grant primitive (new event type) plus conflict-resolution semantics ships separately if needed. Out of scope.

- **F3 — Multi-source-aware grant visibility.** The substrate's `single-source-per-aggregate-type today` commitment defers this to Phase II. Each source remains authority over its own log: a remote source's authorization state at the time it emitted an event is what's recorded in the remote source's log; ingesting peers can refuse to apply such events to their own views (and that refusal is itself a recorded event), but cannot retroactively unmake the remote source's log.

- **F4 — Permission inheritance / role composition.** "Role SC inherits from role Clinician." Modelable but adds match-algorithm complexity. Not requested; defer.

- **F5 — Caching for hot-path authorization.** Every dispatch reads `role_permission_grants`, `user_role_scopes`, and (often) a containment projection. Phase I is fine without caching; a substrate-internal LRU keyed by `(userId, permName, scopeClass, scopeValue)` is a Phase II optimization if profiling demands it.

- **F6 — Action bundling / `PermissionGroup` primitive.** Apps in practice construct several Actions per projection (CRUD plus custom verbs). Today each gets its own Permission. A `PermissionGroup` primitive (named bundle of permission names, grantable in one stroke) would compress the YAML and grant model. Pure-app-side bundling via YAML macros is already possible without substrate change; a substrate-level primitive is an opt-in AOP follow-up.

- **F7 — Substrate-defined CRUD Action templates per `ProjectionSpec`.** The substrate knows projection row shapes; it could in principle define standard create/update/delete Action classes per registered `ProjectionSpec`, with field-level permissions derived from the row shape. This is a substantial Layer-2 convention extension — a separate brainstorming exercise of its own scope. Not bundled into CUR-1331. The v1 design here (one Permission per Action) is forward-compatible: F6 and F7 both layer as additional ways to declare grants and unfold to individual `(permission, scopeClass)` pairs at evaluation time without changing the match algorithm or grant data model.

- **F8 — Per-row authorization predicate (`RowFilterSpec`).** Today's model authorizes *actions* against scopes; a row a user can read is determined at view-subscription time by the projection's scope contract or by the view-level `ViewScopeRegistry` binding in `reaction`. Some domains need *per-row* visibility predicates that cannot be reduced to a single scope: the multiplayer-game scenario (`docs/scenarios/multiplayer-game.md`) needs "you can only read your own hand of cards, not the opponent's, even though both rows are in the same `hands` view." The shape of the primitive (working name `RowFilterSpec`, attached to a `ProjectionSpec` or a `ViewScopeBinding`) would let a projection or binding declare a row-level visibility predicate evaluated against `(principal, row, effectiveAuthorization)`; rows that fail evaluation are suppressed from subscriptions and reads. Today's workaround is to make every privacy boundary its own aggregate so view-level scoping suffices; this works but multiplies aggregates and projections. F8 ships as an AOP-discipline primitive when a real use case arrives; the multiplayer-game scenario is the motivating sketch.

## Decisions considered and rejected

- **Hardcoded substrate scope-class enum extended with `patient`.** Rejected: violates the lib's domain-neutral commitment. The same charter clause that says "Diary belongs in hht_diary" applies to "Site / Patient" as named substrate concepts. Generalizing to app-registered `ScopeClassSpec`s is the correct cleanup.

- **Scope value on the role-to-permission grant** (so a grant carries `(role, permission, scopeClass, scopeValue)` directly). Rejected: implies synthesized `SC@SiteA` roles, exploding role names by N sites and breaking the role-as-template mental model the portal uses today.

- **Both grant-side and assignment-side scope binding, with intersection semantics.** Rejected: maximum flexibility, but intersection-of-wildcards-with-values produces non-intuitive behaviors and an incoherent YAML grammar.

- **Multi-role union across all of a user's assigned roles simultaneously.** Rejected: the portal's user-experience and audit story treat one role as actively assumed at a time (`activeRole`). The substrate filters assignments by `activeRole`; "change hats to act as another role" is a session-level operation. Within the active role, union across multiple scope assignments is the rule.

- **Sentinel `'*'` string for wildcard in `scope_value`.** Rejected: collides with the (vanishingly rare but real) case of a legitimate scope value being the literal `'*'`. Instead, `ScopeValue` is a sealed type with three variants and three distinct JSON shapes (`bound`, `value-wildcard`, `total-wildcard`).

- **Re-check authorization at execute time** (rather than transactional authorize+execute). Rejected: introduces read amplification, an incoherent rollback story (storage doesn't unmake emitted events), and still leaves narrow windows. Transactional snapshot is the cleaner guarantee.

- **App-supplied scope-expander callback** for hierarchy. Rejected: violates the no-app-supplied-policy-logic commitment. Hierarchy must come from event-derived projections that the substrate reads (the `ContainmentRef` mechanism).

- **`Principal` carries a scope-context map** (`{site: A, patient: P-42}`) alongside `activeRole`. Rejected: under the union-within-active-role semantics, the substrate auto-discovers every scope the user is assigned to; no per-session scope selection is needed. Audit context for "the user was viewing site A's UI when they did this" can be stamped by the action into the emitted event payload (app-domain audit, not substrate authorization).

## References

- `spec/prd-permissions-as-events.md` — base PRD for permissions-as-events.
- `spec/prd-action-dispatch.md` — the dispatch pipeline this design extends.
- `spec/prd-library-charter.md` — the epistemic-layer framing and AOP discipline.
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md` — the projection model this design's match algorithm reads against.
- Linear: CUR-1331 (this design), CUR-869 (multi-role symptom), CUR-988 (cascading 403 symptom), CUR-1170 (portal cutover, blocked-by), CUR-1192 (origin of the `AuthorizationPolicy` interface), CUR-1317 (libify).

---

## EVS-PRD-scoped-permissions: Scope-aware authorization model

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-action-dispatch, EVS-PRD-permissions-as-events

### Assertions

A. The library SHALL register scope classes via a composition-time ScopeClassRegistry; an action's scoped permission SHALL refer to a scope class by registered name.

B. The library SHALL refuse composition when a ScopeClassRegistry contains duplicate names, cycles in containment, a containment ref pointing at an unregistered parent class, or a containment ref naming columns not present in the referenced projection.

C. The library SHALL record user-role-scope assignments as role_assigned / role_unassigned events in the same event log as application state, with aggregate id deterministically derived from (user_id, role, scope) via canonical JSON.

D. The library SHALL evaluate authorization solely from event-derived projections (role_permission_grants, user_role_scopes, and any app-registered containment projections); the policy SHALL NOT consult external authorities.

E. The library SHALL deny scoped permission requests with reason scopeUnresolvable when the action either supplies no scope value, supplies a TotalWildcardScope, or supplies a scope value whose class does not match the permission's declared scope class.

F. The library SHALL allow a scoped permission request when at least one of the user's active-role assignments matches the requested scope value via equality, value-wildcard, total-wildcard, or hierarchy containment expansion.

G. The library SHALL fail closed on missing containment rows: if any hop in the containment chain has no row in its declared projection, the match SHALL return false rather than treating the missing row as a permissive default.

H. The library SHALL evaluate authorize-stage policy reads and the execute-stage event appends inside the same storage transaction; a revocation committed concurrently with an in-flight dispatch SHALL NOT affect that dispatch's outcome.

I. The library SHALL stamp the requested scope value onto authorization_denied events when the scope value was resolvable, so the audit log captures the precise denial circumstance.

### Rationale

Replaces the legacy substrate `ScopeClass { global, site, self }` enum and `Principal.activeSite` with a domain-neutral, app-registered scope-class machinery. See the design prose above for the algorithm and motivating use cases; see `spec/prd-permissions-as-events.md` for the parent obligation around closed-under-events authorization.

---

### Changelog

- 2026-05-14 | d3eee322 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Scope-aware authorization model* | **Hash**: d3eee322

## EVS-DEV-scope-class-registry-validation: Composition-time scope-class registry validation

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `ScopeClassRegistry` SHALL throw `StateError` at construction when two `ScopeClassSpec`s share the same `name`.

B. `ScopeClassRegistry` SHALL throw `StateError` at construction when a `ContainmentRef.parentClass` names a class that is not in the registry.

C. `ScopeClassRegistry` SHALL throw `StateError` at construction when a `ContainmentRef.projection` cannot be resolved by the supplied projection lookup, or when its declared `keyColumn` or `parentColumn` is not a column of that projection.

D. `ScopeClassRegistry` SHALL throw `StateError` at construction when the class-containment graph contains a cycle.

E. `ScopeClassRegistry` SHALL expose lookup-by-name and an ancestor-chain walk such that `isAncestor(a, d)` returns true iff `a` appears in `d`'s containment chain (inclusive of `d`).

---

### Changelog

- 2026-05-14 | 4a76c916 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Composition-time scope-class registry validation* | **Hash**: 4a76c916

## EVS-DEV-scope-value-json: Sealed ScopeValue JSON contract

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `BoundScope.toJson` SHALL produce exactly `{"class": <class_>, "value": <value>}`, with no additional keys.

B. `ValueWildcardScope.toJson` SHALL produce exactly `{"class": <class_>, "wildcard_value": true}`, with no additional keys.

C. `TotalWildcardScope.toJson` SHALL produce exactly `{"wildcard_class": true}`, with no additional keys.

D. `ScopeValue.fromJson` SHALL be a complete-on-shape polymorphic decoder: it returns `BoundScope`, `ValueWildcardScope`, or `TotalWildcardScope` for inputs matching exactly one of the three shapes, and SHALL throw `FormatException` for any other object (including objects whose discriminator keys are present but whose value is not the literal `true`, or whose key set is ambiguous between shapes).

E. The round-trip `ScopeValue.fromJson(v.toJson()) == v` SHALL hold for every concrete `ScopeValue` instance constructible via the public constructors.

---

### Changelog

- 2026-05-14 | 1e192982 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Sealed ScopeValue JSON contract* | **Hash**: 1e192982

## EVS-DEV-containment-resolver: Containment-chain walk via TableProjections

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `ContainmentResolver.resolve(from, target)` SHALL return `from` unchanged when `from.class_ == target`.

B. `ContainmentResolver.resolve(from, target)` SHALL return `null` when `target` is not in `from.class_`'s ancestor chain in the registry.

C. `ContainmentResolver.resolve(from, target)` SHALL walk the ancestor chain by reading each hop's `ContainmentRef.projection` (via the injected transactional read), returning the resolved ancestor `BoundScope` at class `target`.

D. `ContainmentResolver.resolve` SHALL return `null` when any hop's projection returns zero rows for the current key, or returns a row whose `parentColumn` is missing, non-string, or empty (fail-closed on missing containment data).

---

### Changelog

- 2026-05-14 | 8b7a3f36 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Containment-chain walk via TableProjections* | **Hash**: 8b7a3f36

## EVS-DEV-scoped-permissions-match-algorithm: TableBackedAuthorizationPolicy match semantics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `TableBackedAuthorizationPolicy.isPermitted` SHALL deny with `DenyReason.notGranted` when `role_permission_grants` has no row for the principal's `activeRole` and the permission name.

B. `TableBackedAuthorizationPolicy.isPermitted` SHALL deny with `DenyReason.scopeUnresolvable` when `permission.scopeClass != null` xor `scopeValue == null` (the dispatcher's invariant is violated).

C. The policy SHALL read `user_role_scopes` for `(principal.userId, principal.activeRole)` BEFORE evaluating the role-permission grant. If no row exists, the policy SHALL return `Deny(notGranted)` for both scoped and unscoped permissions — auth-time claims of role membership are not trusted; the `(userId, activeRole)` binding is verified against the substrate's event-derived projection. For scoped permissions, the row set is reused for the scope-match step (no second query).

D. For a scoped permission, the policy SHALL allow the request when at least one user-role-scope assignment matches the requested scope under first-match-wins union: assigned `TotalWildcardScope` matches any request; assigned `ValueWildcardScope(class=C)` matches when the request's class equals `C` or descends from `C`; assigned `BoundScope(class=C, value=V)` matches a request with the same `(class, value)`, or a descendant whose containment resolves to `V` at class `C`.

E. The policy SHALL deny with `DenyReason.notGranted` when no assignment matches the requested scope, including when containment resolution returns `null` for every assignment (fail-closed propagation from the resolver).

F. The policy SHALL return `Deny(notGranted)` for principals that are not `UserPrincipal` (anonymous principals have no role assignments).

---

### Rationale

The membership-first gate in assertion C closes a trust hole that existed in the policy's pre-`62b2bcc` shape, where unscoped permissions short-circuited on the role-permission-grant read and never consulted `user_role_scopes`. That earlier shape implicitly trusted the caller-supplied `Principal.activeRole` — anyone who could submit an action could claim any role and exercise that role's unscoped permissions, regardless of whether the substrate had ever recorded a `role_assigned` event binding them to it.

The substrate's trust model (see CLAUDE.md, "Trust boundaries") trusts only `Principal.userId`; everything else, including which roles the user holds, is derivable from the event log. Reading `user_role_scopes` first restores that discipline: the `(userId, activeRole)` binding is verified against the event-derived projection before any permission is honoured under that role. The check is uniform — scoped and unscoped permissions both pay the cost — because the alternative (special-casing unscoped permissions) is the exact bug this commit fixed.

Reusing the assignment row set for the scope-match step keeps the cost a single projection read per `isPermitted` call regardless of whether the permission is scoped, preserving the dispatcher's per-action latency budget.

### Changelog

- 2026-05-24 | 87555bb8 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-05-14 | 899af570 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions
- 2026-05-24 | - | - | Michael Lewis (<michael.lewis.c@gmail.com>) | Align /C with shipped membership-first gate (62b2bcc); add Rationale on the trust-model fix

*End* *TableBackedAuthorizationPolicy match semantics* | **Hash**: 87555bb8

## EVS-DEV-effective-permissions-shape: effectivePermissionsFor surface

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `AuthorizationPolicy.effectivePermissionsFor(principal)` SHALL return an `EffectiveAuthorization` carrying the active role, the `Set<Permission>` granted to that role, and the `List<ScopeAssignment>` of the user's scope assignments under that role.

B. `effectivePermissionsFor` SHALL return `EffectiveAuthorization.empty` for principals that are not `UserPrincipal`, AND for `UserPrincipal`s whose `(userId, activeRole)` binding is absent from `user_role_scopes` — the substrate independently verifies role membership before reporting any permissions held under that role, mirroring the membership gate in `isPermitted`.

C. `ScopeAssignment` SHALL carry exactly one `ScopeValue` (the assigned scope), so that clients composing the surface against their own projections see assignments as sealed-variant values rather than encoded strings.

D. `EffectiveAuthorization.empty` SHALL carry an empty active role, an empty `rolePermissions` set, and an empty `scopeAssignments` list, and SHALL satisfy structural equality with another empty instance.

---

### Changelog

- 2026-05-24 | deab9862 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-05-14 | b688e6ed | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions
- 2026-05-24 | - | - | Michael Lewis (<michael.lewis.c@gmail.com>) | Align /B with membership-gate fix (62b2bcc): empty returned for verified-absent-membership

*End* *effectivePermissionsFor surface* | **Hash**: deab9862

## EVS-DEV-transactional-authorize-execute: Dispatch tx encompasses authorize + execute + persist

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-action-dispatch, EVS-PRD-scoped-permissions

### Assertions

A. The dispatcher SHALL open one storage transaction for the authorize, execute, and stage-8 persist phases of a single dispatch; the `AuthorizationPolicy` SHALL receive the active `Txn` so its projection reads share the read-snapshot with the appended events.

B. When the authorize stage produces a `Deny` from within the dispatch transaction, the dispatcher SHALL append the `authorization_denied` event in the same transaction so the policy reads and the recorded denial commit atomically.

C. When the execute stage throws or the stage-8 append throws, the dispatch transaction SHALL be rolled back, and the dispatcher SHALL emit the corresponding denial event (`execution_failed`) in a separate append after the rollback.

D. The transactional posture SHALL ensure that a role or scope revocation committed concurrently with an in-flight dispatch does not change that dispatch's outcome; the revocation SHALL take effect on subsequent dispatches.

---

### Changelog

- 2026-05-14 | 6461dd31 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Dispatch tx encompasses authorize + execute + persist* | **Hash**: 6461dd31

## EVS-DEV-role-assignment-aggregate-id: Canonical-JSON aggregate id for role assignments

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-scoped-permissions

### Assertions

A. `roleAssignmentAggregateId(userId, role, scope)` SHALL return the canonical-JSON (JCS, RFC 8785) encoding of the object `{"user_id": userId, "role": role, "scope": scope.toJson()}`.

B. Distinct `(userId, role, scope)` tuples SHALL produce distinct aggregate ids; identical tuples SHALL produce byte-identical aggregate ids regardless of construction order.

C. The aggregate id SHALL be safe against segment-encoding ambiguity: a userId, role, or scope value containing characters like `:` or `/` SHALL NOT collide with a different tuple's id.

---

### Changelog

- 2026-05-14 | abb4d0a5 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Canonical-JSON aggregate id for role assignments* | **Hash**: abb4d0a5

## EVS-DEV-scope-unresolvable-denial: Dispatcher denial when Action.scopeFor is unusable

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-action-dispatch, EVS-PRD-scoped-permissions

### Assertions

A. The dispatcher SHALL invoke `Action.scopeFor(permission, parsedInput)` for every permission whose `scopeClass != null`, before opening the dispatch transaction.

B. The dispatcher SHALL emit an `authorization_denied` event with `deny_reason: scopeUnresolvable` and short-circuit the dispatch when `Action.scopeFor` returns `null` for a scoped permission.

C. The dispatcher SHALL emit an `authorization_denied` event with `deny_reason: scopeUnresolvable` and short-circuit the dispatch when `Action.scopeFor` returns a `TotalWildcardScope` for a scoped permission (the value carries no class to match against the permission's `scopeClass`).

D. The dispatcher SHALL emit an `authorization_denied` event with `deny_reason: scopeUnresolvable` and short-circuit the dispatch when `Action.scopeFor` returns a `ScopeValue` whose `class_` does not equal the permission's declared `scopeClass`.

E. When a scope value was returned (any of cases C or D, but not B), the dispatcher SHALL stamp the offending scope value's JSON onto the denial event's payload under the `scope` key, so the audit log captures the precise denial circumstance.

### Changelog

- 2026-05-14 | 4a2db650 | - | Developer (<dev@example.com>) | Initial authoring under CUR-1331 scope-aware permissions

*End* *Dispatcher denial when Action.scopeFor is unusable* | **Hash**: 4a2db650
