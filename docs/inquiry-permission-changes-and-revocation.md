# Inquiry: Applying Permission Changes & Revocation on Boot

> Q&A summary captured on 2026-06-29. Describes how permission changes
> enter the system in the `event_sourcing` substrate (as consumed by
> `cure-hht/hht_diary`'s portal server), and the feasibility of
> auto-revoking removed permissions during the boot/seed process.

## The inquiry

1. After first initialization, how do changes to permissions get into the
   system? Recollection: restarting the portal server rereads the
   permissions file and applies the differences.
2. Is it possible to modify the boot process to detect *removed*
   permissions and emit revoke events as appropriate?
3. Is revoking permissions even implemented and tested?

## Findings

### 1. How permission changes enter the system

Permissions are **events**, not config rows (`spec/prd-permissions-as-events.md`).
The authorization policy (`TableBackedAuthorizationPolicy`) reads only the
event-derived projections — `role_permission_grants` and
`user_role_scopes` — never the YAML seed at decision time. The YAML file
is a *seed*, not a live source of truth.

On startup the server calls `bootstrapActionPermissions(...)`
(`event_sourcing/lib/src/permissions/bootstrap_action_permissions.dart`):

1. **Load** the YAML seed (`YamlSeedLoader.loadFromFile`).
2. **Validate** it (unknown permission names / unregistered scope classes
   produce a `PolicyFailSafe` that denies everything rather than booting a
   bad config).
3. **Diff & apply** via `PermissionSeedApplier.apply`:
   - Read current `role_permission_grants` rows → `inView`.
   - Compute `inSeed` from the YAML.
   - `missing = inSeed - inView` → append one `permission_granted` event
     per missing pair.
   - Re-running with no changes emits **zero** events (idempotent).

Role/scope assignments follow the same pattern in
`bootstrap_role_assignments.dart`, emitting `role_assigned` for missing
entries against the `user_role_scopes` projection.

**So the recollection is correct:** restart the portal server → seed file
is reread → the diff against the current projections is computed → only
the new differences are appended as events. There is no in-process
reload; restart is the only shipped path
(`walkthrough_10_reset_test.dart`: "there is no in-process reset
endpoint — restart is the only supported path").

#### Important caveat: the diff is additive-only

Both appliers compute a third set — **drift**, entries in the view but not
in the seed — but only *report* it for observability; they do **not**
emit revocations:

```dart
final drift = inView.difference(inSeed).toList()..sort();   // permission_seed_applier.dart:62
final drift = inView.difference(inSeedIds).toList()..sort(); // bootstrap_role_assignments.dart:85
```

- Adding a line to the YAML → applied on next restart. ✅
- Removing a line from the YAML → **not** applied; the prior grant stays
  live because its event remains in the append-only log and nothing
  revokes it.

This is by design — it keeps the closed-under-events guarantee intact and
avoids a silent file edit destroying a grant without an auditable
revocation event.

### 2 & 3. Is revocation implemented/tested, and can boot auto-revoke?

**Revocation is fully implemented and tested everywhere except the seed
reconciliation path:**

- **Event types + payloads** — `permission_revoked`
  (`PermissionRevokedPayload`) and `role_unassigned`
  (`RoleUnassignedPayload`) exist with unit tests.
- **Projection removal is wired and tested** —
  `role_permission_grants` declares `removeEventTypes: {'permission_revoked'}`
  (`role_permission_grants_spec_test.dart:58` asserts the row is removed);
  `user_role_scopes` declares `removeEventTypes: {'role_unassigned'}`
  (verified in `user_role_scopes_spec_test.dart`).
- **An action path emits them** — the `reaction` example ships
  `UnassignRoleAction` (`role_admin_actions.dart:125`), a permission-gated,
  idempotency-tracked, audited action emitting `role_unassigned` through
  normal dispatch.
- **Downstream reacts** — the `AuthorizationWatcher`
  (`authorization_watcher.dart:90`) force-logs-out the affected user on
  `role_unassigned`, and everyone with a role on `permission_revoked`
  (`authz_watcher_test.dart`, e2e `authz_test.dart`).

**Auto-revoking removed entries on boot is feasible and small.** The drift
set is already computed; the change is to loop over `drift` and append a
`permission_revoked` / `role_unassigned` event for each, reusing the
existing payloads and aggregate-id reconstruction. Closed-under-events is
preserved — revokes are just events attributed to the seed
`AutomationInitiator`.

#### Why it should be opt-in and guarded, not default

The current additive-only behavior protects against a real foot-gun.
Making the YAML authoritative (desired-state) means a bad file silently
destroys access:

- A truncated/corrupted seed, or booting the wrong environment's config,
  would mass-revoke grants/assignments — unattended, on startup.
- Because the watcher force-logs-out on revoke/unassign, a bad boot would
  kick every affected user out of their live session.
- Drift legitimately includes **runtime-granted** state (e.g. roles
  assigned via `AssignRoleAction`), which by design never appears in the
  static seed. Naive pruning would revoke exactly those on next restart.
  So "in view but not in seed" ≠ "should be revoked" in general.

#### Recommended shape if implemented

- A flag (e.g. `pruneDrift: false` default) on `bootstrapActionPermissions`
  / `bootstrapRoleAssignments` so existing callers keep additive-only
  behavior.
- A safety bound (refuse to prune above N% of rows, or require an explicit
  confirmation flag) to neutralize the corrupted-file scenario.
- Prune only entries whose initiator was the seed service, so
  runtime-granted assignments survive ("the file owns seed-managed grants;
  runtime owns the rest").

Implementation would live in `event_sourcing`; `hht_diary`'s portal server
would then pass the flag.

## Key files

| Component | Path |
|-----------|------|
| Permission bootstrap orchestration | `event_sourcing/lib/src/permissions/bootstrap_action_permissions.dart` |
| Permission seed diff/apply (additive) | `event_sourcing/lib/src/permissions/permission_seed_applier.dart` |
| Role-assignment seed diff/apply (additive) | `event_sourcing/lib/src/permissions/bootstrap_role_assignments.dart` |
| YAML seed loader | `event_sourcing/lib/src/permissions/yaml_seed_loader.dart` |
| Grant projection (`removeEventTypes: permission_revoked`) | `event_sourcing/lib/src/permissions/role_permission_grants_spec.dart` |
| Role projection (`removeEventTypes: role_unassigned`) | `event_sourcing/lib/src/permissions/user_role_scopes_spec.dart` |
| Revoke/unassign payloads | `event_sourcing/lib/src/permissions/permission_revoked_payload.dart`, `role_unassigned_payload.dart` |
| Admin revoke action | `reaction/example/lib/server/role_admin_actions.dart` |
| Force-logout on revoke | `reaction/lib/src/server/authorization_watcher.dart` |
| PRD | `spec/prd-permissions-as-events.md` |
