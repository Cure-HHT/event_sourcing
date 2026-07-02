# Roadmap — permission-model extensions

Deferred extensions to the substrate's scope-aware permission model
(`spec/scoped-permissions.md`). None of these have scaffolding in code;
each is a deliberate deferral. Several state a verified baseline — the
shipped mechanism the extension builds on.

The shipped match algorithm combines equality, value-wildcard,
total-wildcard, and containment matching under first-match-wins
union-of-allows semantics
(`event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart`).
`ContainmentReference` carries column-name strings. Every dispatch
performs fresh, uncached projection reads for authorization; the batched
view-row read added for subscription snapshots does not touch the
authorization path.

## Range / numeric scope classes

**Baseline.** `ScopeValue` is a 3-variant sealed type
(`BoundScope` / `ValueWildcardScope` / `TotalWildcardScope`) matched by
equality, wildcard, and containment only.

**Remaining.** A fourth variant plus ordered comparison (numeric,
lexical, date ranges) and the corresponding seed grammar, shipped as a
future primitive under the Append-Only Primitives discipline when a real
use case arises.

## Explicit deny-grants

**Baseline.** The model is pure first-match-wins union-of-allows; the
event types are allow-only (`permission_revoked` removes an allow, it is
not a deny).

**Remaining.** A deny event type, conflict-resolution semantics between
allows and denies, and a `DenyReason` — for cases like "this role cannot
edit, even at scopes it is otherwise assigned to."

## Role composition / inheritance

**Baseline.** Roles are flat strings; a single `activeRole` is resolved
per dispatch; there is no role graph.

**Remaining.** A role-relation model, transitive resolution across the
graph, and cycle detection — for "role A inherits role B's permissions."

## Hot-path authorization caching

**Baseline.** Each dispatch reads `role_permission_grants`,
`user_role_scopes`, and (often) a containment projection fresh via
`findViewRowsInTxn`. No authorization results are cached.

**Remaining.** A substrate-internal cache (for example an LRU keyed by
`(userId, permName, scopeClass, scopeValue)`) with invalidation on grant
changes. Profiling-gated: added only if measurement shows the fresh
reads are a bottleneck.

## `PermissionGroup` bundling

**Baseline.** Grants are flat `PermissionSeed`s; the YAML seed loader
accepts only `roles` and `grants`.

**Remaining.** A `PermissionGroup` type (a named bundle of permission
names, grantable in one stroke), a seed-schema extension, and expansion
to individual `(permission, scopeClass)` pairs at evaluation time. Pure
app-side bundling via YAML macros is already possible without a substrate
change; a substrate-level primitive is the opt-in follow-up.

## CRUD Action templates per `ProjectionSpec`

**Baseline.** Actions are hand-authored; there is no coupling between a
`ProjectionSpec` and any Action.

**Remaining.** Substrate-defined standard create/update/delete Action
classes derived from a registered `ProjectionSpec`'s row shape, with
field-level permissions. A substantial Layer-2 convention extension that
needs its own design pass first. The one-Permission-per-Action model
today is forward-compatible: bundling and templates both layer as
additional ways to declare grants that unfold to individual
`(permission, scopeClass)` pairs without changing the match algorithm or
grant data model.

## Per-row authorization predicate (`RowFilterSpec`)

**Baseline.** Scope-based row *narrowing* exists — `ViewScopeRegistry`
plus `ScopeDescendantExpander` narrow a scoped view subscription to the
descendant rows the principal covers. This is the documented workaround
a per-row predicate would generalize.

**Remaining.** A `RowFilterSpec` primitive — a row-level visibility
predicate evaluated against `(principal, row, effectiveAuthorization)`,
attached to a `ProjectionSpec` or a `ViewScopeBinding`, with rows failing
evaluation suppressed from subscriptions and reads. The motivating sketch
is `docs/scenarios/multiplayer-game.md` (read only your own hand of
cards, not the opponent's, though both rows live in the same view).
Today's workaround is to make each privacy boundary its own aggregate so
view-level scoping suffices — correct but it multiplies aggregates and
projections.

## Multi-source-aware grant visibility

Grant visibility across multiple authorities depends on the
canonicalization layer. It is recorded with the rest of that work in
`spec/roadmap/multi-source-editing.md`.

## Typed containment-ref column mapping

**Baseline.** `TableProjectionSpec` rows are a row-key plus arbitrary
columns, and `ContainmentReference` names its key and parent columns as
strings.

**Remaining.** A more typed projection-row contract for scope-class
projections (for example a `ScopeIndexProjection` subtype with declared
key and parent-value types), tightening type discipline at the cost of
more boilerplate. A future tightening, not load-bearing.
