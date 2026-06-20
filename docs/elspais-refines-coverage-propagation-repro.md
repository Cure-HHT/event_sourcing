# Bug: `Refines:` coverage propagation is requirement-level only — assertion-qualified targets (`/A`) are dropped

**For:** an agent working in the **elspais** source worktree (not this repo).
**Reported from:** Cure-HHT event_sourcing repo, while closing CUR-1528 test-coverage gaps.
**Status (2026-06-20):** `Refines:` coverage propagation now works **at the requirement level** (a covered child marks its parent's assertions covered). The remaining defect is that it is **coarse**: an assertion-qualified `Refines:` target such as `EVS-PRD-parent/A` has its `/A` suffix dropped, so propagation cannot be narrowed to the specific assertion the child refines.

## Intended behavior (requirement-author's model)

> `Refines:` does not itself imply satisfaction, but it **does propagate its own
> coverage up** to the thing it Refines. E.g. IF `REQ-2 Refines: REQ-1/A`, and
> `REQ-2` has 100% test-coverage, then `REQ-1/A` (and ONLY `/A`) has test
> coverage. `REQ-2` itself gets coverage from code carrying `// Implements:
> REQ-2/*` references.

So the coverage chain should be **per-assertion**:

```text
test  --Verifies-->  REQ-2/A                (REQ-2 reaches 100% test coverage)
REQ-2 --Refines-->   REQ-1/A                (propagates UP to /A specifically)
=> REQ-1/A is test-covered; REQ-1/B stays uncovered (nothing refines it)
```

`Refines` is requirement→requirement inheritance that rolls coverage **upward**
to the *specific targeted assertion* — distinct from claiming the child fully
*satisfies* the parent.

## Observed behavior (current)

Propagation **does** roll child coverage up across a `Refines:` edge — but only
at requirement granularity. The assertion suffix on the `Refines:` target is
dropped during parsing: `**Refines**: EVS-PRD-parent/A` is recorded as a plain
requirement→requirement `refines` edge to `EVS-PRD-parent` with no `/A` target.
Consequently a single covered child marks **every** assertion of the parent
covered, not just the one it refines.

Concretely in the Cure-HHT `event_sourcing` repo (verified 2026-06-20):

- `EVS-PRD-portability/A,B` show **covered** purely because the `EVS-DEV-postgres-backend`
  child (which refines `portability`) has coverage — although nothing actually
  tests "pure-Dart core" (A) or "runs on every runtime" (B).
- `EVS-PRD-library-charter` shows **all 9 assertions covered**, including `G`
  (ALCOA+ / 21 CFR Part 11) and `I` (the Layer-1/Layer-2 documentation
  discipline), which no test exercises — they inherit coverage from covered
  children.
- Propagation is **upward only**: `EVS-DEV-find-all-events-extended-filters`
  (a child of the well-covered `EVS-PRD-event-log`) stays **uncovered** until a
  test directly verifies it — coverage does not flow parent → child.

This coarse behavior is acceptable as the *default* for requirement-level
`Refines:` edges, but it means coverage numbers cannot be trusted at assertion
granularity, and assertions that should stay visibly uncovered (e.g. charter/G,
which needs human attestation) are silently marked covered.

## The actionable defect: retain assertion-qualified `Refines:` targets

To make propagation assertion-precise, an assertion-qualified `Refines:` target
must be parsed and stored as an assertion-level edge, and the coverage rollup
must honor it (propagate to `parent/A` only, not all of `parent`).

### Minimal reproduction

`spec/prd-parent.md`:

```markdown
# EVS-PRD-parent: Parent

**Level**: prd | **Status**: Active | **Implements**: -

## Assertions

A. The system SHALL do the refined-and-covered thing.

B. The system SHALL do an unrelated thing (control: must stay uncovered).

*End* *Parent*
```

`spec/prd-child.md`:

```markdown
# EVS-PRD-child: Child

**Level**: prd | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-parent/A

## Assertions

A. The system SHALL do the refined-and-covered thing, in detail.

*End* *Child*
```

`test/child_test.dart` (or any scanned test file for the project):

```dart
// Verifies: EVS-PRD-child/A
void main() {
  test('child A', () { /* ... */ });
}
```

### Expected vs actual

| query | expected | actual |
|---|---|---|
| `get_test_coverage("EVS-PRD-child")`  | A covered (100%) | A covered (100%) ✓ |
| `get_test_coverage("EVS-PRD-parent")` | A covered (propagated via `Refines: …/A`), **B uncovered** | A covered **and B also covered** (suffix dropped → whole-parent rollup) ✗ |

The failing distinction is the parent's `B`: it should stay uncovered because
nothing refines it, but the dropped `/A` suffix turns the edge into a
whole-requirement rollup that covers `B` too.

## pytest sketch for the elspais suite

```python
def test_refines_propagation_is_assertion_scoped(tmp_project):
    tmp_project.add_spec("prd-parent.md", parent_with_assertions("A", "B"))
    tmp_project.add_spec("prd-child.md", child(refines="EVS-PRD-parent/A", assertions="A"))
    tmp_project.add_test("child_test", verifies=["EVS-PRD-child/A"])
    graph = tmp_project.build_graph()

    parent_cov = graph.test_coverage("EVS-PRD-parent")
    assert "EVS-PRD-parent/A" in parent_cov.covered           # passes (propagates)
    assert "EVS-PRD-parent/B" not in parent_cov.covered       # FAILS today (suffix dropped)
```

## Diagnostic pointers for the elspais agent

1. **Refines-target parsing.** Confirm that `**Refines**: EVS-PRD-parent/A`
   retains `/A` as an assertion-level edge target. Repo evidence shows the
   suffix is stripped — `get_requirement(child).parents` records the edge to
   `EVS-PRD-parent` with no `/A`. That stripping is the root cause.
2. **Coverage rollup over refines edges.** The rollup now walks `refines` edges
   (requirement-level propagation works). Make it honor an assertion-qualified
   target: propagate to `parent/A` only, not to every assertion of `parent`.
3. **Direction.** Propagation is **upward** (child coverage → parent assertion),
   matching the one-way `Refines` traceability direction (specific → generic).
   This is already correct.

## Note on multi-assertion label format (separate, now fixed)

The working `// Verifies:` multi-label format is slash-joined per
`multi_separator = "/"`: `// Verifies: EVS-PRD-foo/A/E` binds both A and E. As
of the 2026-06-20 elspais update the comma+bare-slash shorthand
`// Verifies: EVS-PRD-foo/A, /C, /E` also binds A, C, and E (previously it bound
only A). Recorded here for completeness.
