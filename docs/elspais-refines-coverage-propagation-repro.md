# Bug: `Refines:` does not propagate child coverage up to the targeted parent assertion

**For:** an agent working in the **elspais** source worktree (not this repo).
**Reported from:** Cure-HHT event_sourcing repo, while closing CUR-1528 test-coverage gaps.

## Intended behavior (requirement-author's model)

> `Refines:` does not itself imply satisfaction, but it **does propagate its own
> coverage up** to the thing it Refines. E.g. IF `REQ-2 Refines: REQ-1/A`, and
> `REQ-2` has 100% test-coverage, then `REQ-1/A` has 100% test-coverage. `REQ-2`
> itself gets coverage from code carrying `// Implements: REQ-2/*` references.

So the coverage chain should be:

```text
test  --Verifies-->  REQ-2/A                (REQ-2 reaches 100% test coverage)
REQ-2 --Refines-->   REQ-1/A                (REQ-2's coverage propagates UP)
=> REQ-1/A is test-covered  (no direct test on REQ-1 needed)
```

`Refines` is requirement→requirement inheritance that rolls coverage **upward**
to the *specific targeted assertion* — distinct from claiming the child fully
*satisfies* the parent.

## Observed behavior

Coverage does **not** propagate across a `Refines:` edge, even when the edge
target is assertion-qualified (`REQ-1/A`). The parent assertion stays
uncovered. Additionally, the assertion suffix on the `Refines:` target appears
to be dropped during parsing — the resulting edge is recorded as a plain
requirement→requirement `refines` edge to `REQ-1` with no `/A` target.

## Real-repo confirmation (the experiment that motivated this)

In the Cure-HHT `event_sourcing` repo:

- `EVS-PRD-subscription` is at **100% test coverage** (assertions A,B,C,D each
  have `// Verifies:` tests).
- `EVS-PRD-library-charter/B` ("deliver event + materialized-state updates
  reactively") is the charter assertion that `subscription` refines.

Steps run:

1. Baseline: `spec/prd-subscription.md` header has `**Refines**: EVS-PRD-library-charter`.
   `get_test_coverage(EVS-PRD-library-charter)` → B **uncovered**.
2. Edited the header to assertion-target the charter assertion:
   `**Refines**: EVS-PRD-library-charter/B`.
3. `refresh_graph(full=true)`.
4. `get_test_coverage(EVS-PRD-library-charter)` → B **still uncovered**.
   `get_requirement(EVS-PRD-subscription).parents` → still
   `{id: "EVS-PRD-library-charter", edge_kind: "refines"}` (no `/B` retained).

Expected after step 3: charter/B covered (inheriting subscription's 100%).
Actual: unchanged. (Edit reverted afterward.)

Charter assertions that *are* covered in this repo (A, C, D, E, H) are each
covered by a **direct** `// Verifies: EVS-PRD-library-charter/<label>`
annotation on a test — never by refines-propagation. That is the current
workaround, but it defeats the purpose of `Refines:` inheritance.

## Minimal self-contained reproduction fixture

Three files in an elspais test project (namespace/levels per the project's
`.elspais.toml`; shown here in the EVS format this repo uses). The `Implements:`
field is irrelevant to the repro — only `Refines:` + a verifying test matter.

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
| `get_test_coverage("EVS-PRD-parent")` | **A covered** (propagated via Refines), B uncovered | A **uncovered**, B uncovered ✗ |

The failing assertion is the parent row: `EVS-PRD-parent/A` should be covered
because a fully-covered child refines it; instead it is uncovered. `B` is the
negative control and correctly stays uncovered (nothing refines it).

## pytest sketch for the elspais suite

```python
def test_refines_propagates_child_coverage_to_targeted_parent_assertion(tmp_project):
    tmp_project.add_spec("prd-parent.md", parent_with_assertions("A", "B"))
    tmp_project.add_spec("prd-child.md", child(refines="EVS-PRD-parent/A", assertions="A"))
    tmp_project.add_test("child_test", verifies=["EVS-PRD-child/A"])
    graph = tmp_project.build_graph()

    child_cov = graph.test_coverage("EVS-PRD-child")
    assert child_cov.covered == {"EVS-PRD-child/A"}          # passes today

    parent_cov = graph.test_coverage("EVS-PRD-parent")
    assert "EVS-PRD-parent/A" in parent_cov.covered           # FAILS today
    assert "EVS-PRD-parent/B" not in parent_cov.covered       # negative control
```

## Diagnostic pointers for the elspais agent

1. **Refines-target parsing.** Confirm whether `**Refines**: EVS-PRD-parent/A`
   retains `/A` as an assertion-level edge target, or whether the suffix is
   stripped to a requirement-level edge (the repo evidence suggests it is
   stripped — `get_requirement(child).parents` shows no `/A`). If stripped,
   that's the first defect: assertion-qualified Refines targets must be
   parsed/stored.
2. **Coverage rollup over refines edges.** In the test-coverage computation,
   confirm whether `refines` edges are walked when accumulating an assertion's
   covering test nodes. If only direct `// Verifies:` references and
   test→code→requirement (implements) chains are counted, refines-propagation
   is simply not implemented for the `tested` dimension. Decide whether
   propagation is per-assertion (`child` → `parent/A` only) or whole-child →
   whole-parent. The author's intent is **per-assertion** (`Refines: parent/A`
   propagates to `parent/A`, not to `parent/B`).
3. **Direction.** Propagation is **upward** (child coverage → parent assertion),
   matching the one-way `Refines` traceability direction (specific → generic).

## Note on multi-assertion label format (separate, minor)

While diagnosing, confirmed the working `// Verifies:` multi-label format is
slash-joined per `multi_separator = "/"`: `// Verifies: EVS-PRD-foo/A/E` binds
both A and E. The comma+bare-slash form `// Verifies: EVS-PRD-foo/A, /C, /E`
binds **only A** (the bare `/C, /E` are dropped). Not necessarily a bug — but if
that shorthand is meant to be supported, it currently silently under-binds.
