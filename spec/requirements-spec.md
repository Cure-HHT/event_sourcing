# Formal Requirements Specification

## Purpose

This document defines the **canonical grammar, structure, and authoring rules** for all formal requirements in this repository's `spec/` directory.

It is the **single source of truth** for how requirements are written, identified, hashed, decomposed, and referenced. Both humans and automated agents MUST follow this specification.

This document intentionally avoids workflow, tooling, or process guidance. Those belong in tooling or developer documentation.

---

## Normative Model

- Requirements define **obligations**, not descriptions.
- Obligations are stated using **SHALL** or **SHALL NOT**.
- Each obligation appears **exactly once** in the repository.
- Traceability is **one-way only**: more specific requirements reference more generic requirements via `Refines:` metadata.

---

## Requirement Identity

### Requirement IDs

Each requirement is uniquely identified by an ID of the form:

```
EVS-{level}-{component}[-{assertion}]
```

Where:

- `EVS` is this repo's namespace prefix (matches the `[project] namespace` in `.elspais.toml`).
- `level` is a three-letter audience tag:
  - `prd` = PRD = Product Requirements Documentation
  - `dev` = DEV = Development Specification
  - (Ops level intentionally not used; see `README.md`.)
- `component` is a kebab-case noun describing the requirement subject. Should be short, specific, and stable. Examples: `event-store`, `provenance-entry-schema`, `canonical-json`.
- `assertion` is an optional single-letter label `[A-Z]` for a specific Assertion within the requirement (see "Assertions" below). Multiple assertions in one reference use `+`: e.g. `EVS-dev-event-store-A+B+C`.

Examples:

- `EVS-prd-event-store`
- `EVS-dev-provenance-entry-schema-G`
- `EVS-dev-canonical-json`

Component names MUST be unique within a level. Renaming a component is permitted but counts as a breaking change to any external reference.

---

## Requirement Header Grammar

Each requirement MUST begin with a header in the following exact form:

```markdown
# EVS-{id}: {Short Descriptive Title}

**Level**: {prd | dev} | **Status**: {Draft | Review | Active | Deprecated} | **Refines**: {EVS-{id}, EVS-{id} | -}
```

Optional additional metadata lines MAY follow the primary line, one per line. The most common is:

```markdown
**Satisfies**: {EVS-{id} | -}
```

Rules:

- `Refines` lists **only less-specific requirements** (e.g. a `dev` requirement Refines a `prd` requirement; a `prd` requirement Refines a higher-level `prd`).
- `Satisfies` is reserved for cases where this requirement instantiates a template/principle requirement (rare in this repo).
- `Implements` is **deprecated** as of this repo's authoring; use `Refines` for the inheritance relationship and `Satisfies` for template instantiation.
- Parent requirements MUST NOT reference children.
- Use `-` if the requirement has no parent at this relationship type.

---

## Assertions (Normative Content)

### Assertion Block

All testable obligations MUST appear in an `## Assertions` section.

```markdown
## Assertions

A. The library SHALL ...
B. The library SHALL ...
```

### Assertion Rules

- Each assertion MUST:
  - use SHALL,
  - express exactly one obligation,
  - be independently decidable as true or false,
  - be self-contained — assertions MUST NOT contain cross-references to other requirements (cite related requirements in Rationale or in `Refines:` / `Satisfies:` metadata instead).
- Assertion labels:
  - MUST be uppercase letters A–Z,
  - MUST be unique within the requirement,
  - MUST remain stable over time,
  - MUST NOT be reused once removed (**IMPORTANT**).
- If more than 26 assertions are required, the requirement MUST be split into smaller components.

### Assertion References

Tests and other verification artifacts reference:

- the entire requirement: `EVS-dev-event-store`, or
- a specific assertion: `EVS-dev-event-store-F`, or
- multiple assertions: `EVS-dev-event-store-A+B+C`.

Per-test annotation format (Dart):

```dart
// Verifies: EVS-dev-event-store-F
test('appends an event with monotonic sequence', () { ... });
```

Per-class implementation annotation format (Dart):

```dart
// Implements: EVS-dev-event-store-A+B+C — append + ordering invariants
class EventStore { ... }
```

(Yes, the *test* keyword is `Verifies`; the *production code* keyword is `Implements`. The deprecation note above applies to REQ→REQ relationships in spec headers, not to code annotations.)

### Verification Modes

A requirement's assertions are verified by automated tests. Two complementary modes are recognized; both produce pass/fail results, both use the same `// Verifies:` annotation, and elspais does not distinguish them at scan time.

**Behavioral verification.** The default. A test exercises the library's API with specific inputs, observes outputs, and asserts they match the assertion's claim. Most assertions are verified this way.

**Structural / scan verification.** For assertions that constrain code structure rather than runtime behavior — e.g., "all calls to API X go through the allowlisted dispatcher", "no module outside the auth subsystem imports identity-provider clients", "the only writers to materialized table T are inside the materializer" — verification can take the form of a test that scans production source files for forbidden or required patterns and asserts the codebase obeys the rule. Scan tests are pass/fail like behavioral tests; they verify the structural commitments that PRDs make about how the library is composed (e.g., "single dispatch flow", "evaluated solely from event-derived projections", "rules are events, not callbacks").

A scan test typically reads the source tree from the test's working directory, applies a regex or AST query against production files only (excluding the test files and any explicit allowlist), and asserts the result set is empty (forbidden pattern) or non-empty / equal (required pattern).

```dart
// Verifies: EVS-prd-action-dispatch-A
test('only the dispatcher writes to the event log', () { ... });
```

The choice between behavioral and scan verification is made per assertion. Some assertions need both — a behavioral test for the runtime path and a scan test for the structural guarantee that no other path exists.

---

## Rationale Block (Optional, Non-Normative)

A requirement MAY include a `Rationale`, `Description`, `Discussion`, or other non-normative blocks. These are for context only and are NOT part of the testable requirements.

Rationale blocks MAY exist before and after the Assertion block. Any section not titled "Assertions" SHALL be treated as a Rationale block.

```markdown
## {Rationale Block Type}
<explanation>
```

Rules:

- Rationale MUST NOT introduce new obligations.
- Rationale MUST NOT restate assertions.
- Rationale MUST NOT use SHALL or MUST language.

---

## Acceptance Criteria

Acceptance Criteria SHALL NOT be used.

Requirements MUST be written such that the assertions themselves constitute the acceptance conditions.

---

## Compositional Requirements

A compositional requirement defines a **normative obligation boundary** that is satisfied through the combined effect of multiple lower-level requirements.

Compositional requirements:

- state a single obligation,
- do not enumerate behaviors,
- do not reference contributing requirements,
- rely on downstream `Refines:` declarations for composition.

Composition is inferred, never declared.

---

## Decomposition Rules

### Refinement

A child requirement refines a parent when it:

- adds specificity,
- adds constraints,
- commits to mechanisms or guarantees.

The child MUST refer to the parent via `Refines:`.

### Cascade

Multiple requirements MAY exist at the same Level refining a shared higher-level obligation. This is valid and expected.

---

## Leaf Requirements

A requirement is a leaf when:

- all obligations are fully expressed as labeled assertions, and
- further decomposition would only restate the same obligations or turn them into tests.

Leaf requirements are the attachment points for implementation and verification.

---

## Prescriptive Language Requirement

Requirements MUST be prescriptive, not descriptive.

Allowed:

- "The library SHALL ..."
- "The event store SHALL ..."

Forbidden:

- "The library does ..."
- "The event store has ..."

Requirements define what must be true, not what currently exists.

### Voice — final-state, not delta

Specs are written in the final-state voice. Avoid framing in terms of removal, replacement, or contrast with a prior version. The library is greenfield; there is no "previous version" to define against. Phrases like "removed", "no longer", "does NOT require" are forbidden in normative text. State the present obligation directly.

---

## Library Neutrality

This repo is a **library**. Requirements MUST be neutral with respect to consumer applications: do not name specific server roles (e.g. "diary server", "portal server"), specific sponsors, or specific deployment topologies. Use role-based names instead (e.g. "originator", "relay", "controller", "downstream consumer").

If a requirement is unavoidably consumer-specific, it does not belong in this repo — it belongs in the consumer's spec/.

---

## When a Section Needs a Requirement ID

A section requires an `EVS-` ID if and only if it introduces at least one normative obligation.

Explanatory, contextual, or illustrative sections MUST NOT have requirement IDs.

---

## Document Structure Rules

- Requirement documents SHOULD use a flat heading structure.
- `EVS-` blocks SHOULD be a top-level section.
- Subheadings within a requirement are limited to:
  - Assertions
  - Rationale (or other non-normative names)

---

## Hash Definition

Each requirement MUST end with a Footer including a content hash:

```markdown
*End* *{Title}* | **Hash**: {value}
```

The hash SHALL be calculated from:

- every line AFTER the Header line,
- every line BEFORE the Footer line.

Hashes are computed automatically by `elspais hash update`.
