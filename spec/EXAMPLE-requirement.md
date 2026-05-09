# EVS-DEV-example: Example Requirement Title

**Level**: DEV | **Status**: Draft | **Refines**: -

## Assertions

A. The library SHALL demonstrate the assertion format.
B. The library SHALL show proper use of SHALL language.

## Rationale

This is an example requirement demonstrating the proper format. Delete this file after reviewing the structure.

---

**Format Notes** (delete this section):

- **Title line**: `# EVS-{level}-{component}: Title` where level is `prd` or `dev`.
- **Metadata line**: Level, Status, and Refines (use `-` if there is no parent).
- **Assertions**: Labeled A–Z, each using SHALL for required behavior.
- **Rationale**: Optional explanation section (non-normative).
- **Footer**: `*End* *Title* | **Hash**: XXXXXXXX` — hash computed by `elspais hash update`.
- **`Implements:` is deprecated for REQ→REQ relationships in spec headers.** Use `Refines:` (default) for inheritance, or `Satisfies:` for template/principle instantiation. The deprecation does not apply to per-class `// Implements:` annotations in production code.

Run `elspais format` for more templates and `elspais validate` to check this file.

*End* *Example Requirement Title* | **Hash**: 00000000
