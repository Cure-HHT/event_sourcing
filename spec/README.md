# Formal Requirements System

## Intent

This repository uses a formal requirements system to define, implement, and verify the library's specification, design, implementation, and verification.

Requirements are written so they can be directly verified, traced one-way from implementation to obligation, audited without manual cross-referencing, and maintained without redundancy.

Use **`requirements-spec.md`** for the authoritative rules and grammar.

---

## Directory Purpose

The `spec/` directory contains **formal requirements only**.

- **spec/**: Normative obligations defining what must be true of the library.
- **spec/roadmap/**: The only place future work is recorded (see `spec/roadmap/README.md`); non-normative.
- **docs/**: Explanatory documentation — the consumer guide (`event-sourcing-guide.md`), e2e-testing notes, naming conventions, and speculative scenario sketches (`scenarios/`).

If it defines *what must be true*, it belongs in `spec/`.
If it explains *how to do something* or *why a decision was made*, it belongs in `docs/`.

---

## Levels

This repo uses three requirement levels:

- **PRD** (`EVS-PRD-...`) — product-level obligations: what the library provides to its consumers.
- **OPS** (`EVS-OPS-...`) — operational obligations: release management, deployment of derived artifacts, secret rotation, and similar operational concerns that pertain to the library itself (not to consuming applications).
- **DEV** (`EVS-DEV-...`) — implementation obligations: how the library realizes the PRDs and OPS requirements.

The component name (the kebab-case slug after the level) is **stable**: once a requirement has been authored under a given name, renaming it is a breaking change to any reference in code, tests, results, and other requirements.

---

## File organization

A `spec/` file MAY contain:

- **One or more requirement blocks** — each is a complete `EVS-{TYPE}-{component}` requirement per the grammar in `requirements-spec.md`. elspais detects a requirement block by the `EVS-{TYPE}-{component}` pattern in the heading text, **not by heading depth** — `# EVS-PRD-foo`, `## EVS-PRD-foo`, and `### EVS-PRD-foo` are all valid markers.
- **Remainder sections** — any heading that does not match the `EVS-{TYPE}-{component}` pattern is treated by elspais as non-normative prose. Use them for cross-system narrative, architecture orientation, decisions-rejected commentary, and reading-order guidance that wouldn't fit naturally inside any single requirement's Rationale.
- **Mermaid diagrams** — render in any markdown-aware tool; treated as remainder content by elspais.
- **Image links** — for renderers that display them inline (e.g., GitHub).

Conventions (recommended, not enforced by elspais):

- One file per design topic. `spec/prd-<topic>.md` if the file holds PRDs only; `spec/dev-<topic>.md` if it holds DEVs only; `spec/<topic>.md` if it mixes levels.
- For multi-requirement files, treat the file as a "book with chapters": `#` is the file title; `##` is each chapter (some chapters are requirement blocks `## EVS-PRD-...`, others are remainder sections like `## Overview`); `###` are subsections within each chapter (`### Purpose`, `### Assertions`, `### Rationale`). This keeps the file passing default markdownlint MD025 (single H1) while keeping the requirement-block markers visually distinct from a third-level heading.
- A topic file holding multiple requirement blocks SHOULD also have remainder chapters that orient a first-time reader (overview, architecture, reading order, decisions-rejected, open questions, future work).
- Single-requirement files are also fine for narrowly-scoped requirements (the existing `prd-action-dispatch.md`, `prd-event-log.md`, etc., each have `#` as the requirement-block heading because they hold only one requirement).
- **Cross-system narrative belongs in a remainder section of the spec/ file it contextualizes — NOT in a separate prose document.** Keep the source of truth singular.

---

## Lifecycle of a design spec

Designs are authored in place in `spec/`:

1. **Author.** A new design lands directly as `spec/<topic>.md` prose —
   overview, architecture, decisions considered and rejected. elspais
   treats the file as non-normative remainder content until a
   requirement-block heading appears.
2. **Stabilize.** As the design firms up (usually alongside
   implementation), normative `EVS-{TYPE}-{component}` requirement
   blocks are added to the same file. The surrounding prose stays as
   remainder sections; there is no separate brainstorm document and no
   migration step.
3. **Maintain.** The file always describes the present state of the
   design. Deferred ideas move to `spec/roadmap/`; everything else in
   the file is true of the shipped library.

DEV-level requirements continue to be authored alongside code per
CLAUDE.md "Requirement traceability". They MAY be added to the existing
`spec/<topic>.md` as new requirement blocks, or split into a companion
`spec/dev-<topic>.md` if the DEV bulk warrants it.
