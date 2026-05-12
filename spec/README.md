# Formal Requirements System

## Intent

This repository uses a formal requirements system to define, implement, and verify the library's specification, design, implementation, and verification.

Requirements are written so they can be directly verified, traced one-way from implementation to obligation, audited without manual cross-referencing, and maintained without redundancy.

Use **`requirements-spec.md`** for the authoritative rules and grammar.

---

## Directory Purpose

The `spec/` directory contains **formal requirements only**.

- **spec/**: Normative obligations defining what must be true of the library.
- **docs/**: Explanatory documentation, ADRs, plans, and design specs.

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

- **One or more requirement blocks** — each is a complete `EVS-{TYPE}-{component}` requirement per the grammar in `requirements-spec.md`.
- **Remainder sections** — any `#`-headed section that is not a requirement block is treated by elspais as non-normative prose. Use them for cross-system narrative, architecture orientation, decisions-rejected commentary, and reading-order guidance that wouldn't fit naturally inside any single requirement's Rationale.
- **Mermaid diagrams** — render in any markdown-aware tool; treated as remainder content by elspais.
- **Image links** — for renderers that display them inline (e.g., GitHub).

Conventions (recommended, not enforced by elspais):

- One file per design topic. `spec/prd-<topic>.md` if the file holds PRDs only; `spec/dev-<topic>.md` if it holds DEVs only; `spec/<topic>.md` if it mixes levels.
- A topic file holding multiple requirement blocks SHOULD also have remainder sections that orient a first-time reader (overview, architecture, why-these-PRDs-together, decisions-rejected, open questions, future work).
- Single-requirement files are also fine for narrowly-scoped requirements (the existing `prd-action-dispatch.md`, `prd-event-log.md`, etc.).
- **Cross-system narrative belongs in a remainder section of the spec/ file it contextualizes — NOT in a separate prose document.** Keep the source of truth singular.

---

## Lifecycle of a design spec

Design ideas evolve through three stages:

1. **Brainstorm.** Output goes to `docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`. Prose-heavy. Captures the conversational design process, alternatives considered + rejected, and open questions. Transient scaffolding intended for reader and reviewer orientation, not for long-term residency.
2. **Stabilize.** When the design has settled, author the corresponding `spec/<topic>.md` (or `spec/prd-<topic>.md`) containing the normative requirement blocks plus the cross-system narrative carried over as remainder sections (overview, architecture mermaid, decisions-rejected commentary, future work). The spec/ file is now the single authoritative source.
3. **Archive.** The original brainstorm doc is deleted (or moved to `docs/archive/<year>/`). Its content has migrated; keeping it would be a DRY violation and risks drift.

DEV-level requirements continue to be authored alongside code per CLAUDE.md "Requirement traceability". They MAY be added to the existing `spec/<topic>.md` as new requirement blocks, or split into a companion `spec/dev-<topic>.md` if the DEV bulk warrants it.

Roadmap and time-evolving status documents (e.g., `docs/superpowers/specs/2026-05-11-roadmap.md`) are a different artifact from design specs and stay in `docs/superpowers/specs/`. They are not normative.

Implementation plans live at `docs/superpowers/plans/YYYY-MM-DD-<topic>-implementation.md` and reference `spec/` requirement IDs.
