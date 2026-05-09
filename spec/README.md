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

This repo uses two requirement levels:

- **PRD** (`EVS-prd-...`) — product-level obligations: what the library provides to its consumers.
- **DEV** (`EVS-dev-...`) — implementation obligations: how the library realizes the PRDs.

Operations level is intentionally not used here. The library is pure-Dart with no deployment or runtime-ops surface; ops obligations live in the consuming application repos (e.g. `cure-hht/hht_diary`).
