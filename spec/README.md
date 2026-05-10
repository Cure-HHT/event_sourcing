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
