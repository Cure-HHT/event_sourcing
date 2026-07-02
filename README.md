# event_sourcing

Reactive, append-only event-sourcing primitives for regulated Dart and
Flutter applications, with companion libraries for canonical JSON
serialization and provenance tracking. Built for FDA 21 CFR Part 11
compliant audit trails.

## Packages

| Package | Purpose |
| --- | --- |
| [`event_sourcing/`](event_sourcing/) | Core library: storage, sync, ingest, materialization, action dispatch, permissions |
| [`canonical_json_jcs/`](canonical_json_jcs/) | JCS (RFC 8785) JSON canonicalization |
| [`provenance/`](provenance/) | Append-only provenance chain types |

`event_sourcing` depends on the other two via path-deps. The packages are
intentionally small and pure-Dart so they can be reused by server-side
and web deployments as well as the mobile client.

### Demos

`event_sourcing/example/`, `event_sourcing/example_action_permissions/`,
and `event_sourcing/example_clinical_scopes/` are intra-lib worked
examples that exercise the public API. `example_action_permissions/`
hosts a Flutter dual-pane shell + shelf-based server demonstrating the
action-dispatch + permission-snapshot flow end to end.
`example_clinical_scopes/` is a Flutter + shelf demo of hierarchy-scoped
reads — a `region → site → participant` model with Investigator (site-
scoped), Overseer (region-scoped, two-hop), and Admin roles, where each
user's reactive participant list is narrowed by the read-path
`ScopeDescendantExpander`.

## Roadmap

Deliberate future work is recorded in `spec/roadmap/` — the only place
in the repo where unbuilt capability is described. The headline item
is multi-source (multi-user) editing:
`spec/roadmap/multi-source-editing.md`. The
single-source-per-aggregate-type invariant holds today; the dormant
multi-source machinery activates under that roadmap item.

## Setup

After cloning, run once per clone:

```sh
scripts/setup.sh
```

This sets `core.hooksPath = .githooks` (shared across all worktrees of
the clone) and pre-populates the hook environments. Pre-commit
framework runs hooks listed in `.pre-commit-config.yaml` on every
commit; gitleaks runs additionally on push. Hooks include
trailing-whitespace / EOF / merge-conflict checks, gitleaks (secret
scanning), markdownlint, and `dart format`.

Requires `pre-commit` on PATH; if absent, the script prints install
instructions (`pipx install pre-commit`, `brew install pre-commit`, or
`pip install --user pre-commit`) and exits.

## License

AGPLv3 — see [LICENSE](LICENSE).
