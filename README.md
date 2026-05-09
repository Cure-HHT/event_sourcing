# event_sourcing

Reactive, append-only event-sourcing primitives for Dart, with companion
libraries for canonical JSON serialization and provenance tracking. Built
for FDA 21 CFR Part 11 compliant audit trails and consumed by the
[`cure-hht/hht_diary`](https://github.com/Cure-HHT/hht_diary) clinical
diary platform.

## Packages

| Package | Purpose |
| --- | --- |
| [`event_sourcing/`](event_sourcing/) | Core library: storage, sync, ingest, materialization, action dispatch, permissions |
| [`canonical_json_jcs/`](canonical_json_jcs/) | JCS (RFC 8785) JSON canonicalization |
| [`provenance/`](provenance/) | Append-only provenance chain types |

`event_sourcing` depends on the other two via path-deps. The packages are
intentionally small and pure-Dart so they can be reused on the portal
server as well as the mobile client.

### Demos

`event_sourcing/example/` and `event_sourcing/example_action_permissions/`
are intra-lib worked examples that exercise the public API. The latter
hosts a Flutter dual-pane shell + shelf-based server demonstrating the
action-dispatch + permission-snapshot flow end to end.

## Roadmap

Three phases follow this kick-start; the architectural memos behind them
live in this repo's `docs/superpowers/` and in session memory.

1. **Phase I — substrate redesign.** Replace `watchEvents` /
   `watchView` / `watchFifo` with a unified `subscribe<T>(filter, mode)`
   API; promote per-aggregate (`AggregateMode<T>`) to primary; typed
   `Materializer<T>` outputs typed state. Cut `0.4.0`.
2. **Phase II — multi-source editing.** Materializer rule grammar
   (settings event types: `set_canonicalizer`, `delegate_canonicalization`,
   ...), `CanonicalView` vs `ProposalView`, hash-chain merge under
   parallel sources, per-entry-type resolution policies. Cut `0.5.0`.
3. **Phase III — consumer migration.** Bump the `hht_diary` pin and sweep
   call sites for the new API. Happens in `cure-hht/hht_diary`, not
   here.

The single-source-per-aggregate-type invariant holds in v1; substrate
machinery for multi-source is dormant until Phase II activates it.

## Setup

After cloning, run once per clone:

```sh
scripts/setup.sh
```

This sets `core.hooksPath = .githooks` (shared across all worktrees of
the clone) and pre-populates the hook environments. Pre-commit
framework runs hooks listed in `.pre-commit-config.yaml` on every
commit and push. Hooks include trailing-whitespace / EOF / merge-conflict
checks, gitleaks (secret scanning), markdownlint, and `dart format`.

Requires `pre-commit` on PATH; if absent, the script prints install
instructions (`pipx install pre-commit`, `brew install pre-commit`, or
`pip install --user pre-commit`) and exits.

## Origin

Extracted from [`cure-hht/hht_diary`](https://github.com/Cure-HHT/hht_diary)
on 2026-05-08 from branch `CUR-1192-actions-demo` at commit `200b4e3a`
(CUR-1317). The pre-extraction history lives in that repo; this repo
starts with a single root commit so the cut point is unambiguous.

## License

AGPLv3 — see [LICENSE](LICENSE).
