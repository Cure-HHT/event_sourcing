# EVS-DEV-flow-token: Flow Correlation Token

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-action-dispatch

## Purpose

A flow correlation token is an opaque value that a consuming application mints on an originating action and that the substrate carries, unchanged, onto every event the action emits — and across every deployment the event reaches. It lets an application reconstruct a multi-step flow end to end even when part of the flow crosses a non-event-sourced gap (for example a push-notification hop), without the substrate ascribing any meaning to the token.

## Assertions

A. The dispatcher SHALL accept an optional opaque correlation token on an action submission.

B. The dispatcher SHALL thread that token onto every event it appends during that dispatch (including denial events).

C. The token SHALL be preserved unchanged when an event carrying it is delivered to, ingested by or restored from another deployment.

D. The token SHALL be opaque to the substrate — neither parsed nor interpreted — and SHALL exclude cleartext one-time passwords, recovery tokens, and session tokens.

## Rationale

Correlation across non-event-sourced gaps needs a token minted on an originating intent and echoed by the downstream events the flow produces. The substrate already records such a token on every event; this requirement formalizes its contract: accept it on submission, thread it onto every emitted event, preserve it unchanged across every deployment the event reaches, and keep it opaque and free of cleartext secrets. Recording the token on every emitted event and preserving it across hops makes end-to-end tracing possible without weakening the immutable log; keeping it opaque and secret-free keeps short-lived credentials out of the forever-immutable record. The token is a generic library primitive; consuming applications mint and interpret it for their own flows.

## Changelog

- 2026-09-25 | 784df968 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | C: no path recovers a database's own events; delivery, ingest and restore remain. Code or tests citing C are listed in the integration report
- 2026-09-25 | 9bd2b75a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend C: the token is preserved across delivery, ingest, recovery and restore; deployments do not relay events they ingested
- 2026-08-10 | 45a08e62 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | a02a8238 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Flow Correlation Token* | **Hash**: 784df968
