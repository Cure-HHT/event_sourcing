# Roadmap — substrate-level authentication closure

Deferred closure of the last unaudited trust input: the caller-supplied
`Principal.userId` on action submissions and event metadata (CLAUDE.md,
"Trust boundaries", and `EVS-PRD-library-charter` assertion H).

## `AuthenticationProvider` closure

**Baseline.** Identity is still accepted on faith for in-process
deployments: `event_sourcing/lib/src/actions/principal.dart` implements
the charter's assertion H as the acknowledged unaudited input — the
substrate does not authenticate which user a caller claims to be. What is
*not* trusted is already derived from the log: the `(userId, activeRole)`
binding is independently verified against the `user_role_scopes`
projection (`table_backed_authorization_policy.dart`) before any
permission is honoured under that role. Wire-side, the credential gap is
closed by consumer-supplied validators composed into the shelf pipeline:
the `PrincipalAuthValidator` seam plus `auth_middleware.dart` map a wire
credential to a `Principal` and refuse invalid credentials. No
substrate-level identity-verification seam exists.

**Remaining.** An `AuthenticationProvider` pluggable interface consulted
before authorize, participating in the closed-under-events guarantee, so
that in-process deployments close the userId-on-faith gap the way
wire-side deployments already do through their validators/middleware.
Cross-process deployments need no further work here — their validator or
middleware already closes it for that deployment.
