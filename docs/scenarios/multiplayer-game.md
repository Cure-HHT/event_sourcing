# Designing a multiplayer card-game backend on `event_sourcing`

A turn-based card game (Hearts) deployed as a portal-style topology: a single
authoritative Dart server holds the substrate (one log, Postgres), and Flutter
clients (browser + mobile) talk to it through `reaction`'s `RemoteScope`. Each
table is its own aggregate; concurrent matches are independent log subtrees
correlated only by their event-type vocabulary. This sketch leans hard on the
substrate's per-aggregate ordering and reactive `subscribe<T>` for in-match
correctness, and flags the hidden-information problem as the place where Layer
2 conventions don't quite fit.

## 1. Initialization and use

**Storage and topology.** Server uses `PostgresBackend` (one DB per region).
Browser/mobile clients hold no local log — they're thin `RemoteScope`
consumers. The server is the single `Source` for every match aggregate;
clients never run their own substrate, so the v1 single-source-per-aggregate-
type constraint is honoured trivially. (Predict-then-reconcile would require
multi-source, which is dormant in v1 — defer.)

**Actions** (`Action<TInput, TResult>`):

- `JoinTableAction` — unscoped (any authenticated user can join an open
  table), emits `player_joined`.
- `StartMatchAction` — scoped `match.host`, emits `match_started` +
  `hand_dealt` (one per seat).
- `PlayCardAction` — scoped `match.play`; `scopeFor` returns `BoundScope(
  'match', matchId)`. `validate` enforces it's the player's turn, suit-
  following rules, card-in-hand. Emits `card_played`, and conditionally
  `trick_won` + `next_trick_started`.
- `ConcedeAction` — scoped `match.play`, emits `player_conceded` +
  `match_concluded`.
- `KickPlayerAction` — scoped `table.moderate` for hosts, emits
  `player_kicked`.

**Projections** (`ProjectionRegistry`):

- `match_state` (`AggregateProjectionSpec` on aggregate type `match`):
  deep-merged public state — turn number, current player, trick-in-progress,
  score, phase.
- `match_seats` (`TableProjectionSpec`, composite key `[matchId, seatIndex]`):
  per-seat public facts — player id, seat label, cards-remaining-count (not
  contents).
- `player_hands` (`TableProjectionSpec`, composite key `[matchId, playerId]`):
  private hand contents. **This is the hidden-info view; see §3.**
- `move_log` (`TableProjectionSpec`, one row per `card_played` event, key =
  `sequence`): replay timeline.
- `table_lobby` (Aggregate on `table`): joinable tables, state, occupancy.
- `player_stats` (Aggregate keyed by `userId`): wins/losses across matches,
  fed by `match_concluded` events.

**Auth model.** Roles: `Player`, `Spectator`, `TableHost`, `Admin`. Scope
classes: `table` (top), `match` (contained in `table`). Permissions:
`match.play` (scoped to `match`), `match.spectate` (scoped to `match`),
`table.moderate` (scoped to `table`), `match.host` (scoped to `match`).
Containment projection `match_table_index` maps `matchId → tableId` so a
`TableHost` assigned at `BoundScope('table', T)` automatically gets
`match.host` for every match under that table.

**Cross-process.** Flutter clients compose `RemoteScope` (HTTP + multiplexed
WS). Each opens one WS subscription per view they need (`match_state` filtered
by `aggregates: {matchId}`, `move_log` filtered the same way, plus
`player_hands` filtered to the player's own row). Submissions go through
`ActionSubmitter.submit`. The `AuthzWatcher` machinery already handles
mid-match kicks (close WS with `4003 permissions_changed`) for free.

## 2. Layer 1 properties that are load-bearing

**Per-aggregate-per-Source ordering** is the single most distinctive Layer 1
guarantee for this domain. Hearts cares deeply that "player A played the
7-of-clubs" precedes "trick A won by player B" precedes "player C leads the
next trick." The server can accept `PlayCardAction` submissions concurrently
from four sockets without any application-level locking: the substrate's per-
aggregate ordering serializes all events for a given match aggregate, and the
dispatcher's transactional `authorize → execute → persist` cycle (CLAUDE.md
/ dispatch pipeline §) means a submission either appends to the head of that
match's log or denies cleanly. A banking ledger needs ordering too, but it
can usually re-establish total order from timestamps and account balances
after the fact; a card game cannot — turn N+1 is undefined until turn N has
committed, so the substrate's *synchronous* ordering inside the transaction
is what lets `validate` simply call "is it your turn?" against the
materialized `match_state`. No saga, no lock manager.

**Append-atomic-with-row-update** is the second. A `card_played` event mutates
the public `match_state` row (turn advances), the player's `player_hands` row
(card removed), and inserts into `move_log` — three projection writes in one
transaction with the append. UI subscribers see all three changes together or
none. Without this, the substrate's flagship `Snapshot → EndOfReplay → Delta`
stream would let a spectator briefly see the card played but their opponent's
"cards remaining" still old — visible glitches in a real-time multiplayer UI.
Append-only also gives spectator replay (`Events()` mode subscription starting
from `sequence = 0` of the match's events) deterministic re-derivation:
cheating disputes are litigated against the hash chain, not a server log file.

Hash chain integrity is nice-to-have for tournament forensic audit but isn't
load-bearing the way it is for clinical trial data.

## 3. Layer 2 machinery — and one substrate gap

Out-of-the-box conventions that fit:

- **Tombstone semantics** for `match_concluded` on the `match_state` aggregate
  row? Tempting, but match concluding shouldn't *delete* the state — replay
  needs it. Better: leave `match_state` materialized, gate writes via
  permission revocation (auto-revoke `match.play` on `match_concluded`). The
  match becomes "locked / read-only" because no action's permission resolves
  any more.
- **Delta merge** is correct for `match_state` (turn count, current player,
  scores accumulate).
- **Per-table scope** + containment to `match` slots straight into
  `EVS-PRD-scoped-permissions`.

**The hidden-information problem is the load-bearing gap.** Spec
`spec/reaction-remote.md` (§row-level narrowing) does row-level filtering on
`aggregates`, not on arbitrary row predicates. A `player_hands` view keyed by
`(matchId, playerId)` can be filtered by aggregate id only if the projection's
`aggregateId` is the composite key — feasible by making `hand` its own
aggregate type with `aggregateId = '$matchId:$playerId'` and adding a scope
class `hand` contained in `match`, with a `hand_player_index` containment
projection that resolves `handId → playerId`. Then a row-level subscription
narrows to "hands where the player is me." This works but is the substrate
pushed hardest: it requires apps to model every privacy boundary as its own
aggregate-with-scope. For "opponent's discard pile that's visible to the table
but not to spectators" you'd need yet another aggregate. **Gap to flag:** the
substrate has no per-row authorization predicate; everything must reduce to
aggregate-id narrowing. For a game with three or four privacy tiers per match,
this gets baroque, and a `RowFilterSpec` on `ProjectionSpec` (declarative,
composing the same containment primitives) would map the domain more directly.

**Spectator views** are then naturally derived projections: a
`match_state_public` projection with `SelectedFields([...])` strips any leaked
field; spectators get `match.spectate` and never `match.private`. **Replay**
uses `subscribe<T>` with `Events()` mode and a `sequence` start, replaying any
concluded match deterministically — the substrate's reactive primitive doubles
as the replay engine.
