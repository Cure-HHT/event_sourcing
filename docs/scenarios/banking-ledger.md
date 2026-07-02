# Banking Ledger on `event_sourcing` — Design Sketch

A regional bank's core ledger is the most natural fit imaginable for this
substrate: append-only is not an architectural choice but a regulatory
requirement, ALCOA+ is what FFIEC examiners read off the disk, and double-entry
bookkeeping turns the substrate's "append-atomic-with-row-update" guarantee
into the difference between a solvent bank and a fraud case. What the substrate
ships out of the box covers the audit story almost completely; what it doesn't
directly enforce — the balanced-pair invariant and Reg-E-style point-in-time
reconstruction queries — falls cleanly to app-level Actions and `Events()`-mode
subscriptions, with one substrate gap worth calling out.

## 1. Initialization and use

**Backend.** `PostgresBackend` against an HA Postgres cluster (synchronous
replication, PITR, WORM archival of WAL to an immutable bucket). Sembast is
irrelevant here — every channel terminates at the core ledger; there are no
offline clients holding writable logs.

**Sources and sync.** Single-source-per-aggregate-type: the core ledger is
*the* `Source` for every `account` and `transaction` aggregate. ATMs,
web/mobile banking, branch teller stations, and clearinghouse batch importers
are all *clients*, not Sources — they connect via `RemoteScope` over the
`reaction` wire and submit `ActionSubmission`s. This keeps the 0.x single-
source-per-aggregate-type constraint satisfied and matches the regulatory
reality (one canonical log per chartered institution).

**Actions** (`Action<TInput, TResult>`, all `Idempotency.required` except
read-throughs):

- `OpenAccount` — emits `account_opened` (entry type `account`, version 1).
- `Deposit`, `Withdraw` — emit a *pair* of events: a `posting` on the customer
  account and an offsetting `posting` on the bank's internal cash/GL account,
  both inside the same `ExecutionResult.events` list (so they share the
  dispatch transaction).
- `Transfer` — emits two `posting` events (debit source, credit destination)
  on a `transfer` aggregate.
- `AuthorizeHold` / `ReleaseHold` — emits `hold_placed` / `hold_released`
  against an `account_hold` aggregate; the balance projection nets these out.
- `ReverseTransaction` — emits a `posting_reversed` event referencing the
  original `posting.event_id`. The original event stays in the log forever
  (see Layer-2 override below).
- `EndOfDayBatch` — a batch action that emits one `batch_settled` aggregate
  event plus N `posting` events; one dispatch, one transaction, one audit
  record.

**Projections.** `AggregateProjectionSpec` for `account_current_state`
(balance, status, holds total). `TableProjectionSpec` for `transaction_ledger`
(one row per `posting`, inserted on `posting_recorded`, never removed).
`TableProjectionSpec`s for `available_balance_by_account`,
`daily_branch_totals`, and `ctr_candidates_today` (BSA $10k aggregation watch).

**Auth.** Roles: `Customer`, `Teller`, `BranchSupervisor`, `ATMAgent`,
`ClearinghouseBatch`, `Auditor`, `ComplianceOfficer`. Scope classes form a
containment hierarchy: `institution → branch → customer → account`, registered
with `ContainmentRef` projections so a `BranchSupervisor` assigned at
`branch=B1` can authorize actions scoped to any account contained at B1.
Permissions: `account.deposit`, `account.withdraw`, `transfer.execute`,
`transaction.reverse` (held only by `BranchSupervisor`+), `report.read`
(`Auditor`), `batch.settle` (`ClearinghouseBatch`).

## 2. Layer 1 guarantees that are load-bearing

Two Layer-1 facts are existential for this domain in a way they wouldn't be
for, say, a multiplayer game:

**Append-atomic-with-row-update.** A `Deposit` action emits a customer-credit
posting and a bank-cash-debit posting; the substrate appends both events *and*
updates the balance projection row inside one Postgres transaction. If the
projection ever diverged from the log — money credited to the row but the
posting event missing, or vice versa — the bank would have either invented
money out of thin air or lost a customer's deposit. For a game, projection
drift is a bug; here it is an insolvency event. The substrate's `appendInTxn`
contract is what makes the balance row legally equivalent to the log.

**Hash chain + provenance + ALCOA+.** The cryptographic chain is what makes
the log *court-admissible*. When an FFIEC examiner or a Reg E disputes-
resolution officer asks "show me account A's complete history and prove
nothing was retroactively altered," the substrate hands them a sequence and a
chain that any third party (the bank's external auditor, a federal examiner
running their own verifier) can recompute from the raw rows. Provenance
preserves the chain across the mobile-app → core-ledger → counterparty-bank
hops on outbound wires. ALCOA+'s "Attributable" + "Contemporaneous" mean every
posting carries `initiator=Principal(userId=teller-jones, activeRole=Teller)`
and a server-stamped timestamp — exactly what a court needs.

## 3. Layer 2 machinery — what to use, override, and extend

**Use as-shipped.** Role/permission/scope with containment (`institution →
branch → customer → account`), `AggregateProjectionSpec` for account state,
`TableProjectionSpec` for the immutable ledger, the dispatcher's
`Idempotency.required` semantics (ATM retries are constant; double-charging is
catastrophic), seeded `permission_granted` events for the role matrix.

**Override.** **No tombstones, anywhere.** The default
`AggregateProjectionSpec.tombstoneEventTypes` semantic ("event type X deletes
the row") is rejected outright. Closing an account emits `account_closed` that
the balance projection treats as a status change; reversing a transaction
emits `posting_reversed` that the ledger projection inserts as a *new
compensating row* referencing the original. The original posting row stays.
Nothing in the ledger view is ever deleted — this is Layer-2 reinterpretation
of the substrate's defaults, expressed by simply not naming any event types in
`tombstoneEventTypes` / `removeEventTypes`.

**Enforce at the action layer.** Double-entry balance is an `Action.validate`
and `Action.execute` invariant: every `Deposit`/`Withdraw`/`Transfer`
constructs an `ExecutionResult` whose `events` sum to zero across debit/credit
pairs, and `validate` rejects unbalanced inputs. The substrate has no concept
of "balanced pair" and shouldn't — this is domain logic. Funds-availability
rules (1-day hold on out-of-state checks) live in `Action.execute` for
`Deposit`, which emits both a `posting_recorded` and a `hold_placed` with a
`releasesAt` timestamp; a scheduled `ReleaseExpiredHolds` action sweeps daily.

**Build on Layer 1 via `Events()` mode.** Point-in-time account reconstruction
("balance as of 2024-09-30 23:59:59 EST" for a Reg E dispute or a tax
statement) is *not* served by the live `account_current_state` projection. The
auditor's tooling subscribes with `eventStore.subscribe<StoredEvent>(filter,
Events())` — or queries `backend.findAllEvents` with a `clientTimestamp`
bound — and folds postings client-side. This is the expected pattern from the
guide's "Two layers of trust" section.

**Substrate gap worth flagging.** The substrate's `Principal.userId` is
"trusted on faith from auth." For a bank, the `PrincipalAuthValidator` mounted
by the `reaction` server layer becomes a regulatorily-scrutinized trust input
— it must integrate with the bank's hardware-token / MFA / FIDO2 stack, not
the shipped `TrustingAuthValidator`. That's an *expected* deployment concern
(the substrate enumerates it as a trust boundary), but for banking the
validator itself becomes audit material on par with `StorageBackend`. Worth
calling out in any deployment-time architecture review.
