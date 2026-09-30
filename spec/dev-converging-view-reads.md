# EVS-DEV-converging-view-reads: Reads of a converging view

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-materializer, EVS-PRD-subscription

## Purpose

What a read of a materialized view returns while the instance's copy of the view is catching up with the log (EVS-DEV-view-convergence), and how the library's own decisions treat such a view. While a view converges, the library serves only rows it knows are settled, reports every other requested row as pending, and says, with every read, that the view is converging; it never presents a converging view as current.

Terms used below: a view is **current** for an instance in a transaction when the instance's copy of the view is current in that transaction, and **converging** otherwise (terms of EVS-DEV-view-convergence). A copy is current when no event past its watermark is one its definition folds, so an event outside the view's definition leaves it current. A row of an aggregate view is **settled** in a transaction when the view is current, or when the log holds no event past the copy's watermark that the copy's definition folds into that aggregate's row and no security finding past it that marks that aggregate; a table view has no settled row while it converges.

## Assertions

A. Every read of a view's rows that the library offers a consumer -- a read of all rows, a read of rows by key, and the initial replay of an `AggregateMode` subscription -- SHALL read whether the view is current and its rows in one storage transaction, and SHALL return that state with the rows.

B. A read of a view that is converging for the instance SHALL return no row that is not settled.

C. A read by key of a view that is converging for the instance SHALL report as pending, distinct from a row and from an absent row, each requested key whose row it does not report as settled.

D. Where every build registering the instance's copy of a view defines the view's interest predicate, and for a table view its row-key and row-data functions, as the instance does, every row a read reports as settled SHALL equal the row that a replay of the log, as of the read's transaction, produces for its key under the instance's registered definitions.

E. An `AggregateMode` subscription on a view that is converging for the instance SHALL deliver no row that is not settled, SHALL report as pending each aggregate it names whose row is not settled, and SHALL end its initial replay with a marker carrying the view's state.

F. <RETIRED> A live subscription redelivers newly settled rows on a period.

G. When a view becomes current for an instance, each of the instance's live `AggregateMode` subscriptions on it SHALL deliver the row of every aggregate it last reported pending, or every row it would snapshot when it reported the view converging without naming aggregates, before it reports the view current.

H. Every library operation that decides from a view's rows, the authorization policy's reads of the role-assignment and permission-grant views among them, SHALL, when the view is converging for the instance in the transaction in which it would read those rows, refuse with a typed, transient refusal that names the view, appending no event.

I. The library's permission bootstrap and permission-seed operations SHALL wait until every view they read is current for the instance before they read it, and SHALL throw a typed error naming each such view still converging and its copy's progress when a deadline the caller supplies passes first.

J. The library SHALL let a caller read, for each view the instance registers, whether the view is current or converging for the instance and its copy's progress.

K. <RETIRED> A caller awaits a set of views becoming current.

## Rationale

**Which layer?** The state a read reports is a Layer 2 claim: "current" and "settled" mean the rows are what a replay of the log produces under the library's default projection conventions and the registered definitions, as far as the copy's watermark shows. It is not a Layer 1 fact about the log; the watermark is storage state kept beside the views (EVS-PRD-destinations/L).

**Why "for an instance"?** Builds that share a database may define a view differently, and each reads its own copy. A canary that adds or promotes a view reads a copy that is converging while the serving revision reads its own copy. The serving revision's copy becomes converging only when another build stores an event that copy's definition folds -- an event of an entry type it folds, or a security finding -- and stays so until the serving instance's next catch-up folds it, a window of one short transaction.

**Why read the state and the rows in one transaction (assertion A)?** A state read in one transaction and rows read in another can straddle a commit that moves the log past the watermark, so rows could be reported under a "current" read before it. Reading both in one transaction makes the reported state the state of the rows returned.

**Why withhold unsettled rows rather than return them flagged (assertions B, C and E)?** An unsettled row lacks events the log holds, and a consumer handed a row with a flag it overlooks shows data that no replay produces. Withholding it, and naming the key as pending, makes the lag visible and the data trustworthy: what a read returns as a row is a replay's row. The rule forbids serving an unsettled row, not withholding a settled one, so a backend that cannot cheaply tell which aggregates lie past the watermark may report every requested key pending. The cost is visible: a screen backed by a converging view shows its rows as pending while the copy catches up, and a table view returns nothing until it is current. A read never writes, so a read does not fold a pending row itself.

**What does the equality rest on (assertion D)?** The fingerprint covers what the library can read of a definition. An interest predicate, and a table view's row-key and row-data functions, are code the library cannot digest, so two builds that differ only there share a copy, and the rows one folds are not those the other's replay produces. That is a consumer precondition stated in the assertion; closing it is on the roadmap (`spec/roadmap/projections.md`). A deployment that changes only such a function runs `rebuildView` once no build with the other function still serves the database.

**Why deliver pending rows before reporting current (assertion G)?** A subscription that received only the settled rows and then heard "current" would hold an incomplete set it believes complete; delivering what it lacks first keeps a subscriber's state a replay's whenever it is reported current. A subscriber sees a pending row once the view is current; a copy is converging only while an event it folds lies past its watermark, and its catch-up folds such events within one short transaction of their store.

**Why must the library's own decisions refuse (assertion H)?** An authorization decided from a role-assignment view that lacks events could deny what the log grants, and the dispatcher records a denial as an `authorization_denied` event -- an outcome no replay of the log reproduces, which breaks the closed-under-events guarantee for action outcomes. The refusal is transient and appends nothing: it is not a policy decision, and the same submission succeeds once the view is current. The permission views are table views, which have no settled row while they converge, so refusing only on unsettled rows would refuse just as often. The cost is symmetric between builds that share a database with different permission views: each build's copy becomes converging when the other stores a role-assignment or permission-grant event, or a security finding, and each refuses submissions until its own next catch-up folds it. A build whose permission view is a new copy refuses every submission until that copy has folded the whole log, so a release that changes a permission view is best rolled out by moving traffic after its copy is current, or as a stop-then-start.

**Why does the wait take a deadline (assertion I)?** The permission bootstrap and seed operations run while an application starts and wait for current views, since a seed decided on a converging view could append role assignments the log already holds. A copy stays converging while its catch-up keeps failing, or while it folds a long log. An unbounded wait would hang the start of the application with no sign of why; the deadline turns it into a typed error that names the views and how far they have come. Any other caller reads the state and progress of each view (assertion J) and decides for itself how long to wait.

## Changelog

- 2026-09-25 | e95c070e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Terms: a copy is current when no event past its watermark is one its definition folds, so another build's events outside the definition never make a view converging. Retire F (periodic redelivery of settled rows) and K (await with a deadline; callers read the state with J). Rationale: the cost of another build's appends is one short catch-up, symmetric between the builds. No code or test references these assertions
- 2026-09-25 | ca7ebe32 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Settled-row term: a security finding past the watermark that marks the aggregate unsettles its row. No code or test references the term
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Rewrite for view copies (all letters reassigned; no code or test references them): a view is current when the instance's copy is current, and a row is settled when no event past the copy's watermark folds into it. Retire the counts-for-instance rule (old A, now a term). Amend the equality (D): its precondition is that the builds sharing a copy define its functions alike. Merge old H and M into F: a live subscription delivers newly settled rows at least once a second. Refusal (H) no longer mentions conflicted rows
- 2026-09-25 | ce6623ff | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of I: a conflicted settled row refuses the same decisions until reconciled
- 2026-09-25 | ce6623ff | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend the settled-row term, A and H: no rounds, a row is settled when no counting gap covers it. Merge I and J into I: the refusal is decided in the transaction that would read the rows. Re-letter K-N to J-M (no code or test references them)
- 2026-09-25 | 316a834c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-N: a converging view serves only settled rows and reports its state with every read; the library's own decisions refuse or wait, with a deadline, while a view they read converges

*End* *Reads of a converging view* | **Hash**: e95c070e
