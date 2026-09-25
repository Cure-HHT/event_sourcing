# EVS-DEV-converging-view-reads: Reads of a converging view

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-materializer, EVS-PRD-subscription

## Purpose

What a read of a materialized view returns while the view is converging (EVS-DEV-view-convergence), and how the library's own decisions treat such a view. A view is **current** for an instance when no gap that counts for the instance is recorded on it, and **converging** otherwise. While a view converges, the library serves only the rows it knows are settled, reports every other requested row as pending, and says, with every read, that the view is converging; it never presents a converging view as current. A row of an aggregate view is **settled** for an instance when no gap of the view that counts for the instance covers the row's aggregate (terms of EVS-DEV-view-convergence); a table view has no settled row while it converges.

## Assertions

A. The library SHALL treat a view as converging for an instance while the view, or a pair of the view, carries a gap that counts for the instance, and as current for the instance otherwise.

B. Every read of a view's rows that the library offers a consumer -- a read of all rows, a read of rows by key, and the initial replay of an `AggregateMode` subscription -- SHALL read the view's convergence state and its rows in one storage transaction, and SHALL return that state with the rows.

C. A read of a view that is converging for the instance SHALL return no row that is not settled.

D. A read by key of a view that is converging for the instance SHALL report each requested key whose row is not settled as pending, distinct from a row and from an absent row.

E. Where every build that has stored an event in the database since the view's pairs were seeded either registers the view with the instance's interest or does not register it, and the instance's registered version of each entry type the view's pairs name is at least that pair's stored target, every row a read reports as settled SHALL equal the row that a replay of the log, as of the read's transaction, produces for its key under the instance's registered definitions.

F. An `AggregateMode` subscription on a view that is converging for the instance SHALL deliver no row that is not settled, SHALL report as pending each aggregate it names whose row is not settled, and SHALL end its initial replay with a marker carrying the view's convergence state.

G. When a view becomes current for an instance, each of the instance's live `AggregateMode` subscriptions on it SHALL deliver the row of every aggregate it last reported pending, or every row it would snapshot when it reported the view converging without naming aggregates, before it reports the view current.

H. The library SHALL deliver each row that a convergence transaction of the instance settles to the instance's live `AggregateMode` subscriptions on the view, after that transaction commits.

I. Every library operation that decides from a view's rows, the authorization policy's reads of the role-assignment and permission-grant views among them, SHALL, when the view is converging for the instance in the transaction in which it would read those rows, refuse with a typed, transient refusal that names the view, appending no event.

J. The library's permission bootstrap and permission-seed operations SHALL wait until every view they read is current for the instance before they read it, and SHALL throw a typed error naming each such view still converging and its latest convergence progress when a deadline the caller supplies passes first.

K. The library SHALL let a caller read, for each view the instance registers, whether the view is current or converging for the instance and the view's latest convergence progress.

L. The library SHALL let a caller await the moment a set of named views is current for the instance, and SHALL end that wait with a typed error naming each view still converging and its latest convergence progress when a deadline the caller supplies passes first.

M. An instance with a live `AggregateMode` subscription on a view that is converging for it SHALL read the view's convergence state at least once a second until the view is current for it.

## Rationale

**Why "for an instance" (assertion A)?** Builds that share a database may register different minors of an entry type. While a newer build's promotion runs, or after an older build's fold lowered a target, the rows are a mix of the older and the newer minor. The newer build cannot serve them as its replay, so for it the view converges. The older build reads rows of its major at any minor -- a minor step only adds fields with defaults (EVS-DEV-version-compatibility) -- so a promotion gap above its minor does not count for it, and a canary that promotes a view does not make the serving revision refuse or withhold anything. A catch-up gap counts for every build that registers the view.

**Which layer?** The state a read reports is a Layer 2 claim: "current" and "settled" mean the rows are what a replay of the log produces under the library's default projection conventions and the registered definitions, as far as the library's convergence records show. It is not a Layer 1 fact about the log; the records it rests on are storage state kept beside the views (EVS-PRD-destinations/L).

**Why read the state and the rows in one transaction (assertion B)?** A state read in one transaction and rows read in another can straddle the commit of a convergence transaction or of a recording: rows read after a new gap was recorded could be reported under a "current" read before it. Reading both in one transaction makes the reported state the state of the rows returned.

**Why withhold unsettled rows rather than return them flagged (assertions C, D and F)?** An unsettled row may be in a shape the reading build cannot map -- a row of an older major during a stop-then-start promotion, or a table-view row a refold has not reached -- and a consumer handed a row with a flag it overlooks shows data that no replay produces. Withholding it, and naming the key as pending, makes the gap visible and the data trustworthy: what a read returns as a row is a replay's row. The cost is visible: a screen backed by a converging view shows its rows as pending during a deploy's convergence window, a view with a whole-log gap returns only the rows its scan has re-derived, and a table view returns nothing until its refold reaches the head. A read never writes, so a read does not re-derive a pending row itself.

**What does the equality rest on (assertion E)?** Two conditions, both stated in the assertion. An instance of an older minor reads rows a newer build re-derived at the newer minor, which carry defaulted fields its own replay would not produce; that is the minor-compatibility rule, not a convergence failure. And the library records a difference only for the entry types a view's interest names and for a whole-view row: two builds whose interests for one view differ outside the named entry types -- in aggregate types, in whether system events are included, or in a predicate -- record nothing for each other, so the rows the narrower build folds lack events the wider interest matches, and the library cannot see it. That is a Layer 2 limitation recorded on the roadmap (`spec/roadmap/projections.md`); a deployment that changes a view's interest outside its named entry types runs `rebuildView` once no build with the other interest still serves the database.

**Why re-deliver pending rows before reporting current (assertion G)?** A subscription that received only the settled rows and then heard "current" would hold an incomplete set it believes complete. Delivering what it lacks first keeps a subscriber's state a replay's whenever it is reported current. The rows this instance's convergence settles reach its subscribers as they settle (assertion H); convergence another instance runs is observed through the persisted gaps, which an instance with such a subscription reads at least once a second (assertion M), a fixed library constant that bounds how long a subscriber waits past convergence for one cheap read.

**Why must the library's own decisions refuse (assertion I)?** An authorization decided from a role-assignment view that lacks events could deny what the log grants, and the dispatcher records a denial as an `authorization_denied` event -- an outcome no replay of the log reproduces, which breaks the closed-under-events guarantee for action outcomes. The refusal is transient and appends nothing: it is not a policy decision, and the same submission succeeds once the view is current. The permission views are table views, which have no settled row while they converge, so refusing only on unsettled rows would refuse just as often. The cost falls on a build for which a gap counts: a build that adds an entry type to a permission view's interest, or registers a newer minor of a permission entry type, beside a serving build that stores those events refuses the submissions it serves each time such a store records a gap, until convergence catches up, for the whole of that overlap, while the serving build is unaffected. A release that changes a permission view's interest or minor is therefore best rolled out by moving traffic after convergence has caught up, or as a stop-then-start. A settled row that is conflicted refuses the same decisions, permanently until a reconciliation closes it (`EVS-DEV-branch-conflicts`), so the two refusals together keep the library from deciding on a row that holds no single state.

**Why do the waits take a deadline (assertions J and L)?** The permission bootstrap and seed operations run while an application starts and wait for current views, since a seed decided on a converging view could append role assignments the log already holds. A view may stay converging for as long as another build keeps recording gaps on it, or while the only page able to converge it is hidden (EVS-DEV-view-convergence-scheduling). An unbounded wait would hang the start of the application with no sign of why; the deadline turns it into a typed error that names the views and how far they have come.

## Changelog

- 2026-09-25 | ce6623ff | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of I: a conflicted settled row refuses the same decisions until reconciled
- 2026-09-25 | ce6623ff | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend the settled-row term, A and H: no rounds, a row is settled when no counting gap covers it. Merge I and J into I: the refusal is decided in the transaction that would read the rows. Re-letter K-N to J-M (no code or test references them)
- 2026-09-25 | 316a834c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-N: a converging view serves only settled rows and reports its state with every read; the library's own decisions refuse or wait, with a deadline, while a view they read converges

*End* *Reads of a converging view* | **Hash**: ce6623ff
