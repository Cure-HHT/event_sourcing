# EVS-DEV-destination-retry-budget: Destination retry budget and attempt outcomes

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

This requirement fixes when a queue item's retry budget is spent, bounded both by a number of attempts and by time, and how the drainer treats a delivery outcome that states the delivery implementation did not attempt delivery.

## Assertions

A. The drainer SHALL treat an item's retry budget as spent when the attempts recorded on it reach the budget's attempt bound, or when the sum, over each two consecutive recorded attempts, of the time between them, capped at the longest delay the retry curve in effect allows after the earlier of the two plus the delivery cycle's cadence, reaches the budget's time bound.

B. The fill SHALL record the time of each transform failure in the transform failure record, and SHALL treat the record's retry budget as spent when the failures it records reach the budget's attempt bound, or when the sum, over each two consecutive recorded failures, of the time between them, capped at the longest delay the retry curve in effect allows after the earlier of the two plus the delivery cycle's cadence, reaches the budget's time bound.

C. On a send outcome stating that the delivery implementation did not attempt delivery, the drainer SHALL record no attempt and leave the head pending with its recorded attempts unchanged.

D. After a send outcome stating that the delivery implementation did not attempt delivery, the drainer SHALL send nothing further on that destination in the same pass.

## Rationale

**Why sum the gaps between recorded attempts, each capped at the retry curve's delay plus the cadence (assertions A and B)?** The budget bounds how long retryable failures hold an item at the head while the destination retries it. Between two attempts the drainer waits at most the delay its retry curve sets plus the cadence at which passes run, so a gap up to that cap is time the item really sat at the head, and the budget measures the stall a consumer sees whatever the ratio of curve to cadence. Any longer gap is time in which the destination was not retrying: its delivery implementation declined to send (assertion C), the device slept or was offline, or no drainer ran. Capping each gap counts none of that time beyond the wait the drainer would have made anyway, so a pause at the receiver, however long, spends at most one capped gap, and so does a night with the device switched off. The sum is computed from the item's own recorded attempt times and the policy in effect, so the status derivation at the start of a pass reaches the same decision the drainer reached when it recorded the attempt, and an item recorded alone before a restart is wedged before any send, as for the attempt bound. The longest delay the curve allows includes its jitter, so a jittered wait is counted in full. The attempt times come from the delivery cycle's clock, the curve from its policy and the cadence from the cycle's configuration, all of which the application supplies and the library trusts; the times stay on the queue item beside the attempts, where an operator reads them. A transform failure record keeps the time of each failure and measures its bound the same way, so a failing transform and a failing send spend one budget alike.

**Why record nothing for an outcome that attempted nothing (assertions C and D)?** A delivery implementation that knows it will not deliver now (its receiver asked it to wait, its transport is paused) did not send: recording an attempt would spend an attempt and start the time bound on a send that did not happen. The head stays exactly as it was, and the time the pause lasts spends nothing either, since the gap it leaves between two recorded attempts is capped at the retry curve's delay plus the cadence (assertion A). Sending nothing more on the destination in that pass keeps the drainer from asking again at once, in a loop, a delivery implementation that has just said it will not deliver; the next pass tries again at the cadence. On a delivery channel the number and link the pre-send fence assigned are reassigned at the next fence, so an outcome that attempted nothing leaves no gap in the channel's numbering.

## Changelog

- 2026-09-26 | 6c2b3476 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | A and B: each counted gap is capped at the retry curve's delay plus the delivery cycle's cadence, so the time bound measures the time an item really sat at the head. No code or test references A or B
- 2026-09-25 | d73ab499 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 793a7b7a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-D: the retry budget is spent by attempts or by the time between consecutive recorded attempts, each gap capped at the retry curve's delay so that a declined pause, a sleeping device or a stopped drainer spends nothing, for a send and for a failing transform alike (the transform failure record keeps each failure's time); an outcome that attempted nothing records no attempt and ends the destination's pass

*End* *Destination retry budget and attempt outcomes* | **Hash**: 6c2b3476
