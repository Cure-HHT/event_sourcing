# Roadmap: security findings

Deferred work for the review of integrity anomalies. See `README.md` for
how roadmap entries are read.

## Clearing a finding

**Baseline (what exists).** Every integrity anomaly the library detects
is recorded as one reserved security finding with a stable
`finding_id`, its kind, its evidence and its detector
(`EVS-DEV-security-findings`). The library's default views mark every
aggregate a finding names as having an outstanding finding, and the
mark stays for as long as the finding is held.

**Remaining.** A reserved "security finding cleared" event, appended on
a person's decision through a public operation, naming the
`finding_id` it clears and recording the reason. The default views
drop the mark of an aggregate once every finding naming it is cleared.
It is a new reserved entry type, in the reserved entry-type namespace
(`EVS-DEV-destination-drain/L`), so it ships as a data-format minor
step: a build that does not know it stores it and keeps the mark, and
no release's public append accepts it from application code.
Open points: who may clear (a permission of the role/permission/scope
model), whether a receiver may clear a sender's finding or only one it
detected, and whether a clearing event travels on every channel as a
finding does.

## Reviewing findings

**Baseline (what exists).** Findings are ordinary events: a
subscription to the reserved finding entry type reads them, and the
default views carry the finding identities on each marked row.

**Remaining.** A default view of uncleared findings, keyed by
`finding_id`, with the detector, kind, evidence and the aggregates it
names, and a review workflow on top of it: an application route that
shows a finding's evidence beside the suspect data, and records the
reviewer's decision as the clearing event.

## Timestamp anchors in the chain walk

**Baseline (what exists).** An application can record a third-party
timestamp over a chain head hash as an application event. The chain
verification operation walks the storage chain and every origin chain
it can resolve and records every finding of its verdict.

**Remaining.** Let the walk check each anchored head: that the anchored
hash is the stored hash of the event at the anchored position, and
that no event before it changed since, recording a finding otherwise.
Needs a declared shape for the anchor event so the walk can recognise
it without application code.

## Walking the chain in resumable chunks

**Baseline (what exists).** The chain verification operation holds
nothing an append waits for: on Postgres it reads one read-only
snapshot, and on Sembast it reads outside any transaction. It fixes its
upper bound when it starts and walks to it in one pass, so a full walk
of a large log takes as long as reading the whole log.

**Remaining.** Walk in bounded chunks that record their progress, so a
walk interrupted by a restart resumes where it stopped and a long
Postgres snapshot is never held, with the findings of each chunk
recorded as it completes.
