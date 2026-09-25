# Delivery Continuity

## Overview

A destination's queue marks an item sent when the receiver acknowledges it, and the drainer does not send it again. Either end can later move back in time: a receiver restored from a backup, restored to a point in time, or failed over to a replica that had not received the latest commits; a sending device restored from a backup of its database. Either move silently separates what the sender believes it delivered from what the receiver holds. This file specifies how the library detects such a move from the delivery channel itself and records it truthfully in the log.

The library realigns a channel automatically on the common, safe paths: a lost acknowledgement, a receiver behind the sender, and a sender behind the receiver, the last two combined when both moved. Every other inconsistency is an integrity anomaly: the side that detects it records a security finding (`EVS-DEV-security-findings`), stores what it received as it received it, and delivery continues. A channel no automatic path realigns re-anchors: the sender sends a break delivery that follows the receiver's record and states the break, and the receiver accepts it and records its own finding. No channel stops for an integrity reason. How an application interprets the recorded facts is left to it.

### Terms

- **Channel.** One registration of a natively serializing destination on one sending database, identified by the sending database's identity (`EVS-DEV-event-store-open/F`), the destination identifier and the registration identifier (the event identifier of the registration event). A destination deleted and registered again starts a new channel; the old one ends.
- **Delivery.** One batch envelope the drainer sends on a channel. It carries at least one event.
- **Delivery number.** The position of an accepted delivery in its channel, counted from 1 with no gaps. The channel numbers deliveries, not events, so a destination's filter leaves no gap in the numbering.
- **Link.** The delivery hash of the delivery a delivery follows; null for delivery 1.
- **Delivery hash.** The SHA-256 of the canonical form of the channel, the delivery number, the link, the hashes of the events the delivery carries, for each of them whether one of its causal parents was withheld from the channel, and the break the delivery states, if any.
- **Receiver record.** The number and hash of the last delivery the receiver accepted on a channel (number 0 and a null hash before the first). The receiver derives it from its own log and returns it with every acknowledgement and every refusal.
- **Sender channel record.** The receiver record as the sender last established it; the next delivery is numbered from it.
- **Retained delivery.** At a delivery number, the delivery the sender's queue last marked sent at that number on the channel, with its number and hash, from an item enqueued no earlier than the break of the channel's latest re-anchor. A re-anchor therefore ends the retention of every delivery sent before it, and at each number at most one delivery is retained. A delivery the sender holds only because a recovery brought its events back is not retained.
- **Resume item.** A queue item a resume or a re-anchor enqueued: a resend of a retained delivery, a skip event, a break, or an earlier resume event a re-anchor sends again.
- **Check-in.** A pull that asks the receiver for its record of the channel, and for the channels it holds for the sending database under the destination's identifier, before anything is sent.
- **Resume.** An automatic realignment of a channel to the receiver record, recorded in one **resume event**. A resume after the receiver fell behind resends the retained deliveries the receiver lacks. A resume after the sender fell behind recovers the deliveries the sender lacks from the receiver; its resume event is the **skip event**.
- **Re-anchor.** The realignment of a channel that no resume explains: a security finding, and a **break delivery** that follows the receiver record and states the sender channel record it replaces.
- **Succession.** A database that has authored no application events restores a reset sender's deliveries from a receiver and declares itself that sender's successor. A database's **succession lineage** is the identities it succeeded, transitively, as the succession events a log holds state them.

### How a receiver record is read

```text
receiver record R, sender channel record S

  R == S ...................................... in step: send
  R names the delivery in flight .............. lost acknowledgement:
                                                 mark it sent, S := R
  R.number < S.number, every number in
    (R.number, S.number] retained, and the
    retained R.number + 1 links to R.hash ..... receiver behind:
                                                 resend them as sent
  R.number > S.number and the receiver's
    delivery at S.number has S's hash
    (null at number 0) ........................ sender behind:
                                                 recover (S, R], skip
                                                 event first
  anything else, or R from another
    receiver database ......................... finding, re-anchor:
                                                 break delivery after R,
                                                 refill from the start
```

### Reading order

`EVS-PRD-delivery-channel` states what the library guarantees. `EVS-DEV-delivery-channel` fixes the sender's side of a channel: the delivery hash, the numbering, the envelope and how responses are read. `EVS-DEV-delivery-receiver` fixes the receiver's check, its audit, its record, its findings and its endpoint. `EVS-DEV-delivery-resume` fixes the check-in, the reading of a receiver record and the two resumes; `EVS-DEV-channel-findings` fixes the sender's findings and the re-anchor; `EVS-DEV-resume-event` fixes what the resume events record. `EVS-DEV-sender-succession` fixes succession. The drainer, halt and recovery mechanics these build on are `EVS-DEV-destination-drain` and `EVS-DEV-destination-drain-lock`. The finding event is specified in `spec/dev-security-findings.md`. The verification of origin chains across a recorded fork, and the causal parents whose withheld-parent record a delivery carries, are specified in `spec/causal-history.md`; the branch conflicts a skip event records are specified in `spec/branch-conflicts.md`.

## EVS-PRD-delivery-channel: Delivery channel continuity

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations, EVS-PRD-ingest, EVS-PRD-library-charter

### Purpose

A delivery channel is the path from one sending database to one receiver through one registration of a destination that uses the library's native batch format. The library numbers the deliveries on each channel and links each to the one before, the receiver accepts only the delivery that follows the last one it accepted, and the receiver's record of the channel, derived from its own log, returns to the sender with every acknowledgement. When one end has moved back in time on a path the library can realign safely, it realigns the channel and records the realignment in the log. In every other case it records a security finding and re-anchors the channel, so delivery always continues. A database that has authored nothing and takes over a reset sender's data declares the succession in the log. A destination whose receiver is not this library does not use the native batch format and is outside this requirement.

### Assertions

A. The library SHALL treat each registration of a destination that uses the library's native batch format, on each sending database, as one delivery channel, identified by the sending database's identity, the destination identifier and the registration.

B. Every delivery the library sends on a channel SHALL carry a delivery number one greater than the number of the last delivery the sender knows the receiver accepted on that channel, the delivery hash of that delivery as its link, and its own delivery hash, which covers the channel, the number, the link, the hash of every event the delivery carries, the record of which of those events has a causal parent withheld from the channel, and the break the delivery states, if any.

C. A receiver SHALL accept a delivery on a channel only when its number is one greater than, and its link equals the hash of, the last delivery it accepted on that channel, and SHALL refuse every other delivery, other than a re-presentation of that last delivery, with a refusal naming its record of the channel.

D. The receiver's record of each channel (the number and the hash of the last delivery it accepted on it) SHALL be derivable from the receiver's event log alone.

E. Every acknowledgement of a delivery and every refusal of one SHALL carry the receiver's record of the channel.

F. The sender SHALL mark a queued item delivered only on a receiver record, returned with an acknowledgement, a refusal or a check-in, that names the delivery the sender made of that item.

G. After the sending process opens the database, and whenever the drain lock passes to it from another process, the library SHALL send nothing on a channel until it has obtained the receiver's record of that channel and committed what that record calls for.

H. When the receiver's record is behind the sender's record, the sender retains a delivery for every number above the receiver's record up to its own, and the first of those links to the receiver's record, the library SHALL send again, on the channel, every retained delivery after the receiver's record, each with the events, number, link and hash it was first sent with, and SHALL record the resume as one event in the log.

I. When the sender's record is a delivery the receiver holds, or is number 0, and the receiver's record is ahead of it, the library SHALL recover from the receiver, before it sends anything further on the channel, every event of the deliveries after the sender's record that the sender does not hold, keeping each event's identity, sealed hash and predecessor link, and SHALL append none of them as a new event.

J. The library SHALL commit, in the transaction that stores the events a recovery brings back and before storing any of them, exactly one skip event that records the channel, the receiver record the channel resumes after, the deliveries recovered, the origin positions of the events recovered, the origin positions of the abandoned range the recovery does not bring back, the highest event of the sender's origin chain the served deliveries and the sender's own delivery record prove shared as the branch point, the abandoned head of that chain, and the aggregates in conflict.

K. The library SHALL send a skip event as the first delivery on the channel it resumes.

L. When both ends of a channel have moved back in time, the library SHALL resume the channel after the later of the two records, having recovered to the sender or sent again to the receiver what that end lacks, and SHALL record nothing for deliveries that neither end holds.

M. <RETIRED> A receiver record that no automatic path explains re-anchors the channel with a security finding (assertion X).

N. <RETIRED> A recovery that brings back a resume event of the recovering database's own identity records a security finding and continues (EVS-DEV-channel-findings).

O. <RETIRED> A resume called for again proceeds as any resume; each is an event in the sender's log.

P. The library SHALL provide a receiver endpoint that accepts deliveries and serves, to a caller the deployment authenticates for the channel's sender, the receiver's record of a channel, the channels it holds for that sender and for the identities that sender succeeded, and the deliveries of a range of a channel, reconstructed from the receiver's event log.

Q. A database that has authored no event of an application entry type and restores a predecessor sender's deliveries from a receiver SHALL record, in the transaction that stores them, a reserved succession event naming the predecessor's identity and, for each channel it restores, the last delivery it restores.

R. The library SHALL refuse, before storing anything, a restore of a predecessor's deliveries into a database that has authored an event of an application entry type.

S. After a receiver accepts a succession, it SHALL record a security finding for each later delivery it accepts from the predecessor sender.

T. The library's default interpretation of authorship SHALL TREAT the events a successor authors after its succession event as continuing the authorship of the predecessor that event names.

U. The library SHALL deliver each skip event, each succession event and each security finding on every channel of the sending database, whatever that destination's filter.

V. The library SHALL deliver an event a recovery brought back on every other channel of the sending database whose filter selects it, and SHALL NOT deliver it on the channel it was recovered from other than in the refill of a re-anchor of that channel.

W. <RETIRED> A second live copy of a sending database produces security findings where its events meet the other copy's, and delivery continues (assertions X and Z).

X. When a receiver record equals neither the sender's record nor the delivery in flight and calls for no resume, or comes from another receiver database than the channel's, the library SHALL record a security finding naming both records and SHALL send, as the channel's next delivery, a break delivery that follows the receiver's record and states the sender's record it replaces.

Y. A receiver SHALL accept a break delivery that follows its record of the channel, and SHALL record a security finding naming the break in the transaction that accepts it.

Z. The library SHALL NOT stop a delivery channel, nor refuse other than as a transient failure a delivery that follows the receiver's record, because of an integrity anomaly, and SHALL record each integrity anomaly it detects on a channel as a security finding.

### Rationale

**What the channel is (assertion A).** A delivery is a statement between two databases. The sender is the database identity, minted inside the database and checked at every open (`EVS-DEV-event-store-open/F`), so a backup restore brings back the same sender gone back in time, while a reset or a reinstall is a new sender. A destination deleted and registered again starts a new channel at delivery 1 (`EVS-PRD-destinations/O`). A destination that serializes through an application transform has no receiver of this library.

**Why number and link deliveries, and derive the record from the log (assertions B to F)?** A filter leaves gaps in the events by design, so the channel numbers what it sends, and the link binds each delivery to its predecessor's content. The Layer 1 claim, under the storage precondition (`EVS-PRD-destinations/L`), is that every delivery a receiver accepted follows the one before it by number and link; a break follows the receiver's record like any delivery, so the claim has no exception. Derived from the log, the receiver's record goes back exactly as far as the events do after a restore; returned on every response, it tells the sender of a receiver's move at its next delivery. Marking an item delivered only on a record naming its delivery makes the acknowledgement evidence rather than a status code.

**Why check in (assertion G)?** A restored sender cannot know it at boot, and an idle one would not otherwise talk to its receiver. A check-in at each process start, and whenever another process held the drain lock since, costs one request per channel. A database restored under a running process is caught at its next delivery, whose refusal carries the receiver's record.

**The automatic paths (assertions H to L).** A receiver behind gets the retained deliveries again exactly as sent, nothing filtered again; the first links to the receiver's record, which proves the common point. A sender behind recovers its lost events as the events they are, since appending them again would forge a second authorship. The skip event makes the fork explicit; it is committed with the recovered events and stored before them, so no holder sees a recovered event unexplained, and it is sent first, so the receiver learns of the fork before it meets the continuing branch. When both ends moved back, each gets what it lacks, and deliveries neither end holds are unknown to both, so their numbers are used again.

**Why record and re-anchor rather than stop (assertions X to Z)?** Anything outside the automatic paths (a clone, tampering, a second restore, a receiver restored to a point the sender cannot resend from, a different receiver database) needs a judgement the library has no basis for. A stop would hold the sender's records until a person acts, while a device's storage can be lost meanwhile, so the library records the facts and keeps delivering. The break states, as the next delivery, where the two ends' histories diverged, and each end records its own finding. A delivery whose delivery hash does not recompute is refused as transient, with a finding, so the sender sends it again. Such a delivery never verified at the receiver, so its refusal is a transport failure, not an integrity stop: when every resend fails the same way, the retry budget runs out and the queue head wedges with the ordinary budget-exhausted cause, and the operator's recovery of the wedged head is the exit (`EVS-PRD-destinations`). An event the receiver cannot store as an event is kept in a finding and the rest of the delivery accepted. The only other refusals are a caller the deployment does not authenticate for the sender, and permanent failures of the application's validation or the data format, which wedge the queue head with an operator or upgrade exit (`EVS-PRD-destinations`).

**One live copy per database identity.** A restore replaces the database it restores. A second live copy (a cloned file, or a restored backup run beside the original) is detected, not prevented: both copies keep delivering, the automatic paths realign the channel as their deliveries meet, and the anomalies (a skip event of its own identity a copy never appended, forks and reused positions no skip covers) are findings where they are met. A person reads them and decides which copy lives. Until then the copies can realign the channel back and forth, each delivery of one making the other a sender behind that recovers it, with a skip and a finding each time; the library accepts this, since it is bounded by the copies' own activity and every step of it is recorded.

**Why the library's own receiver endpoint (assertion P)?** A recovery can use what the receiver serves only as far as it can check it, so the endpoint serves every delivery reconstructed from the receiver's log. The deployment's authentication maps a caller to the sender identities it may deliver for and read.

**Why succession only into a database that has authored nothing (assertions Q to T)?** A reset device that restores its predecessor's data would otherwise read as a second editor of it; the succession event states the continuity. A database that has authored application events has a history of its own that may overlap, so the restore is refused: a precondition of the operation, not an integrity check. A later delivery from the predecessor is a second live source, accepted and recorded. Continuity of authorship is a Layer 2 convention over the Layer 1 succession event.

**Why every channel (assertions U and V)?** A restore affects the sender's whole origin chain and every anomaly it found concerns what its receivers hold, so skip, succession and finding events reach every receiver. A recovered event is this database's own, which its other receivers may never have received; it goes back on the channel it came from only when that channel re-anchors, since its receiver may then have lost it.

**Trust.** The receiver's record and every served delivery are claims by an authenticated endpoint. The sender's checks of a served delivery prove consistency with its own delivery record, not authorship: the hashes are unkeyed and nothing is signed. A failed check is a finding and the data is stored as served. The receiver is trusted not to fabricate events under the sender's or a predecessor's identity, to serve every delivery it accepted, and not to claim a record ahead of its log; every realignment a record causes is an event in the sender's log, so a wrong claim is attributable. The destination transport, extended to records and served deliveries, and the deployment's authentication binding a caller to sender identities, are the trusted inputs this widens.

### Changelog

- 2026-09-25 | 0bbd7c97 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: a persistent delivery_hash_mismatch is a transport failure that ends in the budget-exhausted wedge with the operator's recovery as its exit, not an integrity stop; two live copies may realign a channel back and forth, accepted as bounded by their activity. No assertion changes
- 2026-09-25 | 0bbd7c97 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | V: a recovered event is delivered again on the channel it came from in the refill of a re-anchor of that channel. Z: a delivery that follows the receiver's record may be refused as a transient failure (its delivery hash does not recompute), never permanently, for an integrity anomaly. No code or test references V or Z
- 2026-09-25 | 71908718 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 4b7d5f53 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Security-finding model: retire M, N, O and W (no stop, no repeated-resume rule, no second-copy stop); add X (a receiver record no automatic path explains records a finding and re-anchors the channel with a break delivery), Y (the receiver accepts a break and records a finding) and Z (no channel stops and no delivery that follows the record is refused for an integrity reason). B: the delivery hash covers the break. J: the skip event records the abandoned positions the recovery does not bring back. S: a later delivery from a superseded predecessor is accepted and recorded as a finding. U: security findings reach every channel. No code or test references any of these letters
- 2026-09-25 | 393ebe07 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 0d39053a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | I: a sender whose record is number 0 recovers from a receiver ahead of it. J: the skip records the abandoned positions at which the sender holds no event of its own origin chain, and as its branch point the highest event proven shared by the served deliveries and the sender's own delivery record
- 2026-09-25 | a301a896 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of M to O and W: a second live copy's events are refused as an unrecorded fork or a reused origin position; a stop on a channel of the sender's own that its log does not know ends only through a person's decision outside the automatic paths
- 2026-09-25 | a301a896 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | d606d9cc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | H: a receiver behind resumes when every delivery above its record is retained and the first links to it; O: counts from the later of the previous resume and a person's clearing of a stop, and a resume that would discard a pending skip stops; Rationale shortened to the reason for each rule, and a stop on a fork in the sender's own chain ends only through a person's decision
- 2026-09-25 | 68b4e4f6 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Keep each recovered event's sealed hash (I); the skip records the abandoned positions at which the sender holds no recovered event (J); remove the refusal record; deliver skip and succession events, not every resume event, on every channel (U); add W: a second live copy of a sending database is detected and stops the channel, and the Rationale states the one-live-copy assumption; state the limit of the check-in for a restore under a running process; re-letter Q to V
- 2026-09-25 | 64781bce | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Revise to automatic resumes for a lost acknowledgement, a receiver behind (resend retained deliveries exactly as sent) and a sender behind (recovery with one skip event committed with the recovered events), with every other case stopping the channel with both records; remove lost and closed channels, the shared-channel report and closure, and the refill under the current configuration; add the stops on a recovered own resume event and on a repeated resume, recovered events delivered on the other channels, and succession only into a database that has authored no application events; check in at process start and when the drain lock changes hands; renumber from I onward
- 2026-09-25 | d37d0854 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-X: delivery channels, gap-free numbering and linking with the withheld-parent record under the delivery hash, the receiver's log-derived record on every acknowledgement and refusal, the check-in after each drain-lock acquisition with the receiver's list of the sender's channels, resumes after receiver and sender regression with the rewind and skip events, recovery of a sender's own events including those on lost channels, double regression, the repeated-record and shared-channel wedges, the receiver endpoint, coalesced refusal records and the default delivery-channels view, sender succession, skip and succession events on every channel, and the closing of lost and shared channels

*End* *Delivery channel continuity* | **Hash**: 0bbd7c97

## EVS-DEV-delivery-channel: Delivery channel sender mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the sender's side of a delivery channel: which destinations are channels, how the delivery hash is computed, the sender channel record, when the drainer assigns a delivery's number and link, the batch envelope, and how the drainer reads the receiver's responses.

### Assertions

A. The library SHALL apply the channel mechanics to every destination that serializes natively and to no other destination.

B. The library SHALL refuse, before anything is written, to register a destination that serializes natively and does not implement the channel pull operation.

C. The library SHALL compute a delivery's hash as the SHA-256, in lowercase hexadecimal, of the canonical JSON of an object with exactly the keys `channel` (an object with exactly `sender_database_id`, `destination_id` and `registration_id`), `delivery_number`, `previous_delivery_hash` (null for delivery 1), `event_hashes` (the `event_hash` of each event the delivery carries, in the order it carries them), `parent_withheld` (for each event the delivery carries, in the same order, the boolean the delivery records for whether one of its parents was withheld from the channel) and `break` (null, or the sender channel record the delivery's break replaces, as an object with exactly `delivery_number` and `delivery_hash`).

D. The library SHALL keep, for each channel, a sender channel record holding the number and hash of the receiver record the sender last established, the receiver database identity the receiver's first response named, the drain epoch of the channel's last completed check-in, and the queue position of the break item and the fill position rewound from of the channel's latest re-anchor, written when the registration is written with number 0, a null hash, no receiver identity, no check-in epoch and no re-anchor.

E. The library SHALL change a sender channel record only in a transaction that commits, on that channel, a send outcome, a check-in decision, a resume or a re-anchor.

F. The fill SHALL record on each queue item, when it enqueues it, the withheld-parent record of the events it carries.

G. The drainer SHALL assign a delivery's number (the sender channel record's number plus one) and link (the record's hash) in the pre-send fence transaction, from the sender channel record read there.

H. The drainer SHALL start a send only when the sender channel record its pre-send fence reads equals the one the payload was built from.

I. The drainer SHALL write a delivery's number and delivery hash in the send fence record its pre-send fence transaction writes and on the attempt the send produces.

J. The change that marks a queue item sent SHALL record the delivery number and delivery hash it was acknowledged under.

K. A native batch envelope SHALL carry exactly the keys `batch_format_version` (`"3"`), `batch_id`, `sender_hop`, `sender_identifier`, `sender_software_version`, `sent_at`, `channel` (an object with exactly `sender_database_id`, `destination_id` and `registration_id`), `delivery_number`, `previous_delivery_hash`, `delivery_hash`, `events` (at least one), `parent_withheld` (one boolean per event, in the order of `events`) and `break` (as the delivery hash covers it).

L. The library SHALL provide the decoder that maps a receiver's acknowledgement or refusal body to the send outcome it states.

M. For a destination that serializes natively, the drainer SHALL mark the head sent only on a receiver record, returned with any response to a delivery or with a check-in, whose delivery number and delivery hash are those of the delivery it sent.

N. The drainer SHALL handle an accepting outcome carrying another record, and an `out_of_sequence` refusal, as a receiver record returned for that delivery, never as a permanent failure.

O. <RETIRED> A receiver refuses a delivery only out of sequence, for its caller's authentication, or as a permanent failure the drain wedges on (EVS-DEV-delivery-receiver); every integrity anomaly is a security finding.

P. <RETIRED> A receiver accepts an unrecorded fork and records a security finding (EVS-DEV-chain-verification).

Q. The drainer SHALL wedge the head with cause `acknowledgement_invalid`, in the transaction that records the attempt, on an accepting outcome that carries no record.

R. The library SHALL provide the decoder that maps a pull response to one of: served, a transient failure or a permanent failure, and a destination's pull operation SHALL report through that decoder's outcomes.

S. The library's decoder of a receiver's acknowledgement or refusal body SHALL map a `delivery_hash_mismatch` refusal to a transient failure.

### Rationale

**Why natively serializing destinations only (assertions A and B)?** Only a receiver running this library derives a record from its log; a natively serializing destination that cannot pull could never check in, so it is refused at registration.

**Why this hash, record and envelope (assertions C to E and K)?** The receiver recomputes the hash from the envelope, and a verifier from the receiver's accepted-delivery audit, which keeps every hashed field; the break is hashed so its statement travels under the chain. The sender channel record holds what the drainer decides by and changes only with the outcome it reflects. The format step is part of the data-format major step (`EVS-DEV-version-compatibility/C`).

**Why number at the fence (assertions F to J)?** A number fixed at enqueue would be consumed by every item a resume retires unsent. Assigned at the pre-send fence, it makes every retry carry the same number, link and hash, and a resend after the same record reproduce the retained delivery exactly. The send fence record names the delivery in flight, which is how a lost acknowledgement is recognised.

**What the responses mean (assertions L to N and Q to S)?** A record naming a delivery is the only evidence the receiver accepted it, so a transport that returns success and drops the body wedges the head. An out-of-sequence refusal states where the channel stands (`EVS-DEV-delivery-resume`). A `delivery_hash_mismatch` refusal is transient, so the same delivery is sent again: the delivery never verified at the receiver, which makes it a transport failure, and one that persists ends in the budget-exhausted wedge with the operator's recovery of the head as its exit, not in an integrity stop. A `rejected` refusal is a permanent failure the drain wedges on, with an operator or upgrade exit (`EVS-DEV-destination-drain/J`): an undecodable batch, the application's validation, or an unsupported data-format major. No integrity anomaly produces a refusal. A pull that fails permanently is told apart from one worth repeating.

### Changelog

- 2026-09-25 | f5ac34be | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of S: a persistent delivery_hash_mismatch ends in the budget-exhausted wedge, a transport failure with an operator exit, not an integrity stop. No assertion changes
- 2026-09-25 | f5ac34be | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | D: the sender channel record keeps the break item and the rewound-from fill position of the channel's latest re-anchor. Add S: a delivery_hash_mismatch refusal is a transient failure. No code or test references D or S
- 2026-09-25 | 4145d047 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | a8abe9a5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | C and K: the delivery hash and the envelope carry `break`. D and E: no unconfirmed-resume mark; the record changes with a re-anchor, not with an operator's recovery or cancellation of a stop. Retire O: no supersession, succession or `rejected` stop; a `rejected` refusal is a permanent failure. R: no sender-superseded pull outcome. No code or test references any of these letters
- 2026-09-25 | 9c076a50 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale names the assertions it covers without a range over a retired letter
- 2026-09-25 | 9c076a50 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | M: the head is marked sent on a receiver record naming the delivery it sent, returned with any response or a check-in, not only with an accepting outcome, so a lost acknowledgement found at a check-in is never read as the sender falling behind. No code or test references M
- 2026-09-25 | b66f71c5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | O: stop on a supersession refusal of a delivery or a pull, on a succession refusal, and, with the reason `delivery_rejected`, on a `rejected` refusal; retire P: an unrecorded fork is a `rejected` refusal; Rationale shortened
- 2026-09-25 | 036c891a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Split into single-obligation assertions and move the receiver's side to EVS-DEV-delivery-receiver; the envelope lists the channel keys; the sender channel record also changes on an operator's recovery or cancellation of a stop; the envelope checks move to the receiver; re-letter
- 2026-09-25 | 2217f6be | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | The fill records each queue item's withheld-parent record (D); remove channel closure (the closed-channel check, the closure audit, the report operation and the view's closure) and the pull's closed flag; the sender channel record keeps the first response's receiver identity and whether the latest resume is unconfirmed in place of the resume record; the receiver check admits an event whose originator and last provenance entry name the sender; supersession and succession refusals stop the channel; every operation that accepts a native delivery takes the caller's sender-identity set; remove R
- 2026-09-25 | c3b44c16 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-R: natively serializing destinations are channels and implement the pull and report, the delivery hash covering the withheld-parent record, the sender channel record with the receiver's identity, number and link assigned at the pre-send fence and written to the send fence record, the esd/batch@3 envelope, the receiver's check including closed channels, its accepted-delivery audit and per-event stamp, its record, closure and working copy with a check that walks each channel's chain, the acknowledgement and refusal bodies naming the receiver and the fork_unrecorded refusal, and their mapping in the drain, the coalesced refusal audit, the declarative default delivery-channels view, the authenticated receiver endpoint with its log-derived pull, channel listing and shared-channel report, the pull outcomes, and the three receiver audit event types

*End* *Delivery channel sender mechanics* | **Hash**: f5ac34be

## EVS-DEV-delivery-receiver: Delivery channel receiver mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the receiver's side of a delivery channel: the checks of an incoming delivery, the accepted-delivery audit and per-event stamp, the record derived from them, the findings the receiver records about a channel, the acknowledgement and refusal bodies, the authentication of a caller, and the receiver endpoint's pull and channel listing.

### Assertions

A. Ingest SHALL refuse, by name and before any write, a batch that is not in the native batch format, one whose `parent_withheld` does not hold exactly one boolean per event, and one that carries no event.

B. The receiver SHALL read its record of a delivery's channel inside the transaction that ingests the delivery and before any write.

C. The receiver SHALL acknowledge, appending no event, a delivery whose number and hash equal its record of the channel.

D. The receiver SHALL refuse, with an out-of-sequence refusal naming its record, every other delivery whose number is not the record's number plus one or whose link is not the record's hash.

E. <RETIRED> A delivery from a superseded sender is accepted with a security finding (assertion U).

F. <RETIRED> A delivery carrying an event of another originator is accepted with a security finding (assertion T).

G. In the transaction that accepts a delivery, the receiver SHALL append exactly one reserved ingest audit of event type `ingest.delivery_accepted` whose data carries exactly `database_id` (the receiver), `channel`, `delivery_number`, `delivery_hash`, `previous_delivery_hash`, `event_ids`, `event_hashes` (the identifier and the hash of each event the delivery carried, as carried and in its order), `parent_withheld` and `break` (as the delivery carried them).

H. The receiver SHALL record a `delivery` object carrying exactly the channel, `delivery_number` and `parent_withheld` (the delivery's boolean for that event) in the provenance entry it stamps on each event it ingests from a delivery.

I. The receiver's record of a channel SHALL be the `delivery_number` and `delivery_hash` of the `ingest.delivery_accepted` audit with the highest number among those naming that channel that the receiver authored, or number 0 and a null hash when there is none, read from that audit event as the chain index locates it by channel.

J. <RETIRED> The receiver reads its record from the log through the chain index (assertion I), which the chain verification checks against the log.

K. <RETIRED> The receiver's delivery checks keep each channel's accepted audits gap-free, and the chain verification detects a later change to them.

L. The receiver endpoint SHALL answer an accepted delivery, and a re-presented one, with an acknowledgement carrying exactly `channel`, `receiver_database_id`, `record` (an object with exactly `delivery_number` and `delivery_hash`) and `outcome` (`accepted` or `represented`).

M. The receiver endpoint SHALL answer a refused delivery with a refusal carrying exactly `channel`, `receiver_database_id`, `record`, `refusal` (`out_of_sequence`, `delivery_hash_mismatch`, or `rejected` for every refusal ingest names by another reason), `reason` (for `rejected`, the reason ingest named; otherwise null) and `refused_event_id` (for `rejected`, the event the refusal concerns, or null when it concerns the whole delivery; otherwise null).

N. Every operation of the library that accepts a native delivery, and the receiver endpoint's pull, SHALL refuse, before any read of a channel and any write, a delivery or a pull naming a channel whose sender database is not in the set of sender database identities the deployment's authentication states the caller may act for.

O. The receiver endpoint's pull SHALL return the receiver's database identity, the receiver's record of the named channel, the delivery hash of the receiver's `ingest.delivery_accepted` audit for each delivery number the pull names within that record, and, for each delivery of the range it asks for, the delivery's number, link, hash, withheld-parent record and break and, in the order that audit lists them, the stored record of each event it names, or, for an event it holds only in a security finding's evidence, the record that evidence carries.

P. The receiver endpoint's pull SHALL answer, naming the delivery, that it cannot serve a delivery within its record for which it holds no accepted audit or holds neither as an event nor in a security finding's evidence every event the audit names.

Q. <RETIRED> A pull naming a superseded sender is served as any pull.

R. The receiver endpoint's pull SHALL, when asked for the channels of a sender database, optionally under one destination identifier, list each channel its log records of that sender and of every identity in that sender's succession lineage as the receiver's succession events state it, each with the receiver's record of it.

S. The library SHALL declare `ingest.delivery_accepted` as an event type of its reserved ingest audit entry type, and SHALL append it only through the ingest of a delivery.

T. The receiver SHALL accept a delivery carrying an event whose originator entry or last provenance entry does not name the channel's sender database, recording for each such event a security finding of kind `foreign_event` naming the channel, the delivery number and the event.

U. The receiver SHALL accept a delivery from a sender database that a succession its log holds names as predecessor, recording a security finding of kind `predecessor_live` naming the channel, the delivery number and the successor.

V. In the transaction that accepts a delivery whose `break` is not null, the receiver SHALL record a security finding of kind `channel_break` naming the channel, the break's sender record and the receiver record the delivery follows.

W. The receiver SHALL refuse with refusal `delivery_hash_mismatch` a batch whose `delivery_hash` is not the hash of its channel, number, link, events, withheld-parent record and break, and SHALL record for it, in a transaction that writes nothing else, a security finding of kind `delivery_hash_mismatch` naming the channel, the delivery number, the carried delivery hash and the hash it recomputes to.

### Rationale

**Why these checks (assertions A to D and W)?** A batch that does not decode or cover its events cannot be tied to a delivery of the channel, so it is a `rejected` refusal. A batch whose delivery hash does not recompute was changed on the way or computed wrongly; the receiver records the finding and refuses it as transient, so the sender sends it again and a copy damaged in transit is replaced by a sound one, the finding recorded once however often the same batch arrives. The channel check runs inside the ingest transaction, so racing deliveries of one channel serialize; a re-presentation of the last accepted delivery is a retry and is acknowledged; the out-of-sequence refusal returns the record the sender realigns to.

**Why an audit per delivery, a stamp per event, and the index (assertions G to I)?** The record must be derivable from the log for every delivery, including one whose events the receiver already holds. The audit lists each event with its hash as sent, so a pull or a verifier recomputes the delivery hash from the log. Only audits the receiver authored count, and the chain index, checked against the log by the chain verification (`EVS-DEV-chain-verification`), locates the latest one per channel without a scan.

**Why these responses, authenticated by sender identity (assertions L to N)?** Every response names the receiver's identity, so a sender can tell that another database answered. Every accepting operation takes the set of sender identities the deployment's authentication grants the caller, so no handler lets one sender deliver on, or read, another's channel; an unauthenticated caller's claims are refused, not recorded.

**Why serve from the log (assertions O, P and R)?** View rows omit deleted entries and events no view folds, so a recovery served from them would restore another history. The channel listing shows a check-in the channels the sender does not know, and a successor what to restore across its predecessor's lineage.

**Why accept and record (assertions T to V)?** A channel carries only what its sender authored or recovered as its own (`EVS-PRD-destinations/C`), and a succeeded predecessor has no live source. An event of another originator, or a predecessor's delivery, contradicts the channel; it may be tampering or a clone, so the receiver stores it as received and records the fact. A break is recorded from the receiver's side too, so each end's log holds the divergence it saw.

**Why a declared audit type (assertion S)?** The public append operations refuse it and ingest checks its shape (`EVS-DEV-destination-drain/L`).

### Changelog

- 2026-09-25 | d6e1707c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | A: a delivery hash that does not recompute is no longer a rejected refusal; add W: it is refused as delivery_hash_mismatch with a security finding. M: the delivery_hash_mismatch refusal value. O and P: an event held only in a security finding's evidence is served from it. No code or test references any of these letters
- 2026-09-25 | a9ec85b4 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Retire E, F and Q: a superseded sender's delivery or pull and an event of another originator are no longer refused; add T and U, which accept them and record security findings. A, G and O: the break is checked, audited and served. M: no `sender_superseded` or `succession_refused` refusal and no successor key; `rejected` is a permanent failure. Add V: a break delivery records a `channel_break` finding. No code or test references any of these letters
- 2026-09-25 | f1c45b42 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Retire K: the receiver's delivery checks keep the accepted audits gap-free and the chain verification detects a later change, so no separate report walks them. No code or test references K
- 2026-09-25 | 6405ebac | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | I: the record is read from the latest authored audit as the chain index locates it; retire J: no working copy; K walks the accepted-audit chain only; M: one `rejected` refusal value carries ingest's named reason for every integrity refusal; Rationale shortened
- 2026-09-25 | 84eed41c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-S: the receiver's side of a delivery channel, split from EVS-DEV-delivery-channel into single-obligation assertions: the envelope checks, the channel check in the ingest transaction, the accepted-delivery audit and per-event stamp, the log-derived record, its working copy and the chain check, the response bodies, authentication by sender identity, the log-derived pull, and a channel listing that covers the sender's succession lineage; remove the refusal audit and the default delivery-channels view

*End* *Delivery channel receiver mechanics* | **Hash**: d6e1707c

## EVS-DEV-delivery-resume: Channel check-in and resume

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds what the drainer does with a receiver record: the check-in that holds a channel, the reading of a record against the sender's own, the marking of a delivery whose acknowledgement was lost, the resume after the receiver fell behind with its resend, the recovery after the sender fell behind with its skip event, and the keeping of resume items through an operator's recovery.

### Assertions

A. The drainer SHALL start no send on a channel until it has committed a check-in decision for that channel after the latest of these points: the process opening the database, an acquisition of the drain lock by an epoch that is not one greater than the last epoch this process held, and an operator's recovery of the channel's wedged head.

B. The drainer SHALL check a channel in by a pull naming the sender channel record's number and asking for the channels the receiver holds for the sending database under the destination's identifier.

C. While a channel awaits a check-in response, the drainer SHALL keep it held, continuing to fill it and to honour halt requests on it.

D. The drainer SHALL repeat a check-in that fails transiently under the destination's retry policy.

E. The drainer SHALL write the check-in epoch only in the transaction that commits the decision the check-in's response calls for.

F. The library's read of delivery status SHALL report each held channel.

G. <RETIRED> A response from another receiver database re-anchors the channel (EVS-DEV-channel-findings).

H. On a receiver record whose number and hash are those of the delivery the send fence record names for the pending head, the drainer SHALL, in one transaction, set the sender channel record to the receiver record and mark the pending head sent under that delivery.

I. The drainer SHALL resume the channel as a receiver behind on a receiver record whose number is below the sender channel record's when the sender retains a delivery for every number above the record's up to the sender channel record's and the retained delivery numbered one above the record's links to the record's hash.

J. <RETIRED> A receiver record below the sender's that assertion I does not resume from re-anchors the channel (EVS-DEV-channel-findings).

K. The drainer SHALL resume the channel as a sender behind on a receiver record whose number is above the sender channel record's when the receiver's hash at the sender channel record's number, as the check-in reported it or a pull the drainer makes for it returns, is the sender channel record's hash, or is null at number 0, which needs no report.

L. <RETIRED> A receiver record that calls for no resume re-anchors the channel (EVS-DEV-channel-findings).

M. A receiver-behind resume SHALL enqueue, in ascending delivery-number order, one queue item marked as a resume item per delivery number above the receiver record up to the sender channel record's, carrying the events, withheld-parent record and break of the retained delivery with that number in its order.

N. The drainer SHALL commit a receiver-behind resume in one transaction that verifies the drain lock and that retires the channel's pending items, deleting each that carries no attempt and tombstoning each that carries attempts, rewinds the fill position below the lowest event they carry, enqueues the resume items, sets the sender channel record to the receiver record and appends a resume event of direction `receiver_behind`.

O. On a sender behind, the drainer SHALL pull every delivery numbered above the sender channel record and up to the receiver record, and SHALL check, in number order, that each delivery's link chains from the sender channel record's hash, that each recomputes to its hash, and that every event it carries passes ingest's integrity verification, names the sender's own database identity in its originator entry, and carries as its last entry the receiver's, whose arrival hash is the hash the delivery lists for that event and to which the event, with that entry removed and its sequence number restored to the entry's origin sequence number, hashes.

P. A recovery SHALL store each served event whose sealed hash names no event the sender holds, in ascending order of origin position, as the receiver served it with the sender's own provenance entry appended (naming the sender's database identity and recording the channel and delivery number it was recovered from), under a new local sequence number.

Q. A sender-behind resume SHALL enqueue its skip event alone as the channel's next item, marked as a resume item.

R. The drainer SHALL commit a sender-behind resume in one transaction that verifies the drain lock and that appends the skip event before storing the recovered events, stores them, retires the channel's pending items, deleting each that carries no attempt and tombstoning each that carries attempts, rewinds the fill position below the lowest event they carry, enqueues the skip event and sets the sender channel record to the receiver record.

S. In the transaction of an operator's recovery of a channel's wedged head, the library SHALL enqueue again, in their order and ahead of every item a later fill enqueues, the wedged head when it is marked as a resume item and each pending item behind it so marked, each marked as a resume item and carrying the events, withheld-parent record and break it carried.

T. <RETIRED> A channel of the sender that its log does not record is a security finding, and nothing is recovered from it (EVS-DEV-channel-findings).

U. <RETIRED> A check-in that finds a channel the sender does not know decides the checked-in channel as its receiver record calls for (EVS-DEV-channel-findings).

V. When a check-in's pull reports a permanent failure, the drainer SHALL wedge the channel's pending head with cause `check_in_failed`, and while no head is pending SHALL keep the channel held.

W. The drainer SHALL read as the retained delivery at a delivery number the delivery its queue last marked sent at that number on the channel from an item enqueued no earlier than the break item the sender channel record names, or from any item when it names none, and SHALL read no delivery as retained at a number where there is no such delivery.

X. In the transaction of a receiver-behind or a sender-behind resume, the drainer SHALL enqueue again, after the items the resume enqueues and in their order, each pending item the resume retires that carries a skip event or a break, marked as a resume item and carrying the events and withheld-parent record it carried and no break.

### Rationale

**Why hold per channel until a check-in (assertions A to F)?** The drain epoch rises at every acquisition of the drain lock (`EVS-DEV-destination-drain-lock/A`), so a process sees whether another held the lock in between; only then, after an open, or after an operator recovered the head, can the channel have moved unseen. The epoch is written only with the decision, so a drainer that stops part way checks in again. The hold is per channel and reported (`EVS-DEV-destination-drain/T`).

**Why this reading (assertions H, I and K)?** A record naming the delivery the send fence names is that delivery's acknowledgement (`EVS-PRD-destinations/J`); a delivery an earlier resume retired never matches. A receiver behind is resumed when every delivery it lacks can be resent exactly and the first links to its record, which proves the common point by content. A receiver ahead whose delivery at the sender's number carries the sender's hash holds everything the sender does; at number 0 both hashes are null. Every other record re-anchors (`EVS-DEV-channel-findings`).

**Why new resume items (assertions M, N and S)?** A sent item never changes status (`EVS-DEV-destination-drain/B`), so a resend is a new item carrying the retained delivery's events, withheld-parent record and break, which the fence numbers to the retained hash. Pending items were never accepted, so they are retired and refilled; one with attempts is tombstoned to keep them (`EVS-PRD-destinations/J`). An operator's recovery refills from the log, which would re-filter a resend and never enqueues a skip or a break, so it enqueues the resume items again, first.

**Why these checks (assertion O)?** A recovery admits events of the sender's own identity, which ingest records as an anomaly, so it checks each against its own channel: the chain, each hash, ingest's integrity verification and the reconstruction of the event as the receiver accepted it. A failed check is a finding and the event is still stored as served (`EVS-DEV-channel-findings`).

**Why one transaction, skip event first (assertions P to R)?** A recovered event keeps its identity, sealed hash and predecessor link and gains the sender's own entry; its origin position may be one the continuing branch reuses, so it takes a new local sequence number. The skip is appended first in the same transaction, so no recovered event is held without its skip and on every channel the skip precedes the recovered events (`EVS-DEV-destination-drain/X`). A recovery that fails to commit stores nothing and is repeated at the next check-in. A recovery holds back the sender's appends while it commits; storing a large one in resumable chunks is recorded in `spec/roadmap/sync.md`.

**Why one retained delivery per number (assertion W)?** A re-anchor numbers the channel again from the receiver's record, so the queue can hold sent items at one number from before and after it; only the deliveries since the latest re-anchor are on the chain the receiver accepted. Within that span a number repeats only through a resend, and the one sent last is the one the receiver was last given. Reading the retained delivery this way gives the receiver-behind reading, the resend and the resend check one delivery at each number.

**Why keep a pending skip or break through a resume (assertion X)?** A resume retires the channel's pending items because they were never accepted and the fill enqueues them again, but the fill never enqueues a skip event or a break on its own channel. A skip or break retired unsent would never reach that receiver, which would then meet the fork the skip records without it. The resume therefore enqueues them again behind its own items, as an operator's recovery does. A retired break goes again without its break: the resume has realigned the channel on an automatic path, so the break no longer states the record the delivery replaces, and a receiver would record a sender record the channel had already moved past. Its finding still travels, naming both records, as a re-anchor sends earlier breaks.

**Why wedge on a permanent pull failure (assertion V)?** A pull that reaches no receiver endpoint of this library is a deployment fault, not an integrity anomaly, so it takes the drain's permanent-failure path with an operator exit.

### Changelog

- 2026-09-25 | 2bbafa9b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | X: a retired break enqueued again after a resume carries no break, so a receiver records no sender record the channel has moved past; its finding still travels. No code or test references X
- 2026-09-25 | 6c0875ec | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add W: the retained delivery at a number is the one last sent at it since the channel's latest re-anchor. Add X: a resume enqueues again, behind its own items, each pending skip or break it retires. No code or test references W or X
- 2026-09-25 | 97522cd4 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | dff246f8 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Security-finding model: retire G, L, T and U, which the re-anchor and the channel findings replace; A: an operator's recovery of the head is a check-in point, with no stop to cancel; M and S: resume items carry the break; N: no unconfirmed-resume mark; O: the pull is checked, and a failed check no longer stores nothing; R: no unconfirmed-resume mark; add V: a permanent pull failure wedges the head with cause `check_in_failed`. No code or test references any of these letters
- 2026-09-25 | 0b182922 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 94fed4f3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | K: the sender's hash at number 0 is null, so a sender whose record is 0 resumes as a sender behind. Add U: a check-in that stops on an unknown channel first marks sent the delivery in flight its receiver record names. Rationale names the assertions it covers without a range over a retired letter
- 2026-09-25 | cb770a1a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 296c8ad6 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | N and R: a resume tombstones a pending item that carries attempts, keeping them, and deletes the others. T: a channel the sender does not know stops the channel checked in with reason `unknown_channel`, recording the channels found; no unknown-channels event. No code or test references N, R or T
- 2026-09-25 | 58137aa5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | I: a receiver behind resumes when every delivery above its record is retained and the first links to its hash; retire J: no `record_not_retained` stop; L: stop on any record that calls for no resume; Rationale shortened
- 2026-09-25 | 750d57ad | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Split into single-obligation assertions, moving the stops to EVS-DEV-channel-stop and the resume event shapes to EVS-DEV-resume-event; add the `record_not_retained` stop for a receiver behind to a delivery the sender did not send; an operator's recovery or cancellation of a stop is a check-in point; an operator's recovery keeps a channel's resume items (S); re-letter
- 2026-09-25 | 896ff51f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Revise to check in at process start and when the drain lock changes hands; read a record as receiver changed, in step, lost acknowledgement (send fence record only), receiver behind, sender behind or no common point; resend retained deliveries exactly through new pending items; verify the whole recovery, then store it with the skip event appended first in one transaction; one resume event type with a direction; stops through a halt of purpose channel_stop with reasons; the repeated-resume stop and the resend-mismatch stop; the unknown-channels event and its shape; remove the receiver's placement check of a resume event, lost channels, the common-point search, the halt of purpose resume and its cancellation, the stop-skip, the shared-channel report, the hold for another channel to the same receiver, recovered_channels, recovered_count, superseded_deliveries, fill_position and possibly_incomplete_aggregates; renumber
- 2026-09-25 | 5d6fd499 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-T: the per-epoch check-in and hold with a repeated check-in once a blocking halt or wedge is cleared, the classification of a receiver record and of lost channels, adoption of a delivery whose outcome was not recorded, the common point of each recovered channel, verified recovery of a sender's own events stored in origin order and left out of the views until a skip records them, the library's channel stop through a halt of purpose channel_stop with a skip over what a partial recovery stored, the resume halt and the resume recovery, the rewind and skip events with the channels they recovered, sealed branch points and the forks by successor, and how the branch point, abandoned head and recovered and unrecovered positions are determined, the reserved resume and succession entry types, the receiver's check of a resume event and its closing of recovered lost channels, the repeated-record, closed-channel, superseded, succession-refused and fork-unrecorded wedges, independent channels, the cancellation of a stale resume request, the shared-channel report, and the hold of a channel while another channel to the same receiver is mid-resume

*End* *Channel check-in and resume* | **Hash**: 2bbafa9b

## EVS-DEV-channel-findings: Channel findings and re-anchoring

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the security findings a sender records about a channel and the re-anchor that realigns a channel no resume explains: the finding and break a re-anchor commits, the unknown channels a check-in reveals, a resend that no longer matches, and the checks of a recovery that fail.

### Assertions

A. The drainer SHALL re-anchor a channel on a receiver record that names a receiver database identity other than the one the sender channel record holds, and on one from the channel's receiver that equals neither the sender channel record nor the delivery in flight and calls for no resume.

B. The drainer SHALL commit a re-anchor in one transaction that verifies the drain lock and that appends a security finding of kind `channel_unexplained` naming the channel, the sender channel record, the receiver record and the recorded and responding receiver database identities; retires the channel's pending items, deleting each that carries no attempt and tombstoning each that carries attempts; rewinds the channel's fill position to its start; enqueues that finding alone as the channel's next item, marked as a resume item and carrying as its break the sender channel record; enqueues after it, in ascending local sequence order, each marked as a resume item carrying no break, every skip event naming the channel's registration and every earlier `channel_unexplained` finding a re-anchor of that registration enqueued as its break; and sets the sender channel record to the receiver record, its receiver identity to the responding one, and its re-anchor to the break item's queue position and the fill position the re-anchor rewound from.

C. The drainer SHALL send each queue item with the envelope's `break` set to the break the item carries, and null for an item that carries none.

D. For each channel a check-in lists under the destination's identifier whose sender is the checking database and whose registration identifier names no registration event in the sender's log, the drainer SHALL append, in the transaction that commits the check-in's decision, a security finding of kind `unknown_channel` naming that channel, and SHALL recover nothing from that channel.

E. When a resend item whose link is the link of the retained delivery with that number computes at the pre-send fence a delivery hash other than that retained delivery's, the drainer SHALL append, in that fence transaction, a security finding of kind `resend_mismatch` naming the channel, the number and both hashes, and SHALL send the item as computed.

F. For each served delivery or event that fails a check of a sender-behind pull, for each delivery within the receiver's record that the receiver answers it cannot serve, and for each served resume or succession event whose originator entry names the sender's database identity and whose sealed hash names no event the sender holds, the drainer SHALL record a security finding, of kind `hash_mismatch` for an event whose hash or an arrival hash does not recompute, of kind `recovery_unverified` naming the check that failed for every other failed check, or of kind `own_resume_recovered`, naming, for the last two, the channel, the delivery number and, where one applies, the event and its sealed hash.

G. The drainer SHALL store a served event for which it records a finding of a sender-behind pull as it stores every other served event, when the event is one it can store as an event.

### Rationale

**Why re-anchor, and refill from the start (assertions A to C)?** A record no resume explains leaves no point the sender can prove shared, and only a delivery that follows the receiver's record will be accepted. The sender records both records and both receiver identities, and its finding is the break: the next delivery on the channel, whose hash covers the sender's record it replaces. The receiver may lack any of the sender's events, so the fill starts again and idempotent ingest admits only what is missing; the cost is proportional to the channel's history and paid only on an anomaly. What the fill never enqueues on the channel goes too: the channel's own skip events and earlier breaks are sent right after the break, so the receiver holds each skip before it meets the continuing branch again, and the refill carries the events recovered from the channel (`EVS-DEV-destination-drain/W`), which a receiver restored past the recovery no longer holds. The break item marks where the retained deliveries start again.

**Why unknown channels are recorded, not recovered (assertion D)?** A receiver can hold a channel of this sender its log does not know: a restore to before a registration, or another copy of the sender. Recovering from it would be guessing at a history. The finding names the channel alone, not the receiver's record of it, which moves while another copy delivers on it, so one unknown channel is one finding. The checked-in channel continues as its own record calls for. The listing also names the channels of the sender's predecessors, whose registrations a successor never holds; a predecessor's channel is not the sender's to explain, and a predecessor still delivering is recorded where it is met, by the receiver.

**Why record and store what does not verify (assertions E to G)?** A resend that no longer hashes as sent means the sender's own record changed; it is recorded and sent as held. Every later resend of the same resume links to the changed hash and so hashes differently too; only a resend whose link is as retained and whose hash differs is a finding, so the first alteration a resume meets records one finding and the resends after it in that resume record none. A served delivery that does not chain or recompute, an event that fails a check, a delivery the receiver cannot serve, and a resume or succession event of the sender's own identity it does not hold (a second live copy, or a restore to before one of its own resumes or its succession) are facts about what the receiver holds; the served events are stored as served, beside the findings, and the recovery completes.

### Changelog

- 2026-09-25 | 57efe00e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 103bd46b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | D: unknown_channel applies only to listed channels whose sender is the checking database, so a successor records none for its predecessors' channels. E: resend_mismatch is recorded only for a resend whose link is the retained delivery's link, so an alteration records one finding, not one per later resend. F: own_resume_recovered also covers a served succession event of the sender's own identity that it does not hold. No code or test references D, E or F
- 2026-09-25 | 040fa13d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: a re-anchor enqueues after its break the channel's skip events and earlier breaks, and records its break item and rewound-from fill position. D: an unknown_channel finding names the channel alone. F: a hash that does not recompute is a hash_mismatch finding. G: a served record that cannot be stored as an event is kept in a finding. No code or test references any of these letters
- 2026-09-25 | b5572dbc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 877c5dad | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-G: the re-anchor of a channel no resume explains, with its `channel_unexplained` finding sent as a break delivery and a refill from the channel's start; `unknown_channel`, `resend_mismatch`, `own_resume_recovered` and `recovery_unverified` findings, and served events stored as served

*End* *Channel findings and re-anchoring* | **Hash**: 57efe00e

## EVS-DEV-resume-event: Resume and channel records

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the shape of the events the sender appends about its channels: the resume event with the skip keys, how the branch point, abandoned head and unrecovered positions are determined, and the reserved entry types.

### Assertions

A. The resume event SHALL carry exactly `id` (the destination), `database_id` (the sender), `registration_id`, `direction` (`receiver_behind` or `sender_behind`), `resume_after` (the receiver record, as an object with exactly `delivery_number` and `delivery_hash`), `previous_record` (the sender channel record before the resume, in that shape), `drainer_epoch`, and the skip keys, each null for direction `receiver_behind`.

B. The skip keys SHALL be exactly `recovered_deliveries` (an object with exactly `first` and `last`, the delivery numbers the recovery pulled), `recovered_sequences` (the ascending inclusive ranges `[first, last]` of the origin positions of the events the recovery stored), `unrecovered_sequences`, `branch_point` and `abandoned_head` (each an object with exactly `sequence_number`, the event's origin position, and `event_hash`, its sealed hash; `abandoned_head` null exactly when the recovery stored no event, and `branch_point` null when the recovery stored no event or no event is proven shared), and `conflicted_aggregates`.

C. A skip event's `unrecovered_sequences` SHALL be the ascending inclusive ranges of the origin positions above the branch point, or above 0 when the branch point is null, and up to the abandoned head, that `recovered_sequences` does not hold, and no range when the abandoned head is null.

D. The drainer SHALL determine the abandoned head as the event with the highest origin position among the events the recovery stores.

E. The drainer SHALL determine the branch point as the event with the highest origin position below the abandoned head among the events the sender holds as authored that are proven shared, each by a served event with that event's identity and sealed hash, by the retained delivery whose number and hash are those of the sender channel record the resume starts from carrying it, or by the predecessor link of the lowest-positioned event the recovery stores naming it, and as null when no such event is proven shared.

F. <RETIRED> A skip event records its forks and reused origin positions as the range above its branch point and up to its abandoned head.

G. <RETIRED> A channel the sender does not know is recorded as a security finding (EVS-DEV-channel-findings).

H. The library SHALL declare the resume event and the succession event as reserved destination audit entry types, `system.destination_channel_resumed` and `system.destination_sender_succeeded`, each with an event type of its own.

### Rationale

**Why these resume event fields (assertions A and B)?** They state, from the sender's log alone, what the resume did; positions are ranges because a recovery can bring back thousands, and each recovered event names the channel and delivery it came from.

**Why unrecovered positions as the range less what was recovered (assertion C)?** Every position of the abandoned range the recovery did not bring back is listed, whether the sender holds a continuing event there or nothing: a filtered-out event, an unserved delivery and a reused position all show. It does not claim an event of the chain ever sat there.

**Why this branch point and abandoned head (assertions D and E)?** The sender cannot tell an authored event its backup kept from one it appended after the restore, so the branch point is the highest authored event it can prove lies on both branches: one a served delivery carries under the same identity and sealed hash, one of its retained delivery at the record the resume starts from, or the predecessor of the lowest recovered event. Each proven event lies at or below the true fork, so the branch point may be lower than the fork, or null, never higher; a lower one widens the accepted range and counts more authored events as continuing, the price of not guessing. Positions are recorded with sealed hashes, because the sender's stored hash of a recovered event is its own re-stamp.

**Why a range, not a list of forks?** A legitimate restore's forks and reused positions all lie above the branch point and, for the lowest event of each fork and every reused position, at or below the abandoned head, so any holder checks each fork and reuse against the range, in whatever order it received the events, and records a finding for any the range does not cover (`EVS-DEV-chain-verification`). A skip appended with nothing else after the restore is itself a successor at the fork, inside its own range.

**Why reserved destination audits (assertion H)?** Each records an operation on one destination of one database, so the destination-audit shape rules and ingest checks apply (`EVS-DEV-destination-drain/H`, `K`, `L`).

### Changelog

- 2026-09-25 | 0de54883 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 7d87fc16 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | C: `unrecovered_sequences` is every position of the abandoned range that `recovered_sequences` does not hold. No code or test references C
- 2026-09-25 | 7824ea5c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: `branch_point` is null also when no event is proven shared. C: `unrecovered_sequences` lists only positions at which the sender holds no event of its own origin chain, counted above 0 when the branch point is null. E: the sender's retained delivery at the record the resume starts from also proves its events shared, and the branch point is null when nothing is proven. Rationale: the branch point may be lower than the true fork or null, never higher. No code or test references B, C or E
- 2026-09-25 | 1ed86539 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: no `forks` key, since the branch point and abandoned head bound the range of positions a restore reuses; retire F: no fork list and no skip counted but not listed; retire G and amend H: no unknown-channels event, a channel the sender does not know stops the channel. No code or test references B, F, G or H
- 2026-09-25 | 3906ffa0 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | F: the skip event is counted among the events carrying a predecessor, and is not listed among the successors, so a restore with nothing appended records its fork; Rationale shortened
- 2026-09-25 | 0ed5489a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-H: the resume event and its skip keys, split from EVS-DEV-delivery-resume, with `unrecovered_sequences` defined as the abandoned positions at which the sender holds no recovered event, the abandoned head, the branch point, the forks by successor, the unknown-channels event and the reserved entry types

*End* *Resume and channel records* | **Hash**: 0de54883

## EVS-DEV-sender-succession: Sender succession

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds how a database that has authored no application events restores a predecessor sender's deliveries from a receiver, the succession event it appends, how a receiver accepts it and what it records, the lineage the library derives from succession events, and the restores the library refuses.

### Assertions

A. The library SHALL offer a restore operation that, through the pull of a destination registered in the successor, obtains the channels the receiver lists for a named predecessor database and pulls each listed channel's deliveries from 1 up to the receiver's record of it.

B. The restore operation SHALL check that every pulled channel's deliveries chain by link from delivery 1 (whose link is null), that each recomputes to its hash, and that every event it carries passes ingest's integrity verification, names the channel's sender in its originator entry, and carries as its last entry the receiver's, whose arrival hash is the hash the delivery lists for that event, and SHALL record a security finding of kind `hash_mismatch` for each event whose hash or an arrival hash does not recompute and of kind `recovery_unverified` for each delivery or event that fails another check.

C. The restore operation SHALL store, in one transaction, every carried event the successor does not hold, as served, in lineage order with the earliest predecessor first and, within each identity, in ascending order of origin position, each with the successor's provenance entry appended recording the channel and delivery number it was pulled from.

D. The library SHALL append the succession event only in the transaction in which the restore operation stores the predecessor's events, with data carrying exactly `id` and `registration_id` (the successor's destination the restore pulled through), `database_id` (the successor), `predecessor_database_id`, and `predecessor_channels` (for each channel restored, an object with exactly `channel` (an object with exactly `sender_database_id`, `destination_id` and `registration_id`), `delivery_number` and `delivery_hash`, the last delivery restored).

E. A receiver SHALL handle a delivered succession event it already holds as it handles any event it already holds, applying no succession check to it.

F. A receiver SHALL refuse, as it refuses a caller it does not authenticate for a channel's sender, a delivery carrying a succession event it does not hold when the endpoint's caller may not act for both the successor and the predecessor.

G. The library SHALL offer a read of the succession lineage of a sender database identity, derived solely from the succession events the log holds: the predecessors it succeeded, transitively, and its successor, if any.

H. The restore operation SHALL refuse, before storing anything, a restore into a successor whose log holds an event of an application entry type it authored, one into a successor whose log holds a succession event it authored, one naming the successor's own identity, and one for which the receiver lists no channel.

I. A receiver SHALL accept a succession event it does not hold, recording a security finding of kind `succession_contradicted` naming the succession and the contradiction, when a succession its log holds already names the predecessor, when, for a channel the event names that the receiver's log records, the receiver's own record of that channel is other than the named delivery, or when its log holds a channel of the predecessor, or of an identity in the predecessor's succession lineage, that the event does not name.

### Rationale

**Why restore every listed channel from delivery 1 (assertions A and B)?** The successor holds none of its predecessor's history, so delivery 1's null link anchors the chain that binds what is served to each channel as the receiver holds it: consistency, not authorship (`EVS-PRD-delivery-channel`, Trust). The listing covers the predecessor's lineage, so a device reset twice restores what its predecessor restored. A failed check is a finding and the events are stored as served.

**Why one transaction, in lineage and origin order (assertions C and D)?** Each identity's write order per aggregate then holds in the successor's log (`EVS-PRD-event-log/C`), and a restore happened whole, with its succession recorded, or not at all. The public append operations refuse every reserved entry type (`EVS-DEV-destination-drain/L`), so no operation can claim a succession otherwise; the succession event reaches every channel (`EVS-DEV-destination-drain/X`).

**Why these receiver rules (assertions E, F and I)?** A succession met a second time is a duplicate. A caller not authenticated for both identities is refused, so one sender's credential cannot take over another's data. A predecessor already succeeded, a named delivery other than the receiver's record of a channel it holds (the predecessor delivered after the restore), or a channel the event does not name (the successor did not restore what this receiver holds) are contradictions the receiver records, accepting the succession. The succession event travels on every channel of the successor, so it reaches receivers that never held a channel it names; such a receiver has no record of that channel to compare, and the difference is no contradiction.

**Why a lineage read (assertion G)?** The Layer 2 continuity of authorship (`EVS-PRD-delivery-channel/T`) needs one reading of a lineage, shared by the verification, the conflict rules and the channel listing, derived solely from succession events.

**Why refuse these restores (assertion H)?** A successor with application events or a succession of its own has a history the restore would merge with another; a person decides. It is a precondition of the operation, checked in the restore's one transaction, not an integrity check.

### Changelog

- 2026-09-25 | 3905d868 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | I: the delivery comparison applies only to named channels the receiver's log records, so a receiver that never held a named channel records no contradiction. No code or test references I
- 2026-09-25 | f2ffa392 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: a hash that does not recompute is a hash_mismatch finding. No code or test references B
- 2026-09-25 | 8e5a0695 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | cfc962aa | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | B: a failed check records a `recovery_unverified` finding instead of storing nothing. C: the restored events are stored as served. F: only the authentication condition refuses a succession; add I: the other contradictions accept it with a `succession_contradicted` finding. No code or test references any of these letters
- 2026-09-25 | 47f24079 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | F: a receiver refuses a succession when its log holds a channel of the predecessor or its lineage that the event does not name; Rationale shortened
- 2026-09-25 | af933232 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Split into single-obligation assertions; the restore pulls every channel the receiver lists for the predecessor's succession lineage and stores it in lineage and origin order; the succession event names each restored channel in full; a receiver handles a succession event it already holds as a duplicate; the restore refuses a successor that already appended a succession in place of one naming an already-named predecessor; re-letter
- 2026-09-25 | f9cf8790 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Revise to succession only into a database that has authored no application events, refused before anything is stored otherwise; verify every channel before storing and store it with the succession event in one transaction; the fill delivers the succession event, so the succession delivery request is removed; the receiver checks only the channels it holds and no longer requires every channel named; move the restore refusals from B to E
- 2026-09-25 | 50a1ab00 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-D: the restore of a predecessor's channels, closed ones included, verified from delivery 1 and stored in origin order across channels, the succession event appended only in the transaction that completes the restore and never over aggregates the successor already authored on, delivered through a succession delivery request the drainer's fill performs, the receiver's acceptance of a succession, and the succession lineage read

*End* *Sender succession* | **Hash**: 3905d868
