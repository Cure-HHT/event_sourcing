# Delivery Continuity

## Overview

A destination's queue marks an item sent when the receiver acknowledges it, and the drainer does not send it again. Either end can later move back in time: a receiver restored from a backup, restored to a point in time, or failed over to a replica that had not received the latest commits; a sending device restored from a backup of its database. Either move silently separates what the sender believes it delivered from what the receiver holds. This file specifies how the library detects such a move from the delivery channel itself and records it truthfully in the log.

The library realigns a channel automatically on the two common, safe paths: a lost acknowledgement, and a receiver behind the sender, which gets the deliveries it lacks again exactly as they were sent. A receiver ahead of the sender, naming a delivery the sender never attempted, reveals a sender that went back in time: the sender records a security finding (`EVS-DEV-security-findings`) and keeps delivering, and the application rebuilds it from the receiver as a successor. Every other inconsistency is recorded as a security finding by the side that detects it. A channel that no automatic path realigns continues on a new generation, numbered from delivery 1 and filled again from the start of the sender's log. No channel stops for an integrity reason. How an application interprets the recorded facts is left to it.

### Terms

- **Channel.** One generation of one registration of a natively serializing destination on one sending database, identified by the sending database's identity (`EVS-DEV-event-store-open/F`), the destination identifier, the registration identifier (the event identifier of the registration event) and the generation. A destination deleted and registered again starts a new registration; the old one ends.
- **Generation.** A counter of a registration's channels, 1 for the first. The sender starts the next generation when no automatic path realigns the current one.
- **Delivery.** One batch envelope the drainer sends on a channel. It carries at least one event.
- **Delivery number.** The position of an accepted delivery in its channel, counted from 1 with no gaps. The channel numbers deliveries, not events, so a destination's filter leaves no gap in the numbering.
- **Link.** The delivery hash of the delivery a delivery follows; null for delivery 1.
- **Delivery attributes.** An object a delivery carries beside its events, holding the optional per-delivery facts a later release of the data-format major adds. The delivery hash covers it and the receiver keeps it as carried.
- **Delivery hash.** The SHA-256 of the canonical form of the channel, the delivery number, the link, the hashes of the events the delivery carries and the delivery attributes.
- **Receiver record.** The number and hash of the last delivery the receiver accepted on a channel (number 0 and a null hash before the first). The receiver derives it from its own log and returns it with every acknowledgement and every refusal.
- **Sender channel record.** The registration's current generation, the receiver record the sender last established on it, and the receiver database identity that answered on it (none before the first response); the next delivery is numbered from it.
- **Channel's receiver database.** The receiver database identity the sender channel record holds, or, while it holds none, whichever database responds.
- **Attempted delivery.** A delivery, with its number and hash, that the send fence record names or that an attempt recorded on a queue item of the registration (pending, wedged or tombstoned) carries.
- **Retained delivery.** At a delivery number, the delivery the sender's queue last marked sent at that number under the registration's current generation, with its number and hash. A number the sender adopted from a lost acknowledgement, with no item marked sent at it, has no retained delivery.
- **Resume.** The realignment of a channel whose receiver fell behind: the retained deliveries it lacks are sent again exactly as sent, recorded in one **resume event**.
- **Sender regression.** A receiver record ahead of the sender's: the receiver accepted deliveries the sender no longer knows it sent.
- **Succession.** A database that has authored no application events restores a predecessor sender's deliveries from a receiver and declares itself that sender's successor. A database's **succession lineage** is the identities it succeeded, transitively, as the succession events a log holds state them.

### How a receiver record is read

```text
receiver record R (with a response to a delivery) from database B,
sender channel record S (holding receiver identity S.receiver, if any),
delivery in flight D (number S.number + 1)

  S.receiver held and B != S.receiver ....... channel_unexplained
                                               finding, new generation
  otherwise, in this order:
  R.number == S.number + 1 and R names an
    attempted delivery ...................... acknowledged (or a lost
                                               acknowledgement): S := R,
                                               S.receiver := B, and mark
                                               the head sent when R
                                               names D
  R == S .................................... in step: nothing to realign
  R.number < S.number, every number in
    (R.number, S.number] retained, and the
    retained R.number + 1 links to R.hash ... receiver behind: resend
                                               them exactly as sent
  R.number > S.number and R names no
    attempted delivery ...................... sender regression:
                                               sender_regressed finding,
                                               new generation
  anything else (a record at or below S
    that calls for no resend, or above
    S + 1 naming a superseded attempt) ...... channel_unexplained
                                               finding, new generation
```

### Reading order

`EVS-PRD-delivery-channel` states what the library guarantees. `EVS-DEV-delivery-channel` fixes the sender's side of a channel: the delivery hash, the numbering, the envelope and how responses are read. `EVS-DEV-delivery-receiver` fixes the receiver's check, its audit, its record, its findings and its endpoint. `EVS-DEV-delivery-resume` fixes the reading of a receiver record, the resume and the new generation; `EVS-DEV-resume-event` fixes the resume event and the reserved entry types. `EVS-DEV-sender-succession` fixes succession, the path by which an application rebuilds a regressed sender. The drainer and recovery mechanics these build on are `EVS-DEV-destination-drain` and `EVS-DEV-destination-drain-lock`. The finding event is specified in `spec/dev-security-findings.md`; the verification of origin chains, and the causal parents every event carries, in `spec/causal-history.md`.

## EVS-PRD-delivery-channel: Delivery channel continuity

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations, EVS-PRD-ingest, EVS-PRD-library-charter

### Purpose

A delivery channel is the path from one sending database to one receiver through one registration of a destination that uses the library's native batch format. The library numbers the deliveries on each channel and links each to the one before, the receiver accepts only the delivery that follows the last one it accepted, and the receiver's record of the channel, derived from its own log, returns to the sender with every acknowledgement and refusal. When the receiver has fallen behind, the library sends it again what it lacks and records the resume in the log. When the sender has fallen behind, or the two records admit no automatic explanation, the library records a security finding and continues on a new generation of the channel, so delivery always continues. A database that has authored nothing and takes over a predecessor sender's data declares the succession in the log. A destination whose receiver is not this library does not use the native batch format and is outside this requirement.

### Assertions

A. The library SHALL treat each generation of each registration of a destination that uses the library's native batch format, on each sending database, as one delivery channel, identified by the sending database's identity, the destination identifier, the registration and the generation.

B. Every delivery the library sends on a channel SHALL carry a delivery number one greater than the number of the last delivery the sender knows the receiver accepted on that channel, the delivery hash of that delivery as its link, and its own delivery hash, which covers the channel, the number, the link, the hash of every event the delivery carries and the delivery's attributes.

C. A receiver SHALL accept a delivery on a channel only when its number is one greater than, and its link equals the hash of, the last delivery it accepted on that channel, and SHALL refuse every other delivery, other than a re-presentation of that last delivery, with a refusal naming its record of the channel.

D. The receiver's record of each channel (the number and the hash of the last delivery it accepted on it) SHALL be derivable from the receiver's event log alone.

E. Every acknowledgement of a delivery and every refusal of one SHALL carry the receiver's record of the channel.

F. The sender SHALL mark a queued item delivered only on a receiver record, returned with an acknowledgement or a refusal, that names the delivery the sender made of that item.

G. <RETIRED> The receiver's record reaches the sender on every acknowledgement and refusal (assertion E), with no separate pull before sending.

H. When the receiver's record is behind the sender's record, the sender retains a delivery for every number above the receiver's record up to its own, and the first of those links to the receiver's record, the library SHALL send again, on the channel, every retained delivery after the receiver's record, each with the events, number, link and hash it was first sent with, and SHALL record the resume as one event in the log.

I. When a receiver record from the channel's receiver database is ahead of the sender's record and names no delivery the sender attempted on the channel, the library SHALL record a security finding of sender regression naming both records.

J. <RETIRED> A sender regression is recorded as a finding (assertion I) and the channel continues on a new generation (assertion X); the application rebuilds the sender as a successor (assertion Q).

K. <RETIRED> A sender regression is recorded as a finding (assertion I); no resume event of the sender's own is delivered.

L. When both ends of a channel have moved back in time, the library SHALL realign the channel by sending again the retained deliveries the receiver lacks where the sender retains them and they link to the receiver's record, and by a new generation otherwise, and SHALL record nothing for deliveries that neither end holds.

M. <RETIRED> A receiver record that no automatic path explains continues the channel on a new generation with a security finding (assertion X).

N. <RETIRED> A sender regression is recorded as a security finding (assertion I).

O. <RETIRED> A resume called for again proceeds as any resume; each is an event in the sender's log.

P. The library SHALL provide a receiver endpoint that accepts deliveries and serves, to a caller the deployment authenticates for the channel's sender, the channels it holds for that sender and for the identities that sender succeeded, each with the receiver's record of it, and the deliveries of a range of a channel, reconstructed from the receiver's event log.

Q. A database that has authored no event of an application entry type and restores a predecessor sender's deliveries from a receiver SHALL record, in the transaction that stores them, a reserved succession event naming the predecessor's identity and, for each channel it restores, the last delivery it restores.

R. The library SHALL refuse, before storing anything, a restore of a predecessor's deliveries into a database that has authored an event of an application entry type.

S. <RETIRED> A delivery from a predecessor sender after its succession is accepted as any delivery; where its events meet the successor's, ingest records the forks and reused positions as security findings.

T. The library's default interpretation of authorship SHALL TREAT the events a successor authors after its succession event as continuing the authorship of the predecessor that event names.

U. The library SHALL deliver each succession event and each security finding on every channel of the sending database, whatever that destination's filter.

V. <RETIRED> A successor's restored events are its predecessor's and are not delivered on the successor's channels (EVS-DEV-destination-drain).

W. <RETIRED> A second live copy of a sending database produces security findings where its events meet the other copy's, and delivery continues (assertions X and Z).

X. When a receiver record is not the sender's record, is not numbered one above the sender's record naming a delivery the sender attempted at that number, and calls for no resend of retained deliveries, or comes from another receiver database than the channel's, the library SHALL continue the registration on a new generation, numbered from delivery 1 and filled again from the start of the sending database's log.

Y. <RETIRED> A new generation starts at delivery 1 and every receiver accepts it as a channel of its own (assertions A and C).

Z. The library SHALL NOT stop a delivery channel, nor refuse other than as a transient failure a delivery that follows the receiver's record, because of an integrity anomaly, and SHALL record each integrity anomaly it detects on a channel as a security finding.

### Rationale

**What the channel is (assertion A).** A delivery is a statement between two databases. The sender is the database identity, minted inside the database and checked at every open (`EVS-DEV-event-store-open/F`), so a backup restore brings back the same sender gone back in time, while a reset or a reinstall is a new sender. A destination deleted and registered again starts a new registration at generation 1 (`EVS-PRD-destinations/O`). The generation lets a registration start its numbering again without a special delivery: to a receiver, a new generation is a channel it has not seen. A destination that serializes through an application transform has no receiver of this library.

**Why number and link deliveries, and derive the record from the log (assertions B to F)?** A filter leaves gaps in the events by design, so the channel numbers what it sends, and the link binds each delivery to its predecessor's content. The Layer 1 claim, under the storage precondition (`EVS-PRD-destinations/L`), is that every delivery a receiver accepted follows the one before it on its channel by number and link. Derived from the log, the receiver's record goes back exactly as far as the events do after a restore; returned on every response, it tells the sender of a move at its next delivery, with no polling. A sender restored while idle learns of it when it next delivers. Marking an item delivered only on a record naming its delivery makes the acknowledgement evidence rather than a status code. A record naming, at the number after the sender's record, a delivery the sender attempted there is the truth about where the channel stands, even when the item that carried it was wedged and recovered, or retired, before the acknowledgement arrived: the sender adopts it, and the events of that delivery that it filled again are delivered again and admitted idempotently. A record ahead naming a delivery the sender never attempted reads as a regression. A record further ahead naming an attempt the sender made before a resume or a new generation moved its record back shows a receiver that came forward again (a failover back to a copy that held it); no automatic path explains it, so it is recorded as unexplained and the channel continues on a new generation. The delivery attributes are covered by the hash and kept as carried, so a later release of the same data-format major can add per-delivery facts that every receiver of that major stores and serves.

**Why resend exactly (assertion H)?** A receiver behind gets the retained deliveries again exactly as sent, nothing filtered again; the first links to the receiver's record, which proves the common point by content, and the receiver's chain of deliveries is again the sender's.

**Why a sender regression is a finding, not a recovery (assertions I, L and X)?** A record is compared only with the records of the database that answered on the channel; a record from any other database is a different receiver, which assertion X routes to a new generation. A sender restored from an older copy of its database has forgotten events it authored and delivered, and may have authored new ones at the same positions. Recovering them into the same identity would need the library to decide which history continues. It does not decide: the sender records the regression and keeps delivering on a new generation, the receiver stores everything as it arrives and records the forks and reused positions where the two histories overlap, and the application rebuilds the sender from the receiver as a successor (assertions Q to T), a fresh database holding everything the receiver holds. When both ends moved back, the resend covers what the receiver lacks, the rebuild what the sender lacks, and deliveries neither end holds are unknown to both, so their numbers are used again. A sender restored from a backup taken before its destination was registered registers it again under a new registration, a channel at generation 1 whose receiver record is 0, so no sender regression is recorded at the sender: the receiver's fork and reused-position findings are the only record of it, and the application reads them to decide on a rebuild. A receiver that moves back after a successor restored from it no longer holds the predecessor deliveries between its record and the one the succession event names, and nobody sends them again: the predecessor is retired, and the successor delivers only what it authored. The succession event reaches the receiver again on the successor's channels, and the receiver records the gap as a security finding naming the channel and both records; the deliveries between are held by the successor, and a person reconciles them.

**Why record and continue rather than stop (assertions X and Z)?** Anything else (a clone, tampering, a receiver restored to a point the sender cannot resend from, a different receiver database) needs a judgement the library has no basis for. A stop would hold the sender's records until a person acts, while a device's storage can be lost meanwhile, so the library records both records and continues on a new generation. The new generation refills from the start of the sender's log, and idempotent ingest admits only what the receiver lacks; the cost is proportional to the channel's history and paid only on an anomaly. A delivery whose delivery hash does not recompute is refused as transient, with a finding at the receiver, so the sender sends it again and a copy damaged in transit is replaced. A mismatch that repeats on every resend comes from the sender's computation or from a hop that rewrites every batch the same way; the retry budget runs out and the queue head wedges with the ordinary budget-exhausted cause. Recovering the head alone does not end it, because the refill computes the same hash; the exit is correcting the build or the transport and then the operator's recovery of the head (`EVS-PRD-destinations`). The receiver's finding and the sender's wedge event record the episode at both ends. An event the receiver cannot store as an event is kept in a finding and the rest of the delivery accepted. The only other refusals are a caller the deployment does not authenticate for the sender, and permanent failures of the application's validation or the data format, which wedge the queue head with an operator or upgrade exit (`EVS-PRD-destinations`).

**One live source per database identity.** The delivery guarantees assume each sender identity has one live source at a time: a restore replaces the database it restores, and a successor replaces its predecessor. A second live source (a cloned file, or a restored backup run beside the original) is detected, not prevented. Each copy's deliveries make the other's records look regressed or unexplained, so each records findings and starts new generations, and where their events meet the receiver records forks and reused positions (`EVS-PRD-hash-chain-integrity`). A person reads the findings and decides which copy lives. The churn is bounded by the copies' own activity, and every step is recorded.

**Why the library's own receiver endpoint (assertion P)?** A succession restore can use what the receiver serves only as far as it can check it, so the endpoint serves every delivery reconstructed from the receiver's log. The deployment's authentication maps a caller to the sender identities it may deliver for and read.

**Why succession only into a database that has authored nothing (assertions Q, R and T)?** A rebuilt, reset or reinstalled device that restores its predecessor's data would otherwise read as a second editor of it; the succession event states the continuity. A database that has authored application events has a history of its own that may overlap, so the restore is refused: a precondition of the operation, not an integrity check. Continuity of authorship is a Layer 2 convention over the Layer 1 succession event. Deciding to rebuild a regressed sender is the application's; the library offers the restore.

**Why every channel (assertion U)?** A succession concerns the sender's whole history, and every finding a sender records concerns what its receivers hold, so both reach every receiver whatever it filters.

**Trust.** The receiver's record and every served delivery are claims by an authenticated endpoint. The successor's checks of a served delivery prove consistency with the chain of deliveries, not authorship: the hashes are unkeyed and nothing is signed. A failed check is a finding and the data is stored as served. The receiver is trusted not to fabricate events under a predecessor's identity, to serve every delivery it accepted, and not to claim a record ahead of its log; a record ahead of the sender's is recorded as a finding in the sender's log, so a wrong claim is attributable. The destination transport, extended to records and served deliveries, and the deployment's authentication binding a caller to sender identities, are the trusted inputs this widens.

### Changelog

- 2026-09-26 | c4286b3d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | X: a record that is not the sender's, not the adopted next delivery and calls for no resend continues on a new generation, so a record naming an attempt superseded by a resume or a new generation no longer stalls the channel. Rationale: a record ahead naming a superseded attempt is unexplained, not a regression; a sender restored from before its destination's registration is recorded only by the receiver's fork and reuse findings; a receiver that moves back after a succession records the gap as a finding. No code or test references X
- 2026-09-25 | 95ecd28b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 3318f73c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | I: only a record from the channel's receiver database that names no delivery the sender attempted is a sender regression. X: a record naming any delivery the sender attempted is not unexplained. Terms: the sender channel record holds the answering receiver identity; the channel's receiver database; attempted delivery. Rationale: a record naming an attempted delivery is adopted; a persistent delivery hash mismatch ends only once the build or transport is corrected. No code or test references I or X
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: A and B: a channel is one generation of a registration, and the delivery hash covers the delivery attributes in place of the withheld-parent record and the break. F: no check-in. Retire G (no check-in; the record rides every response), J, K and V (no recovery under the sender's own identity and no skip event), S (no predecessor-live finding) and Y (no break delivery). I: a receiver record ahead of the sender's is a sender regression recorded as a finding. L: a double regression realigns by the resend or a new generation, the rebuild as a successor covering the sender. P: the endpoint serves the channel listing with records and the deliveries of a range. U: succession events and findings reach every channel. X: a record no automatic path explains continues the registration on a new generation filled again from the start. Rationale rewritten. No code or test references any of these letters
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

*End* *Delivery channel continuity* | **Hash**: c4286b3d

## EVS-DEV-delivery-channel: Delivery channel sender mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the sender's side of a delivery channel: which destinations are channels, how the delivery hash is computed, the sender channel record, when the drainer assigns a delivery's number and link, the batch envelope, and how the drainer reads the receiver's responses.

### Assertions

A. The library SHALL apply the channel mechanics to every destination that serializes natively and to no other destination.

B. The library SHALL refuse, before anything is written, to register a destination that serializes natively and does not implement the channel pull operation.

C. The library SHALL compute a delivery's hash as the SHA-256, in lowercase hexadecimal, of the canonical JSON of an object with exactly the keys `channel` (an object with exactly `sender_database_id`, `destination_id`, `registration_id` and `generation`), `delivery_number`, `previous_delivery_hash` (null for delivery 1), `event_hashes` (the `event_hash` of each event the delivery carries, in the order it carries them) and `attributes` (the delivery's attributes object, as carried).

D. The library SHALL keep, for each registration of a destination that serializes natively, a sender channel record holding the current generation, the number and hash of the receiver record the sender last established on it, and the receiver database identity that answered on the current generation, written when the registration is written with generation 1, number 0, a null hash and no receiver identity.

E. The library SHALL change a sender channel record only in a transaction that commits, on that registration, a send outcome, a resume or a new generation.

F. <RETIRED> A delivery carries its attributes object (assertions C and K); the library sends it empty.

G. The drainer SHALL assign a delivery's number (the sender channel record's number plus one) and link (the record's hash) in the pre-send fence transaction, from the sender channel record read there.

H. The drainer SHALL start a send only when the sender channel record its pre-send fence reads equals the one the payload was built from.

I. The drainer SHALL write a delivery's number and delivery hash in the send fence record its pre-send fence transaction writes and on the attempt the send produces.

J. The change that marks a queue item sent SHALL record the generation, delivery number and delivery hash it was acknowledged under.

K. A native batch envelope SHALL carry exactly the keys `batch_format_version` (`"3"`), `batch_id`, `sender_hop`, `sender_identifier`, `sender_software_version`, `sent_at`, `channel` (an object with exactly `sender_database_id`, `destination_id`, `registration_id` and `generation`), `delivery_number`, `previous_delivery_hash`, `delivery_hash`, `events` (at least one) and `attributes` (an object, empty in every delivery the library sends).

L. The library SHALL provide the decoder that maps a receiver's acknowledgement or refusal body to the send outcome it states.

M. For a destination that serializes natively, the drainer SHALL mark the head sent only on a receiver record, returned with any response to a delivery, whose delivery number and delivery hash are those of the delivery it sent.

N. The drainer SHALL handle an accepting outcome carrying another record, and an `out_of_sequence` refusal, as a receiver record returned for that delivery, never as a permanent failure.

O. <RETIRED> A receiver refuses a delivery only out of sequence, for its caller's authentication, or as a permanent failure the drain wedges on (EVS-DEV-delivery-receiver); every integrity anomaly is a security finding.

P. <RETIRED> A receiver accepts an unrecorded fork and records a security finding (EVS-DEV-chain-verification).

Q. For a destination that serializes natively, the drainer SHALL wedge the head with cause `acknowledgement_invalid`, in the transaction that records the attempt, on an accepting outcome that carries no record.

R. The library SHALL provide the decoder that maps a pull response to one of: served, a transient failure or a permanent failure, and a destination's pull operation SHALL report through that decoder's outcomes.

S. The library's decoder of a receiver's acknowledgement or refusal body SHALL map a `delivery_hash_mismatch` refusal to a transient failure.

### Rationale

**Why natively serializing destinations only, each with a pull (assertions A and B)?** Only a receiver running this library derives a record from its log. The pull is how a successor restores a predecessor's deliveries through a destination it registers (`EVS-DEV-sender-succession`), so a natively serializing destination that cannot pull is refused at registration.

**Why this hash, record and envelope (assertions C to E and K)?** The receiver recomputes the hash from the envelope, and a verifier from the receiver's accepted-delivery audit, which keeps every hashed field. The generation is in the channel, so deliveries of two generations never share a hash. The attributes object is hashed and kept whatever it holds, so a later release of the same data-format major adds per-delivery facts to it without a format step; the library sends it empty. The sender channel record holds what the drainer decides by and changes only with the outcome it reflects. The format step is part of the data-format major step (`EVS-DEV-version-compatibility/C`).

**Why number at the fence (assertions G to J)?** A number fixed at enqueue would be consumed by every item a resume retires unsent. The sent item records its generation as well, so a resume reads retained deliveries of the current generation only. Assigned at the pre-send fence, it makes every retry carry the same number, link and hash, and a resend after the same record reproduce the retained delivery exactly. The send fence record names the delivery in flight, which is how a lost acknowledgement is recognised: the retry is a re-presentation, and the receiver's answer names it.

**What the responses mean (assertions L to N and Q to S)?** A record naming a delivery is the only evidence the receiver accepted it, so a transport that returns success and drops the body wedges the head. An out-of-sequence refusal states where the channel stands (`EVS-DEV-delivery-resume`). A `delivery_hash_mismatch` refusal is transient, so the same delivery is sent again: the delivery never verified at the receiver, so a copy damaged in transit is replaced. One that persists comes from the sender's build or a hop that rewrites every batch alike; it ends in the budget-exhausted wedge, and its exit is correcting the build or the transport, then the operator's recovery of the head, since a refill computes the same hash. A `rejected` refusal is a permanent failure the drain wedges on, with an operator or upgrade exit (`EVS-DEV-destination-drain/J`): an undecodable batch, the application's validation, or an unsupported data-format major. No integrity anomaly produces a refusal. A pull that fails permanently is told apart from one worth repeating.

### Changelog

- 2026-09-26 | 29357550 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | J: the sent item records the generation it was acknowledged under. Q: scoped to destinations that serialize natively. No code or test references J or Q
- 2026-09-25 | 613fa092 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 8146b4bd | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | D: the receiver identity is the one that answered on the current generation. Rationale of S: a persistent delivery hash mismatch ends once the build or transport is corrected. No code or test references D
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: C and K: the channel carries its generation, and the hash and envelope carry an `attributes` object in place of `parent_withheld` and `break`. D and E: the sender channel record keeps the generation, with no check-in epoch or re-anchor fields, and changes with a send outcome, a resume or a new generation. Retire F (no withheld-parent record). M: no check-in. Rationale: the pull serves succession restores. No code or test references any of these letters
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

*End* *Delivery channel sender mechanics* | **Hash**: 29357550

## EVS-DEV-delivery-receiver: Delivery channel receiver mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the receiver's side of a delivery channel: the checks of an incoming delivery, the accepted-delivery audit and per-event stamp, the record derived from them, the findings the receiver records about a channel, the acknowledgement and refusal bodies, the authentication of a caller, and the receiver endpoint's pull and channel listing.

### Assertions

A. Ingest SHALL refuse, by name and before any write, a batch that is not in the native batch format, one whose `attributes` is not an object, and one that carries no event.

B. The receiver SHALL read its record of a delivery's channel inside the transaction that ingests the delivery and before any write.

C. The receiver SHALL acknowledge, appending no event, a delivery whose number and hash equal its record of the channel.

D. The receiver SHALL refuse, with an out-of-sequence refusal naming its record, every other delivery whose number is not the record's number plus one or whose link is not the record's hash.

E. <RETIRED> A delivery from a predecessor sender is accepted as any delivery (EVS-PRD-delivery-channel).

F. <RETIRED> A delivery carrying an event of another originator is accepted with a security finding (assertion T).

G. In the transaction that accepts a delivery, the receiver SHALL append exactly one reserved ingest audit of event type `ingest.delivery_accepted` whose data carries exactly `database_id` (the receiver), `channel`, `delivery_number`, `delivery_hash`, `previous_delivery_hash`, `event_ids`, `event_hashes` (the identifier and the hash of each event the delivery carried, as carried and in its order) and `attributes` (as the delivery carried it).

H. The receiver SHALL record a `delivery` object carrying exactly the channel and `delivery_number` in the provenance entry it stamps on each event it ingests from a delivery.

I. The receiver's record of a channel SHALL be the `delivery_number` and `delivery_hash` of the `ingest.delivery_accepted` audit with the highest number among those naming that channel that the receiver authored, or number 0 and a null hash when there is none.

J. <RETIRED> The receiver reads its record from its accepted-delivery audits (assertion I).

K. <RETIRED> The receiver's delivery checks keep each channel's accepted audits gap-free, and the chain verification detects a later change to them.

L. The receiver endpoint SHALL answer an accepted delivery, and a re-presented one, with an acknowledgement carrying exactly `channel`, `receiver_database_id`, `record` (an object with exactly `delivery_number` and `delivery_hash`) and `outcome` (`accepted` or `represented`).

M. The receiver endpoint SHALL answer a refused delivery with a refusal carrying exactly `channel`, `receiver_database_id`, `record`, `refusal` (`out_of_sequence`, `delivery_hash_mismatch`, or `rejected` for every refusal ingest names by another reason), `reason` (for `rejected`, the reason ingest named; otherwise null) and `refused_event_id` (for `rejected`, the event the refusal concerns, or null when it concerns the whole delivery; otherwise null).

N. Every operation of the library that accepts a native delivery, and the receiver endpoint's pull, SHALL refuse, before any read of a channel and any write, a delivery or a pull naming a channel whose sender database is not in the set of sender database identities the deployment's authentication states the caller may act for.

O. The receiver endpoint's pull SHALL return the receiver's database identity, the receiver's record of the named channel, and, for each delivery of the range it asks for, the delivery's number, link, hash and attributes and, in the order its `ingest.delivery_accepted` audit lists them, the stored record of each event it names, or, for an event it holds only in a security finding's evidence, the record that evidence carries.

P. The receiver endpoint's pull SHALL answer, naming the delivery, that it cannot serve a delivery above its record of the channel, and one within its record for which it holds no accepted audit or holds neither as an event nor in a security finding's evidence every event the audit names.

Q. <RETIRED> A pull naming a predecessor sender is served as any pull.

R. The receiver endpoint's pull SHALL, when asked for the channels of a sender database, list each channel, of every generation, that its log records of that sender and of every identity in that sender's succession lineage as the receiver's succession events state it, each with the receiver's record of it.

S. The library SHALL declare `ingest.delivery_accepted` as an event type of its reserved ingest audit entry type, and SHALL append it only through the ingest of a delivery.

T. The receiver SHALL accept a delivery carrying an event whose originator entry or last provenance entry does not name the channel's sender database, recording for each such event a security finding of kind `foreign_event` naming the channel, the delivery number and the event.

U. <RETIRED> A delivery from a predecessor sender is accepted as any delivery; ingest records forks and reused positions where its events meet the successor's.

V. <RETIRED> A new generation is a channel the receiver has not seen, accepted from delivery 1 (assertion D).

W. The receiver SHALL refuse with refusal `delivery_hash_mismatch` a batch whose `delivery_hash` is not the hash of its channel, number, link, events and attributes, and SHALL record for it, in a transaction that writes nothing else, a security finding of kind `delivery_hash_mismatch` naming the channel, the delivery number, the carried delivery hash and the hash it recomputes to.

X. The receiver SHALL accept a delivery whatever names its `attributes` object holds, keeping that object as the delivery carried it in the delivery hash it recomputes, in its `ingest.delivery_accepted` audit and in the deliveries its pull serves.

### Rationale

**Why these checks (assertions A to D and W)?** A batch that does not decode or cover its events cannot be tied to a delivery of the channel, so it is a `rejected` refusal. A batch whose delivery hash does not recompute was changed on the way or computed wrongly; the receiver records the finding and refuses it as transient, so the sender sends it again and a copy damaged in transit is replaced by a sound one, the finding recorded once however often the same batch arrives. The channel check runs inside the ingest transaction, so racing deliveries of one channel serialize; a re-presentation of the last accepted delivery is a retry and is acknowledged; the out-of-sequence refusal returns the record the sender realigns to.

**Why an audit per delivery and a stamp per event (assertions G to I)?** The record must be derivable from the log for every delivery, including one whose events the receiver already holds. The audit lists each event with its hash as sent, and the attributes as carried, so a pull or a verifier recomputes the delivery hash from the log. Only audits the receiver authored count. The backend's indexes locate the latest audit of a channel.

**Why these responses, authenticated by sender identity (assertions L to N)?** Every response names the receiver's identity, so a sender can tell that another database answered. Every accepting operation takes the set of sender identities the deployment's authentication grants the caller, so no handler lets one sender deliver on, or read, another's channel; an unauthenticated caller's claims are refused, not recorded.

**Why serve from the log (assertions O, P and R)?** View rows omit deleted entries and events no view folds, so a restore served from them would restore another history. A delivery above the receiver's record, which a receiver that moved back after listing the channel no longer holds, is answered as one it cannot serve. The channel listing shows a successor what to restore across its predecessor's lineage and every generation of its channels.

**Why accept and record (assertion T)?** A channel carries only what its sender authored (`EVS-PRD-destinations/C`). An event of another originator contradicts the channel; it may be tampering or a clone, so the receiver stores it as received and records the fact.

**Why keep attributes it does not know (assertion X)?** A later release of the same data-format major adds per-delivery facts as attributes. A receiver that dropped or refused one would break the delivery hash, or the sender's delivery, for a fact it does not need to interpret; keeping the object as carried lets it store and serve that fact until a release that reads it.

**Why a declared audit type (assertion S)?** The public append operations refuse it and ingest checks its shape (`EVS-DEV-destination-drain/L`).

### Changelog

- 2026-09-26 | 21c20c41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | P: the pull answers that it cannot serve a delivery above its record. No code or test references P
- 2026-09-25 | cec0f804 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: A, G, O and W: `attributes` in place of `parent_withheld` and `break`. H: the per-event `delivery` object carries no `parent_withheld`. I: the record is read from the latest authored audit, with no library-kept index. R: the listing covers every generation, with no destination filter. Retire U (no predecessor-live finding) and V (no break delivery). Add X: a receiver keeps, hashes and serves delivery attributes it does not know. No code or test references any of these letters
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

*End* *Delivery channel receiver mechanics* | **Hash**: 21c20c41

## EVS-DEV-delivery-resume: Channel resume and new generation

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds what the drainer does with a receiver record: the marking of a delivery whose acknowledgement was lost, the resume after the receiver fell behind with its resend, and the new generation, with its security finding, that the drainer starts on every other record that does not match its own.

### Assertions

A. <RETIRED> The drainer reads the receiver's record from the response to each delivery (EVS-DEV-delivery-channel).

B. <RETIRED> The receiver's record reaches the drainer with each response (EVS-DEV-delivery-channel).

C. <RETIRED> A channel is never held for its receiver's record, which reaches the drainer with each response (EVS-DEV-delivery-channel).

D. <RETIRED> A delivery that fails transiently is retried under the destination's retry policy (EVS-DEV-destination-drain).

E. <RETIRED> The sender channel record changes with a send outcome, a resume or a new generation (EVS-DEV-delivery-channel).

F. <RETIRED> The delivery status read reports no held channel; a channel's state is its sender channel record (EVS-DEV-delivery-channel).

G. <RETIRED> A response from another receiver database starts a new generation (assertion Y).

H. On a receiver record from the channel's receiver database whose number is the sender channel record's number plus one and whose hash is the delivery hash of a delivery the send fence record names or an attempt recorded on a queue item of the registration carries, the drainer SHALL, in one transaction, set the sender channel record's number and hash to the receiver record's and its receiver identity to the responding one, and, when the record names the delivery the send fence record names for the pending head, mark the pending head sent under that delivery.

I. The drainer SHALL resume the channel as a receiver behind on a receiver record whose number is below the sender channel record's when the sender retains a delivery for every number above the record's up to the sender channel record's and the retained delivery numbered one above the record's links to the record's hash.

J. <RETIRED> A receiver record below the sender's that assertion I does not resume from starts a new generation (assertion Y).

K. On a receiver record from the channel's receiver database whose number is above the sender channel record's and that names neither a delivery the send fence record names nor one an attempt recorded on a queue item of the registration carries, the drainer SHALL start a new generation of the registration, recording a security finding of kind `sender_regressed`.

L. <RETIRED> A receiver record that calls for no resume starts a new generation (assertions K and Y).

M. A receiver-behind resume SHALL enqueue, in ascending delivery-number order, one queue item per delivery number above the receiver record up to the sender channel record's, carrying the events and attributes of the retained delivery with that number in its order.

N. The drainer SHALL commit a receiver-behind resume in one transaction that verifies the drain lock and that retires the channel's pending items, deleting each that carries no attempt and tombstoning each that carries attempts, rewinds the fill position below the lowest event they carry, removes the registration's transform failure record, enqueues the resend items, sets the sender channel record to the receiver record and appends a resume event.

O. <RETIRED> A sender regression starts a new generation with a finding (assertion K); nothing is pulled from the receiver under the sender's own identity.

P. <RETIRED> A sender regression starts a new generation with a finding (assertion K); nothing is pulled from the receiver under the sender's own identity.

Q. <RETIRED> A sender regression starts a new generation with a finding (assertion K).

R. <RETIRED> A sender regression starts a new generation with a finding (assertion K).

S. <RETIRED> An operator's recovery of a wedged resend item rewinds the fill position below the events it carried, and the fill enqueues them again (EVS-DEV-destination-drain).

T. <RETIRED> A channel the sender does not know is not examined; a new generation is started only for the checked channel's own record.

U. <RETIRED> A channel the sender does not know is not examined; a new generation is started only for the checked channel's own record.

V. <RETIRED> A pull that fails permanently wedges the operation that made it (EVS-DEV-delivery-channel).

W. The drainer SHALL read as the retained delivery at a delivery number the delivery its queue last marked sent at that number under the registration's current generation, and SHALL read no delivery as retained at a number where there is none.

X. <RETIRED> A new generation retires the pending items and refills from the start of the log (assertion Z).

Y. On a response naming a receiver database identity other than the one the sender channel record holds, when it holds one, or on a receiver record from the channel's receiver database that is not the sender channel record, calls for no receiver-behind resume, and either has a number not above the sender channel record's or has a number more than one above it and names a delivery the send fence record names or one an attempt recorded on a queue item of the registration carries, the drainer SHALL start a new generation of the registration, recording a security finding of kind `channel_unexplained`.

Z. The drainer SHALL start a new generation in one transaction that verifies the drain lock and that appends the security finding the record calls for, under the detector role `sender`, naming the channel, the sender channel record, the receiver record and the recorded and responding receiver database identities; retires the registration's pending items, deleting each that carries no attempt and tombstoning each that carries attempts; rewinds the registration's fill position to the start of the log; removes the registration's transform failure record; and sets the sender channel record to the next generation, number 0, a null hash and the responding receiver identity.

### Rationale

**Why this reading (assertions H, I, K and Y)?** A record naming the delivery the send fence names is that delivery's acknowledgement (`EVS-PRD-destinations/J`), whether it arrives with the first response or with the answer to a re-presentation after the first response was lost. A record naming another delivery the sender attempted at that number is equally true: the receiver accepted it although its acknowledgement never arrived, and the item that carried it was since wedged and recovered, halted for a reconfiguration, or retired by a resume. The sender adopts the record and marks nothing sent, since the item that carried the delivery is no longer pending; its events were filled again and are delivered again after the record, which idempotent ingest admits. No item is marked sent at an adopted number, so a later resume finds no retained delivery there and starts a new generation instead. A record more than one ahead that names a delivery the sender attempted names one a resume or a new generation has since moved the sender's record back past: the receiver came forward again (a failover back to a copy that held it), which no automatic path explains, and it is not a regression, since the sender knows it sent that delivery. The first response on a generation names the receiver that the sender compares every later response with; before it, any receiver is the channel's. A receiver behind is resumed when every delivery it lacks can be resent exactly and the first links to its record, which proves the common point by content. A receiver ahead has accepted deliveries the sender does not know it sent: the sender went back in time, or another copy of it delivered. Every other record, and a response from another receiver database, admits no automatic explanation. The receiver's record reaches the drainer with every response, so no pull precedes sending.

**Why new resend items (assertions M and N)?** A sent item never changes status (`EVS-DEV-destination-drain/B`), so a resend is a new item carrying the retained delivery's events and attributes, which the fence numbers to the retained hash. Pending items were never accepted, so they are retired and refilled; one with attempts is tombstoned to keep them (`EVS-PRD-destinations/J`). The transform failure record goes with the rewind, as it does on an operator's recovery, so failures counted before the rewind spend nothing of the budget of what the refill evaluates; a new generation removes it for the same reason. An operator's recovery of a wedged resend item rewinds the fill below its events, so they are enqueued again by the fill; the receiver is behind and accepts whatever follows its record.

**Why the last delivery sent at a number in the current generation (assertion W)?** A resume reads retained deliveries only at numbers up to the sender channel record's. Every earlier generation used the same numbers, so the sent item records its generation and only the current generation's deliveries are read. Within a generation a number repeats only through a resend, which is identical. A number the sender adopted from a lost acknowledgement has no item marked sent at it and is not retained, so a receiver behind it is not resumed and the channel continues on a new generation.

**Why a new generation with a finding (assertions K, Y and Z)?** A receiver ahead has accepted a delivery the sender never attempted. The sender cannot prove a common point, and only a delivery that follows the receiver's record would be accepted. Rather than guess, it records both records and both receiver identities in a finding and starts the channel again: the next generation is a channel the receiver has not seen, numbered from 1, and the fill starts again from the beginning of the sender's log, so the receiver can lack none of the sender's events; idempotent ingest admits only what it lacks, and records forks and reused positions where the sender's history differs from what it holds. The finding is an event of the sender's log, which the fill delivers on every channel. A sender restored to before a generation it started may start one the receiver already holds; the receiver's record of it then starts the next, with a finding of its own.

### Changelog

- 2026-09-26 | 0409f85d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Y: a record not above the sender channel record, or more than one above it naming an attempted delivery, that is not the sender channel record and calls for no resume starts a new generation with `channel_unexplained`, so no record leaves a channel without a rule. W: a retained delivery is read only from items marked sent under the current generation, so an adopted number is not retained. N and Z: the resume and the new generation remove the transform failure record. C and F: the retired notes name where their subject is stated. No code or test references any of these letters
- 2026-09-25 | 3fcf0d7d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 86c9bb4a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | H: a record from the channel's receiver database naming, at the next number, any delivery the sender attempted (the send fence record's, or an attempt on a pending, wedged or tombstoned item) sets the sender channel record and adopts the responding receiver identity, and marks the head sent only when it names the head's delivery. K: a sender regression is a record ahead naming no attempted delivery. Y: another receiver identity is unexplained only when the sender channel record holds one. No code or test references H, K or Y
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: title; retire A to F and V (no check-in or hold), O, P, Q, R and X (no sender-behind recovery, skip event or break), and S (an operator's recovery refills a wedged resend). K: a receiver record ahead of the sender's starts a new generation with a `sender_regressed` finding. M and N: resend items carry attributes and no resume mark; the resume event has no direction. W: the retained delivery is the last one sent at the number on the registration. Add Y: a record no automatic path explains starts a new generation with a `channel_unexplained` finding; add Z: the new generation's transaction. No code or test references any of these letters. Z: the finding is recorded under the detector role `sender`
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

*End* *Channel resume and new generation* | **Hash**: 0409f85d

## EVS-DEV-resume-event: Resume and succession event types

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds the shape of the resume event the sender appends when its receiver fell behind, and the reserved entry types of the resume and succession events.

### Assertions

A. The resume event SHALL carry exactly `id` (the destination), `database_id` (the sender), `registration_id`, `generation`, `resume_after` (the receiver record, as an object with exactly `delivery_number` and `delivery_hash`), `previous_record` (the sender channel record's number and hash before the resume, in that shape) and `drainer_epoch`.

B. <RETIRED> A resume records only the receiver-behind realignment (assertion A).

C. <RETIRED> A resume records only the receiver-behind realignment (assertion A).

D. <RETIRED> A resume records only the receiver-behind realignment (assertion A).

E. <RETIRED> A resume records only the receiver-behind realignment (assertion A).

F. <RETIRED> A resume records only the receiver-behind realignment (assertion A).

G. <RETIRED> A channel whose records do not match starts a new generation with a security finding (EVS-DEV-delivery-resume).

H. The library SHALL declare the resume event and the succession event as reserved destination audit entry types, `system.destination_channel_resumed` and `system.destination_sender_succeeded`, each with an event type of its own.

### Rationale

**Why these resume event fields (assertion A)?** They state, from the sender's log alone, which channel was resumed, from which record to which, and by which drainer; the resend items carry the events.

**Why reserved destination audits (assertion H)?** Each records an operation on one destination of one database, so the destination-audit shape rules and ingest checks apply (`EVS-DEV-destination-drain/H`, `K`, `L`).

### Changelog

- 2026-09-25 | cf99714e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: title; A: the resume event records only a receiver-behind resume and carries the generation, with no direction or skip keys; retire B to E (no skip keys, branch point or abandoned head). No code or test references any of these letters
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

*End* *Resume and succession event types* | **Hash**: cf99714e

## EVS-DEV-sender-succession: Sender succession

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel

### Purpose

This requirement holds how a database that has authored no application events restores a predecessor sender's deliveries from a receiver, the succession event it appends, how a receiver accepts it and the finding it records when the succession names deliveries it no longer holds, the lineage the library derives from succession events, and the restores the library refuses. It is the path by which an application rebuilds a sender that went back in time, and a reset or reinstalled device takes over its predecessor's data.

### Assertions

A. The library SHALL offer a restore operation that, through the pull of a destination registered in the successor, obtains the channels the receiver lists for a named predecessor database and pulls each listed channel's deliveries from 1 up to the receiver's record of it.

B. The restore operation SHALL check that every pulled channel's deliveries chain by link from delivery 1 (whose link is null), that each recomputes to its hash, and that every event it carries recomputes to its event hash, names the channel's sender in its originator entry, and carries as its last entry the receiver's, whose arrival hash is the hash the delivery lists for that event, and SHALL record, under the detector role `restore`, a security finding of kind `hash_mismatch` for each event whose hash or an arrival hash does not recompute and of kind `restore_unverified` for each delivery, and each event other than a record it keeps in an `event_malformed` finding, that fails another check.

C. The restore operation SHALL store, in one transaction, every carried event the successor does not hold, as served, in lineage order with the earliest predecessor first, within each identity in ascending order of origin position, and at one origin position in ascending order of the registration identifier, generation and delivery number of the lowest pulled delivery carrying the event, each with the successor's provenance entry appended recording the channel and delivery number it was pulled from.

D. The library SHALL append the succession event only in the transaction in which the restore operation stores the predecessor's events, with data carrying exactly `id` and `registration_id` (the successor's destination the restore pulled through), `database_id` (the successor), `predecessor_database_id`, and `predecessor_channels` (for each channel restored, an object with exactly `channel` (an object with exactly `sender_database_id`, `destination_id`, `registration_id` and `generation`), `delivery_number` and `delivery_hash`, the last delivery restored).

E. A receiver SHALL handle a delivered succession event it already holds as it handles any event it already holds, applying no succession check to it.

F. A receiver SHALL refuse, as it refuses a caller it does not authenticate for a channel's sender, a delivery carrying a succession event it does not hold when the endpoint's caller may not act for both the successor and the predecessor.

G. The library SHALL offer a read of the succession lineage of a sender database identity, derived solely from the succession events the log holds: the predecessors it succeeded, transitively, and its successor, if any.

H. The restore operation SHALL refuse, before storing anything, a restore into a successor whose log holds an event of an application entry type it authored, one into a successor whose log holds a succession event it authored, one naming the successor's own identity, one for which the receiver lists no channel, and one for which a pull answers that it cannot serve a delivery the restore asks for.

I. <RETIRED> A receiver accepts a succession event it does not hold as any event; where the predecessor's and the successor's events meet, ingest records forks and reused positions.

J. <RETIRED> The restore's predecessor, fork and reused-position findings are stated with ingest's in the chain verification requirement (`spec/causal-history.md`).

K. A receiver that stores a succession event it does not hold SHALL record, for each channel the event's `predecessor_channels` names of which the receiver holds an accepted delivery and whose named delivery number is above the receiver's record of that channel, one security finding of kind `succession_ahead` naming the channel, the receiver's record and the named delivery, in the transaction that stores the succession event, and SHALL store the succession event as received.

### Rationale

**Why restore every listed channel from delivery 1 (assertions A and B)?** The successor holds none of its predecessor's history, so delivery 1's null link anchors the chain that binds what is served to each channel as the receiver holds it: consistency, not authorship (`EVS-PRD-delivery-channel`, Trust). The listing covers every generation of the predecessor's channels and its lineage, so a sender that went back in time is rebuilt with everything its receiver holds, both sides of any fork included, and a device reset twice restores what its predecessor restored. A failed check is a finding and the events are stored as served.

**Why one transaction, in lineage and origin order (assertions C and D)?** Each identity's write order per aggregate on each branch of its origin chain then holds in the successor's log (`EVS-PRD-event-log/C`), every event follows the predecessor it links to, and events of two branches at one origin position are stored in one order every restore of the same listing reproduces. A restore happened whole, with its succession recorded, or not at all. The public append operations refuse every reserved entry type (`EVS-DEV-destination-drain/L`), so no operation can claim a succession otherwise; the succession event reaches every channel (`EVS-DEV-destination-drain/X`). A restore holds back the successor's appends while it commits; a successor that has authored nothing has none to hold back. Storing a large restore in resumable chunks is recorded in `spec/roadmap/sync.md`.

**Why these receiver rules (assertions E and F)?** A succession met a second time is a duplicate. A caller not authenticated for both identities is refused, so one sender's credential cannot take over another's data. Two other situations are the application's choice, and the library records nothing for them: a predecessor still delivering, whose later events extend its own origin chain on its own channels without forking it, and a restore that did not cover every channel a receiver holds.

**Why record a succession ahead of the receiver (assertion K)?** A receiver that moved back after a successor restored from it no longer holds the predecessor deliveries between its record and the one the succession names. Nobody sends them again: the predecessor is retired, and the successor delivers only what it authored (`EVS-DEV-destination-drain/V`). The succession event states what the receiver held when the successor restored, so the receiver compares it with its own record when it stores the event and records the gap as a finding, then continues. The deliveries between are held by the successor, and a person reconciles them. A receiver that holds no accepted delivery of a named channel may never have been that channel's receiver, so it records nothing for it.

**Why the restore checks the origin chains?** A regressed sender's predecessor delivered two histories that overlap: the events its receiver held from before the regression, and those it authored again at the same positions afterwards. The receiver recorded the overlap in its own log, but those findings are the receiver's and are not among the predecessor's deliveries. The restore therefore applies ingest's predecessor, fork and reused-position checks to what it stores (EVS-DEV-chain-verification), meets the overlap again and records it in the successor's log under the role `restore`, so the successor's default views mark the forked aggregates as they do at the receiver (`EVS-PRD-materializer`), and its stamping of causal parents starts from a recorded fork, not an unrecorded one.

**Why a lineage read (assertion G)?** The Layer 2 continuity of authorship (`EVS-PRD-delivery-channel/T`) needs one reading of a lineage, shared by canonicalization and the channel listing, derived solely from succession events.

**Why refuse these restores (assertion H)?** A successor with application events or a succession of its own has a history the restore would merge with another; a person decides. A pull that cannot serve a delivery the listing named (the receiver moved back since it listed the channel, or lost an event) would leave a gap in what the succession event claims; the restore stores nothing and the application retries. It is a precondition of the operation, checked in the restore's one transaction, not an integrity check. Deciding when to rebuild a regressed sender (for example, once its queue has delivered what it held, so the receiver holds it too) is the application's.

### Changelog

- 2026-09-26 | c24246dc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | 65cb86a0 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | B: the kind `recovery_unverified` is named `restore_unverified`, and a record kept in an `event_malformed` finding records no `restore_unverified`. H: a restore refuses when a pull cannot serve a delivery it asks for. Add K: a receiver storing a succession event that names a delivery above its record of a channel it holds records a `succession_ahead` finding. Rationale: the lineage read is shared by canonicalization and the channel listing. No code or test references B, H or K
- 2026-09-25 | 7e365328 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | aeb85756 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | 7e729461 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | B: events are checked against their event hash, not an unnamed set of ingest checks. C: events at one origin position are ordered by the registration, generation and delivery number of the lowest delivery carrying them. Retire J: the restore's predecessor, fork and reused-position findings are stated once, with ingest's, in EVS-DEV-chain-verification/K and /L. Rationale of E and F: a predecessor still delivering after its succession is not detected, since it forks nothing. No code or test references B, C or J
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Simplification: D: the channel carries its generation. Retire I (no succession-contradicted finding). Purpose and Rationale: succession is how an application rebuilds a regressed sender. No code or test references any of these letters. B: the restore records its findings under the detector role `restore`
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

*End* *Sender succession* | **Hash**: c24246dc
