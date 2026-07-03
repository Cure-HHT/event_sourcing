# Collaborative Document Editing on `event_sourcing`

Collaborative rich-text editing is the cleanest case where the
`event_sourcing` substrate is a *partial* fit: its append-only log and reactive
`subscribe<T>` are nearly tailor-made for "broadcast every edit to every
connected author with audit-grade history", but its Layer 2 deep-merge
convention is the wrong fold for character-level edits, and per-aggregate
ordering provides serialization without OT/CRDT convergence. The honest design
uses the substrate as a server-of-record event bus plus an audit trail, and
layers an OT/CRDT engine on top of `Events()` mode.

## 1. Initialization and use

**Composition.** Server-of-record runs `PostgresBackend` (the substrate is the
authoritative log; this is exactly what `prd-portability.md` aims for). Clients
are browser/desktop Flutter using `RemoteScope` from `reaction` — they do not
run an `EventStore` of their own. The mobile/offline story is the awkward
one: see the optimism discussion below.

**Entry types and aggregates.** One aggregate type `document`, with
`aggregateId = documentId`. Workspace and per-workspace membership are
separate aggregate types (`workspace`, `workspace_membership`) so they have
independent canonicalization and projections. Comments and presence are
*separate* aggregates (see §3).

**Actions.**

```text
InsertText(documentId, anchor, text)            -> InsertTextResult
DeleteRange(documentId, anchorStart, anchorEnd) -> DeleteRangeResult
FormatSpan(documentId, anchorStart, anchorEnd, mark) -> ()
AddComment(documentId, anchor, threadId, body)  -> CommentId
Mention(documentId, commentId, mentionedUserId) -> ()
ShareDocument(documentId, userId, role)         -> ()  // per-doc override
```

Each declares `Permission('document.edit', scopeClass: 'document')` (or
`'document.comment'`, `'document.admin'`). `scopeFor` returns
`BoundScope(class_: 'document', value: input.documentId)`. The dispatcher runs
the standard parse/validate/authorize/execute pipeline; each action's
`execute` returns a single `EventDraft` whose `data` carries the operation
(anchor + payload), not the resulting document state. The substrate stamps
sequence, hash, provenance, action invocation id, and `lib_format_version` —
all of which become load-bearing in §2.

**Scope classes.**

```dart
ScopeClassRegistry(classes: [
  ScopeClassSpec(name: 'workspace'),
  ScopeClassSpec(name: 'document', containedIn: ContainmentRef(
    parentClass: 'workspace',
    projection: 'document_workspace_index',
    keyColumn: 'document_id',
    parentColumn: 'workspace_id',
  )),
]);
```

A user assigned `editor @ workspace=acme` is permitted to edit any document in
`acme` via containment expansion. Per-document overrides are a `role_assigned`
event whose scope is `BoundScope(class_: 'document', value: docId)` — same
machinery, no special case.

**Sync topology.** Editors connect via `RemoteScope` and `watch<T>` per open
document. The reaction WS does the per-subscription scope narrowing described
in `spec/reaction-remote.md`, so events from documents the user lacks access
to never traverse the wire. Permission revocation closes the WS with `4003
permissions_changed`; permission grants emit `stale_data`.

**Optimistic local apply.** Substrate-style "every edit round-trips" gives
~50-200ms keystroke latency over the WAN — unacceptable for typing. So the
client *must* maintain a local OT/CRDT shadow that applies edits immediately,
queues them as actions to the server, and reconciles when the server's
authoritative event sequence arrives via the `watch<T>` stream. This shadow
is not the substrate's responsibility, and there is no `LocalScope`-style
substrate-on-client (no Sembast mirror of the doc) — the log of record is
server-side.

## 2. Layer 1 properties that are load-bearing

Two stand out:

**Reactive subscribe with append-atomic-with-row-update.** The flagship. The
wire delivers each `Delta<DocumentEdit>` to every other connected editor in
committed order, with the guarantee that the server's view-row update for that
edit committed in the same Postgres transaction as the event append. Unlike a
banking ledger (where end-of-day consistency is fine), real-time editors fork
into inconsistent states the moment two clients see the same sequence in
different orders. The substrate's per-aggregate-per-Source ordering means
**every connected editor sees the exact same operation sequence**, which is
the only honest input an OT/CRDT layer can build on top of.

**Append-only history + provenance chain.** "Show me the document at any
prior sequence" is free; legal/discovery for collaborative docs (Notion/
Confluence are subpoenaed all the time) gets ALCOA+ guarantees structurally.
Per-event `initiator` plus `metadata.provenance` answers "who typed what,
when, from which install" — blame view and undo-by-author both fall out. A
banking ledger has audit but doesn't need per-character provenance; a
collaborative editor genuinely does.

## 3. Layer 2 machinery — what fits, what doesn't

**The deep-merge fold is wrong for text.** `AggregateProjectionSpec`'s
"missing keys preserve, null clears, deep-merge successive payloads" produces
a `Map`, not a rope or piece-table. An `InsertText` event's payload is
`{anchor: ..., text: "h"}`; deep-merging the next one overwrites `anchor` and
`text` rather than splicing. This is the genuine limit. The app falls back to
**Layer 1**: subscribe with `Events()` mode and run an app-side OT/CRDT fold
on the server to materialize `documents.current_text`. The substrate's
projection interpreter materializes only *metadata* views — `documents_index`
(title, last-edited, owners), `document_comments` (per-thread, deep-merge
works fine here), `document_activity_log` (per-edit row). The rendered text is
an app projection, not a substrate one.

**Concurrent edits and the OT layer.** Per-aggregate-per-Source ordering
means: once two `InsertText@5` events from clients A and B are appended, every
reader sees them in the same order. But the substrate makes no claim that
this order *converges* the two clients' optimistic shadows — A's local shadow
applied its insert at offset 5, then receives B's insert at offset 5 from the
wire; without transformation, A's cursor is now wrong. The OT/CRDT layer
above the substrate is responsible for transforming the second-arriving op
against the first. The substrate's contribution is the *canonical total
order* both clients reconcile against, plus the at-least-once
replay-from-snapshot guarantee that makes a dropped WS recoverable.

**Aggregates.** One `document` aggregate per page (all text ops serialize); a
separate `comment_thread` aggregate per thread (so commenting doesn't
serialize behind typing, and threads have independent permission scopes); a
separate `presence` aggregate per (document, user) **emitted to a different
log entirely**, or better, *not as events at all* — cursor presence is
ephemeral, high-frequency, and not audit-worthy. Forcing presence through the
hash chain would balloon storage 100x; use a sidechannel (Redis pub/sub or a
reaction-style WS broadcast outside the substrate).

**Tombstones vs. hide-not-delete.** Legal/discovery argues for hide-not-delete
on `DeleteDocumentAction` — emit `document_archived` rather than a tombstone
event type, leaving the row visible through an "include archived" filter.
Substrate's tombstone convention is opt-in via `tombstoneEventTypes` in the
spec; leave it empty.

**Honest gaps.** The substrate provides no optimistic concurrency token, no
client-side conflict resolution primitive, no CRDT merge semantics. It also
doesn't help with offline editing on clients without a local substrate: a
Flutter mobile client wanting true offline collaborative editing would need
either (a) `LocalScope` + an `event_sourcing` install per device with
bi-directional ingest (the multi-source roadmap item,
`spec/roadmap/multi-source-editing.md`, currently dormant) or (b) an
app-side queue of pending actions replayed when the WS reconnects.
Option (b) is what ships today.
