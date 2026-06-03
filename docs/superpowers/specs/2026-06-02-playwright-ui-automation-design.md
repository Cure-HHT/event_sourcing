# Playwright UI automation via the Flutter web semantics tree

Status: brainstorm-stage design (CUR-1307)
Date: 2026-06-02

## Problem

Flutter web renders through the CanvasKit renderer, which paints the
entire application into a single `<canvas>` element. There is no
per-widget DOM — no `<button>`, `<input>`, or text nodes — so Playwright,
which locates elements through the DOM, has nothing to target. The legacy
HTML renderer that exposed more DOM has been removed from modern Flutter,
so "switch renderers" is not an option.

CUR-1307 asks us to enable reliable Playwright UI automation of the
Flutter web client. QA selected this approach (Option 1) over the
officially-supported `flutter drive`-on-web path (Option 2), because it
yields a real Playwright suite, a language-agnostic harness, and pushes
test identity into the shared `reaction_widgets` library — per the
earlier ticket comment ("take care of this when we build the ActionWidget
library").

## Key facts (verified)

- Flutter web exposes an opt-in **accessibility / semantics tree**: a
  parallel DOM of `<flt-semantics>` elements, rooted under a
  `flt-semantics-host` container, carrying roles, labels, and stable
  identifiers alongside the canvas.
- `SemanticsProperties.identifier` surfaces on web as a
  **`flt-semantics-identifier`** attribute on the corresponding DOM node.
  Playwright selector: `[flt-semantics-identifier="submit-note"]`. The
  identifier is author-chosen and locale-independent — unlike labels,
  which are localized.
- The semantics tree is **off by default** (a performance optimization).
  It can be force-enabled programmatically with
  `SemanticsBinding.instance.ensureSemantics()` (guarded by `kIsWeb`),
  avoiding the invisible "Enable accessibility" placeholder button.
- `ValueKey` does **not** map to `flt-semantics-identifier`. The
  `Semantics(identifier:)` wrap is the only seam for a stable web
  selector.

## The two kinds of identity

The naive reading of the ticket — "bake test identity into
`reaction_widgets`" — collides with the library's headless charter
(`spec/prd-reaction.md:321`: the widget layer ships NO rendered or styled
widgets). Resolving that tension splits identity into two distinct kinds:

```text
  WHAT A TEST NEEDS              WHERE IT CAN LIVE
  ----------------------------   --------------------------------------
  1. Click / type targets        Consumer render code ONLY.
     (the button, the field,     The library ships no button to label.
      the dropdown, the row)      -> convention + example, not lib API

  2. Lifecycle / outcome state   The library CAN own this.
     (submitting? success?       The state lives inside the builder
      denied? loading? stale?)   (ActionState / ViewState), not the app
```

The `ActionBuilder` and `ViewBuilder` `build()` methods delegate
rendering entirely (`return widget.builder(...)`), so the actual
interactable widgets — `FilledButton`, `TextField`, `DropdownButton`,
`ListTile` — live in consumer code. The library has no opportunity to
label them, and labeling them would require shipping styled widgets,
which the charter forbids. Interactable identity is therefore
irreducibly a consumer responsibility.

Lifecycle state is the opposite: it is owned by the builder and invisible
to a DOM scraper except as localized status text. Exposing it as a
machine-readable semantics node is the genuinely reusable, charter-
compatible contribution the ticket was pointing at.

## Charter reconciliation

A `Semantics` node is **not** a "rendered or styled widget": it paints
nothing, applies no theming, and is modality-agnostic. It is automation/
accessibility metadata. The headless assertion forbids buttons, list
widgets, theming, and modality-aware affordances — none of which a
`Semantics` wrapper is. The library may therefore emit a non-painting
`Semantics` node without violating the charter.

During implementation we will, in `spec/prd-reaction.md` (in-place, via
elspais), both:

- author a clarifying **normative** assertion under
  `EVS-PRD-reaction-widget-contract` stating that the Builder primitives
  MAY surface an optional, non-painting semantic identifier carrying
  lifecycle state for automation, and that this does not constitute a
  rendered/styled widget; and
- add a **non-normative remainder** chapter, "Automation instrumentation
  for downstream widget libraries", carrying the consumer how-to (see
  Downstream auto-instrumentation below).

## Design

### Layer 1 — Library hook (`reaction_widgets`)

Add an optional `String? semanticIdentifier` to `ActionBuilder` and
`ViewBuilder`. When non-null, the builder wraps its delegated child in a
single, layout-neutral, non-painting `Semantics`:

```dart
Semantics(
  identifier: semanticIdentifier!,
  value: <stateToken>,   // re-emitted on every state transition
  child: widget.builder(...),
)
```

State tokens:

- `ActionBuilder`: `idle | submitting | success | denied | failed`
  (one per `ActionState` variant).
- `ViewBuilder`: `loading | ready | stale` (one per `ViewState`
  variant).

Properties:

- **Default `null` → zero behavior change** for every existing consumer.
- **Layout-neutral** — `Semantics` is a zero-impact wrapper; no `Column`
  or `Stack` is introduced, so consumer layout is unchanged.
- The identifier gives Playwright a stable selector
  (`[flt-semantics-identifier="submit-note"]`) whose `value` flips to
  `success` / `denied`, so the suite asserts on outcome **without
  scraping localized status text**.

This is the only production-code change in `reaction_widgets`. The
library ships no helper widget and no slug utility (YAGNI — see
Downstream auto-instrumentation).

### Layer 2 — Example app annotation (the vertical slice)

The slice proves the full path: **login -> submit a note -> assert it
appears in the notes list.**

- `reaction/example/lib/client/main.dart` (web entry): force-enable the
  tree at boot — `if (kIsWeb) SemanticsBinding.instance.ensureSemantics();`.
  Always-on for the demo; production apps may gate this behind a flag.
- `login_screen.dart`: `Semantics(identifier: 'login-username')` on the
  username `TextField`; `'login-button'` on the `FilledButton`.
- `submit_note_form.dart`: `'submit-note-title'` on the title
  `TextField`; `'submit-note-workspace'` on the workspace
  `DropdownButton`; `'submit-note-button'` on the `FilledButton` (the
  click target); and `semanticIdentifier: 'submit-note'` threaded into
  the `ActionBuilder` for outcome state. Note the deliberate split: the
  `ActionBuilder` hook wraps the whole form subtree, so `submit-note` is
  the **status/container** node carrying `value=success|denied|...`,
  while `submit-note-button` is the discrete element Playwright clicks.
- `notes_list.dart`: wrap each row's `ListTile` in
  `Semantics(identifier: 'note-row', value: note.title)` so the suite can
  assert a row carrying the freshly-submitted title exists.

Admin-panel grant/revoke flows are intentionally **not** annotated in
this pass.

### Layer 3 — Playwright harness (`reaction/example/e2e/`, TypeScript)

```text
reaction/example/e2e/
  package.json            @playwright/test
  playwright.config.ts    baseURL + webServer entries
  tests/submit-note.spec.ts
scripts/run-e2e.sh        build web -> serve bundle + boot demo server -> playwright test
```

`run-e2e.sh` orchestrates: `flutter build web -t lib/client/main.dart`,
serve `build/web` (static server), boot the demo server
(`dart run bin/server.dart`), then `npx playwright test`. The two server
processes are declared as Playwright `webServer` entries so the runner
starts and tears them down.

`submit-note.spec.ts` drives, all selectors via `flt-semantics-identifier`
scoped under `flt-semantics-host`:

1. Wait for `flt-semantics-host` and the first semantics node (CanvasKit
   load is async).
2. Log in (`login-username`, `login-button`).
3. Type a unique title into `submit-note-title`; select a workspace via
   `submit-note-workspace`.
4. Click `submit-note-button`; wait for the `submit-note` status node's
   `value` to become `success`.
5. Assert a `note-row` whose `value` matches the submitted title exists.

### Data flow

```text
Playwright
  -> served Flutter web bundle (CanvasKit)
  -> app boot: ensureSemantics() exposes flt-semantics DOM
  -> select [flt-semantics-identifier="..."]; click / type
  -> Flutter dispatches; ActionBuilder.submit -> RemoteScope
  -> dart demo server appends event, recomputes view
  -> view Update streams back over WS -> ViewBuilder re-renders
  -> notes_list row's Semantics node appears in the DOM
  -> Playwright assertion passes
```

## Downstream auto-instrumentation

> **Home for this guidance.** This section is non-normative consumer
> how-to. Per `spec/README.md:52` (cross-system narrative belongs in a
> remainder section of the spec/ file it contextualizes, not a separate
> prose document), during implementation it migrates verbatim into a
> non-normative remainder chapter of `spec/prd-reaction.md` titled
> "Automation instrumentation for downstream widget libraries", sitting
> beside the new `EVS-PRD-reaction-widget-contract` assertion. The draft
> below is authoritative until that migration.

Downstream apps build their own widget libraries on top of
`reaction_widgets` (e.g. a `MyStandardButton`). They auto-instrument the
**mechanism** by wrapping `Semantics` once inside the custom widget's
`build()`; every instance then inherits annotation without touching raw
`Semantics` at call sites:

```dart
class MyStandardButton extends StatelessWidget {
  const MyStandardButton(this.label, {this.onPressed, this.semanticId, super.key});
  final String label;
  final VoidCallback? onPressed;
  final String? semanticId;

  @override
  Widget build(BuildContext context) => Semantics(
        identifier: semanticId ?? 'btn-${_slug(label)}',  // auto default
        button: true,
        child: FilledButton(onPressed: onPressed, child: Text(label)),
      );
}

MyStandardButton('Submit')                              // id = btn-submit
MyStandardButton('Submit', semanticId: 'submit-note')   // pinned where it matters
```

What cannot be auto-generated is a **unique, stable** identifier value
when two same-labelled controls share a screen — the framework cannot
invent a distinguishing id. So one per-instance disambiguator is
irreducible; the app chooses how cheap it is (a slug-of-label default
with a `semanticId` override only where a durable test handle is needed).
This is strictly cheaper than per-call-site `Semantics`.

For action/view widgets specifically, the library's `semanticIdentifier`
hook **is** the auto-instrument path: a `MySubmitButton` built on
`ActionBuilder` threads its `semanticId` into the builder, so every
instance gets both the click target and the live
`value=submitting|success|denied` for free.

The library ships the convention (this section) and the builder hook —
not a helper widget. `ValueKey` does not map to
`flt-semantics-identifier`, so the `Semantics(identifier:)` wrap is the
real seam; there is no free ride from existing keys.

## Testing

- **Library (TDD):** a `flutter_test` widget test written first, asserting
  that with `semanticIdentifier` set, `tester.getSemantics(...)` exposes
  the identifier and the correct state token across each transition; and
  that with it `null`, no extra semantics node is introduced.
- **End-to-end:** `submit-note.spec.ts` is the integration proof, run
  locally via `scripts/run-e2e.sh`.

## Risks and verification steps

1. **TextField + semantics on web** (flutter/flutter#155323): wrapping a
   `TextField` directly in `Semantics(identifier:)` historically did not
   surface on web due to semantics merging. Verify `submit-note-title`
   actually appears in the DOM on the pinned SDK; fallback:
   `Semantics(identifier:, textField: true, explicitChildNodes: true,
   child:)`, or annotate an enclosing wrapper.
2. **`value` updates surfacing**: verify the lifecycle node's `value`
   flips in the DOM across transitions; fallback: emit a dedicated
   offstage status node with its own `<id>-status` identifier instead of
   folding state into the wrapper's `value`.
3. **CanvasKit async load**: the harness must wait for `flt-semantics-host`
   and the first semantics node before acting; bare timeouts will flake.
4. **Two server processes**: bundle server + demo server lifecycle is
   managed by Playwright `webServer` entries to avoid orphaned processes.

### Downstream-trial findings (hht_diary, 2026-06-03)

The technique was exercised against a real downstream app (the
`clinical_diary` Flutter-web client, driving its CUR-528 font selector) to
validate it outside the example. Three additional web-semantics behaviours
surfaced that downstream annotators should expect — they refine risks 1–2
above:

- **Zero-area semantics nodes are pruned on web.** A dedicated machine-
  readable readout node built as `Semantics(value: x, child:
  const SizedBox.shrink())` (0×0) does **not** appear in the web DOM — the
  engine drops zero-area nodes. Give such a node a non-zero footprint
  (`SizedBox(width: 1, height: 1)`) and `container: true` so it
  materializes. (Relevant to the "dedicated `<id>-status` node" fallback
  in risk 2 — make the fallback node non-zero-area.)
- **`Semantics(value:)` carrier depends on the node's role.** On an
  interactive/role-bearing node (e.g. the `ActionBuilder` wrapper) the
  value surfaces as `aria-label`. On a plain *leaf* node it instead
  renders as the element's **text content** (a nested `<span>`), with no
  `aria-label`. Readers should fall back from `aria-label` to trimmed
  `textContent`. (Refines risk 2's "verify the carrier attribute": there
  is no single carrier — it varies by node role.)
- **A collapsed `DropdownButtonFormField` reuses its option identifier.**
  A closed dropdown renders its *selected* item as a nested node carrying
  the same `font-option-<x>` identifier inside the (pointer-events:none)
  selector container; once the overlay opens, that identifier exists
  twice. Open the dropdown by clicking the visible selected label, and
  select the option via the **last** matching node (the overlay node
  mounts after the collapsed one), with a `getByText(displayName)`
  fallback. General lesson: overlay/menu widgets can duplicate an
  identifier across the collapsed control and the open overlay — prefer
  `.last()` or text-scoped selection for menu items.

These also reinforce the core debugging discipline: when a selector
misses, dump `document.querySelectorAll('[flt-semantics-identifier]')`
(and the open-overlay nodes) to see the actual DOM before adjusting an
annotation or selector.

## Scope boundaries (YAGNI)

In scope: the `reaction_widgets` `semanticIdentifier` hook; the example
app vertical slice (login -> submit -> notes list); the TypeScript
Playwright harness; local run script + README steps; the convention
documentation; one clarifying contract assertion.

Out of scope (deliberately deferred):

- CI integration (Chromium install, headless server orchestration) — a
  follow-up ticket once the harness is proven locally.
- Admin-panel grant/revoke annotation.
- A shipped `Semantics` helper widget or slug utility in
  `reaction_widgets`.
- Any `reaction_widgets_testing` or signals-adapter change.

## Pinned environment

- `reaction/example` targets Flutter `>=3.38.7`, Dart SDK `^3.10.7`.
- Playwright (TypeScript, `@playwright/test`) under
  `reaction/example/e2e/`, Chromium engine.
- **Install approach: host (no container).** Playwright runs directly on
  the dev host. Rationale: the host already has Node, a Flutter SDK,
  Chrome, and a cached Playwright Chromium build, so a host install is the
  fastest path with no container networking glue. The Flutter SDK lives at
  `~/flutter-sdk/flutter/bin` (not on the default PATH — the bundled `dart`
  is what resolves), so `run-e2e.sh` prepends it when `flutter` is absent.
  A container-based, version-pinned setup (the official
  `mcr.microsoft.com/playwright` image) is the natural choice for the
  deferred CI job, where reproducibility outweighs iteration speed.
