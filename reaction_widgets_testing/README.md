# reaction_widgets_testing

Widget-test doubles for apps built on `reaction_widgets`. Add as a
`dev_dependency`:

```yaml
dev_dependencies:
  reaction_widgets_testing:
    path: ../reaction_widgets_testing  # or git ref
```

Provides:

- `FakeReaction` — a deterministic `ReactionScope` implementation with a
  driver API for pushing `AuthStatus` / `ConnectionStatus` / `DispatchResult`s /
  view `Update<T>` events / `EffectiveAuthorization` snapshots.
- `pumpReactionWidget(tester, fake:, child:)` — mounts a widget under
  test with a `FakeReaction` threaded via `ReActionScope`.

Per `EVS-PRD-reaction-widget-contract`-H. See
`spec/prd-reaction.md` for the normative test-double contract.

Lives in a sibling package (rather than inside `reaction_widgets/lib/src/testing/`)
so consumers' release builds don't pull `flutter_test` and its
transitive `test_api` / `matcher` / etc. into shipped binaries.
