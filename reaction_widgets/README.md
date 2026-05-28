# reaction_widgets

Headless Flutter widget primitives for apps built on the `reaction`
package.

This package is **headless**: it ships builder primitives, imperative
listeners, a scope-threading `InheritedWidget`, a permission gate, an
error sink, and widget-test doubles. It ships **no** rendered or styled
widgets. Each downstream app provides its own sugar widgets (buttons,
lists, theming) on top of the builders, sized appropriately for its
modality (mobile vs web vs desktop).

See `spec/prd-reaction.md`, in particular `EVS-PRD-reaction-widget-contract`,
for the normative contract.
