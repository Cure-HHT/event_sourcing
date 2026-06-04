// Reference instrumentation kit for UI automation (CUR-1307).
//
// Demonstrates the "annotate once" pattern from the reaction_widgets
// how-to: a single reusable wrapper that bakes in the web-semantics flags
// every interactive target needs, so call sites stay clean and no bespoke
// `Semantics` is repeated per widget.
//
// On Flutter web a bare `Semantics(identifier:)` around a role-bearing
// child (a button, a text field) is merged away by the flt-semantics
// flattener and the `flt-semantics-identifier` selector disappears.
// `container: true, explicitChildNodes: true` keep the identifier on a
// node of its own. A production app would fold this into its own widget
// kit (e.g. an `AppButton` that wraps its `Semantics` internally); this
// thin wrapper is the example-scale equivalent.
import 'package:flutter/widgets.dart';

class AutomationTarget extends StatelessWidget {
  const AutomationTarget({
    super.key,
    required this.identifier,
    required this.child,
    this.button = false,
    this.field = false,
    this.value,
  });

  /// Stable, locale-independent selector key. Surfaces on web as the
  /// `flt-semantics-identifier` DOM attribute.
  final String identifier;

  /// Marks the node as a button role (for buttons).
  final bool button;

  /// Marks the node as a text field. NOTE: on web this spawns a phantom
  /// DISABLED `<input>` on the wrapper node alongside the real editable
  /// one — tests must fill `input:not([disabled])`.
  final bool field;

  /// Optional machine-readable value (surfaces as `aria-label` on web;
  /// used for row readouts like a note title).
  final String? value;

  final Widget child;

  @override
  Widget build(BuildContext context) => Semantics(
    identifier: identifier,
    button: button,
    textField: field,
    value: value,
    container: true,
    explicitChildNodes: true,
    child: child,
  );
}
