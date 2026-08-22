// Implements: EVS-PRD-reaction-widget-contract/A/B

import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';

/// Threads a [ReactionScope] down the widget tree.
///
/// Mount once near the app root (above `MaterialApp`). Descendants call
/// [ReActionScope.of] to read the scope (and therefore the four library
/// interfaces plus authoritative [ConnectionStatus]).
///
/// Per `EVS-PRD-reaction-widget-contract`-B: descendant widget code is
/// source-identical regardless of whether the composed scope is
/// [LocalScope] or [RemoteScope].
class ReActionScope extends InheritedWidget {
  const ReActionScope({super.key, required this.scope, required super.child});

  /// The composed [ReactionScope] (typically [LocalScope] or
  /// [RemoteScope]) threaded down the tree.
  final ReactionScope scope;

  /// Read the [ReactionScope] from [context].
  ///
  /// Throws [FlutterError] if no [ReActionScope] ancestor is found,
  /// with a message that points the caller at the likely cause
  /// (forgotten root mount).
  static ReactionScope of(BuildContext context) {
    final widget = context.dependOnInheritedWidgetOfExactType<ReActionScope>();
    if (widget == null) {
      throw FlutterError(
        'ReActionScope.of() was called with a context that does not '
        'contain a ReActionScope.\n'
        'Mount a ReActionScope near the root of your app (above '
        'MaterialApp).',
      );
    }
    return widget.scope;
  }

  @override
  bool updateShouldNotify(ReActionScope oldWidget) =>
      !identical(scope, oldWidget.scope);
}
