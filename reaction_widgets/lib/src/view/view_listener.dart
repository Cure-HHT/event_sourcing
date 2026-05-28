// Implements: EVS-PRD-reaction-widget-contract/D, /G

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

/// Imperative side-effect widget for view-subscription updates.
///
/// Subscribes to a view via the composed [ReactionScope]'s [ViewSource]
/// and fires [onUpdate] on every emitted [Update] — WITHOUT rebuilding
/// the [child] subtree. Use for showing modals/snackbars/navigation/
/// analytics in response to view changes, where a rebuild would be
/// inappropriate.
///
/// For a builder-shaped pattern (rendering based on accumulated view
/// state), use [ViewBuilder] instead. Unlike [ViewBuilder], this widget
/// does NOT accumulate rows or surface a [ViewState] — it forwards raw
/// [Update] events directly to [onUpdate] so the caller can decide what
/// to do per emission.
///
/// Headless per `EVS-PRD-reaction-widget-contract`-G: this widget renders
/// only [child]; it adds no decoration of its own.
class ViewListener<T> extends StatefulWidget {
  const ViewListener({
    super.key,
    required this.viewName,
    required this.mapper,
    required this.onUpdate,
    required this.child,
    this.filter,
    this.aggregates,
  });

  /// Registered `ProjectionSpec.viewName` to subscribe to.
  final String viewName;

  /// Applied to each row's raw `Map<String, Object?>` to produce typed
  /// values of [T]. Forwarded to [ViewSource.watch].
  final T Function(Map<String, Object?>) mapper;

  /// Optional substrate-side [SubscriptionFilter]. Forwarded to
  /// [ViewSource.watch].
  final SubscriptionFilter? filter;

  /// Optional aggregate-id allow-list. Forwarded to [ViewSource.watch].
  final Set<String>? aggregates;

  /// Fired on every [Update] emitted by the underlying subscription.
  /// Receives the current [BuildContext] so the callback can navigate,
  /// show a snackbar, etc.
  final void Function(BuildContext context, Update<T> update) onUpdate;

  /// The subtree to render. [ViewListener] does not rebuild [child] in
  /// response to view updates — that is the entire point of the
  /// imperative shape (per `EVS-PRD-reaction-widget-contract`-D).
  final Widget child;

  @override
  State<ViewListener<T>> createState() => _ViewListenerState<T>();
}

class _ViewListenerState<T> extends State<ViewListener<T>> {
  StreamSubscription<Update<T>>? _sub;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_sub != null) return;
    final scope = ReActionScope.of(context);
    _sub = scope.viewSource
        .watch<T>(
          viewName: widget.viewName,
          mapper: widget.mapper,
          filter: widget.filter,
          aggregates: widget.aggregates,
        )
        .listen((u) {
          if (!mounted) return;
          widget.onUpdate(context, u);
        });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.child;
}
