// Implements: EVS-PRD-reaction-widget-contract/C, /G, /I, /J

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';
import 'package:reaction_widgets/src/view/view_state.dart';

/// Builder function for [ViewBuilder]. Receives the current
/// [ViewState] (Loading / Ready / Stale).
typedef ViewBuilderFn<T> =
    Widget Function(BuildContext context, ViewState<T> state);

/// Headless Builder primitive for view subscription.
///
/// Subscribes to a registered view via the composed
/// [ReactionScope]'s [ViewSource], accumulates rows from the substrate's
/// `Snapshot × N → EndOfReplay → Delta / Tombstone × ∞` stream, and
/// surfaces a sealed [ViewState] to the caller-supplied [builder].
///
/// Transitions, per `EVS-PRD-reaction-widget-contract`-I / -J:
///
/// - Default (`isProgressive: false`): start in [Loading]; transition to
///   [Ready] exactly once `EndOfReplay` arrives. Subsequent
///   [Delta] / [Tombstone] updates re-emit [Ready] with the updated row
///   set.
/// - `isProgressive: true`: re-emit [Ready] after every accumulated
///   row during snapshot replay, allowing large-view first-paint
///   without blocking on the full snapshot.
/// - On a [ConnectionStatus] change to [Reconnecting] or [Disconnected],
///   surface [Stale] retaining the last [Ready.rows] as `lastRows`. On
///   reconnect ([Connected]) after a [Stale] state, reset rows and
///   re-enter [Loading] until the fresh snapshot completes — the
///   substrate's auto-reconnect re-issues every active subscribe with
///   a fresh `Snapshot × N → EndOfReplay → live` per
///   `EVS-PRD-cross-process-event-transport`-H.
///
/// Renders nothing of its own (headless, per
/// `EVS-PRD-reaction-widget-contract`-G); delegates rendering entirely
/// to [builder].
///
/// ### Row identity
///
/// The substrate's [Snapshot]/[Delta] variants carry the typed [T]
/// value but not the row's aggregate id (only [Tombstone] does). To
/// support in-place [Delta] updates and [Tombstone] removal,
/// [ViewBuilder] requires [aggregateIdOf] — a pure function from [T]
/// to the aggregate id string. Consumer mappers conventionally include
/// the `aggregateId` field stamped by the substrate's `AggregateFold`,
/// so a typical extractor is `(row) => row.aggregateId` (or
/// `(row) => row['aggregateId'] as String` for `Map`-typed views).
class ViewBuilder<T> extends StatefulWidget {
  const ViewBuilder({
    super.key,
    required this.viewName,
    required this.mapper,
    required this.aggregateIdOf,
    required this.builder,
    this.filter,
    this.aggregates,
    this.isProgressive = false,
  });

  /// Registered `ProjectionSpec.viewName` to subscribe to.
  final String viewName;

  /// Applied to each row's raw `Map<String, Object?>` to produce typed
  /// values of [T]. Forwarded to [ViewSource.watch].
  final T Function(Map<String, Object?>) mapper;

  /// Extracts the aggregate id from a mapped row. Required because the
  /// substrate's [Snapshot]/[Delta] variants do not carry the aggregate
  /// id as a separate field on the wire envelope — they carry only the
  /// typed value [T].
  ///
  /// Per the substrate's deliberate Layer-2 convention (see
  /// `reaction/lib/src/wire/update_codec.dart` and
  /// `event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart`),
  /// the aggregate id is stamped into the raw view-row map under the
  /// `'aggregateId'` key before the consumer's [mapper] is invoked.
  /// Consumers therefore know how to extract it from their row type [T]
  /// — typically `(row) => row.aggregateId` for class-typed views, or
  /// `(row) => row['aggregateId'] as String` for `Map`-typed views.
  final String Function(T) aggregateIdOf;

  /// Caller-supplied renderer. Receives the current [ViewState].
  final ViewBuilderFn<T> builder;

  /// Optional substrate-side [SubscriptionFilter]. Forwarded to
  /// [ViewSource.watch].
  final SubscriptionFilter? filter;

  /// Optional aggregate-id allow-list. Forwarded to [ViewSource.watch].
  final Set<String>? aggregates;

  /// When true, expose partial rows during snapshot replay (rather than
  /// surfacing [Loading] until [EndOfReplay]). Default false. See
  /// `EVS-PRD-reaction-widget-contract`-J.
  final bool isProgressive;

  @override
  State<ViewBuilder<T>> createState() => _ViewBuilderState<T>();
}

class _ViewBuilderState<T> extends State<ViewBuilder<T>> {
  StreamSubscription<Update<T>>? _viewSub;
  StreamSubscription<ConnectionStatus>? _statusSub;

  /// Accumulated rows keyed by aggregate id (via [ViewBuilder.aggregateIdOf]).
  /// Insertion-ordered so [Ready.rows] preserves arrival order.
  final Map<String, T> _rows = <String, T>{};
  bool _replayDone = false;
  ViewState<T> _state = Loading<T>();

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_viewSub != null) return;
    final scope = ReActionScope.of(context);
    _viewSub = scope.viewSource
        .watch<T>(
          viewName: widget.viewName,
          mapper: widget.mapper,
          filter: widget.filter,
          aggregates: widget.aggregates,
        )
        .listen(_onUpdate);
    _statusSub = scope.connectionStatusStream.listen(_onStatus);
  }

  void _onUpdate(Update<T> u) {
    switch (u) {
      case Snapshot<T>(:final value):
        // Substrate emits Snapshot(value: null) for an explicitly-listed
        // aggregate that has no row; skip such absent rows.
        if (value != null) {
          _rows[widget.aggregateIdOf(value)] = value;
        }
        if (_replayDone || widget.isProgressive) _emitReady();
      case Delta<T>(:final value):
        _rows[widget.aggregateIdOf(value)] = value;
        if (_replayDone || widget.isProgressive) _emitReady();
      case Tombstone<T>(:final aggregateId):
        _rows.remove(aggregateId);
        if (_replayDone || widget.isProgressive) _emitReady();
      case EndOfReplay<T>():
        _replayDone = true;
        _emitReady();
    }
  }

  void _emitReady() {
    _setState(Ready<T>(List<T>.unmodifiable(_rows.values)));
  }

  void _onStatus(ConnectionStatus s) {
    switch (s) {
      case Connected():
        // On reconnect after Stale, a fresh snapshot+tail will replay
        // (per EVS-PRD-cross-process-event-transport-H); reset rows and
        // re-enter Loading until EndOfReplay arrives.
        if (_state is Stale<T>) {
          _rows.clear();
          _replayDone = false;
          _setState(Loading<T>());
        }
      case Reconnecting():
      case Disconnected():
        // Retain the last-known row set so the builder can render a
        // "stale data + reconnecting banner" affordance rather than
        // blanking. Per EVS-PRD-reaction-widget-contract-I, this
        // transition is driven by ConnectionStatus, not by inference
        // from stream liveness.
        _setState(Stale<T>(List<T>.unmodifiable(_rows.values), s));
    }
  }

  void _setState(ViewState<T> s) {
    if (!mounted) return;
    setState(() => _state = s);
  }

  @override
  void dispose() {
    _viewSub?.cancel();
    _statusSub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.builder(context, _state);
}
