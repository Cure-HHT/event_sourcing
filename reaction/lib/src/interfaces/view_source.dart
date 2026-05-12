// Implements: EVS-PRD-view-subscriber — defines the ViewSource
// transport-agnostic interface over the substrate's subscribe<T>
// AggregateMode primitive.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Subscribes to row-level updates for a registered `ProjectionSpec`'s
/// materialized view. Mirrors the substrate's `EventStore.subscribe<T>`
/// API exactly for `AggregateMode<T>`-style subscriptions.
///
/// The returned stream delivers:
///
/// 1. `Snapshot<T>` × N — one per row currently in the view.
/// 2. `EndOfReplay<T>` — exactly one marker; subscriber knows the
///    snapshot is complete and may transition UI from skeleton/loading
///    to live.
/// 3. `Delta<T>` / `Tombstone<T>` × ∞ — live updates as events land.
///
/// Two impls ship with `reaction`:
///
/// - [LocalViewSource] (in-process): delegates to
///   `eventStore.subscribe<T>(filter, AggregateMode(viewName, mapper,
///   aggregates))`.
/// - [RemoteViewSource] (cross-process; Plan B-remote): opens a WS
///   subscription with `(subscriptionId, viewName, filter, aggregates)`;
///   deserializes `Update<Map<String, Object?>>` envelopes and applies
///   the consumer's mapper client-side.
abstract interface class ViewSource {
  /// Watch a view's row-level updates.
  ///
  /// - [viewName]: matches a registered `ProjectionSpec.viewName`.
  /// - [mapper]: applied to each row's `Map<String, Object?>` to
  ///   produce typed values.
  /// - [filter]: optional `SubscriptionFilter` on entry/event/aggregate
  ///   types. The filter is applied during replay (snapshot phase); for
  ///   live `Delta` emissions after `EndOfReplay`, only [aggregates]
  ///   narrowing is honored — `SubscriptionFilter.entryTypes` is not
  ///   consulted on the live path. Use [aggregates] to scope live
  ///   delivery to specific aggregate IDs.
  /// - [aggregates]: optional allow-list of aggregate IDs to scope
  ///   delivery (substrate's `AggregateMode.aggregates`).
  ///
  /// The stream uses Dart's standard cancellation semantics — call
  /// `.cancel()` on the resulting subscription to dispose.
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  });
}
