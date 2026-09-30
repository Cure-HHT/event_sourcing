// Implements: EVS-PRD-view-subscriber/A
// defines the ViewSource
// interface whose watch<T> returns Stream<Update<T>> for a given
// (viewName, mapper, filter, aggregates).
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/wire/subscription_messages.dart';

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
/// - `LocalViewSource` (in-process): delegates to
///   `eventStore.subscribe<T>(filter, AggregateMode(...))` with the
///   view name, mapper and aggregates.
/// - `RemoteViewSource` (cross-process): opens a WS
///   subscription with `(subscriptionId, viewName, filter, aggregates)`;
///   deserializes `Update<Map<String, Object?>>` envelopes and applies
///   the consumer's mapper client-side.
// ignore: one_member_abstracts, a pluggable interface with Local and Remote implementations
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
  ///
  /// Errors arrive on the stream's error channel. A
  /// `ViewConvergingRefusal` is transient: the subscription stays open and
  /// its rows follow once the named view is current. A [SubscriptionDenied]
  /// ends the subscription, as does any other error a `RemoteViewSource`
  /// reports for it.
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  });
}

/// Delivered on a [ViewSource.watch] stream's error channel when the server
/// refuses the subscription, naming the refused view and the refusal's
/// [reason]. Terminal: the stream closes after it, and no reconnect
/// re-issues the subscription.
final class SubscriptionDenied implements Exception {
  const SubscriptionDenied({required this.viewName, required this.reason});

  /// The view the refused subscription named.
  final String viewName;

  /// Why the server refused it.
  final SubscriptionDenyReason reason;

  @override
  String toString() =>
      'SubscriptionDenied: subscription_denied for "$viewName" '
      '(${reason.toWire()})';
}
