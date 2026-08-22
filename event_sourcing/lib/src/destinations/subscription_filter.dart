// Implements: EVS-PRD-destinations/B
// SubscriptionFilter: the per-destination
// event-selection predicate that determines which events are enqueued to a
// given destination (entry_type / event_type / aggregate_type allow-lists +
// optional escape-hatch predicate).
import 'package:collection/collection.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';

/// Predicate function signature consulted by [SubscriptionFilter] after
/// the allow-lists have passed.
typedef SubscriptionPredicate = bool Function(StoredEvent event);

/// Predicate that selects which events are enqueued to a Destination.
///
/// Composes the system-event opt-in plus four optional user-event
/// constraints, combined with logical AND:
///
/// 1. [includeSystemEvents] — opt-in for events whose `entryType` is in
///    [kReservedSystemEntryTypeIds]. When `true`, system events bypass
///    [entryTypes] entirely and pass [matches]. When `false` (the
///    default), system events are rejected by [matches] regardless of
///    [entryTypes] content. /
/// 2. [entryTypes] — allow-list over `event.entry_type` for user events.
///    `null` means "any user entry type"; an **empty set** means
///    "nothing matches" (the distinction is deliberate; ).
///    Reserved system entry types route through [includeSystemEvents]
///    and never consult this set.
/// 3. [eventTypes] — allow-list over `event.event_type` with the same
///    null-vs-empty semantics as [entryTypes].
/// 4. [aggregateTypes] — allow-list over `event.aggregateType`. `null`
///    means "any aggregate type"; an empty set means "no aggregate types
///    match". Used by projection specs to restrict intake to a specific
///    aggregate family.
/// 5. [predicate] — optional escape-hatch function consulted only after
///    the allow-lists pass. Returns `true` for the event to match.
///
/// A filter with no constraints matches every user event and (since
/// [includeSystemEvents] defaults to `false`) admits no system events,
/// which is the default for destinations that want unconditional fan-out
/// of user events without forensic visibility into config-change audits.
class SubscriptionFilter {
  const SubscriptionFilter({
    this.entryTypes,
    this.eventTypes,
    this.aggregateTypes,
    this.predicate,
    this.includeSystemEvents = false,
  });

  /// Allow-list over `event.entry_type` for user events. `null` = match
  /// all user entry types; an empty set = match no user entry types.
  /// Reserved system entry types ignore this set and route through
  /// [includeSystemEvents].
  final Set<String>? entryTypes;

  /// Allow-list over `event.event_type`. `null` = match all event types;
  /// an empty set = match no event types.
  final Set<String>? eventTypes;

  /// Allow-list over `event.aggregateType`. `null` = match all aggregate
  /// types; an empty set = match no aggregate types.
  final Set<String>? aggregateTypes;

  /// Escape-hatch consulted after the allow-lists pass. `null` means
  /// "no additional filtering"; a non-null predicate must return `true`
  /// for the event to match.
  final SubscriptionPredicate? predicate;

  /// Opt-in for events whose `entryType` is in
  /// [kReservedSystemEntryTypeIds]. When `true`, [matches] admits every
  /// reserved system entry type regardless of [entryTypes] content (the
  /// list is bypassed for system events). When `false`, [matches]
  /// rejects every reserved system entry type regardless of [entryTypes]
  /// content. Defaults to `false` so destinations carrying user
  /// payloads do not accidentally admit forensic audit events.
  ///
  /// A destination subscribing to forensic / audit visibility on an
  /// upstream node's local-state mutations sets this to `true` (paired
  /// with a possibly-empty [entryTypes] when the destination wants only
  /// system events).
  final bool includeSystemEvents;

  /// Returns `true` iff [event] should be enqueued to the destination
  /// that owns this filter. Deterministic: identical inputs produce
  /// identical outputs, so filter evaluations are reproducible from the
  /// event alone.
  ///
  /// Reserved system entry types (those in [kReservedSystemEntryTypeIds])
  /// are admitted iff [includeSystemEvents] is `true`. The
  /// [entryTypes] allow-list is consulted only for user entry types.
  /// [eventTypes] and [predicate] are consulted for both system and
  /// user events that cleared the entry-type gate.
  bool matches(StoredEvent event) {
    if (kReservedSystemEntryTypeIds.contains(event.entryType)) {
      if (!includeSystemEvents) return false;
      // System event admitted past the entry-type gate; eventTypes /
      // predicate constraints still apply (they refine within the
      // admitted set).
    } else {
      final entryTypes = this.entryTypes;
      if (entryTypes != null && !entryTypes.contains(event.entryType)) {
        return false;
      }
    }
    final eventTypes = this.eventTypes;
    if (eventTypes != null && !eventTypes.contains(event.eventType)) {
      return false;
    }
    final aggregateTypes = this.aggregateTypes;
    if (aggregateTypes != null &&
        !aggregateTypes.contains(event.aggregateType)) {
      return false;
    }
    final predicate = this.predicate;
    if (predicate != null && !predicate(event)) {
      return false;
    }
    return true;
  }

  /// Structural equality over the three allow-list sets plus value
  /// equality on [includeSystemEvents]. Predicate equality is
  /// **identity-based** (`identical(predicate, other.predicate)`) — two
  /// distinct closures with the same source are NOT equal, even if they
  /// would return the same value for every event. Closures have no
  /// defensible structural equality in Dart, so identity is the most
  /// honest semantic: a deliberate choice, not an oversight. Equality
  /// is primarily useful for codec round-trip assertions and config
  /// diffs; subscriptions that need predicate equivalence should use a
  /// named-predicate registry instead.
  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other.runtimeType != runtimeType) return false;
    return other is SubscriptionFilter &&
        const SetEquality<String>().equals(entryTypes, other.entryTypes) &&
        const SetEquality<String>().equals(eventTypes, other.eventTypes) &&
        const SetEquality<String>().equals(
          aggregateTypes,
          other.aggregateTypes,
        ) &&
        includeSystemEvents == other.includeSystemEvents &&
        identical(predicate, other.predicate);
  }

  @override
  int get hashCode => Object.hash(
    const SetEquality<String>().hash(entryTypes),
    const SetEquality<String>().hash(eventTypes),
    const SetEquality<String>().hash(aggregateTypes),
    includeSystemEvents,
    identityHashCode(predicate),
  );
}
