import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:flutter/foundation.dart';

/// The narrower filter "Reconfigure drainer" registers for a destination
/// whose filter is [filter]: one of its entry types (`demo_note` when it has
/// it), everything else as it is. Null when [filter] names fewer than two
/// entry types and cannot be narrowed.
SubscriptionFilter? narrowFilter(SubscriptionFilter filter) {
  final types = filter.entryTypes;
  if (types == null || types.length < 2) return null;
  final keep = types.contains('demo_note')
      ? 'demo_note'
      : (types.toList()..sort()).first;
  return SubscriptionFilter(
    entryTypes: <String>{keep},
    eventTypes: filter.eventTypes,
    aggregateTypes: filter.aggregateTypes,
    includeSystemEvents: filter.includeSystemEvents,
  );
}

/// UI state container for the demo. Holds the cross-panel selection tri-
/// state (aggregate / event / fifo row — mutually exclusive), a binding
/// to the shipped `DestinationRegistry`, and a handle to the process-wide
/// `SyncPolicy` notifier.
///
/// No REQ assertions target this file directly — it is UI plumbing that
/// the widget tasks read through. Per-panel selection highlighting and
/// the DETAIL column both resolve through these getters.
class AppState extends ChangeNotifier {
  AppState({
    required DestinationRegistry registry,
    required this.policyNotifier,
    this.eventStore,
    this.cadence = const Duration(seconds: 1),
  }) : _registry = registry;

  DestinationRegistry _registry;

  /// The registry of the pane's delivery cycle: the destinations it
  /// delivers to, with their configurations. [reconfigureDrainer] replaces
  /// it.
  DestinationRegistry get registry => _registry;

  final ValueNotifier<SyncPolicy> policyNotifier;

  /// The pane's event store, which [reconfigureDrainer] opens a new
  /// registry over. Null in states built without one.
  final EventStore? eventStore;

  /// How often the pane's delivery cycle runs a pass when nothing wakes it
  /// sooner. An append, and every registry operation that commits, wake it
  /// at once.
  final Duration cadence;

  SyncCycle? _cycle;
  Future<SyncCycle>? _starting;

  /// The pane's delivery cycle, once [startDelivery] started it.
  SyncCycle? get cycle => _cycle;

  /// Starts the pane's delivery cycle over [registry]: it fills every
  /// destination's queue from the log and drains it with the policy the
  /// policy bar selects, resolved once per pass. At most one delivery cycle
  /// drains a database; the two panes use two databases, so each runs one.
  /// A call while a start is in flight returns that start.
  Future<SyncCycle> startDelivery() {
    final running = _cycle;
    if (running != null) return Future<SyncCycle>.value(running);
    return _starting ??= () async {
      try {
        final cycle = await SyncCycle.start(
          registry: registry,
          cadence: cadence,
          policyResolver: () => policyNotifier.value,
        );
        _cycle = cycle;
        return cycle;
      } finally {
        _starting = null;
      }
    }();
  }

  /// Closes the pane's delivery cycle, if one is started.
  Future<void> stopDelivery() async {
    final starting = _starting;
    if (starting != null) await starting;
    final cycle = _cycle;
    _cycle = null;
    await cycle?.close();
  }

  Future<void>? _reconfiguring;

  final Set<String> _reconfigured = <String>{};

  /// Destinations whose delivery configuration [reconfigureDrainer]
  /// changed in this pane.
  Set<String> get reconfiguredDestinationIds =>
      Set<String>.unmodifiable(_reconfigured);

  /// Plays the deployment of a new delivery configuration for destination
  /// [id]: closes the pane's delivery cycle, opens a new registry over the
  /// same event store that registers every destination as before except
  /// [id], which it registers with [filter], and starts a delivery cycle
  /// over it. The configuration the new drainer declares for [id] differs
  /// from the one recorded when a halt was honoured, so a recovery of a
  /// halt for reconfiguration is accepted from then on, and the refill
  /// runs under [filter].
  ///
  /// One reconfiguration runs at a time: a call while one is in flight
  /// throws [StateError]. If building the new registry fails, the pane's
  /// delivery cycle is started again over the registry it had.
  Future<void> reconfigureDrainer(String id, SubscriptionFilter filter) {
    if (_reconfiguring != null) {
      throw StateError('a reconfiguration of the drainer is in progress');
    }
    return _reconfiguring = () async {
      try {
        await _reconfigure(id, filter);
      } finally {
        _reconfiguring = null;
      }
    }();
  }

  Future<void> _reconfigure(String id, SubscriptionFilter filter) async {
    final store = eventStore;
    if (store == null) {
      throw StateError("reconfigureDrainer needs the pane's event store");
    }
    final current = _registry.byId(id);
    if (current is! DemoDestination) {
      throw ArgumentError.value(id, 'id', 'not a demo destination here');
    }
    await stopDelivery();
    try {
      final next = DestinationRegistry(eventStore: store);
      for (final d in _registry.all()) {
        await next.addDestination(
          d.id == id ? current.withFilter(filter) : d,
          initiator: const UserInitiator('demo-user-1'),
        );
      }
      _registry = next;
      _reconfigured.add(id);
    } finally {
      // The pane always ends with a delivery cycle over its current
      // registry: the new one, or the one it had if building failed.
      await startDelivery();
      notifyListeners();
    }
  }

  /// Requests a halt of destination [id] for [purpose]; the drainer
  /// honours it by wedging the queue head. Returns the request event's id.
  /// Throws the registry's refusal (a request already open, a head already
  /// wedged).
  Future<String> requestHalt(String id, HaltPurpose purpose) async {
    final eventId = await _registry.requestHalt(
      id,
      initiator: const UserInitiator('demo-user-1'),
      purpose: purpose,
    );
    notifyListeners();
    return eventId;
  }

  /// Cancels destination [id]'s open halt request. Throws the registry's
  /// refusal when none is open.
  Future<void> cancelHalt(String id) async {
    await _registry.cancelHalt(
      id,
      initiator: const UserInitiator('demo-user-1'),
    );
    notifyListeners();
  }

  /// Recovers destination [id]'s wedged head [rowId]. Throws the
  /// registry's refusal (a pending head, a reconfigure halt the drainer's
  /// configuration has not changed for, ...).
  Future<TombstoneAndRefillResult> recover(String id, String rowId) async {
    final result = await _registry.tombstoneAndRefill(
      id,
      rowId,
      initiator: const UserInitiator('demo-user-1'),
    );
    notifyListeners();
    return result;
  }

  /// The pane database's persisted delivery status.
  Future<DeliveryStatus> readDeliveryStatus() => _registry.readDeliveryStatus();

  @override
  void dispose() {
    unawaited(stopDelivery());
    super.dispose();
  }

  String? _selectedAggregateId;
  String? _selectedEventId;
  String? _selectedFifoRowId;
  String? _selectedFifoDestinationId;

  String? get selectedAggregateId => _selectedAggregateId;
  String? get selectedEventId => _selectedEventId;
  String? get selectedFifoRowId => _selectedFifoRowId;

  /// Destination id that owns `_selectedFifoRowId`. FIFO rows in
  /// different destinations can collide on `entry_id` because
  /// `FifoEntry.entryId == eventIds.first` — the library re-uses the
  /// first event's id as the row id. The pair `(destinationId,
  /// entryId)` is what actually identifies a row uniquely.
  String? get selectedFifoDestinationId => _selectedFifoDestinationId;

  void selectAggregate(String? id) {
    _selectedAggregateId = id;
    _selectedEventId = null;
    _selectedFifoRowId = null;
    _selectedFifoDestinationId = null;
    notifyListeners();
  }

  void selectEvent(String? id) {
    _selectedAggregateId = null;
    _selectedEventId = id;
    _selectedFifoRowId = null;
    _selectedFifoDestinationId = null;
    notifyListeners();
  }

  void selectFifoRow(String? destinationId, String? id) {
    _selectedAggregateId = null;
    _selectedEventId = null;
    _selectedFifoRowId = id;
    _selectedFifoDestinationId = destinationId;
    notifyListeners();
  }

  void clearSelection() {
    _selectedAggregateId = null;
    _selectedEventId = null;
    _selectedFifoRowId = null;
    _selectedFifoDestinationId = null;
    notifyListeners();
  }

  /// Every destination currently registered, in registration order. The
  /// FIFO panel renders one column per destination and conditionally
  /// shows the `DemoDestination`-specific knobs (connection / latency /
  /// batch size sliders) on rows whose runtime type carries them; native
  /// destinations get a knob-less column that still renders the FIFO
  /// snapshot, surfacing the storage-shape difference (envelope_metadata
  /// vs wire_payload)
  List<Destination> get destinations => registry.all().toList(growable: false);

  final List<String> _deleted = <String>[];

  /// Destinations this hub deleted and has not registered again, in
  /// deletion order. A deletion keeps the destination's delivered, wedged
  /// and recovered queue items as its delivery record; the FIFO column list
  /// renders a read-only panel of those retained items for each id here
  /// (the live columns come from the registry, which forgets a deleted
  /// destination).
  List<String> get deletedDestinationIds => <String>[
    for (final id in _deleted)
      if (registry.byId(id) == null) id,
  ];

  /// Delegate to `DestinationRegistry.deleteDestination`, record the id so
  /// its retained queue items stay visible, and notify listeners. Throws
  /// what the registry throws, including its refusal while the queue head
  /// is pending.
  Future<void> deleteDestination(String id) async {
    await _registry.deleteDestination(
      id,
      initiator: const UserInitiator('demo-user-1'),
    );
    _deleted
      ..remove(id)
      ..add(id);
    notifyListeners();
  }

  /// Delegate to `DestinationRegistry.addDestination` and notify listeners
  /// so widgets bound to `destinations` rebuild. The demo stamps a
  /// stable `UserInitiator('demo-user-1')` on every UI-driven mutation
  /// so the resulting audit events are visibly attributable to the
  /// demo's single seat.
  Future<void> addDestination(DemoDestination destination) async {
    await _registry.addDestination(
      destination,
      initiator: const UserInitiator('demo-user-1'),
    );
    notifyListeners();
  }
}
