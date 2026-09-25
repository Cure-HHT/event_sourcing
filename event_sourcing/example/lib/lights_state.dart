import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/foundation.dart';

/// One light's state.
@immutable
class LightState {
  const LightState({required this.isOn, required this.lastToggledAt});

  final bool isOn;

  /// Client timestamp of the press that last toggled the light.
  final DateTime lastToggledAt;

  @override
  bool operator ==(Object other) =>
      other is LightState &&
      other.isOn == isOn &&
      other.lastToggledAt == lastToggledAt;

  @override
  int get hashCode => Object.hash(isOn, lastToggledAt);

  @override
  String toString() => 'LightState(isOn: $isOn, at: $lastToggledAt)';
}

/// App-side state of the three RGB lights, keyed by color.
///
/// Each press of a color's button toggles that light. The state is computed
/// by the app from the raw button-press events -- replayed from the log on
/// [attach], then folded live from `subscribe(Events)` -- rather than
/// written into a library view table: the library's views are written only
/// by its projection interpreter, and an application that wants its own
/// interpretation of the log computes it from the events, as this does.
class LightsState extends ValueNotifier<Map<String, LightState>> {
  LightsState() : super(const <String, LightState>{});

  /// The button-press entry types and the color each one toggles.
  static const Map<String, String> colorByEntryType = <String, String>{
    'red_button_pressed': 'red',
    'green_button_pressed': 'green',
    'blue_button_pressed': 'blue',
  };

  static final SubscriptionFilter _filter = SubscriptionFilter(
    entryTypes: colorByEntryType.keys.toSet(),
  );

  StreamSubscription<Update<StoredEvent>>? _live;

  /// Highest sequence number folded so far; an event at or below it has
  /// already been applied.
  int _folded = 0;

  /// Incremented by every [attach], [detach] and [dispose]; an attach whose
  /// generation is no longer current has been superseded and stops.
  int _generation = 0;

  bool _disposed = false;

  /// Replays the button presses already in [store]'s log, then folds each
  /// new one as it is appended. Returns once the replay is folded.
  ///
  /// Attaching starts from no lights, so a state reattached to another
  /// store (or to a store whose database was reset) folds that store's log
  /// alone. The live subscription opens before the replay reads the log and
  /// buffers what it receives meanwhile, so no press appended during the
  /// replay is missed; the sequence number drops any press both deliver. An
  /// attach superseded by a later attach, a detach or a dispose cancels its
  /// own subscription and applies nothing.
  Future<void> attach(EventStore store) async {
    if (_disposed) throw StateError('LightsState used after dispose');
    final generation = ++_generation;
    final previous = _live;
    _live = null;
    await previous?.cancel();
    if (generation != _generation) return;
    _folded = 0;
    value = const <String, LightState>{};
    final buffered = <StoredEvent>[];
    var replaying = true;
    final live = store.subscribe<StoredEvent>(_filter, const Events()).listen((
      update,
    ) {
      if (update is! Delta<StoredEvent> || generation != _generation) return;
      if (replaying) {
        buffered.add(update.value);
      } else {
        _apply(<StoredEvent>[update.value]);
      }
    });
    _live = live;
    final history = <StoredEvent>[
      for (final entryType in colorByEntryType.keys)
        ...await store.backend.findAllEvents(entryType: entryType),
    ];
    if (generation != _generation) {
      await live.cancel();
      return;
    }
    replaying = false;
    _apply(<StoredEvent>[...history, ...buffered]);
  }

  /// Stops folding live presses, and stops any attach still replaying.
  Future<void> detach() async {
    _generation += 1;
    final live = _live;
    _live = null;
    await live?.cancel();
  }

  void _apply(List<StoredEvent> events) {
    final ordered = events.toList()
      ..sort((a, b) => a.sequenceNumber.compareTo(b.sequenceNumber));
    final next = Map<String, LightState>.of(value);
    var changed = false;
    for (final event in ordered) {
      if (event.sequenceNumber <= _folded) continue;
      _folded = event.sequenceNumber;
      final color = colorByEntryType[event.entryType];
      if (color == null) continue;
      next[color] = LightState(
        isOn: !(next[color]?.isOn ?? false),
        lastToggledAt: event.clientTimestamp.toUtc(),
      );
      changed = true;
    }
    if (changed) value = Map<String, LightState>.unmodifiable(next);
  }

  @override
  void dispose() {
    _disposed = true;
    unawaited(detach());
    super.dispose();
  }
}
