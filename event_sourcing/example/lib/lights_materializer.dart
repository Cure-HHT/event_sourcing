import 'package:event_sourcing/event_sourcing.dart';

/// Maintains an `rgb_lights` materialized view that toggles three lights
/// (red, green, blue) on/off in response to button-press events. Each
/// press flips the corresponding light's `is_on` state and stamps
/// `last_toggled_at`.
///
/// Used by the example's `LightsPanel` to demonstrate `watchView` —
/// rendering changes are driven by the view writes, without any
/// panel-side polling or event-stream filtering.
///
/// View row shape (one per color, keyed by color name):
///
/// ```dart
/// {'color': 'red', 'is_on': true, 'last_toggled_at': '2026-04-25T...'}
/// ```
///
/// This is a standalone fold helper (not a Materializer subclass). Call
/// [LightsMaterializer.applyEvent] after appending a button-press event
/// to update the view, or [LightsMaterializer.applyInTxn] from within
/// a backend transaction.
class LightsMaterializer {
  const LightsMaterializer();

  static const String viewKey = 'rgb_lights';

  /// Map from this materializer's three event types to the color they
  /// toggle. Defines the universe of events that fall into this fold;
  /// the existing button-press destinations route the same event types
  /// downstream too.
  static const Map<String, String> _entryTypeToColor = <String, String>{
    'red_button_pressed': 'red',
    'green_button_pressed': 'green',
    'blue_button_pressed': 'blue',
  };

  /// Returns true if this fold handles [event].
  static bool appliesTo(StoredEvent event) =>
      _entryTypeToColor.containsKey(event.entryType);

  /// Applies [event] to the `rgb_lights` view inside [txn].
  /// No-ops silently if [event.entryType] is not a button-press type.
  static Future<void> applyInTxn(
    Txn txn,
    StorageBackend backend, {
    required StoredEvent event,
  }) async {
    final color = _entryTypeToColor[event.entryType];
    if (color == null) return;

    final priorRaw = await backend.readViewRowInTxn(txn, viewKey, color);
    final priorOn = (priorRaw?['is_on'] as bool?) ?? false;
    final next = <String, Object?>{
      'color': color,
      'is_on': !priorOn,
      'last_toggled_at': event.clientTimestamp.toUtc().toIso8601String(),
    };
    await backend.upsertViewRowInTxn(txn, viewKey, color, next);
  }
}
