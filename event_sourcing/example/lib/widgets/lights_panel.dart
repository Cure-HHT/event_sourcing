import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_datastore_demo/lights_materializer.dart';
import 'package:event_sourcing_datastore_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

/// Three RGB "lights" rendered from the `rgb_lights` materialized view.
/// Each light's `is_on` state is toggled on every press of its
/// corresponding button (RED / GREEN / BLUE in the top action bar).
///
/// Subscribes via `eventStore.subscribe<StoredEvent>(...)` in `Events` mode
/// so the panel re-fetches view rows whenever any event lands — no timer,
/// no legacy `watchView` poll. Demonstrates the reactive read primitive
/// end-to-end: button press → event appended → `LightsMaterializer.applyInTxn`
/// toggles the row → subscription delta fires → panel re-fetches and re-renders.
class LightsPanel extends StatefulWidget {
  const LightsPanel({
    required this.backend,
    required this.eventStore,
    super.key,
  });

  final StorageBackend backend;
  final EventStore eventStore;

  @override
  State<LightsPanel> createState() => _LightsPanelState();
}

class _LightsPanelState extends State<LightsPanel> {
  StreamSubscription<Update<StoredEvent>>? _eventsSub;
  Map<String, _LightState> _state = const <String, _LightState>{};

  @override
  void initState() {
    super.initState();
    _refresh();
    // Re-fetch view rows on every event arrival (including button-press
    // events that trigger LightsMaterializer). No view-level subscription
    // is needed because rgb_lights is populated by a custom in-transaction
    // fold rather than a ProjectionRegistry spec.
    _eventsSub = widget.eventStore
        .subscribe<StoredEvent>(
          const SubscriptionFilter(
            entryTypes: <String>{
              'red_button_pressed',
              'green_button_pressed',
              'blue_button_pressed',
            },
          ),
          const Events(),
        )
        .listen((_) {
          if (!mounted) return;
          _refresh();
        });
  }

  @override
  void dispose() {
    _eventsSub?.cancel();
    super.dispose();
  }

  Future<void> _refresh() async {
    try {
      final rows = await widget.backend.findViewRows(
        LightsMaterializer.viewKey,
      );
      if (!mounted) return;
      setState(() {
        _state = <String, _LightState>{
          for (final Map<String, Object?> row in rows)
            row['color']! as String: _LightState(
              isOn: (row['is_on'] as bool?) ?? false,
              lastToggledAt: row['last_toggled_at'] as String?,
            ),
        };
      });
    } catch (_) {
      // Non-fatal; next event tick retries.
    }
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(color: DemoColors.bg, border: demoBorder),
      padding: const EdgeInsets.all(8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          const Text('LIGHTS', style: DemoText.header),
          const SizedBox(height: 8),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceEvenly,
            children: <Widget>[
              _light('red', DemoColors.red),
              _light('green', DemoColors.green),
              _light('blue', DemoColors.blue),
            ],
          ),
          const SizedBox(height: 8),
          const Text(
            'press RED / GREEN / BLUE in the\n'
            'action bar to toggle the matching\n'
            'light. driven by subscribe, not poll.',
            style: TextStyle(
              color: DemoColors.pending,
              fontFamily: DemoText.fontFamilyMonospace,
              fontSize: 12,
            ),
          ),
        ],
      ),
    );
  }

  Widget _light(String color, Color paint) {
    final s = _state[color];
    final isOn = s?.isOn ?? false;
    // Off: dimmed to ~15% alpha so the color identity stays visible but
    // the light reads as inactive. On: full brightness with a yellow
    // outline matching the panel's selection cue (DemoColors.selectedOutline).
    final fill = isOn ? paint : paint.withAlpha(38);
    return Column(
      mainAxisSize: MainAxisSize.min,
      children: <Widget>[
        Container(
          width: 64,
          height: 64,
          decoration: BoxDecoration(
            color: fill,
            border: Border.all(
              color: isOn ? DemoColors.selectedOutline : DemoColors.border,
              width: 3,
            ),
          ),
        ),
        const SizedBox(height: 4),
        Text(
          color,
          style: const TextStyle(
            color: DemoColors.fg,
            fontFamily: DemoText.fontFamilyMonospace,
            fontSize: 12,
          ),
        ),
        Text(
          isOn ? 'on' : 'off',
          style: TextStyle(
            color: isOn ? DemoColors.sent : DemoColors.pending,
            fontFamily: DemoText.fontFamilyMonospace,
            fontSize: 12,
          ),
        ),
      ],
    );
  }
}

class _LightState {
  const _LightState({required this.isOn, required this.lastToggledAt});
  final bool isOn;
  final String? lastToggledAt;
}
