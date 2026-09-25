import 'package:event_sourcing_demo/lights_state.dart';
import 'package:event_sourcing_demo/widgets/styles.dart';
import 'package:flutter/material.dart';

/// Three RGB "lights" rendered from [LightsState]. Each light's on/off
/// state is toggled by every press of its button (RED / GREEN / BLUE in
/// the top action bar).
///
/// [LightsState] folds the raw button-press events -- replayed from the log,
/// then live from `subscribe(Events)` -- so the panel re-renders when an
/// event lands: button press, event appended, [LightsState] toggles the
/// color, the panel rebuilds. No timer and no view table are involved.
class LightsPanel extends StatelessWidget {
  const LightsPanel({required this.lights, super.key});

  final LightsState lights;

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<Map<String, LightState>>(
      valueListenable: lights,
      builder: (context, state, _) => _panel(state),
    );
  }

  Widget _panel(Map<String, LightState> state) {
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
              _light(state, 'red', DemoColors.red),
              _light(state, 'green', DemoColors.green),
              _light(state, 'blue', DemoColors.blue),
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

  Widget _light(Map<String, LightState> state, String color, Color paint) {
    final isOn = state[color]?.isOn ?? false;
    // Off: dimmed to ~15% alpha so the color identity stays visible but
    // the light reads as inactive. On: full brightness with a yellow
    // outline matching the panel's selection cue (DemoColors.selectedOutline).
    final fill = isOn ? paint : paint.withAlpha(38);
    return Column(
      key: ValueKey<String>('light-$color-${isOn ? 'on' : 'off'}'),
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
