// Widget test: a button press changes the rendered light.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/lights_state.dart';
import 'package:event_sourcing_demo/widgets/lights_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  testWidgets('pressing red turns the red light on', (tester) async {
    final lights = LightsState();
    late EventStoreBundle bundle;
    late SembastBackend backend;
    await tester.runAsync(() async {
      final db = await newDatabaseFactoryMemory().openDatabase('panel.db');
      backend = SembastBackend(database: db);
      bundle = await bootstrapEventStore(
        backend: backend,
        source: const Source(
          hopId: 'mobile-device',
          identifier: 'panel-test',
          softwareVersion: 'demo@1.0.0',
        ),
        entryTypes: allDemoEntryTypes,
        destinations: const <Destination>[],
      );
      await lights.attach(bundle.eventStore);
    });

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(body: LightsPanel(lights: lights)),
      ),
    );
    expect(find.byKey(const ValueKey<String>('light-red-off')), findsOneWidget);

    await tester.runAsync(() async {
      await bundle.eventStore.append(
        entryType: 'red_button_pressed',
        aggregateId: 'press-1',
        aggregateType: 'RedButtonPressed',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const UserInitiator('demo-user-1'),
      );
      for (var i = 0; i < 200 && !(lights.value['red']?.isOn ?? false); i++) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
    });
    await tester.pump();
    expect(find.byKey(const ValueKey<String>('light-red-on')), findsOneWidget);
    expect(
      find.byKey(const ValueKey<String>('light-green-off')),
      findsOneWidget,
    );

    await tester.runAsync(() async {
      await lights.detach();
      await bundle.eventStore.close();
      await backend.close();
    });
    lights.dispose();
  });
}
