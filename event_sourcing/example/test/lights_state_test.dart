// Tests for the app-side LightsState fold: the lights are computed from the
// raw button-press events (replay, then live), not written into a library
// view table.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/lights_state.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _source = Source(
  hopId: 'mobile-device',
  identifier: 'lights-test',
  softwareVersion: 'demo@1.0.0',
);

Future<(EventStoreBundle, SembastBackend)> _open(
  DatabaseFactory factory,
  String name,
) async {
  final db = await factory.openDatabase(name);
  final backend = SembastBackend(database: db);
  final bundle = await bootstrapEventStore(
    backend: backend,
    source: _source,
    entryTypes: allDemoEntryTypes,
    destinations: const <Destination>[],
  );
  return (bundle, backend);
}

Future<void> _press(EventStore store, String entryType) async {
  await store.append(
    entryType: entryType,
    aggregateId: 'press-${DateTime.now().microsecondsSinceEpoch}',
    aggregateType: demoAggregateTypeByEntryTypeId[entryType]!,
    eventType: 'finalized',
    data: const <String, Object?>{},
    initiator: const UserInitiator('demo-user-1'),
  );
}

/// Waits until [lights] reports [color] as [on].
Future<void> _until(
  LightsState lights,
  String color, {
  required bool on,
}) async {
  for (var i = 0; i < 200; i++) {
    if ((lights.value[color]?.isOn ?? false) == on) return;
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  fail('$color never became ${on ? 'on' : 'off'}: ${lights.value}');
}

void main() {
  test('three presses of red turn it on, off, on', () async {
    final factory = newDatabaseFactoryMemory();
    final (bundle, backend) = await _open(factory, 'lights-live.db');
    final lights = LightsState();
    await lights.attach(bundle.eventStore);
    expect(lights.value['red']?.isOn ?? false, isFalse);

    await _press(bundle.eventStore, 'red_button_pressed');
    await _until(lights, 'red', on: true);
    await _press(bundle.eventStore, 'red_button_pressed');
    await _until(lights, 'red', on: false);
    await _press(bundle.eventStore, 'red_button_pressed');
    await _until(lights, 'red', on: true);
    expect(lights.value['green']?.isOn ?? false, isFalse);
    expect(lights.value['blue']?.isOn ?? false, isFalse);

    await lights.detach();
    lights.dispose();
    await bundle.eventStore.close();
    await backend.close();
  });

  test('replay after reopen reproduces the state', () async {
    final factory = newDatabaseFactoryMemory();
    final (first, firstBackend) = await _open(factory, 'lights-replay.db');
    final live = LightsState();
    await live.attach(first.eventStore);
    await _press(first.eventStore, 'red_button_pressed');
    await _press(first.eventStore, 'green_button_pressed');
    await _press(first.eventStore, 'green_button_pressed');
    await _press(first.eventStore, 'blue_button_pressed');
    await _until(live, 'blue', on: true);
    final before = Map<String, LightState>.of(live.value);
    await live.detach();
    live.dispose();
    await first.eventStore.close();
    await firstBackend.close();

    final (second, secondBackend) = await _open(factory, 'lights-replay.db');
    final replayed = LightsState();
    await replayed.attach(second.eventStore);
    expect(replayed.value, before);
    expect(replayed.value['red']!.isOn, isTrue);
    expect(replayed.value['green']!.isOn, isFalse);
    expect(replayed.value['blue']!.isOn, isTrue);
    await replayed.detach();
    replayed.dispose();
    await second.eventStore.close();
    await secondBackend.close();
  });

  test('reattaching to a second store folds that store alone', () async {
    final factory = newDatabaseFactoryMemory();
    final (first, firstBackend) = await _open(factory, 'lights-first.db');
    final lights = LightsState();
    await lights.attach(first.eventStore);
    for (var i = 0; i < 5; i++) {
      await _press(first.eventStore, 'green_button_pressed');
    }
    await _press(first.eventStore, 'red_button_pressed');
    await _until(lights, 'red', on: true);
    expect(lights.value['green']!.isOn, isTrue);

    // A second database whose sequence numbers restart below the first's.
    final (second, secondBackend) = await _open(factory, 'lights-second.db');
    await _press(second.eventStore, 'blue_button_pressed');
    await lights.attach(second.eventStore);
    expect(lights.value.keys, <String>['blue']);
    expect(lights.value['blue']!.isOn, isTrue);

    // Presses on the first store no longer reach it; the second's do.
    await _press(first.eventStore, 'red_button_pressed');
    await _press(second.eventStore, 'red_button_pressed');
    await _until(lights, 'red', on: true);
    expect(lights.value.keys.toSet(), <String>{'blue', 'red'});

    await lights.detach();
    lights.dispose();
    await first.eventStore.close();
    await firstBackend.close();
    await second.eventStore.close();
    await secondBackend.close();
  });

  test('disposing during attach applies nothing and leaves no live '
      'subscription', () async {
    final factory = newDatabaseFactoryMemory();
    final (bundle, backend) = await _open(factory, 'lights-dispose.db');
    await _press(bundle.eventStore, 'red_button_pressed');

    for (final yields in <int>[0, 1, 2, 3, 5, 8]) {
      final lights = LightsState();
      var notified = 0;
      lights.addListener(() => notified += 1);
      final attaching = lights.attach(bundle.eventStore);
      for (var i = 0; i < yields; i++) {
        await Future<void>.delayed(Duration.zero);
      }
      lights.dispose();
      await attaching;
      await _press(bundle.eventStore, 'red_button_pressed');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(notified, lessThanOrEqualTo(1), reason: 'yields=$yields');
    }

    await bundle.eventStore.close();
    await backend.close();
  });
}
