// Verifies: EVS-PRD-cross-process-event-transport/J
// ReactionHandlers
//   accepts and exposes the optional WebSocket keepalive interval that the
//   `subscriptions` handler threads into shelf_web_socket's webSocketHandler.
//   The actual ping emission is shelf_web_socket's own (tested) behavior; this
//   only verifies our API surface wires the parameter and defaults to null.

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import '../local/test_support/reaction_test_harness.dart';

void main() {
  late ReactionTestHarness substrate;

  setUp(() async {
    substrate = await ReactionTestHarness.open();
  });

  tearDown(() async {
    await substrate.close();
  });

  ReactionHandlers build({Duration? pingInterval}) => ReactionHandlers(
    eventStore: substrate.eventStore,
    dispatcher: substrate.dispatcher,
    policy: substrate.dispatcher.authorization,
    pingInterval: pingInterval,
  );

  test('pingInterval defaults to null (no keepalive)', () async {
    final h = build();
    addTearDown(h.dispose);
    expect(h.pingInterval, isNull);
  });

  test('pingInterval round-trips a supplied interval', () async {
    final h = build(pingInterval: const Duration(seconds: 20));
    addTearDown(h.dispose);
    expect(h.pingInterval, const Duration(seconds: 20));
  });
}
