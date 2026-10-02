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

  // Verifies: EVS-PRD-cross-process-event-transport/J
  test('pingInterval defaults to null (no keepalive)', () async {
    final h = build();
    addTearDown(h.dispose);
    expect(h.pingInterval, isNull);
  });

  // Verifies: EVS-PRD-cross-process-event-transport/J
  test('pingInterval round-trips a supplied interval', () async {
    final h = build(pingInterval: const Duration(seconds: 20));
    addTearDown(h.dispose);
    expect(h.pingInterval, const Duration(seconds: 20));
  });
}
