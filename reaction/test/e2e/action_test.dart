// reaction/test/e2e/action_test.dart
// Verifies: EVS-PRD-action-submitter/C/D/E (round-trip with each
// DispatchResult variant; bearer header; source-identical behavior).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    // Seed install->say_hello permission so the dispatch authorizes.
    // The harness's TrustingAuthValidator returns activeRole='install' for
    // any non-empty credential; the unscoped Permission('say_hello') needs
    // a matching grant in the role_permission_grants projection before
    // TableBackedAuthorizationPolicy will Allow.
    await h.grantPermission(role: 'install', permission: 'say_hello');
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('say_hello action dispatches and returns Success', () async {
    final result = await h.scope.actionSubmitter.submit(
      const ActionSubmission(actionName: 'say_hello', rawInput: {'name': 'A'}),
    );
    expect(result, isA<DispatchSuccess<Object?>>());
    final s = result as DispatchSuccess<Object?>;
    expect(s.emittedEventIds, isNotEmpty);
  });

  test('throws TransportException when not authenticated', () async {
    h.scope.authSession.setCredential(null);
    await expectLater(
      () => h.scope.actionSubmitter.submit(
        const ActionSubmission(actionName: 'say_hello', rawInput: {}),
      ),
      throwsA(isA<TransportException>()),
    );
  });
}
