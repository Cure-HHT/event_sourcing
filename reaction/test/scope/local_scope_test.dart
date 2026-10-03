// LocalScope is the in-process composition root. It owns four
// externally-constructed Local* impls, exposes them via the
// ReactionScope interface, reports Connected for its entire lifetime
// since in-process composition has no transport to lose, and throws
// StateError on interface access post-dispose.
//
// The setUp wires a real in-memory substrate via ReactionTestHarness
// so the Local* impls are constructed against live handles — no mocks.
// LocalScope itself is the only unit under test here; the per-impl
// behaviour is covered in reaction/test/local/.
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import '../local/test_support/reaction_test_harness.dart';

void main() {
  group('LocalScope', () {
    late ReactionTestHarness harness;
    late LocalScope scope;
    late LocalAuthSession auth;
    late LocalActionSubmitter submitter;
    late LocalViewSource views;
    late LocalPermissionSource perms;

    setUp(() async {
      harness = await ReactionTestHarness.open();
      auth = LocalAuthSession(defaultActiveRole: 'r');
      submitter = LocalActionSubmitter(
        dispatcher: harness.dispatcher,
        authSession: auth,
      );
      views = LocalViewSource(eventStore: harness.eventStore);
      perms = LocalPermissionSource(
        eventStore: harness.eventStore,
        policy: harness.dispatcher.authorization,
      );
      scope = LocalScope(
        authSession: auth,
        actionSubmitter: submitter,
        viewSource: views,
        permissionSource: perms,
      );
    });

    tearDown(() async {
      // dispose() may have already been called by the test; calling it
      // again is a no-op since _isDisposed is idempotent.
      await scope.dispose();
      await auth.dispose();
      await perms.dispose();
      await harness.close();
    });

    // Verifies: EVS-PRD-reaction-scope/A
    test('implements ReactionScope and exposes the four interfaces', () {
      expect(scope, isA<ReactionScope>());
      expect(scope.authSession, same(auth));
      expect(scope.actionSubmitter, same(submitter));
      expect(scope.viewSource, same(views));
      expect(scope.permissionSource, same(perms));
    });

    // Verifies: EVS-PRD-reaction-scope/C
    test('connectionStatus is Connected synchronously', () {
      expect(scope.connectionStatus, equals(const Connected()));
    });

    // Verifies: EVS-PRD-reaction-scope/C
    test('connectionStatusStream does NOT emit (always-connected)', () async {
      final emissions = <ConnectionStatus>[];
      final sub = scope.connectionStatusStream.listen(emissions.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await sub.cancel();
      expect(emissions, isEmpty);
    });

    // Verifies: EVS-PRD-reaction-scope/A
    test('dispose() makes interface getters throw StateError', () async {
      await scope.dispose();
      expect(() => scope.authSession, throwsStateError);
      expect(() => scope.actionSubmitter, throwsStateError);
      expect(() => scope.viewSource, throwsStateError);
      expect(() => scope.permissionSource, throwsStateError);
      expect(() => scope.connectionStatus, throwsStateError);
      expect(() => scope.connectionStatusStream, throwsStateError);
    });
  });
}
