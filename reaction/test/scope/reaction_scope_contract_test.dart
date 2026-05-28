// Verifies: EVS-PRD-reaction-scope/E (source-identical consumer code)
//
// Cross-impl contract test: runs the same assertions against both
// LocalScope and RemoteScope to enforce source-identical behavior per
// EVS-PRD-reaction-scope-E. The set of assertions here is intentionally
// the intersection of behaviours both impls must satisfy — known
// asymmetries (e.g. LocalScope.connectionStatusStream does NOT throw
// post-dispose while RemoteScope's does) are deliberately NOT asserted
// so the same test passes against both impls. Per-impl behaviours live
// in `local_scope_test.dart` / the remote suite.

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import '../local/test_support/reaction_test_harness.dart';

/// A handle to a built [ReactionScope] plus any per-impl extra disposers
/// that must run after the scope itself is disposed.
class _ScopeFixture {
  _ScopeFixture({required this.scope, required this.disposeExtras});

  final ReactionScope scope;
  final Future<void> Function() disposeExtras;
}

void main() {
  group('ReactionScope contract', () {
    for (final entry in <(String, Future<_ScopeFixture> Function())>[
      ('LocalScope', _buildLocalScope),
      ('RemoteScope (offline)', _buildRemoteScope),
    ]) {
      final (label, builder) = entry;

      group(label, () {
        late _ScopeFixture fixture;
        late ReactionScope scope;

        setUp(() async {
          fixture = await builder();
          scope = fixture.scope;
        });

        tearDown(() async {
          // dispose() is idempotent on both impls, so calling here is safe
          // even if the test under test already disposed.
          await scope.dispose();
          await fixture.disposeExtras();
        });

        test('exposes all four interfaces non-null pre-dispose', () {
          expect(scope.authSession, isNotNull);
          expect(scope.actionSubmitter, isNotNull);
          expect(scope.viewSource, isNotNull);
          expect(scope.permissionSource, isNotNull);
        });

        test('connectionStatus is never null', () {
          expect(scope.connectionStatus, isNotNull);
        });

        test(
          'connectionStatusStream is broadcast (supports late subscribers)',
          () async {
            final s1 = scope.connectionStatusStream.listen((_) {});
            final s2 = scope.connectionStatusStream.listen((_) {});
            await s1.cancel();
            await s2.cancel();
          },
        );

        test('dispose() makes all accessors throw StateError', () async {
          await scope.dispose();
          expect(() => scope.authSession, throwsStateError);
          expect(() => scope.actionSubmitter, throwsStateError);
          expect(() => scope.viewSource, throwsStateError);
          expect(() => scope.permissionSource, throwsStateError);
          expect(() => scope.connectionStatus, throwsStateError);
        });
      });
    }
  });
}

/// Builds a [LocalScope] over a fully-wired in-memory substrate via
/// [ReactionTestHarness]. The auth/perm/harness handles aren't reachable
/// through the [ReactionScope] interface itself, so the fixture closes
/// over them and surfaces a disposer that tears them down after the
/// scope itself disposes (matching the order in `local_scope_test.dart`).
Future<_ScopeFixture> _buildLocalScope() async {
  final harness = await ReactionTestHarness.open();
  final auth = LocalAuthSession(defaultActiveRole: 'r');
  final submitter = LocalActionSubmitter(
    dispatcher: harness.dispatcher,
    authSession: auth,
  );
  final views = LocalViewSource(eventStore: harness.eventStore);
  final perms = LocalPermissionSource(
    eventStore: harness.eventStore,
    policy: harness.dispatcher.authorization,
  );
  final scope = LocalScope(
    authSession: auth,
    actionSubmitter: submitter,
    viewSource: views,
    permissionSource: perms,
  );
  return _ScopeFixture(
    scope: scope,
    disposeExtras: () async {
      await auth.dispose();
      await perms.dispose();
      await harness.close();
    },
  );
}

/// Builds a [RemoteScope] offline — no server, no real connection. The
/// scope's lazy-connect design means construction itself does no I/O,
/// which is exactly what we want for a structural contract test.
Future<_ScopeFixture> _buildRemoteScope() async {
  final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
  return _ScopeFixture(scope: scope, disposeExtras: () async {});
}
