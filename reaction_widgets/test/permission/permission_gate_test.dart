// Verifies: EVS-PRD-reaction-widget-contract/G
// (permission-gate sub-clause)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

/// Build a minimal [EffectiveAuthorization] whose `rolePermissions`
/// contains exactly the named permissions (with `scopeClass: null`).
/// `activeRole` and `scopeAssignments` are non-load-bearing for these
/// tests; the widget only walks `rolePermissions` by name.
EffectiveAuthorization _authWith({Set<String> permissions = const {}}) =>
    EffectiveAuthorization(
      activeRole: 'role',
      rolePermissions: {for (final name in permissions) Permission(name)},
      scopeAssignments: const <ScopeAssignment>[],
    );

/// Settle the test after emitting a permission update.
///
/// A broadcast `StreamController.add` dispatches to listeners on a
/// microtask; the listener's `setState` then schedules a frame. We pump
/// twice to drain any queued microtasks and any frame they scheduled.
Future<void> _settleStream(WidgetTester tester) async {
  await tester.pump();
  await tester.pump();
}

void main() {
  group('PermissionGate', () {
    testWidgets('renders child when active Principal holds permission', (
      tester,
    ) async {
      final fake = FakeReaction(
        initialPermission: _authWith(permissions: {'edit'}),
      );

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          fallback: Text('DENIED'),
          child: Text('GRANTED'),
        ),
      );

      expect(find.text('GRANTED'), findsOneWidget);
      expect(find.text('DENIED'), findsNothing);
    });

    testWidgets('renders fallback when permission not held', (tester) async {
      final fake = FakeReaction(
        initialPermission: _authWith(permissions: {'view'}),
      );

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          fallback: Text('DENIED'),
          child: Text('GRANTED'),
        ),
      );

      expect(find.text('DENIED'), findsOneWidget);
      expect(find.text('GRANTED'), findsNothing);
    });

    testWidgets('reactively re-renders on PermissionSource.stream changes', (
      tester,
    ) async {
      final fake = FakeReaction(
        initialPermission: _authWith(permissions: {'view'}),
      );

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          fallback: Text('DENIED'),
          child: Text('GRANTED'),
        ),
      );

      expect(find.text('DENIED'), findsOneWidget);

      fake.drivePermission(_authWith(permissions: {'view', 'edit'}));
      await _settleStream(tester);
      expect(find.text('GRANTED'), findsOneWidget);
      expect(find.text('DENIED'), findsNothing);

      // And back the other way — revoking the permission flips us
      // back to fallback in the same frame.
      fake.drivePermission(_authWith(permissions: {'view'}));
      await _settleStream(tester);
      expect(find.text('DENIED'), findsOneWidget);
      expect(find.text('GRANTED'), findsNothing);
    });

    testWidgets('renders fallback when permissionSource.current is null', (
      tester,
    ) async {
      // No `initialPermission` and no `drivePermission` call -> `current`
      // is null (i.e., not authenticated / snapshot not loaded yet).
      final fake = FakeReaction();

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          fallback: Text('NO_AUTH'),
          child: Text('GRANTED'),
        ),
      );

      expect(find.text('NO_AUTH'), findsOneWidget);
      expect(find.text('GRANTED'), findsNothing);
    });
  });
}
