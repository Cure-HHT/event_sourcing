// Verifies: EVS-PRD-reaction-widget-contract/F

import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

/// Assertion F of the widget contract: widget code MUST access the
/// substrate only via the four `reaction` interfaces (and the
/// `ReactionScope` that bundles them). Widget code MUST NOT reach
/// past the reaction package into substrate-internal handles like
/// `EventStore`, `ActionDispatcher`, `ProjectionRegistry`,
/// `StorageBackend`, `AuthorizationPolicy`, or `IdempotencyStore`.
///
/// Substrate *value* types (e.g., `Update`, `Snapshot`, `Delta`,
/// `SubscriptionFilter`, `EffectiveAuthorization`, `Permission`,
/// `ActionSubmission`, `DispatchResult`) flow through the reaction
/// interface signatures and ARE allowed to appear in widget code.
///
/// This test scans `lib/src/` for bare references to the disallowed
/// substrate-internal type names (word-boundary matched, so
/// `eventStoreRef` and other identifiers containing the substring do
/// not false-positive).
void main() {
  test('no widget source references disallowed substrate-internal types', () {
    const disallowedTypes = {
      'EventStore',
      'ActionDispatcher',
      'ProjectionRegistry',
      'StorageBackend',
      'AuthorizationPolicy',
      'IdempotencyStore',
    };

    final libDir = Directory('lib/src');
    expect(
      libDir.existsSync(),
      isTrue,
      reason: 'lib/src/ must exist (running from reaction_widgets root?)',
    );

    final violations = <String>[];

    for (final entity in libDir.listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith('.dart')) continue;
      // Skip the testing/ directory — FakeReaction's dartdoc may
      // legitimately reference substrate-internal type names when
      // describing what real implementations route through. The
      // assertion-F contract targets production widget code, not test
      // fakes.
      if (entity.path.contains('/testing/') ||
          entity.path.contains(r'\testing\')) {
        continue;
      }
      final src = entity.readAsStringSync();
      for (final t in disallowedTypes) {
        final pattern = RegExp(r'\b' + t + r'\b');
        final hits = pattern.allMatches(src).length;
        if (hits > 0) {
          violations.add('${entity.path}: $t x $hits');
        }
      }
    }

    expect(
      violations,
      isEmpty,
      reason:
          'Widget code MUST access the substrate only via the four '
          "reaction interfaces and ReactionScope (EVS-PRD-reaction-widget-"
          'contract/F). Disallowed substrate-internal types found in:\n  '
          '${violations.join("\n  ")}\n'
          'Refactor to use a reaction interface that abstracts the '
          "substrate type, or expose what you need via reaction's barrel.",
    );
  });
}
