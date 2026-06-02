// example_clinical_scopes/test/clinical_app_widget_test.dart
//
// Pure widget test using the shipped reaction_widgets_testing doubles
// (FakeReaction + pumpReactionWidget), per
// EVS-PRD-reaction-widget-contract-H. No real server — the fake drives
// every observable transition deterministically.
//
// This test verifies the UI REACTS to the scoped view rows it is given:
// seeded with one site's participants the list shows exactly those; given
// the admin's full set it grows to include the cross-region participant.
// It does NOT exercise the real ScopeDescendantExpander — the e2e test
// (clinical_scoping_e2e_test.dart) covers that end-to-end through the
// loopback server. Here we only prove the list renders the rows handed
// to it.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

import 'package:example_clinical_scopes/client/participants_list.dart';
import 'package:example_clinical_scopes/shared/clinical_types.dart';

void main() {
  testWidgets(
    'ParticipantsList renders exactly the scoped rows it is given, and '
    'grows when a wider scope yields more rows',
    (tester) async {
      final fake = FakeReaction();
      addTearDown(fake.dispose);

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: const Scaffold(body: ParticipantsList()),
      );

      // Pre-EndOfReplay: Loading.
      expect(find.byType(CircularProgressIndicator), findsOneWidget);

      // --- Investigator/Overseer scope (site-A): exactly P-1, P-2. ---
      fake.emitViewUpdate<Participant>(
        'participants',
        const Snapshot<Participant>(
          value: Participant(id: 'P-1', name: 'Ann', siteId: 'site-A'),
          sequence: 1,
        ),
      );
      fake.emitViewUpdate<Participant>(
        'participants',
        const Snapshot<Participant>(
          value: Participant(id: 'P-2', name: 'Bob', siteId: 'site-A'),
          sequence: 2,
        ),
      );
      fake.emitViewUpdate<Participant>(
        'participants',
        const EndOfReplay<Participant>(sequence: 2),
      );
      await tester.pump();

      expect(find.text('Ann'), findsOneWidget);
      expect(find.text('Bob'), findsOneWidget);
      // The cross-region participant is NOT in this scope.
      expect(find.text('Dan'), findsNothing);
      expect(find.byType(CircularProgressIndicator), findsNothing);

      // --- Wider (Admin) scope: a live delta adds the out-of-region
      //     participant P-9; the list grows to include it. ---
      fake.emitViewUpdate<Participant>(
        'participants',
        const Delta<Participant>(
          value: Participant(id: 'P-9', name: 'Dan', siteId: 'site-C'),
          sequence: 3,
          cause: 'participant_registered',
        ),
      );
      await tester.pump();

      expect(find.text('Ann'), findsOneWidget);
      expect(find.text('Bob'), findsOneWidget);
      expect(find.text('Dan'), findsOneWidget);
    },
  );
}
