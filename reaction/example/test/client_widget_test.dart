// reaction/example/test/client_widget_test.dart
//
// (Named `client_widget_test.dart`, not `widget_test.dart`, because the
// example's .gitignore ignores the flutter-create-generated
// `test/widget_test.dart` — this real test must stay tracked.)
//
// Demonstrates testing reaction_widgets-based UI with the shipped
// `reaction_widgets_testing` doubles (FakeReaction + pumpReactionWidget),
// per EVS-PRD-reaction-widget-contract-H. No real server, no timing — the
// fake drives every observable transition deterministically:
//
//   - view-row updates (Snapshot / EndOfReplay / Delta) for ViewBuilder,
//   - ConnectionStatus transitions for the Stale path,
//   - permission snapshots for PermissionGate,
//   - queued DispatchResults (incl. in-flight futures) for ActionBuilder.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

import 'package:reaction_example/client/notes_list.dart';
import 'package:reaction_example/client/submit_note_form.dart';

EffectiveAuthorization _editorWest() => EffectiveAuthorization(
  activeRole: 'editor',
  // Permission overrides ==, so the set can't be a const literal.
  rolePermissions: <Permission>{const Permission('submit_note')},
  scopeAssignments: const <ScopeAssignment>[
    ScopeAssignment(
      scope: BoundScope(class_: 'workspace', value: 'west'),
    ),
  ],
);

void main() {
  testWidgets('NotesList: Loading -> Ready -> Stale via ViewBuilder', (
    tester,
  ) async {
    final fake = FakeReaction();
    addTearDown(fake.dispose);

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: const Scaffold(body: NotesList()),
    );

    // Pre-EndOfReplay: Loading.
    expect(find.byType(CircularProgressIndicator), findsOneWidget);

    // Snapshot row + EndOfReplay -> Ready, row rendered.
    fake.emitViewUpdate<Note>(
      'notes_today',
      const Snapshot<Note>(
        value: Note(id: 'n1', workspace: 'west', title: 'First note'),
        sequence: 1,
      ),
    );
    fake.emitViewUpdate<Note>(
      'notes_today',
      const EndOfReplay<Note>(sequence: 1),
    );
    await tester.pump();
    expect(find.text('First note'), findsOneWidget);
    expect(find.byType(CircularProgressIndicator), findsNothing);

    // A live Delta appends a second note.
    fake.emitViewUpdate<Note>(
      'notes_today',
      const Delta<Note>(
        value: Note(id: 'n2', workspace: 'west', title: 'Second note'),
        sequence: 2,
        cause: 'note_created',
      ),
    );
    await tester.pump();
    expect(find.text('Second note'), findsOneWidget);

    // Transport drop -> Stale: banner shown, last rows retained. Two
    // pumps: the first flushes the queued broadcast status event into
    // ViewBuilder's listener, the second renders the resulting setState.
    fake.driveConnectionStatus(const Reconnecting());
    await tester.pump();
    await tester.pump();
    expect(find.textContaining('Reconnecting'), findsOneWidget);
    expect(find.text('First note'), findsOneWidget);
    expect(find.text('Second note'), findsOneWidget);
  });

  testWidgets('PermissionGate hides SubmitNoteForm without submit_note', (
    tester,
  ) async {
    final fake = FakeReaction(initialPermission: EffectiveAuthorization.empty);
    addTearDown(fake.dispose);

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: const Scaffold(body: SubmitNoteForm(snapshot: null)),
    );
    await tester.pump();

    expect(find.text('Submit'), findsNothing);
    expect(
      find.textContaining('do not have submit_note permission'),
      findsOneWidget,
    );
  });

  testWidgets('ActionBuilder drives Submitting spinner then Denied status', (
    tester,
  ) async {
    final auth = _editorWest();
    final fake = FakeReaction(initialPermission: auth);
    addTearDown(fake.dispose);

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: Scaffold(body: SubmitNoteForm(snapshot: auth)),
    );
    await tester.pump();

    // Gate passes: the form is shown.
    expect(find.text('Submit'), findsOneWidget);

    // Queue an in-flight submission we control, type a title, submit.
    final pending = Completer<DispatchResult<Object?>>();
    fake.queueDispatchResultFuture(pending.future);
    await tester.enterText(find.byType(TextField), 'Hello mars');
    await tester.tap(find.text('Submit'));
    await tester.pump();

    // Submitting: spinner in place of the button label.
    expect(find.byType(CircularProgressIndicator), findsOneWidget);
    expect(fake.submittedActions.single.rawInput['title'], 'Hello mars');

    // Resolve as an authorization denial -> Denied status line.
    pending.complete(
      const DispatchAuthorizationDenied<Object?>(Permission('submit_note')),
    );
    await tester.pump();
    expect(find.textContaining('Denied: submit_note'), findsOneWidget);
  });
}
