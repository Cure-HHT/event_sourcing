// Verifies: EVS-PRD-action-dispatch/A/B/C
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/actions/edit_blue_note_action.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'blue-user',
    roles: const <String>{'BlueTeam'},
    activeRole: 'BlueTeam',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.utc(2026, 5, 8, 12),
);

void main() {
  group('EditBlueNoteAction', () {
    final action = EditBlueNoteAction();

    test('declares site-scoped notes.write.blue, idempotency optional', () {
      expect(action.name, 'EditBlueNoteAction');
      expect(
        action.permissions,
        contains(const Permission('notes.write.blue', scopeClass: 'site')),
      );
      expect(action.idempotency, Idempotency.optional);
    });

    test('parseInput accepts {noteId,title,body}: String', () {
      final input = action.parseInput(<String, Object?>{
        'noteId': 'n1',
        'title': 't',
        'body': 'b',
      });
      expect(input.noteId, 'n1');
      expect(input.title, 't');
      expect(input.body, 'b');
    });

    test('parseInput throws FormatException on wrong shape', () {
      expect(
        () => action.parseInput(<String, Object?>{'noteId': 'n1'}),
        throwsA(isA<FormatException>()),
      );
      expect(
        () => action.parseInput(<String, Object?>{
          'noteId': 'n1',
          'title': 't',
          'body': 42,
        }),
        throwsA(isA<FormatException>()),
      );
    });

    test('validate rejects empty title or noteId with ArgumentError', () {
      expect(
        () => action.validate(
          const EditBlueNoteInput(noteId: 'n1', title: '', body: 'b'),
        ),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => action.validate(
          const EditBlueNoteInput(noteId: '', title: 't', body: 'b'),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('execute emits one demo_note event with workspace=blue', () async {
      final result = await action.execute(
        const EditBlueNoteInput(noteId: 'n1', title: 't', body: 'b'),
        _ctx(),
      );
      expect(result.events, hasLength(1));
      final draft = result.events.single;
      expect(draft.eventType, 'demo_note');
      expect(draft.aggregateType, 'demo_note');
      expect(draft.aggregateId, 'n1');
      expect(draft.entryType, 'demo_note');
      expect(draft.data['workspace'], 'blue');
      expect(draft.data['title'], 't');
      expect(draft.data['body'], 'b');
      expect(result.result.noteId, 'n1');
    });
  });
}
