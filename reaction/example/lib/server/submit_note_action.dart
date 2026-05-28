// reaction/example/lib/server/submit_note_action.dart
//
// Submits a note inside a workspace and emits one `note_created` event
// carrying both the workspace and the title. Permission is scoped to
// the `workspace` ScopeClass: a principal whose role grants
// `submit_note` is checked against the per-dispatch BoundScope returned
// by `scopeFor`, so alice (BoundScope(workspace, west)) can submit in
// 'west' but not 'east'.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:uuid/uuid.dart';

/// Input shape for [SubmitNoteAction]: `{workspace: <String>, title: <String>}`.
class NoteInput {
  const NoteInput({required this.workspace, required this.title});

  final String workspace;
  final String title;
}

/// Result shape for [SubmitNoteAction]: `{noteId: <uuid>}`.
class NoteResult {
  const NoteResult({required this.noteId});

  final String noteId;

  Map<String, Object?> toJson() => <String, Object?>{'noteId': noteId};
}

/// Accepts `{workspace, title}`, emits one `note_created` event under
/// aggregateType `note`. The `submit_note` permission is scoped on the
/// `workspace` class — the dispatcher consults [scopeFor] for the
/// per-dispatch BoundScope and the policy verifies the principal holds
/// an assignment that covers it.
class SubmitNoteAction extends Action<NoteInput, NoteResult> {
  SubmitNoteAction({Uuid? uuid}) : _uuid = uuid ?? const Uuid();

  final Uuid _uuid;

  @override
  String get name => 'submit_note';

  @override
  String get description =>
      'Submit a note in a workspace — scoped by the workspace ScopeClass.';

  @override
  Set<Permission> get permissions => {
    const Permission('submit_note', scopeClass: 'workspace'),
  };

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  NoteInput parseInput(Map<String, Object?> raw) {
    final wsVal = raw['workspace'];
    final titleVal = raw['title'];
    if (wsVal is! String) {
      throw FormatException(
        "'workspace' must be a string, got: ${wsVal.runtimeType}",
      );
    }
    if (titleVal is! String) {
      throw FormatException(
        "'title' must be a string, got: ${titleVal.runtimeType}",
      );
    }
    return NoteInput(workspace: wsVal, title: titleVal);
  }

  @override
  void validate(NoteInput input) {
    if (input.workspace.trim().isEmpty) {
      throw ArgumentError.value(
        input.workspace,
        'workspace',
        'must not be empty',
      );
    }
    if (input.title.trim().isEmpty) {
      throw ArgumentError.value(input.title, 'title', 'must not be empty');
    }
  }

  @override
  ScopeValue? scopeFor(Permission perm, NoteInput input) {
    if (perm.name != 'submit_note') return null;
    return BoundScope(class_: 'workspace', value: input.workspace);
  }

  @override
  Future<ExecutionResult<NoteResult>> execute(
    NoteInput input,
    ActionContext ctx,
  ) async {
    final noteId = _uuid.v4();
    return ExecutionResult<NoteResult>(
      result: NoteResult(noteId: noteId),
      events: [
        EventDraft(
          aggregateId: noteId,
          aggregateType: 'note',
          entryType: 'note',
          eventType: 'note_created',
          data: <String, Object?>{
            'workspace': input.workspace,
            'title': input.title,
          },
        ),
      ],
    );
  }
}
