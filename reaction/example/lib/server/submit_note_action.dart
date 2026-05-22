// reaction/example/lib/server/submit_note_action.dart
//
// Sample Action for the reaction example. Submits a single note and
// emits one `note_created` event. Permission: `submit_note` (unscoped).

import 'package:event_sourcing/event_sourcing.dart';
import 'package:uuid/uuid.dart';

/// Input shape for [SubmitNoteAction]: `{title: <String>}`.
class NoteInput {
  const NoteInput({required this.title});

  final String title;
}

/// Result shape for [SubmitNoteAction]: `{noteId: <uuid>}`.
class NoteResult {
  const NoteResult({required this.noteId});

  final String noteId;

  /// JSON-encodable for the wire `DispatchResult` envelope.
  Map<String, Object?> toJson() => <String, Object?>{'noteId': noteId};
}

/// Accepts `{title: <String>}`, validates non-empty, emits one
/// `note_created` event under aggregateType `note`.
class SubmitNoteAction extends Action<NoteInput, NoteResult> {
  SubmitNoteAction({Uuid? uuid}) : _uuid = uuid ?? const Uuid();

  final Uuid _uuid;

  @override
  String get name => 'submit_note';

  @override
  String get description =>
      'Submit a note — sample action for the reaction example.';

  @override
  Set<Permission> get permissions => {const Permission('submit_note')};

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  NoteInput parseInput(Map<String, Object?> raw) {
    final titleVal = raw['title'];
    if (titleVal is! String) {
      throw FormatException(
        "'title' must be a string, got: ${titleVal.runtimeType}",
      );
    }
    return NoteInput(title: titleVal);
  }

  @override
  void validate(NoteInput input) {
    if (input.title.trim().isEmpty) {
      throw ArgumentError.value(input.title, 'title', 'must not be empty');
    }
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
          data: <String, Object?>{'title': input.title},
        ),
      ],
    );
  }
}
