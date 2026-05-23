// reaction/example/lib/client/submit_note_form.dart
//
// Submit-note form gated on PermissionSnapshot. If the active user
// holds Permission('submit_note'), shows a workspace dropdown + title
// field + submit button. Otherwise renders a read-only "no submit
// access" notice.
//
// The form does NOT pre-filter the workspace dropdown by the user's
// scope assignments — PermissionSnapshot does not carry per-scope
// detail. We offer the full known-workspaces list and let the
// substrate enforce: an unauthorized workspace returns
// DispatchAuthorizationDenied, which surfaces in the parent's flash.
// This is the right shape pedagogically — per-dispatch scope checks
// are visibly server-side, not "the UI was wrong."

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';

import 'home_screen.dart' show kKnownWorkspaces;

class SubmitNoteForm extends StatefulWidget {
  const SubmitNoteForm({
    required this.actionSubmitter,
    required this.snapshot,
    required this.onFlash,
    super.key,
  });

  final ActionSubmitter actionSubmitter;
  final PermissionSnapshot? snapshot;
  final ValueChanged<String?> onFlash;

  @override
  State<SubmitNoteForm> createState() => _SubmitNoteFormState();
}

class _SubmitNoteFormState extends State<SubmitNoteForm> {
  final TextEditingController _titleController = TextEditingController();
  String _workspace = kKnownWorkspaces.first;

  @override
  void dispose() {
    _titleController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final title = _titleController.text.trim();
    if (title.isEmpty) return;
    _titleController.clear();
    try {
      final result = await widget.actionSubmitter.submit(
        ActionSubmission(
          actionName: 'submit_note',
          rawInput: <String, Object?>{'workspace': _workspace, 'title': title},
        ),
      );
      widget.onFlash(_messageFor(result));
    } on TransportException catch (e) {
      widget.onFlash('transport: ${e.message}');
    }
  }

  String _messageFor(DispatchResult<Object?> result) {
    return switch (result) {
      DispatchSuccess<Object?>() => 'Note submitted in $_workspace.',
      DispatchAuthorizationDenied<Object?>(:final permission) =>
        'Denied: ${permission.name} (workspace=$_workspace)',
      DispatchValidationDenied<Object?>(:final error) => 'Invalid: $error',
      DispatchParseDenied<Object?>(:final error) => 'Invalid: $error',
      DispatchUnknownAction<Object?>(:final requestedName) =>
        'Unknown action: $requestedName',
      DispatchIdempotencyHit<Object?>() => 'Already submitted.',
      DispatchExecutionFailed<Object?>(:final error) =>
        'Execution failed: $error',
    };
  }

  @override
  Widget build(BuildContext context) {
    final snap = widget.snapshot;
    final hasSubmit =
        snap != null && snap.grants.contains(const Permission('submit_note'));
    if (!hasSubmit) {
      return Container(
        padding: const EdgeInsets.all(8),
        color: Colors.amber.shade50,
        child: const Text(
          'You do not have submit_note permission for any workspace.',
        ),
      );
    }
    return Row(
      children: <Widget>[
        DropdownButton<String>(
          value: _workspace,
          onChanged: (v) {
            if (v != null) setState(() => _workspace = v);
          },
          items: <DropdownMenuItem<String>>[
            for (final ws in kKnownWorkspaces)
              DropdownMenuItem<String>(value: ws, child: Text(ws)),
          ],
        ),
        const SizedBox(width: 8),
        Expanded(
          child: TextField(
            controller: _titleController,
            decoration: const InputDecoration(labelText: 'New note title'),
            onSubmitted: (_) => _submit(),
          ),
        ),
        const SizedBox(width: 8),
        ElevatedButton(onPressed: _submit, child: const Text('Submit')),
      ],
    );
  }
}
