// reaction/example/lib/client/submit_note_form.dart
//
// Submit-note form gated on EffectiveAuthorization. If the active user
// holds Permission('submit_note'), shows a workspace dropdown + title
// field + submit button. Otherwise renders a read-only "no submit
// access" notice.
//
// Pre-filter UX: the dropdown walks the user's scopeAssignments to
// label workspaces the user is actually scoped to (no suffix) vs ones
// they are not ('(no permission)' suffix). The unauthorized entries
// are still selectable on purpose, so the demo can still show the
// substrate-side denial path — a "wrong workspace" submission returns
// DispatchAuthorizationDenied from the substrate, which surfaces in
// the parent's flash. This is the right shape pedagogically: the
// substrate is the perimeter, the UI is a courtesy.

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
  final EffectiveAuthorization? snapshot;
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
      DispatchIdempotencyMismatch<Object?>() =>
        'Same idempotency key submitted with different content; '
            'denied to preserve the audit trail.',
      DispatchExecutionFailed<Object?>(:final error) =>
        'Execution failed: $error',
    };
  }

  /// Compute which known workspaces are scoped to the active user under
  /// their active role. Walks `scopeAssignments` for assignments that
  /// could authorize a `BoundScope(workspace, X)` action:
  ///
  /// - [TotalWildcardScope] — covers every workspace.
  /// - [ValueWildcardScope]`(class_: 'workspace')` — covers every
  ///   workspace.
  /// - [BoundScope]`(class_: 'workspace', value: X)` — covers exactly X.
  ///
  /// Returned set may include workspace identifiers the demo doesn't
  /// know about (e.g., if the server seeds a 'mars' scope). The
  /// dropdown intersects with `kKnownWorkspaces` for what to show.
  Set<String> _authorizedWorkspaces(EffectiveAuthorization snapshot) {
    final result = <String>{};
    for (final assignment in snapshot.scopeAssignments) {
      final scope = assignment.scope;
      switch (scope) {
        case TotalWildcardScope():
          result.addAll(kKnownWorkspaces);
        case ValueWildcardScope(class_: 'workspace'):
          result.addAll(kKnownWorkspaces);
        case BoundScope(class_: 'workspace', :final value):
          result.add(value);
        case BoundScope():
        case ValueWildcardScope():
          // Other scope classes don't authorize workspace-scoped submits.
          break;
      }
    }
    return result;
  }

  @override
  Widget build(BuildContext context) {
    final snap = widget.snapshot;
    final hasSubmit =
        snap != null &&
        snap.rolePermissions.contains(const Permission('submit_note'));
    if (!hasSubmit) {
      return Container(
        padding: const EdgeInsets.all(8),
        color: Colors.amber.shade50,
        child: const Text(
          'You do not have submit_note permission for any workspace.',
        ),
      );
    }
    final authorized = _authorizedWorkspaces(snap);
    return Row(
      children: <Widget>[
        DropdownButton<String>(
          value: _workspace,
          onChanged: (v) {
            if (v != null) setState(() => _workspace = v);
          },
          items: <DropdownMenuItem<String>>[
            for (final ws in kKnownWorkspaces)
              DropdownMenuItem<String>(
                value: ws,
                child: Text(
                  authorized.contains(ws) ? ws : '$ws (no permission)',
                ),
              ),
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
