// reaction/example/lib/client/submit_note_form.dart
//
// Submit-note form, rebuilt on reaction_widgets primitives:
//
//   - `PermissionGate(permission: 'submit_note')` gates the whole form.
//     When the active role does not hold `submit_note`, the gate renders
//     the `fallback` ("no submit access") notice instead. This is the
//     coarse "can the user do X at all?" check the gate is designed for
//     (name-only, scope-agnostic — EVS-PRD-reaction-widget-contract-G).
//   - `ActionBuilder` owns the submission lifecycle: it mints + retains
//     the idempotency key, tracks Idle/Submitting/Success/Denied/Failed,
//     and discards results that arrive after dispose. We render an inline
//     status line straight off the `ActionState` instead of routing
//     through a parent flash.
//
// The fine-grained scope pre-filter (labelling workspaces the user is
// NOT scoped to as "(no permission)") still needs the full
// `EffectiveAuthorization`, which the gate does not expose — so the
// parent passes the live snapshot down for that labelling only. Per-
// dispatch authorization still happens at the substrate: a "(no
// permission)" workspace stays selectable and produces a
// DispatchAuthorizationDenied, surfaced in the ActionBuilder's Denied
// state. The substrate is the perimeter; the UI pre-filter is a courtesy.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

import 'home_screen.dart' show kKnownWorkspaces;
import 'ui.dart';

class SubmitNoteForm extends StatefulWidget {
  const SubmitNoteForm({required this.snapshot, super.key});

  /// Live `EffectiveAuthorization` (from the parent's permission stream),
  /// used only for the scope pre-filter labelling.
  final EffectiveAuthorization? snapshot;

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

  /// Which known workspaces are scoped to the active user under their
  /// active role. Walks `scopeAssignments` for assignments that could
  /// authorize a `BoundScope(workspace, X)` action:
  ///
  /// - [TotalWildcardScope] — covers every workspace.
  /// - [ValueWildcardScope]`(class_: 'workspace')` — covers every
  ///   workspace.
  /// - [BoundScope]`(class_: 'workspace', value: X)` — covers exactly X.
  Set<String> _authorizedWorkspaces(EffectiveAuthorization snapshot) {
    final result = <String>{};
    for (final assignment in snapshot.scopeAssignments) {
      switch (assignment.scope) {
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
    final scheme = Theme.of(context).colorScheme;
    return PermissionGate(
      permission: 'submit_note',
      fallback: SectionCard(
        title: 'Add a note',
        icon: Icons.lock_outline,
        accent: scheme.outline,
        child: Row(
          children: <Widget>[
            Icon(Icons.block, size: 18, color: scheme.outline),
            const SizedBox(width: 8),
            const Expanded(
              child: Text(
                'You do not have submit_note permission for any workspace.',
              ),
            ),
          ],
        ),
      ),
      child: ActionBuilder(
        semanticIdentifier: 'submit-note',
        submissionFactory: () => ActionSubmission(
          actionName: 'submit_note',
          rawInput: <String, Object?>{
            'workspace': _workspace,
            'title': _titleController.text.trim(),
          },
        ),
        builder: (context, state, submit) {
          final snap = widget.snapshot;
          final authorized = snap == null
              ? const <String>{}
              : _authorizedWorkspaces(snap);
          final submitting = state is Submitting;

          void onSubmit() {
            if (_titleController.text.trim().isEmpty) return;
            // submit() reads the title synchronously via the factory;
            // clear afterwards so the field is ready for the next note.
            submit();
            _titleController.clear();
          }

          return SectionCard(
            title: 'Add a note',
            icon: Icons.add_circle_outline,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: <Widget>[
                Row(
                  children: <Widget>[
                    Semantics(
                      identifier: 'submit-note-workspace',
                      child: DropdownButton<String>(
                        value: _workspace,
                        // Declared explicitly so the web icon tree-shaker keeps
                        // the glyph (a bare default arrow rendered as tofu).
                        icon: const Icon(Icons.arrow_drop_down),
                        onChanged: submitting
                            ? null
                            : (v) {
                                if (v != null) setState(() => _workspace = v);
                              },
                        items: <DropdownMenuItem<String>>[
                          for (final ws in kKnownWorkspaces)
                            DropdownMenuItem<String>(
                              value: ws,
                              child: Text(
                                authorized.contains(ws)
                                    ? ws
                                    : '$ws (no permission)',
                              ),
                            ),
                        ],
                      ),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: Semantics(
                        identifier: 'submit-note-title',
                        child: TextField(
                          controller: _titleController,
                          enabled: !submitting,
                          decoration: const InputDecoration(
                            labelText: 'New note title',
                            isDense: true,
                            border: OutlineInputBorder(),
                          ),
                          onSubmitted: (_) => onSubmit(),
                        ),
                      ),
                    ),
                    const SizedBox(width: 12),
                    Semantics(
                      identifier: 'submit-note-button',
                      container: true,
                      explicitChildNodes: true,
                      child: FilledButton(
                        onPressed: submitting ? null : onSubmit,
                        child: submitting
                            ? const SizedBox(
                                width: 16,
                                height: 16,
                                child: CircularProgressIndicator(
                                  strokeWidth: 2,
                                ),
                              )
                            : const Text('Submit'),
                      ),
                    ),
                  ],
                ),
                _SubmitStatus(state: state, workspace: _workspace),
              ],
            ),
          );
        },
      ),
    );
  }
}

/// Renders a one-line status off the [ActionState]. Idle shows nothing.
class _SubmitStatus extends StatelessWidget {
  const _SubmitStatus({required this.state, required this.workspace});

  final ActionState state;
  final String workspace;

  @override
  Widget build(BuildContext context) {
    final String? message = switch (state) {
      Idle() || Submitting() => null,
      Success() => 'Note submitted in $workspace.',
      Denied(:final result) => _denialMessage(result),
      Failed(:final error) => 'transport: $error',
    };
    if (message == null) return const SizedBox.shrink();
    final scheme = Theme.of(context).colorScheme;
    final bool ok = state is Success;
    final Color color = ok ? const Color(0xFF2E7D32) : scheme.error;
    return Padding(
      padding: const EdgeInsets.only(top: 10),
      child: Row(
        children: <Widget>[
          Icon(
            ok ? Icons.check_circle_outline : Icons.error_outline,
            size: 16,
            color: color,
          ),
          const SizedBox(width: 6),
          Expanded(
            child: Text(message, style: TextStyle(color: color)),
          ),
        ],
      ),
    );
  }

  String _denialMessage(DispatchResult<Object?> result) => switch (result) {
    DispatchAuthorizationDenied<Object?>(:final permission) =>
      'Denied: ${permission.name} (workspace=$workspace)',
    DispatchValidationDenied<Object?>(:final error) => 'Invalid: $error',
    DispatchParseDenied<Object?>(:final error) => 'Invalid: $error',
    DispatchUnknownAction<Object?>(:final requestedName) =>
      'Unknown action: $requestedName',
    DispatchIdempotencyMismatch<Object?>() =>
      'Same idempotency key, different content — denied to preserve the '
          'audit trail.',
    DispatchExecutionFailed<Object?>(:final error) =>
      'Execution failed: $error',
    DispatchSuccess<Object?>() ||
    DispatchIdempotencyHit<Object?>() => 'Note submitted in $workspace.',
  };
}
