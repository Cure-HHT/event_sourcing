// reaction/example/lib/client/admin_panel.dart
//
// Admin-only panel for managing user-role-scope assignments. Visible
// only when the active role holds Permission('assign_role'). Shows:
//
//   - A live table of existing assignments, sourced from the substrate's
//     `user_role_scopes` view (admin role grants `view:user_role_scopes`
//     in bootstrap so the WS subscription is accepted).
//   - A "Grant role" form: user + role + workspace dropdown + button
//     submitting the `assign_role` action.
//   - A "Revoke" affordance per row: submits `unassign_role`.
//
// All affordances submit through the action pipeline (audited and
// permission-gated), so even an admin's own actions are visible in
// the substrate's event log as role_assigned / role_unassigned events,
// distinct from the seeded ones only by the initiator stamped on them.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';

import 'home_screen.dart' show kKnownWorkspaces;

/// One row of the user_role_scopes view, materialised for the admin
/// table. The view's row carries the WholePayload of the role_assigned
/// event (user_id, role, scope as JSON).
class _Assignment {
  const _Assignment({
    required this.aggregateId,
    required this.userId,
    required this.role,
    required this.scope,
  });

  final String aggregateId;
  final String userId;
  final String role;
  final ScopeValue scope;

  String get scopeLabel => switch (scope) {
    BoundScope(:final class_, :final value) => '$class_=$value',
    ValueWildcardScope(:final class_) => '$class_=*',
    TotalWildcardScope() => '(all)',
  };
}

const List<String> _grantableRoles = <String>['viewer', 'editor', 'admin'];

class AdminPanel extends StatefulWidget {
  const AdminPanel({
    required this.viewSource,
    required this.actionSubmitter,
    required this.onFlash,
    super.key,
  });

  final ViewSource viewSource;
  final ActionSubmitter actionSubmitter;
  final ValueChanged<String?> onFlash;

  @override
  State<AdminPanel> createState() => _AdminPanelState();
}

class _AdminPanelState extends State<AdminPanel> {
  final _Accumulator _accumulator = _Accumulator();
  late final StreamSubscription<Update<_Assignment>> _sub;

  // Grant-form local state.
  String _userId = 'alice';
  String _role = 'editor';
  String _scopeChoice = kKnownWorkspaces.first; // or '(all)'

  static const String _allScopeMarker = '(all)';
  static const List<String> _knownUsers = <String>[
    'alice',
    'bob',
    'carol',
    'dave',
  ];

  @override
  void initState() {
    super.initState();
    final stream = widget.viewSource.watch<_Assignment>(
      viewName: 'user_role_scopes',
      mapper: _decodeRow,
    );
    _sub = stream.listen(
      _accumulator.apply,
      onError: (Object err) {
        // If the admin loses view:user_role_scopes mid-session, the
        // server will close this subscription. Surface to flash so the
        // user sees what happened.
        widget.onFlash('admin view error: $err');
      },
    );
    _accumulator.addListener(_onChanged);
  }

  static _Assignment _decodeRow(Map<String, Object?> row) {
    final scopeJson = row['scope'];
    final scope = scopeJson is Map
        ? ScopeValue.fromJson(scopeJson.cast<String, Object?>())
        : const TotalWildcardScope();
    return _Assignment(
      aggregateId: row['aggregateId']! as String,
      userId: (row['user_id'] as String?) ?? '?',
      role: (row['role'] as String?) ?? '?',
      scope: scope,
    );
  }

  void _onChanged() {
    if (mounted) setState(() {});
  }

  @override
  void dispose() {
    _accumulator.removeListener(_onChanged);
    unawaited(_sub.cancel());
    _accumulator.dispose();
    super.dispose();
  }

  ScopeValue _selectedScope() => _scopeChoice == _allScopeMarker
      ? const TotalWildcardScope()
      : BoundScope(class_: 'workspace', value: _scopeChoice);

  Future<void> _grant() async {
    await _submitRoleAction('assign_role', _userId, _role, _selectedScope());
  }

  Future<void> _revoke(_Assignment a) async {
    await _submitRoleAction('unassign_role', a.userId, a.role, a.scope);
  }

  Future<void> _submitRoleAction(
    String actionName,
    String userId,
    String role,
    ScopeValue scope,
  ) async {
    try {
      final result = await widget.actionSubmitter.submit(
        ActionSubmission(
          actionName: actionName,
          rawInput: <String, Object?>{
            'user_id': userId,
            'role': role,
            'scope': scope.toJson(),
          },
        ),
      );
      widget.onFlash(switch (result) {
        DispatchSuccess<Object?>() => '$actionName ok: $userId $role',
        DispatchAuthorizationDenied<Object?>(:final permission) =>
          'Denied: ${permission.name}',
        DispatchValidationDenied<Object?>(:final error) => 'Invalid: $error',
        DispatchParseDenied<Object?>(:final error) => 'Invalid: $error',
        DispatchUnknownAction<Object?>(:final requestedName) =>
          'Unknown action: $requestedName',
        DispatchIdempotencyHit<Object?>() => 'Already submitted.',
        DispatchExecutionFailed<Object?>(:final error) =>
          'Execution failed: $error',
      });
    } on TransportException catch (e) {
      widget.onFlash('transport: ${e.message}');
    }
  }

  @override
  Widget build(BuildContext context) {
    final assignments = _accumulator.assignments;
    return Container(
      padding: const EdgeInsets.all(8),
      decoration: BoxDecoration(
        border: Border.all(color: Colors.deepPurple.shade200),
        borderRadius: BorderRadius.circular(4),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          const Text(
            'Admin: user-role-scope assignments',
            style: TextStyle(fontWeight: FontWeight.bold),
          ),
          const SizedBox(height: 8),
          if (!_accumulator.replayComplete)
            const LinearProgressIndicator()
          else if (assignments.isEmpty)
            const Text('(no assignments)')
          else
            Column(
              children: <Widget>[
                for (final a in assignments)
                  Row(
                    children: <Widget>[
                      Expanded(
                        child: Text(
                          '${a.userId} -> ${a.role} @ ${a.scopeLabel}',
                        ),
                      ),
                      TextButton(
                        onPressed: () => _revoke(a),
                        child: const Text('Revoke'),
                      ),
                    ],
                  ),
              ],
            ),
          const Divider(),
          Row(
            children: <Widget>[
              const Text('Grant: '),
              const SizedBox(width: 8),
              DropdownButton<String>(
                value: _userId,
                onChanged: (v) {
                  if (v != null) setState(() => _userId = v);
                },
                items: <DropdownMenuItem<String>>[
                  for (final u in _knownUsers)
                    DropdownMenuItem<String>(value: u, child: Text(u)),
                ],
              ),
              const SizedBox(width: 8),
              DropdownButton<String>(
                value: _role,
                onChanged: (v) {
                  if (v != null) setState(() => _role = v);
                },
                items: <DropdownMenuItem<String>>[
                  for (final r in _grantableRoles)
                    DropdownMenuItem<String>(value: r, child: Text(r)),
                ],
              ),
              const SizedBox(width: 8),
              DropdownButton<String>(
                value: _scopeChoice,
                onChanged: (v) {
                  if (v != null) setState(() => _scopeChoice = v);
                },
                items: <DropdownMenuItem<String>>[
                  for (final ws in kKnownWorkspaces)
                    DropdownMenuItem<String>(value: ws, child: Text(ws)),
                  const DropdownMenuItem<String>(
                    value: _allScopeMarker,
                    child: Text('(all)'),
                  ),
                ],
              ),
              const SizedBox(width: 8),
              ElevatedButton(onPressed: _grant, child: const Text('Grant')),
            ],
          ),
        ],
      ),
    );
  }
}

/// Accumulator buffering snapshots until EndOfReplay, atomic swap on
/// EOR. Same shape as NotesList's accumulator — keeps the admin table
/// from flashing partial-replay state during reconnects.
class _Accumulator extends ChangeNotifier {
  Map<String, _Assignment> _staging = <String, _Assignment>{};
  Map<String, _Assignment> _published = <String, _Assignment>{};
  bool _replayComplete = false;

  bool get replayComplete => _replayComplete;
  List<_Assignment> get assignments =>
      (_published.values.toList()..sort((a, b) {
        final byUser = a.userId.compareTo(b.userId);
        return byUser != 0 ? byUser : a.role.compareTo(b.role);
      }));

  void apply(Update<_Assignment> update) {
    switch (update) {
      case Snapshot<_Assignment>(:final value):
        if (value != null) _staging[value.aggregateId] = value;
      case EndOfReplay<_Assignment>():
        _published = _staging;
        _staging = <String, _Assignment>{};
        _replayComplete = true;
        notifyListeners();
      case Delta<_Assignment>(:final value):
        if (_replayComplete) {
          _published = Map<String, _Assignment>.from(_published)
            ..[value.aggregateId] = value;
          notifyListeners();
        } else {
          _staging[value.aggregateId] = value;
        }
      case Tombstone<_Assignment>(:final aggregateId):
        if (_replayComplete) {
          if (_published.containsKey(aggregateId)) {
            _published = Map<String, _Assignment>.from(_published)
              ..remove(aggregateId);
            notifyListeners();
          }
        } else {
          _staging.remove(aggregateId);
        }
    }
  }
}
