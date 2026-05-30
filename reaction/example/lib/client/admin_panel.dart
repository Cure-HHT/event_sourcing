// reaction/example/lib/client/admin_panel.dart
//
// Admin-only panel for managing user-role-scope assignments. Mounted
// behind a `PermissionGate('assign_role')` in home_screen, so it is only
// in the tree when the active role holds that permission. Rebuilt on
// reaction_widgets primitives:
//
//   - `ViewBuilder<_Assignment>` renders the live `user_role_scopes`
//     table (admin role grants `view:user_role_scopes` in bootstrap so
//     the subscription is accepted). Its Stale branch keeps the last-
//     known assignments visible during a reconnect.
//   - `ActionBuilder` drives both the grant form's button and each row's
//     Revoke button — minting idempotency keys, tracking Submitting, and
//     surfacing Denied/Failed inline.
//
// All affordances submit through the action pipeline (audited and
// permission-gated), so even an admin's own actions appear in the
// substrate's event log as role_assigned / role_unassigned events,
// distinct from the seeded ones only by the initiator stamped on them.

import 'package:event_sourcing/event_sourcing.dart';
// `material.dart` also exports a `ViewBuilder` (from SearchAnchor); hide it
// so the unqualified name resolves to reaction_widgets' view primitive.
import 'package:flutter/material.dart' hide ViewBuilder;
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

import 'home_screen.dart' show kKnownWorkspaces;
import 'ui.dart';

/// Distinct color per role for the chips in the assignment table.
Color _roleColor(String role) => switch (role) {
  'admin' => const Color(0xFF6A1B9A), // purple
  'editor' => const Color(0xFF1565C0), // blue
  'viewer' => const Color(0xFF546E7A), // blue-grey
  _ => const Color(0xFF546E7A),
};

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

  static _Assignment fromRow(Map<String, Object?> row) {
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
}

const List<String> _grantableRoles = <String>['viewer', 'editor', 'admin'];
const String _allScopeMarker = '(all)';
const List<String> _knownUsers = <String>['alice', 'bob', 'carol', 'dave'];

class AdminPanel extends StatefulWidget {
  const AdminPanel({super.key});

  @override
  State<AdminPanel> createState() => _AdminPanelState();
}

class _AdminPanelState extends State<AdminPanel> {
  // Grant-form local state.
  String _userId = 'alice';
  String _role = 'editor';
  String _scopeChoice = kKnownWorkspaces.first; // or '(all)'

  ScopeValue _selectedScope() => _scopeChoice == _allScopeMarker
      ? const TotalWildcardScope()
      : BoundScope(class_: 'workspace', value: _scopeChoice);

  @override
  Widget build(BuildContext context) {
    return SectionCard(
      title: 'Admin · user-role-scope assignments',
      icon: Icons.admin_panel_settings_outlined,
      accent: const Color(0xFF6A1B9A),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          ViewBuilder<_Assignment>(
            viewName: 'user_role_scopes',
            mapper: _Assignment.fromRow,
            aggregateIdOf: (a) => a.aggregateId,
            builder: (context, state) => switch (state) {
              Loading<_Assignment>() => const Padding(
                padding: EdgeInsets.symmetric(vertical: 8),
                child: LinearProgressIndicator(),
              ),
              Ready<_Assignment>(:final rows) => _AssignmentTable(rows: rows),
              Stale<_Assignment>(:final lastRows) => Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: <Widget>[
                  Row(
                    children: <Widget>[
                      Icon(
                        Icons.sync_problem,
                        size: 16,
                        color: Colors.orange.shade800,
                      ),
                      const SizedBox(width: 6),
                      Text(
                        'reconnecting — assignments may be stale',
                        style: TextStyle(color: Colors.orange.shade800),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  _AssignmentTable(rows: lastRows),
                ],
              ),
            },
          ),
          const Divider(height: 24),
          _GrantForm(
            userId: _userId,
            role: _role,
            scopeChoice: _scopeChoice,
            onUserChanged: (v) => setState(() => _userId = v),
            onRoleChanged: (v) => setState(() => _role = v),
            onScopeChanged: (v) => setState(() => _scopeChoice = v),
            scopeFor: _selectedScope,
          ),
        ],
      ),
    );
  }
}

/// Live assignments table; each row carries an ActionBuilder-backed
/// Revoke button.
class _AssignmentTable extends StatelessWidget {
  const _AssignmentTable({required this.rows});

  final List<_Assignment> rows;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    if (rows.isEmpty) {
      return Row(
        children: <Widget>[
          Icon(Icons.inbox_outlined, size: 18, color: scheme.outline),
          const SizedBox(width: 8),
          Text('(no assignments)', style: TextStyle(color: scheme.outline)),
        ],
      );
    }
    final sorted = rows.toList()
      ..sort((a, b) {
        final byUser = a.userId.compareTo(b.userId);
        return byUser != 0 ? byUser : a.role.compareTo(b.role);
      });
    return Column(
      children: <Widget>[
        for (final a in sorted)
          Padding(
            padding: const EdgeInsets.symmetric(vertical: 4),
            child: Row(
              children: <Widget>[
                CircleAvatar(
                  radius: 16,
                  backgroundColor: _roleColor(a.role).withValues(alpha: 0.15),
                  foregroundColor: _roleColor(a.role),
                  child: Text(
                    a.userId.isEmpty ? '?' : a.userId[0].toUpperCase(),
                    style: const TextStyle(
                      fontWeight: FontWeight.bold,
                      fontSize: 13,
                    ),
                  ),
                ),
                const SizedBox(width: 10),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: <Widget>[
                      Text(
                        a.userId,
                        style: const TextStyle(fontWeight: FontWeight.w600),
                      ),
                      const SizedBox(height: 4),
                      Wrap(
                        spacing: 6,
                        children: <Widget>[
                          TagChip(
                            label: a.role,
                            color: _roleColor(a.role),
                            icon: Icons.badge_outlined,
                          ),
                          TagChip(
                            label: a.scopeLabel,
                            color: scheme.outline,
                            icon: Icons.layers_outlined,
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
                _RevokeButton(assignment: a),
              ],
            ),
          ),
      ],
    );
  }
}

/// Per-row revoke control, backed by an ActionBuilder so the unassign
/// submission lifecycle (Submitting spinner, Denied/Failed surfacing) is
/// owned by the headless primitive.
class _RevokeButton extends StatelessWidget {
  const _RevokeButton({required this.assignment});

  final _Assignment assignment;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return ActionBuilder(
      submissionFactory: () => ActionSubmission(
        actionName: 'unassign_role',
        rawInput: <String, Object?>{
          'user_id': assignment.userId,
          'role': assignment.role,
          'scope': assignment.scope.toJson(),
        },
      ),
      builder: (context, state, submit) {
        if (state is Submitting) {
          return const Padding(
            padding: EdgeInsets.all(12),
            child: SizedBox(
              width: 16,
              height: 16,
              child: CircularProgressIndicator(strokeWidth: 2),
            ),
          );
        }
        if (state is Denied || state is Failed) {
          final reason = state is Denied ? state.reason : 'failed';
          return IconButton(
            tooltip: 'Revoke failed: $reason',
            onPressed: submit,
            icon: Icon(Icons.error_outline, color: scheme.error),
          );
        }
        return IconButton(
          tooltip: 'Revoke',
          onPressed: submit,
          icon: Icon(Icons.delete_outline, color: scheme.error),
        );
      },
    );
  }
}

/// Grant form: user + role + scope dropdowns, plus an ActionBuilder-backed
/// Grant button with an inline status line.
class _GrantForm extends StatelessWidget {
  const _GrantForm({
    required this.userId,
    required this.role,
    required this.scopeChoice,
    required this.onUserChanged,
    required this.onRoleChanged,
    required this.onScopeChanged,
    required this.scopeFor,
  });

  final String userId;
  final String role;
  final String scopeChoice;
  final ValueChanged<String> onUserChanged;
  final ValueChanged<String> onRoleChanged;
  final ValueChanged<String> onScopeChanged;
  final ScopeValue Function() scopeFor;

  @override
  Widget build(BuildContext context) {
    return ActionBuilder(
      submissionFactory: () => ActionSubmission(
        actionName: 'assign_role',
        rawInput: <String, Object?>{
          'user_id': userId,
          'role': role,
          'scope': scopeFor().toJson(),
        },
      ),
      builder: (context, state, submit) {
        final submitting = state is Submitting;
        return Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: <Widget>[
            Wrap(
              spacing: 8,
              runSpacing: 8,
              crossAxisAlignment: WrapCrossAlignment.center,
              children: <Widget>[
                Row(
                  mainAxisSize: MainAxisSize.min,
                  children: <Widget>[
                    Icon(
                      Icons.person_add_alt,
                      size: 18,
                      color: Theme.of(context).colorScheme.outline,
                    ),
                    const SizedBox(width: 6),
                    const Text('Grant'),
                  ],
                ),
                _Dropdown(
                  value: userId,
                  items: _knownUsers,
                  enabled: !submitting,
                  onChanged: onUserChanged,
                ),
                _Dropdown(
                  value: role,
                  items: _grantableRoles,
                  enabled: !submitting,
                  onChanged: onRoleChanged,
                ),
                _Dropdown(
                  value: scopeChoice,
                  items: <String>[...kKnownWorkspaces, _allScopeMarker],
                  enabled: !submitting,
                  onChanged: onScopeChanged,
                ),
                FilledButton.icon(
                  onPressed: submitting ? null : submit,
                  icon: submitting
                      ? const SizedBox(
                          width: 16,
                          height: 16,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Icon(Icons.check),
                  label: const Text('Grant'),
                ),
              ],
            ),
            _GrantStatus(state: state, userId: userId, role: role),
          ],
        );
      },
    );
  }
}

class _Dropdown extends StatelessWidget {
  const _Dropdown({
    required this.value,
    required this.items,
    required this.enabled,
    required this.onChanged,
  });

  final String value;
  final List<String> items;
  final bool enabled;
  final ValueChanged<String> onChanged;

  @override
  Widget build(BuildContext context) {
    return DropdownButton<String>(
      value: value,
      // Declared explicitly so the web icon tree-shaker keeps the glyph.
      icon: const Icon(Icons.arrow_drop_down),
      onChanged: enabled
          ? (v) {
              if (v != null) onChanged(v);
            }
          : null,
      items: <DropdownMenuItem<String>>[
        for (final item in items)
          DropdownMenuItem<String>(value: item, child: Text(item)),
      ],
    );
  }
}

class _GrantStatus extends StatelessWidget {
  const _GrantStatus({
    required this.state,
    required this.userId,
    required this.role,
  });

  final ActionState state;
  final String userId;
  final String role;

  @override
  Widget build(BuildContext context) {
    final String? message = switch (state) {
      Idle() || Submitting() => null,
      Success() => 'Granted: $userId -> $role',
      Denied(:final reason) => 'Denied: $reason',
      Failed(:final error) => 'transport: $error',
    };
    if (message == null) return const SizedBox.shrink();
    final scheme = Theme.of(context).colorScheme;
    final bool ok = state is Success;
    final Color color = ok ? const Color(0xFF2E7D32) : scheme.error;
    return Padding(
      padding: const EdgeInsets.only(top: 8),
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
}
