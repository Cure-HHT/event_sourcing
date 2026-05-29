// reaction/example/lib/client/home_screen.dart
//
// Authenticated home screen. The composition hub that wires the
// reaction_widgets primitives together:
//
//   - ReActionErrorListener  — surfaces transport transitions
//                              (reconnecting / disconnected) as snackbars
//                              without rebuilding the tree.
//   - StreamBuilder<EffectiveAuthorization?> — feeds the display pieces
//                              that need the FULL authorization snapshot
//                              (identity role, permissions card, the
//                              submit form's scope pre-filter). The
//                              headless layer deliberately ships no
//                              snapshot-exposing builder, so reading the
//                              raw `permissionSource.stream` here is the
//                              supported pattern for *display*.
//   - PermissionGate('assign_role') — gates the admin panel. This is the
//                              coarse boolean affordance gate the widget
//                              layer DOES ship.
//   - ViewListener<Note>     — fires a snackbar when a live note arrives,
//                              demonstrating an imperative side-effect on
//                              view updates without rebuilding NotesList.
//   - NotesList / SubmitNoteForm / AdminPanel — per-app rendering sugar
//                              built on ViewBuilder / ActionBuilder.
//
// All substrate access flows through `ReActionScope.of(context)` — no
// widget here references EventStore / ActionDispatcher directly
// (EVS-PRD-reaction-widget-contract-F).

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:reaction_widgets/reaction_widgets.dart';

import 'admin_panel.dart';
import 'notes_list.dart';
import 'submit_note_form.dart';
import 'ui.dart';

/// Workspaces known to the demo's bootstrap seed. The UI offers all
/// three; server-side scope enforcement decides per-dispatch whether
/// each is authorized for the active user. The deliberate redundancy
/// ('mars' is never seeded) demonstrates that scope checks happen at
/// the substrate, not at the UI: any user can ASK to submit there, the
/// substrate denies with DispatchAuthorizationDenied.
const List<String> kKnownWorkspaces = <String>['west', 'east', 'mars'];

class HomeScreen extends StatelessWidget {
  const HomeScreen({required this.baseUrl, super.key});

  /// Server base URL for the out-of-band `/admin/revoke` demo endpoint.
  final Uri baseUrl;

  void _snack(BuildContext context, String message) {
    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(SnackBar(content: Text(message)));
  }

  Future<void> _forceLogout(BuildContext context) async {
    final scope = ReActionScope.of(context);
    final principal = scope.authSession.principal;
    if (principal is! UserPrincipal) {
      _snack(context, 'Not signed in as a user.');
      return;
    }
    // Out-of-band admin endpoint: revokes the SEEDED assignment for this
    // user (their starting role + scope from bootstrap), which closes the
    // WS with 4003 and flips the session to Expired.
    final url = baseUrl.replace(
      path: '/admin/revoke',
      queryParameters: <String, String>{'user': principal.userId},
    );
    final client = http.Client();
    try {
      await client.post(url);
      if (context.mounted) {
        _snack(context, 'Force-logout requested for ${principal.userId}.');
      }
    } catch (e) {
      if (context.mounted) _snack(context, 'force-logout failed: $e');
    } finally {
      client.close();
    }
  }

  @override
  Widget build(BuildContext context) {
    final scope = ReActionScope.of(context);
    final principal = scope.authSession.principal;

    return ReActionErrorListener(
      onTransportReconnecting: (ctx) => _snack(ctx, 'Reconnecting…'),
      onTransportDisconnected: (ctx) =>
          _snack(ctx, 'Disconnected — the reconnect policy gave up.'),
      child: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 720),
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: StreamBuilder<EffectiveAuthorization?>(
              stream: scope.permissionSource.stream,
              initialData: scope.permissionSource.current,
              builder: (context, snapshot) {
                final snap = snapshot.data;
                return Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: <Widget>[
                    _IdentityBar(
                      principal: principal,
                      snapshot: snap,
                      onSignOut: () => scope.authSession.setCredential(null),
                      onForceLogout: () => _forceLogout(context),
                    ),
                    const SizedBox(height: 12),
                    _PermissionsCard(snapshot: snap),
                    const SizedBox(height: 12),
                    SubmitNoteForm(snapshot: snap),
                    const SizedBox(height: 12),
                    // PermissionGate renders nothing when the role lacks
                    // assign_role; the panel appears live the moment a grant
                    // lands (the gate listens on permissionSource.stream).
                    const PermissionGate(
                      permission: 'assign_role',
                      child: Padding(
                        padding: EdgeInsets.only(bottom: 12),
                        child: AdminPanel(),
                      ),
                    ),
                    Expanded(
                      // ViewListener subscribes to the same view as NotesList
                      // but for side-effects only: it toasts on each LIVE note
                      // (Delta) without rebuilding the list. Replay rows arrive
                      // as Snapshot, not Delta, so this fires only for notes
                      // added after first paint.
                      child: ViewListener<Note>(
                        viewName: 'notes_today',
                        mapper: Note.fromRow,
                        onUpdate: (ctx, update) {
                          if (update case Delta<Note>(:final value)) {
                            _snack(
                              ctx,
                              'New note in ${value.workspace}: ${value.title}',
                            );
                          }
                        },
                        child: const _NotesSection(),
                      ),
                    ),
                  ],
                );
              },
            ),
          ),
        ),
      ),
    );
  }
}

/// Identity header: avatar + userId + role chip + sign-out / force-logout.
class _IdentityBar extends StatelessWidget {
  const _IdentityBar({
    required this.principal,
    required this.snapshot,
    required this.onSignOut,
    required this.onForceLogout,
  });

  final Principal? principal;
  final EffectiveAuthorization? snapshot;
  final VoidCallback onSignOut;
  final VoidCallback onForceLogout;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final userId = principal is UserPrincipal
        ? (principal! as UserPrincipal).userId
        : '(anonymous)';
    final role = snapshot?.activeRole ?? '(loading)';
    return Card(
      elevation: 0,
      margin: EdgeInsets.zero,
      color: scheme.primaryContainer,
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: <Widget>[
            CircleAvatar(
              backgroundColor: scheme.primary,
              foregroundColor: scheme.onPrimary,
              child: Text(
                userId.isEmpty ? '?' : userId[0].toUpperCase(),
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: <Widget>[
                  Text(
                    userId,
                    style: TextStyle(
                      fontWeight: FontWeight.bold,
                      fontSize: 16,
                      color: scheme.onPrimaryContainer,
                    ),
                  ),
                  const SizedBox(height: 4),
                  TagChip(
                    label: role,
                    color: scheme.primary,
                    icon: Icons.verified_user_outlined,
                  ),
                ],
              ),
            ),
            IconButton(
              tooltip: 'Force-logout (revoke seed)',
              onPressed: onForceLogout,
              icon: const Icon(Icons.gpp_bad_outlined),
            ),
            TextButton.icon(
              onPressed: onSignOut,
              icon: const Icon(Icons.logout),
              label: const Text('Sign out'),
            ),
          ],
        ),
      ),
    );
  }
}

/// Live display of the active role's permissions + scope assignments,
/// rendered as chips. Reads the full EffectiveAuthorization (the headless
/// PermissionGate only answers boolean "holds X?"), so this is the app's
/// own raw-stream consumer for display.
class _PermissionsCard extends StatelessWidget {
  const _PermissionsCard({required this.snapshot});

  final EffectiveAuthorization? snapshot;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final snap = snapshot;
    if (snap == null) {
      return const SectionCard(
        title: 'Your access',
        icon: Icons.shield_outlined,
        child: Text('(loading permissions…)'),
      );
    }
    final perms = snap.rolePermissions.map((p) => p.name).toList()..sort();
    return SectionCard(
      title: 'Your access',
      icon: Icons.shield_outlined,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: <Widget>[
          _ChipRow(
            label: 'Permissions',
            empty: '(none)',
            chips: <Widget>[
              for (final name in perms)
                TagChip(label: name, color: scheme.primary),
            ],
          ),
          const SizedBox(height: 8),
          _ChipRow(
            label: 'Scopes',
            empty: '(none)',
            chips: <Widget>[
              for (final a in snap.scopeAssignments)
                TagChip(
                  label: a.scope.toString(),
                  color: scheme.tertiary,
                  icon: Icons.layers_outlined,
                ),
            ],
          ),
        ],
      ),
    );
  }
}

class _ChipRow extends StatelessWidget {
  const _ChipRow({required this.label, required this.empty, required this.chips});

  final String label;
  final String empty;
  final List<Widget> chips;

  @override
  Widget build(BuildContext context) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: <Widget>[
        SizedBox(
          width: 92,
          child: Text(
            label,
            style: const TextStyle(fontWeight: FontWeight.w600),
          ),
        ),
        Expanded(
          child: chips.isEmpty
              ? Text(empty, style: TextStyle(color: Theme.of(context).colorScheme.outline))
              : Wrap(spacing: 6, runSpacing: 6, children: chips),
        ),
      ],
    );
  }
}

/// Section header above the live notes list.
class _NotesSection extends StatelessWidget {
  const _NotesSection();

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: <Widget>[
        Padding(
          padding: const EdgeInsets.symmetric(vertical: 8),
          child: Row(
            children: <Widget>[
              Icon(Icons.event_note_outlined, size: 18, color: scheme.primary),
              const SizedBox(width: 8),
              Text(
                "Today's notes",
                style: Theme.of(context).textTheme.titleSmall?.copyWith(
                  color: scheme.primary,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ],
          ),
        ),
        const Expanded(child: NotesList()),
      ],
    );
  }
}
