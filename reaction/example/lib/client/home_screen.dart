// reaction/example/lib/client/home_screen.dart
//
// Authenticated home screen. Composes five sections:
//
//   - IdentityBar      — userId + activeRole + "Sign out" / "Force-logout"
//   - PermissionsCard  — current PermissionSnapshot.grants, live via stream
//   - NotesList        — reactive notes_today view with workspace tag
//   - SubmitNoteForm   — workspace dropdown + title + submit button,
//                        permission-gated via PermissionSource.stream
//   - AdminPanel       — visible iff active role holds Permission('assign_role'),
//                        lists user_role_scopes view + grant/revoke forms
//
// Every section that depends on the user's permissions wraps in a
// StreamBuilder<PermissionSnapshot?> on PermissionSource.stream so a
// server-pushed stale_data envelope (handled in RemoteConnection +
// RemoteScope) flows through to a live UI update — alice's submit form
// gains 'east' the moment carol grants her access.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';

import 'admin_panel.dart';
import 'notes_list.dart';
import 'submit_note_form.dart';

/// Workspaces known to the demo's bootstrap seed. The UI offers all
/// three; server-side scope enforcement decides per-dispatch whether
/// each is authorized for the active user. The deliberate redundancy
/// ('mars' is never seeded) demonstrates that scope checks happen at
/// the substrate, not at the UI: any user can ASK to submit there, the
/// substrate denies with DispatchAuthorizationDenied.
const List<String> kKnownWorkspaces = <String>['west', 'east', 'mars'];

class HomeScreen extends StatefulWidget {
  const HomeScreen({required this.scope, required this.baseUrl, super.key});

  final RemoteScope scope;

  /// Server base URL for the out-of-band `/admin/revoke` demo endpoint.
  final Uri baseUrl;

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  String? _flash;

  void _setFlash(String? message) {
    if (mounted) setState(() => _flash = message);
  }

  Future<void> _forceLogout() async {
    final principal = widget.scope.authSession.principal;
    if (principal is! UserPrincipal) {
      _setFlash('Not signed in as a user.');
      return;
    }
    // Out-of-band admin endpoint: revokes the SEEDED assignment for
    // this user (their starting role + scope from bootstrap), which
    // closes the WS with 4003 and flips the session to Expired.
    final url = widget.baseUrl.replace(
      path: '/admin/revoke',
      queryParameters: <String, String>{'user': principal.userId},
    );
    final client = http.Client();
    try {
      await client.post(url);
      _setFlash('Force-logout requested for ${principal.userId}.');
    } catch (e) {
      _setFlash('force-logout failed: $e');
    } finally {
      client.close();
    }
  }

  void _signOut() {
    widget.scope.authSession.setCredential(null);
  }

  @override
  Widget build(BuildContext context) {
    final principal = widget.scope.authSession.principal;
    return Padding(
      padding: const EdgeInsets.all(16),
      child: StreamBuilder<PermissionSnapshot?>(
        stream: widget.scope.permissionSource.stream,
        initialData: widget.scope.permissionSource.current,
        builder: (context, snapshot) {
          final snap = snapshot.data;
          final isAdmin =
              snap != null &&
              snap.grants.contains(const Permission('assign_role'));
          return Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: <Widget>[
              _IdentityBar(
                principal: principal,
                snapshot: snap,
                onSignOut: _signOut,
                onForceLogout: _forceLogout,
              ),
              const Divider(),
              _PermissionsCard(snapshot: snap),
              const SizedBox(height: 12),
              SubmitNoteForm(
                actionSubmitter: widget.scope.actionSubmitter,
                snapshot: snap,
                onFlash: _setFlash,
              ),
              if (_flash != null)
                Padding(
                  padding: const EdgeInsets.only(top: 8),
                  child: Text(
                    _flash!,
                    style: const TextStyle(color: Colors.grey),
                  ),
                ),
              const SizedBox(height: 12),
              if (isAdmin) ...<Widget>[
                AdminPanel(
                  viewSource: widget.scope.viewSource,
                  actionSubmitter: widget.scope.actionSubmitter,
                  onFlash: _setFlash,
                ),
                const SizedBox(height: 12),
              ],
              Expanded(child: NotesList(viewSource: widget.scope.viewSource)),
            ],
          );
        },
      ),
    );
  }
}

/// Top bar: identity + role + scope summary + sign-out + force-logout.
class _IdentityBar extends StatelessWidget {
  const _IdentityBar({
    required this.principal,
    required this.snapshot,
    required this.onSignOut,
    required this.onForceLogout,
  });

  final Principal? principal;
  final PermissionSnapshot? snapshot;
  final VoidCallback onSignOut;
  final VoidCallback onForceLogout;

  @override
  Widget build(BuildContext context) {
    final userId = principal is UserPrincipal
        ? (principal as UserPrincipal).userId
        : '(anonymous)';
    final role = snapshot?.role ?? '(loading)';
    return Row(
      children: <Widget>[
        Expanded(
          child: Text(
            'Signed in: $userId — role: $role',
            style: const TextStyle(fontWeight: FontWeight.bold),
          ),
        ),
        TextButton(
          onPressed: onForceLogout,
          child: const Text('Force-logout (revoke seed)'),
        ),
        const SizedBox(width: 8),
        TextButton(onPressed: onSignOut, child: const Text('Sign out')),
      ],
    );
  }
}

/// Compact permission display so the user can see what their active
/// role grants. Updates live via the enclosing StreamBuilder.
class _PermissionsCard extends StatelessWidget {
  const _PermissionsCard({required this.snapshot});

  final PermissionSnapshot? snapshot;

  @override
  Widget build(BuildContext context) {
    final snap = snapshot;
    final body = snap == null
        ? const Text('(loading permissions…)')
        : Text(
            snap.grants.isEmpty
                ? '(no permissions)'
                : 'Permissions: ${(snap.grants.map((p) => p.name).toList()..sort()).join(", ")}',
          );
    return Container(
      padding: const EdgeInsets.all(8),
      decoration: BoxDecoration(
        color: Colors.blueGrey.shade50,
        borderRadius: BorderRadius.circular(4),
      ),
      child: body,
    );
  }
}
