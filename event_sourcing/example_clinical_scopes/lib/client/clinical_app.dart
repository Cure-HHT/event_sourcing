// example_clinical_scopes/lib/client/clinical_app.dart
//
// Owns the [RemoteScope] for the app's lifetime and threads it down the
// tree via reaction_widgets' [ReActionScope] InheritedWidget. Mirrors
// `reaction/example`'s app.dart, but replaces the login form with a
// user-switcher dropdown over the demo's preset users.
//
// User-switch mechanism: switching the selected user REBUILDS the
// RemoteScope (and thus the ReActionScope) from scratch under the new
// credential. This is the simplest correct approach for a demo — the new
// scope's ViewBuilder opens a fresh, freshly-authenticated WS
// subscription, so the participants list re-narrows to the newly-selected
// user's hierarchy scope. (setCredential re-validates the AuthSession but
// does not re-issue subscriptions already opened on the shared
// connection under the prior credential, so a rebuild is the clean path
// for re-subscribing.)

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

import 'package:example_clinical_scopes/client/participants_list.dart';

/// Server URL: defaults to localhost:8080; override with the
/// `REACTION_SERVER_URL` compile-time define for non-localhost demos:
/// `flutter run --dart-define=REACTION_SERVER_URL=http://10.0.0.5:8080`.
const String _serverUrl = String.fromEnvironment(
  'REACTION_SERVER_URL',
  defaultValue: 'http://127.0.0.1:8080',
);

Uri _resolveServerUrl() => Uri.parse(_serverUrl);

/// The preset demo users the switcher offers, with a human-readable
/// description of the scope each one is assigned.
const Map<String, String> _presetUsers = <String, String>{
  'dr-investigator': 'Investigator — sites A + B (region-West)',
  'dr-overseer': 'Overseer — whole region-West (two-hop)',
  'dr-admin': 'Admin — all participants',
  'dr-unassigned': 'Unassigned — no scope (sees nothing)',
};

class ClinicalScopesApp extends StatefulWidget {
  const ClinicalScopesApp({super.key});

  @override
  State<ClinicalScopesApp> createState() => _ClinicalScopesAppState();
}

class _ClinicalScopesAppState extends State<ClinicalScopesApp> {
  late final Uri _baseUrl;
  late RemoteScope _scope;
  String _selectedUser = _presetUsers.keys.first;

  @override
  void initState() {
    super.initState();
    _baseUrl = _resolveServerUrl();
    _scope = _connect(_selectedUser);
  }

  RemoteScope _connect(String userId) {
    final scope = RemoteScope(baseUrl: _baseUrl);
    scope.authSession.setCredential(userId);
    return scope;
  }

  void _switchUser(String? userId) {
    if (userId == null || userId == _selectedUser) return;
    final previous = _scope;
    setState(() {
      _selectedUser = userId;
      _scope = _connect(userId);
    });
    // Tear down the prior scope after the rebuild so the new tree mounts
    // cleanly on the fresh scope.
    unawaited(previous.dispose());
  }

  @override
  void dispose() {
    unawaited(_scope.dispose());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final theme = ThemeData(
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF006A60)),
    );

    return MaterialApp(
      title: 'Clinical Scopes Example',
      theme: theme,
      home: Scaffold(
        backgroundColor: theme.colorScheme.surface,
        appBar: AppBar(
          backgroundColor: theme.colorScheme.primary,
          foregroundColor: theme.colorScheme.onPrimary,
          title: const Row(
            mainAxisSize: MainAxisSize.min,
            children: <Widget>[
              Icon(Icons.account_tree_outlined),
              SizedBox(width: 8),
              Text('Clinical Scopes'),
            ],
          ),
        ),
        body: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: <Widget>[
              _UserSwitcher(selected: _selectedUser, onChanged: _switchUser),
              const SizedBox(height: 16),
              // Re-key the ReActionScope by user so a switch tears the
              // subtree down and remounts it against the fresh scope.
              Expanded(
                child: ReActionScope(
                  key: ValueKey<String>(_selectedUser),
                  scope: _scope,
                  child: const ParticipantsList(),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

/// Dropdown over the preset demo users.
class _UserSwitcher extends StatelessWidget {
  const _UserSwitcher({required this.selected, required this.onChanged});

  final String selected;
  final ValueChanged<String?> onChanged;

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Card(
      elevation: 0,
      margin: EdgeInsets.zero,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(16),
        side: BorderSide(color: scheme.outlineVariant),
      ),
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Row(
          children: <Widget>[
            Icon(Icons.person_outline, color: scheme.primary),
            const SizedBox(width: 12),
            Expanded(
              child: DropdownButton<String>(
                isExpanded: true,
                value: selected,
                underline: const SizedBox.shrink(),
                onChanged: onChanged,
                items: <DropdownMenuItem<String>>[
                  for (final entry in _presetUsers.entries)
                    DropdownMenuItem<String>(
                      value: entry.key,
                      child: Column(
                        mainAxisSize: MainAxisSize.min,
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: <Widget>[
                          Text(
                            entry.key,
                            style: const TextStyle(fontWeight: FontWeight.w600),
                          ),
                          Text(
                            entry.value,
                            style: TextStyle(
                              fontSize: 11,
                              color: scheme.outline,
                            ),
                          ),
                        ],
                      ),
                    ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
