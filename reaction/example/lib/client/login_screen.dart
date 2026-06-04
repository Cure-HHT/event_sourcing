// reaction/example/lib/client/login_screen.dart

import 'package:flutter/material.dart';
import 'package:reaction_example/client/automation.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

/// Login screen: enter one of the seeded usernames (alice, bob, carol,
/// dave) — the server's RoleAwareTrustingValidator reads
/// `user_role_scopes` and surfaces the user's highest-privileged role
/// (admin > editor > viewer). Any other non-empty string authenticates
/// as anonymous, with which every action is denied.
///
/// Reads the composed scope via `ReActionScope.of(context)` rather than
/// taking it as a constructor param — see app.dart for why.
class LoginScreen extends StatefulWidget {
  const LoginScreen({this.message, super.key});

  /// Optional banner shown above the form (e.g. on Expired transition).
  final String? message;

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen> {
  final TextEditingController _controller = TextEditingController(
    text: 'alice',
  );

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  void _signIn() {
    final username = _controller.text.trim();
    if (username.isEmpty) return;
    ReActionScope.of(context).authSession.setCredential(username);
  }

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    return Center(
      child: SingleChildScrollView(
        padding: const EdgeInsets.all(24),
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 420),
          child: Card(
            elevation: 0,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(20),
              side: BorderSide(color: scheme.outlineVariant),
            ),
            child: Padding(
              padding: const EdgeInsets.all(24),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: <Widget>[
                  CircleAvatar(
                    radius: 28,
                    backgroundColor: scheme.primaryContainer,
                    foregroundColor: scheme.onPrimaryContainer,
                    child: const Icon(Icons.event_note, size: 28),
                  ),
                  const SizedBox(height: 16),
                  Text(
                    'Sign in',
                    textAlign: TextAlign.center,
                    style: Theme.of(context).textTheme.headlineSmall,
                  ),
                  const SizedBox(height: 16),
                  if (widget.message != null) ...<Widget>[
                    Container(
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: scheme.errorContainer,
                        borderRadius: BorderRadius.circular(12),
                      ),
                      child: Row(
                        children: <Widget>[
                          Icon(
                            Icons.info_outline,
                            size: 18,
                            color: scheme.onErrorContainer,
                          ),
                          const SizedBox(width: 8),
                          Expanded(
                            child: Text(
                              widget.message!,
                              style: TextStyle(color: scheme.onErrorContainer),
                            ),
                          ),
                        ],
                      ),
                    ),
                    const SizedBox(height: 16),
                  ],
                  AutomationTarget(
                    identifier: 'login-username',
                    field: true,
                    child: TextField(
                      controller: _controller,
                      decoration: const InputDecoration(
                        labelText: 'Username',
                        hintText: 'alice / bob / carol / dave',
                        helperText:
                            'alice/bob: editor (west/east). carol: admin. '
                            'dave: viewer.',
                        prefixIcon: Icon(Icons.person_outline),
                        border: OutlineInputBorder(),
                      ),
                      onSubmitted: (_) => _signIn(),
                    ),
                  ),
                  const SizedBox(height: 16),
                  AutomationTarget(
                    identifier: 'login-button',
                    button: true,
                    child: FilledButton.icon(
                      onPressed: _signIn,
                      icon: const Icon(Icons.login),
                      label: const Text('Sign in'),
                    ),
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }
}
