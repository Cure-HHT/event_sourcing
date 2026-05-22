// reaction/example/lib/client/login_screen.dart

import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';

/// Login screen: enter any username; the server's TrustingAuthValidator
/// accepts it verbatim and surfaces an `editor` role.
class LoginScreen extends StatefulWidget {
  const LoginScreen({required this.scope, this.message, super.key});

  final RemoteScope scope;

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
    widget.scope.authSession.setCredential(username);
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(24),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: <Widget>[
          if (widget.message != null) ...<Widget>[
            Container(
              padding: const EdgeInsets.all(12),
              color: Colors.amber.shade100,
              child: Text(widget.message!),
            ),
            const SizedBox(height: 16),
          ],
          TextField(
            controller: _controller,
            decoration: const InputDecoration(
              labelText: 'Username',
              hintText: 'alice',
            ),
            onSubmitted: (_) => _signIn(),
          ),
          const SizedBox(height: 16),
          ElevatedButton(onPressed: _signIn, child: const Text('Sign in')),
        ],
      ),
    );
  }
}
