// reaction/example/lib/client/app.dart
//
// Owns the [RemoteScope] for the app's lifetime and routes between
// login / home / expired screens based on [AuthStatus] transitions.

import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_example/client/home_screen.dart';
import 'package:reaction_example/client/login_screen.dart';

/// Server URL: defaults to localhost:8080; override with
/// `REACTION_SERVER_URL` for non-localhost demo deployments.
Uri _resolveServerUrl() {
  final envUrl = Platform.environment['REACTION_SERVER_URL'];
  if (envUrl != null && envUrl.isNotEmpty) return Uri.parse(envUrl);
  return Uri.parse('http://127.0.0.1:8080');
}

class NotesApp extends StatefulWidget {
  const NotesApp({super.key});

  @override
  State<NotesApp> createState() => _NotesAppState();
}

class _NotesAppState extends State<NotesApp> {
  late final Uri _baseUrl;
  late final RemoteScope _scope;
  late final StreamSubscription<AuthStatus> _authSub;
  AuthStatus _status = const NotAuthenticated();

  @override
  void initState() {
    super.initState();
    _baseUrl = _resolveServerUrl();
    _scope = RemoteScope(baseUrl: _baseUrl);
    _status = _scope.authSession.current;
    _authSub = _scope.authSession.stream.listen((next) {
      if (!mounted) return;
      setState(() => _status = next);
    });
  }

  @override
  void dispose() {
    unawaited(_authSub.cancel());
    unawaited(_scope.dispose());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final Widget body = switch (_status) {
      Authenticated() => HomeScreen(scope: _scope, baseUrl: _baseUrl),
      Expired() => LoginScreen(
        scope: _scope,
        message: 'Your session expired — please sign in again.',
      ),
      NotAuthenticated() => LoginScreen(scope: _scope),
    };

    return MaterialApp(
      title: 'Reaction Notes Example',
      home: Scaffold(
        appBar: AppBar(title: const Text('Reaction Notes Example')),
        body: body,
      ),
    );
  }
}
