// reaction/example/lib/client/app.dart
//
// Owns the [RemoteScope] for the app's lifetime, threads it down the
// tree via reaction_widgets' [ReActionScope] InheritedWidget, and routes
// between login / home / expired screens based on [AuthStatus].
//
// Why mount [ReActionScope] here: every descendant widget reads the
// composed scope via `ReActionScope.of(context)` instead of receiving it
// through constructor params. That is the substrate-agnostic seam — the
// same widget tree would work unchanged if `_scope` were a `LocalScope`
// (in-process mobile) instead of a `RemoteScope` (per
// EVS-PRD-reaction-widget-contract-B).
//
// Why app-level routing still subscribes to AuthStatus directly: the
// login/home/expired switch is a *build-time* decision keyed on auth
// state, which is the one thing the headless widget layer deliberately
// does NOT ship a primitive for (it ships ReActionErrorListener for
// imperative side-effects, not whole-screen routing). Driving routing
// from the raw `authSession.stream` here is the supported pattern, just
// as displaying the full EffectiveAuthorization uses a raw
// `permissionSource.stream` in home_screen.

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_example/client/home_screen.dart';
import 'package:reaction_example/client/login_screen.dart';

/// Server URL: defaults to localhost:8080; override with the
/// `REACTION_SERVER_URL` compile-time define (works on every target,
/// including web) for non-localhost demo deployments:
/// `flutter run --dart-define=REACTION_SERVER_URL=http://10.0.0.5:8080`.
const String _serverUrl = String.fromEnvironment(
  'REACTION_SERVER_URL',
  defaultValue: 'http://127.0.0.1:8080',
);

Uri _resolveServerUrl() => Uri.parse(_serverUrl);

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
      Authenticated() => HomeScreen(baseUrl: _baseUrl),
      Expired() => const LoginScreen(
        message: 'Your session expired — please sign in again.',
      ),
      NotAuthenticated() => const LoginScreen(),
    };

    // App-level theme — pure consumer styling; reaction_widgets ships
    // none. Material 3 + a seeded color scheme.
    final theme = ThemeData(
      useMaterial3: true,
      colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF006A60)),
    );

    // Mount the scope once, above MaterialApp, so every descendant reads
    // it via `ReActionScope.of(context)`.
    return ReActionScope(
      scope: _scope,
      child: MaterialApp(
        title: 'Reaction Notes Example',
        theme: theme,
        home: Scaffold(
          backgroundColor: theme.colorScheme.surface,
          appBar: AppBar(
            backgroundColor: theme.colorScheme.primary,
            foregroundColor: theme.colorScheme.onPrimary,
            title: const Row(
              mainAxisSize: MainAxisSize.min,
              children: <Widget>[
                Icon(Icons.bolt),
                SizedBox(width: 8),
                Text('Reaction Notes'),
              ],
            ),
          ),
          body: body,
        ),
      ),
    );
  }
}
