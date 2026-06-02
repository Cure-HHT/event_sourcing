// example_clinical_scopes/test/clinical_scoping_e2e_test.dart
//
// Tier-2 end-to-end test: boots the REAL clinical-scopes demo server
// (`bootstrap()`) onto a loopback shelf server, then connects real
// `RemoteScope` clients over real HTTP + WebSocket. This exercises the
// PRODUCTION ScopeDescendantExpander wired through ReactionHandlers — the
// whole point of the demo — NOT a stub.
//
// Asserts the scope-narrowing of the `participants` read path:
//   - dr-investigator (sites A+B)  -> {P-1, P-2, P-3}, NOT P-9
//   - dr-overseer (region-West)    -> {P-1, P-2, P-3} via two-hop, NOT P-9
//   - dr-admin (TotalWildcard)     -> all {P-1, P-2, P-3, P-9}
//   - dr-unassigned                -> none (read path denied)

import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

import 'package:example_clinical_scopes/server/clinical_bootstrap.dart';

/// A running demo server plus the clients a test has opened against it.
class DemoServer {
  DemoServer._(this._http, this._dispose, this.baseUrl);

  final HttpServer _http;
  final Future<void> Function() _dispose;
  final Uri baseUrl;
  final List<RemoteScope> _clients = <RemoteScope>[];
  bool _stopped = false;

  /// Boot the demo's own `bootstrap()` onto an ephemeral loopback port.
  static Future<DemoServer> start() async {
    final result = await bootstrap();
    final http = await shelf_io.serve(result.router.call, '127.0.0.1', 0);
    return DemoServer._(
      http,
      result.dispose,
      Uri.parse('http://127.0.0.1:${http.port}'),
    );
  }

  /// A fresh, unauthenticated client.
  RemoteScope client() {
    final scope = RemoteScope(baseUrl: baseUrl);
    _clients.add(scope);
    return scope;
  }

  /// A client logged in as [userId], resolved once Authenticated.
  Future<RemoteScope> login(String userId) async {
    final scope = client();
    scope.authSession.setCredential(userId);
    await scope.authSession.stream
        .firstWhere((s) => s is Authenticated)
        .timeout(
          const Duration(seconds: 5),
          onTimeout: () => throw StateError('login($userId) timed out'),
        );
    return scope;
  }

  Future<void> stop() async {
    if (_stopped) return;
    _stopped = true;
    for (final c in _clients) {
      try {
        await c.dispose();
      } on Object {
        // best-effort teardown
      }
    }
    await _http.close(force: true);
    await _dispose();
  }
}

/// Subscribes a client to `participants` and accumulates the live row set,
/// keyed by participant id.
class ParticipantsObserver {
  ParticipantsObserver(RemoteScope scope, {this.label = 'observer'}) {
    _sub = scope.viewSource
        .watch<Map<String, Object?>>(viewName: 'participants', mapper: (m) => m)
        .listen(_onUpdate, onError: errors.add);
  }

  final String label;
  late final StreamSubscription<Update<Map<String, Object?>>> _sub;

  /// Every row ever seen (snapshot or delta), keyed by participant id.
  final Map<String, Map<String, Object?>> rows =
      <String, Map<String, Object?>>{};

  bool sawEndOfReplay = false;
  final List<Object> errors = <Object>[];

  void _onUpdate(Update<Map<String, Object?>> u) {
    switch (u) {
      case Snapshot<Map<String, Object?>>(:final value):
        if (value == null) break;
        final id = value['aggregateId'] as String?;
        if (id != null) rows[id] = value;
      case EndOfReplay<Map<String, Object?>>():
        sawEndOfReplay = true;
      case Delta<Map<String, Object?>>(:final value):
        final id = value['aggregateId'] as String?;
        if (id != null) rows[id] = value;
      case Tombstone<Map<String, Object?>>():
        break;
    }
  }

  Set<String> get ids => rows.keys.toSet();

  Future<void> cancel() => _sub.cancel();
}

/// Poll [condition] until true or [timeout]; throws with [describe].
Future<void> pumpUntil(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 5),
  Duration poll = const Duration(milliseconds: 10),
  String Function()? describe,
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw StateError(
        'pumpUntil timed out: ${describe?.call() ?? 'condition not met'}',
      );
    }
    await Future<void>.delayed(poll);
  }
}

void main() {
  late DemoServer server;

  setUp(() async {
    server = await DemoServer.start();
  });
  tearDown(() => server.stop());

  test('Investigator (sites A+B) sees exactly {P-1, P-2, P-3}, not P-9 '
      '(one-hop site->participant expansion)', () async {
    final inv = await server.login('dr-investigator');
    final obs = ParticipantsObserver(inv, label: 'investigator');
    await pumpUntil(
      () => obs.sawEndOfReplay,
      describe: () => 'investigator end_of_replay; ids=${obs.ids}',
    );
    expect(obs.ids, {'P-1', 'P-2', 'P-3'});
    expect(obs.ids.contains('P-9'), isFalse);
    await obs.cancel();
  });

  test('Overseer (region-West) sees exactly {P-1, P-2, P-3}, not P-9 '
      '(two-hop region->site->participant expansion)', () async {
    final ovr = await server.login('dr-overseer');
    final obs = ParticipantsObserver(ovr, label: 'overseer');
    await pumpUntil(
      () => obs.sawEndOfReplay,
      describe: () => 'overseer end_of_replay; ids=${obs.ids}',
    );
    expect(obs.ids, {'P-1', 'P-2', 'P-3'});
    expect(obs.ids.contains('P-9'), isFalse);
    await obs.cancel();
  });

  test(
    'Admin (TotalWildcardScope) sees all participants {P-1, P-2, P-3, P-9}',
    () async {
      final adm = await server.login('dr-admin');
      final obs = ParticipantsObserver(adm, label: 'admin');
      await pumpUntil(
        () => obs.sawEndOfReplay,
        describe: () => 'admin end_of_replay; ids=${obs.ids}',
      );
      expect(obs.ids, {'P-1', 'P-2', 'P-3', 'P-9'});
      await obs.cancel();
    },
  );

  test('Unassigned user sees no participants (read path denied)', () async {
    final zoe = server.client();
    zoe.authSession.setCredential('dr-unassigned'); // no role assignment
    await zoe.authSession.stream
        .firstWhere((s) => s is Authenticated)
        .timeout(const Duration(seconds: 5));

    final stream = zoe.viewSource.watch<Map<String, Object?>>(
      viewName: 'participants',
      mapper: (m) => m,
    );
    Object? readError;
    try {
      await stream.first.timeout(const Duration(seconds: 3));
    } on Object catch (e) {
      readError = e;
    }
    expect(
      readError,
      isNotNull,
      reason: 'an unassigned (anonymous) principal must be denied the read',
    );
    expect(readError.toString(), contains('subscription_denied'));
  });
}
