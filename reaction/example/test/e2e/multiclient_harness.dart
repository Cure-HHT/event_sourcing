// reaction/example/test/e2e/multiclient_harness.dart
//
// Tier-2 end-to-end test harness: boots the REAL `reaction_example`
// demo server (`bootstrap()`) onto a loopback shelf server, then lets a
// test connect N independent `RemoteScope` clients to it over real
// HTTP + WebSocket. This is the "multiple clients connected to a single
// server" surface that the rest of the suite exercises.
//
// Nothing here re-implements substrate logic. The harness only:
//   - serves the demo's own composition root,
//   - hands out real library clients (`RemoteScope`),
//   - and provides small polling/observation utilities so the scenarios
//     can make deterministic assertions about cross-client behaviour
//     without sprinkling magic sleeps everywhere.

import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_example/server/bootstrap.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

/// A running demo server plus the clients a test has opened against it.
///
/// Call [start] in `setUp`, [stop] in `tearDown` (or rely on the
/// returned [stop] being idempotent). All clients handed out by [client]
/// / [login] are disposed by [stop].
class DemoServer {
  DemoServer._(this._http, this._dispose, this.baseUrl);

  final HttpServer _http;
  final Future<void> Function() _dispose;
  final Uri baseUrl;
  final List<RemoteScope> _clients = <RemoteScope>[];
  bool _stopped = false;

  /// Boot `reaction_example`'s own `bootstrap()` onto an ephemeral
  /// loopback port. State is in-memory and dies with [stop].
  static Future<DemoServer> start() async {
    final result = await bootstrap();
    final http = await shelf_io.serve(result.router.call, '127.0.0.1', 0);
    return DemoServer._(
      http,
      result.dispose,
      Uri.parse('http://127.0.0.1:${http.port}'),
    );
  }

  /// A fresh, unauthenticated client. Caller drives [RemoteScope.authSession].
  RemoteScope client() {
    final scope = RemoteScope(baseUrl: baseUrl);
    _clients.add(scope);
    return scope;
  }

  /// A client logged in as [userId], resolved once [Authenticated] (or
  /// [AnonymousPrincipal] surfaces as [Authenticated] too — the demo's
  /// validator maps unknown bearers to an anonymous principal that the
  /// substrate then denies). Times out rather than hanging a suite.
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

/// Subscribes a client to `notes_today` and accumulates the live row set,
/// keyed by note title (tests use unique titles). Tracks the ordered
/// arrival of LIVE deltas separately so ordering can be asserted.
class NotesObserver {
  NotesObserver(RemoteScope scope, {this.label = 'observer'}) {
    _sub = scope.viewSource
        .watch<Map<String, Object?>>(viewName: 'notes_today', mapper: (m) => m)
        .listen(_onUpdate, onError: errors.add);
  }

  final String label;
  late final StreamSubscription<Update<Map<String, Object?>>> _sub;

  /// Every row ever seen (snapshot or delta), keyed by title.
  final Map<String, Map<String, Object?>> rows =
      <String, Map<String, Object?>>{};

  /// Titles of LIVE deltas (post-EndOfReplay), in arrival order.
  final List<String> liveDeltaTitles = <String>[];

  /// Titles seen in the initial replay (snapshot), in arrival order.
  final List<String> snapshotTitles = <String>[];

  bool sawEndOfReplay = false;
  final List<Object> errors = <Object>[];

  void _onUpdate(Update<Map<String, Object?>> u) {
    switch (u) {
      case Snapshot<Map<String, Object?>>(:final value):
        if (value == null) break;
        final t = value['title'] as String?;
        if (t != null) {
          rows[t] = value;
          snapshotTitles.add(t);
        }
      case EndOfReplay<Map<String, Object?>>():
        sawEndOfReplay = true;
      case Delta<Map<String, Object?>>(:final value):
        if (value == null) break;
        final t = value['title'] as String?;
        if (t != null) {
          rows[t] = value;
          liveDeltaTitles.add(t);
        }
      case Tombstone<Map<String, Object?>>():
        // notes_today never tombstones in the demo.
        break;
    }
  }

  Set<String> get titles => rows.keys.toSet();

  /// Wait until [titles] contains all of [expected], or time out.
  Future<void> waitForTitles(
    Set<String> expected, {
    Duration timeout = const Duration(seconds: 5),
  }) => pumpUntil(
    () => expected.every(titles.contains),
    timeout: timeout,
    describe: () => '[$label] expected titles $expected, have $titles',
  );

  Future<void> cancel() => _sub.cancel();
}

/// Poll [condition] until true or [timeout]; throws with [describe] on
/// timeout. Preferred over fixed `Future.delayed` sleeps so the suite is
/// both faster (returns as soon as the condition holds) and clearer
/// about what it was waiting for.
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

/// Submit `submit_note` and return the [DispatchResult].
Future<DispatchResult<Object?>> submitNote(
  RemoteScope scope, {
  required String workspace,
  required String title,
  String? idempotencyKey,
}) {
  return scope.actionSubmitter.submit(
    ActionSubmission(
      actionName: 'submit_note',
      rawInput: <String, Object?>{'workspace': workspace, 'title': title},
      idempotencyKey: idempotencyKey,
    ),
  );
}

/// Submit `assign_role` (admin only). [scope] is the BoundScope value, or
/// null for a wildcard-class assignment.
Future<DispatchResult<Object?>> assignRole(
  RemoteScope admin, {
  required String userId,
  required String role,
  String? workspace,
}) {
  return admin.actionSubmitter.submit(
    ActionSubmission(
      actionName: 'assign_role',
      rawInput: <String, Object?>{
        'user_id': userId,
        'role': role,
        'scope': workspace == null
            ? <String, Object?>{'wildcard_class': true}
            : <String, Object?>{'class': 'workspace', 'value': workspace},
      },
    ),
  );
}
