import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/principal_codec.dart';

class _Client extends http.BaseClient {
  _Client(this._respond);
  final http.Response Function(http.BaseRequest) _respond;
  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    final r = _respond(req);
    return http.StreamedResponse(Stream.value(r.bodyBytes), r.statusCode);
  }
}

/// Client that delays each `send` call on a per-call Completer, allowing
/// tests to interleave their own state changes between request issuance
/// and response delivery.
class _GatedClient extends http.BaseClient {
  _GatedClient(this._respond);
  final http.Response Function(http.BaseRequest) _respond;
  final List<Completer<void>> gates = <Completer<void>>[];

  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    final gate = Completer<void>();
    gates.add(gate);
    await gate.future;
    final r = _respond(req);
    return http.StreamedResponse(Stream.value(r.bodyBytes), r.statusCode);
  }
}

RemoteConnection connWithClient(http.Client client) => RemoteConnection(
  baseUrl: Uri.parse('http://localhost:1234'),
  httpClient: client,
  wsFactory: (_) => throw UnimplementedError(),
);

void main() {
  test('starts NotAuthenticated', () {
    final session = RemoteAuthSession(
      connection: connWithClient(_Client((_) => http.Response('', 200))),
    );
    expect(session.current, isA<NotAuthenticated>());
  });

  test('setCredential(cred) on 200 transitions to Authenticated', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(
        _Client(
          (req) => req.url.path == '/me'
              ? http.Response(
                  jsonEncode(
                    PrincipalCodec.encode(
                      UserPrincipal(
                        userId: 'alice',
                        roles: {'install'},
                        activeRole: 'install',
                      ),
                    ),
                  ),
                  200,
                )
              : http.Response('', 404),
        ),
      ),
    )..setCredential('alice');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(session.current, isA<Authenticated>());
    final p = (session.current as Authenticated).principal as UserPrincipal;
    expect(p.userId, 'alice');
    expect(p.roles, {'install'});
    expect(p.activeRole, 'install');
  });

  test('setCredential(cred) on 401 transitions to Expired', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(_Client((_) => http.Response('', 401))),
    )..setCredential('alice');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(session.current, isA<Expired>());
  });

  test('setCredential(null) transitions to NotAuthenticated', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(
        _Client(
          (_) => http.Response(
            jsonEncode(
              PrincipalCodec.encode(
                UserPrincipal(
                  userId: 'a',
                  roles: {'install'},
                  activeRole: 'install',
                ),
              ),
            ),
            200,
          ),
        ),
      ),
    )..setCredential('alice');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    session.setCredential(null);
    expect(session.current, isA<NotAuthenticated>());
  });

  test('_validate response arriving after dispose() does not throw', () async {
    final client = _GatedClient(
      (_) => http.Response(
        jsonEncode(
          PrincipalCodec.encode(
            UserPrincipal(
              userId: 'alice',
              roles: {'install'},
              activeRole: 'install',
            ),
          ),
        ),
        200,
      ),
    );
    final session = RemoteAuthSession(connection: connWithClient(client));
    final errors = <Object>[];
    await runZonedGuarded(() async {
      session.setCredential('alice');
      // Let the send() call register its gate.
      await Future<void>.delayed(const Duration(milliseconds: 10));
      await session.dispose();
      // Now complete the in-flight response — _transition runs on a
      // closed controller; with the isClosed guard, no StateError fires.
      expect(client.gates, hasLength(1));
      client.gates.single.complete();
      // Pump the microtask + event-loop tail so the awaited completer
      // resumes and _validate's continuation runs.
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }, (e, st) => errors.add(e));
    expect(errors, isEmpty);
  });

  test('rapid setCredential flips: stale /me response is discarded', () async {
    final client = _GatedClient(
      (_) => http.Response(
        jsonEncode(
          PrincipalCodec.encode(
            UserPrincipal(
              userId: 'alice',
              roles: {'install'},
              activeRole: 'install',
            ),
          ),
        ),
        200,
      ),
    );
    final session = RemoteAuthSession(connection: connWithClient(client))
      ..setCredential('alice');
    // Wait for the gated /me request to register.
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(client.gates, hasLength(1));
    // Newer setCredential supersedes the in-flight one.
    session.setCredential(null);
    expect(session.current, isA<NotAuthenticated>());
    // Release the stale response: it must NOT clobber NotAuthenticated.
    client.gates.single.complete();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(session.current, isA<NotAuthenticated>());
  });
}
