import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_action_submitter.dart';
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

void main() {
  test('throws TransportException when not authenticated', () async {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: _Client((_) => http.Response('', 200)),
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = RemoteAuthSession(connection: conn);
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
    );
    await expectLater(
      () => submitter.submit(
        const ActionSubmission(actionName: 'x', rawInput: {}),
      ),
      throwsA(isA<TransportException>()),
    );
  });

  test(
    '401 on /actions throws TransportException and flips to Expired',
    () async {
      final principalBody = jsonEncode(
        PrincipalCodec.encode(
          UserPrincipal(
            userId: 'alice',
            roles: {'install'},
            activeRole: 'install',
          ),
        ),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://x:1'),
        httpClient: _Client(
          (req) => req.url.path == '/me'
              ? http.Response(principalBody, 200)
              : http.Response('', 401),
        ),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final auth = RemoteAuthSession(connection: conn)..setCredential('alice');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(auth.current, isA<Authenticated>());
      final submitter = RemoteActionSubmitter(
        connection: conn,
        authSession: auth,
      );
      await expectLater(
        () => submitter.submit(
          const ActionSubmission(actionName: 'x', rawInput: {}),
        ),
        throwsA(isA<TransportException>()),
      );
      expect(auth.current, isA<Expired>());
    },
  );

  // The 'submit and decode DispatchResult' happy path is exercised in
  // the E2E suite (Phase 4) where a full substrate response is available.
}
