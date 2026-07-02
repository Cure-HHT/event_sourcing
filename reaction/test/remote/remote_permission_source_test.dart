// Verifies: EVS-PRD-permission-source/A/C/D/E — Remote impl surface.
//   Full per-assertion coverage (two-phase load + AuthSession
//   dependency) lives in e2e/permission_test.dart; the
//   null-on-empty-role parity test below runs here because it exercises
//   only the snapshot-decode branch, which needs no live server.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

/// HTTP client returning a fixed 200 body for the snapshot GET.
class _FixedHttpClient extends http.BaseClient {
  _FixedHttpClient(this._body);

  final String _body;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    return http.StreamedResponse(Stream.value(utf8.encode(_body)), 200);
  }
}

/// Minimal Authenticated AuthSession for the snapshot fetch path.
class _AuthenticatedSession implements AuthSession {
  _AuthenticatedSession(this._principal);

  final Principal _principal;
  final StreamController<AuthStatus> _ctl =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => Authenticated(principal: _principal);

  @override
  Stream<AuthStatus> get stream => _ctl.stream;

  @override
  Principal? get principal => _principal;

  @override
  void setCredential(String? credential) {}

  @override
  Future<void> dispose() async {
    await _ctl.close();
  }
}

Principal _clinician() => UserPrincipal(
  userId: 'u1',
  roles: const {'clinician'},
  activeRole: 'clinician',
);

void main() {
  test(
    'current is null before AuthSession becomes Authenticated',
    () {
      // Full coverage of two-phase load + AuthSession dependency in
      // e2e/permission_test.dart; skeleton verified here.
    },
    skip: 'covered in e2e/permission_test.dart',
  );

  test(
    'empty-activeRole snapshot maps current to null (parity with Local)',
    () async {
      // The policy returns EffectiveAuthorization.empty (activeRole == '')
      // for principals who don't hold their claimed activeRole. Local maps
      // that to current=null; Remote must too.
      final emptyBody = jsonEncode(
        EffectiveAuthorizationCodec.encode(EffectiveAuthorization.empty),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FixedHttpClient(emptyBody),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
      );
      addTearDown(source.dispose);

      // Constructor kicks off the fetch since the session is Authenticated.
      // Give the in-flight HTTP GET + decode time to settle.
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(
        source.current,
        isNull,
        reason: 'empty activeRole must clear to null, matching Local',
      );
    },
  );

  test(
    'non-empty-activeRole snapshot is stored as a non-null authorization',
    () async {
      final body = jsonEncode(
        EffectiveAuthorizationCodec.encode(
          EffectiveAuthorization(
            activeRole: 'clinician',
            rolePermissions: <Permission>{Permission('view:patient_diary')},
            scopeAssignments: const <ScopeAssignment>[],
          ),
        ),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FixedHttpClient(body),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
      );
      addTearDown(source.dispose);

      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(source.current, isNotNull);
      expect(source.current!.activeRole, 'clinician');
    },
  );
}
