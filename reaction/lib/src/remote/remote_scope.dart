import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/remote/remote_action_submitter.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';
import 'package:reaction/src/remote/remote_view_source.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class RemoteScope {
  RemoteScope({
    required Uri baseUrl,
    http.Client? httpClient,
    WebSocketChannel Function(Uri)? wsFactory,
  }) : _connection = RemoteConnection(
         baseUrl: baseUrl,
         httpClient: httpClient ?? http.Client(),
         wsFactory: wsFactory ?? IOWebSocketChannel.connect,
       ) {
    _auth = RemoteAuthSession(connection: _connection);
    _submitter = RemoteActionSubmitter(
      connection: _connection,
      authSession: _auth,
    );
    _views = RemoteViewSource(connection: _connection);
    _perms = RemotePermissionSource(
      connection: _connection,
      authSession: _auth,
    );
  }

  final RemoteConnection _connection;
  late final RemoteAuthSession _auth;
  late final RemoteActionSubmitter _submitter;
  late final RemoteViewSource _views;
  late final RemotePermissionSource _perms;

  AuthSession get authSession => _auth;
  ActionSubmitter get actionSubmitter => _submitter;
  ViewSource get viewSource => _views;
  PermissionSource get permissionSource => _perms;

  Future<void> dispose() async {
    await _perms.dispose();
    await _auth.dispose();
    await _connection.dispose();
  }
}
