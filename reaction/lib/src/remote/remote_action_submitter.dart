// Implements: EVS-PRD-action-submitter/C — RemoteActionSubmitter submits
//   via HTTP POST /actions and decodes the DispatchResult from the
//   response body via DispatchResultCodec.
// Implements: EVS-PRD-action-submitter/D — every outbound submission
//   includes the bearer credential from the co-mounted AuthSession
//   (RemoteConnection.httpPost injects it on every call).
// Implements: EVS-PRD-cross-process-event-transport/F — wire-level
//   bearer credential carriage on action submission.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';

class RemoteActionSubmitter implements ActionSubmitter {
  RemoteActionSubmitter({required this.connection, required this.authSession});

  final RemoteConnection connection;
  final AuthSession authSession;

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    if (authSession.current is! Authenticated) {
      throw const TransportException('not authenticated');
    }
    final url = connection.baseUrl.replace(path: '/actions');
    final res = await connection.httpPost(
      url,
      body: jsonEncode(ActionSubmissionCodec.encode(submission)),
    );
    if (res.statusCode == 401) {
      if (authSession is RemoteAuthSession) {
        (authSession as RemoteAuthSession).onWireUnauthorized();
      }
      throw const TransportException('unauthorized');
    }
    if (res.statusCode != 200) {
      throw TransportException('http ${res.statusCode}');
    }
    return DispatchResultCodec.decode(
      jsonDecode(res.body) as Map<String, Object?>,
    );
  }
}
