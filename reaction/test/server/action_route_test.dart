import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/action_route.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';
import 'package:shelf/shelf.dart';

/// Stub `ActionDispatcher` for route-level tests. Captures the last
/// submission + context for assertions and returns a configured
/// [DispatchResult]. The substrate's `ActionDispatcher.dispatch` is not
/// generic — it always returns `Future<DispatchResult<Object?>>` — so
/// neither is this override.
class _StubDispatcher implements ActionDispatcher {
  _StubDispatcher(this.response);
  final DispatchResult<Object?> response;
  ActionSubmission? lastSubmission;
  ActionContext? lastCtx;

  @override
  Future<DispatchResult<Object?>> dispatch(
    ActionSubmission submission,
    ActionContext ctx,
  ) async {
    lastSubmission = submission;
    lastCtx = ctx;
    return response;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  test('returns 200 + DispatchResult JSON on success', () async {
    final dispatcher = _StubDispatcher(
      const DispatchSuccess<Object?>({'echo': 'hi'}, <String>[]),
    );
    final handler = actionHandler(dispatcher: dispatcher);
    const submission = ActionSubmission(
      actionName: 'sayHello',
      rawInput: {'name': 'A'},
    );
    final body = jsonEncode(ActionSubmissionCodec.encode(submission));
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: body,
      context: {
        'reaction.principal': UserPrincipal(
          userId: 'u-1',
          roles: const {'install'},
          activeRole: 'install',
        ),
      },
    );
    final res = await handler(req);
    expect(res.statusCode, 200);
    expect(res.headers['content-type'], contains('application/json'));
    final decoded = DispatchResultCodec.decode(
      jsonDecode(await res.readAsString()) as Map<String, Object?>,
    );
    expect(decoded, isA<DispatchSuccess<Object?>>());
    expect(dispatcher.lastSubmission?.actionName, 'sayHello');
    expect((dispatcher.lastCtx!.principal as UserPrincipal).userId, 'u-1');
  });

  test('returns 400 on malformed body', () async {
    final handler = actionHandler(
      dispatcher: _StubDispatcher(
        const DispatchSuccess<Object?>(null, <String>[]),
      ),
    );
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: '{not json',
      context: {
        'reaction.principal': UserPrincipal(
          userId: 'u-1',
          roles: const {'install'},
          activeRole: 'install',
        ),
      },
    );
    final res = await handler(req);
    expect(res.statusCode, 400);
  });

  test('returns 500 when no Principal in context', () async {
    final handler = actionHandler(
      dispatcher: _StubDispatcher(
        const DispatchSuccess<Object?>(null, <String>[]),
      ),
    );
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: jsonEncode(
        ActionSubmissionCodec.encode(
          const ActionSubmission(actionName: 'x', rawInput: {}),
        ),
      ),
    );
    final res = await handler(req);
    expect(res.statusCode, 500);
  });

  test('returns 503 + view_converging body + Retry-After when the dispatcher '
      'throws ViewConvergingRefusal', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    // R15: a converging-view refusal from action dispatch reaches
    // the remote caller as a typed, transient 503 naming the view,
    // not an untyped 500.
    final handler = actionHandler(
      dispatcher: _ThrowingDispatcher(
        const ViewConvergingRefusal('user_role_scopes'),
      ),
    );
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: jsonEncode(
        ActionSubmissionCodec.encode(
          const ActionSubmission(actionName: 'x', rawInput: {}),
        ),
      ),
      context: {
        'reaction.principal': UserPrincipal(
          userId: 'u-1',
          roles: const {'install'},
          activeRole: 'install',
        ),
      },
    );
    final res = await handler(req);
    expect(res.statusCode, 503);
    expect(res.headers['retry-after'], isNotNull);
    final body = jsonDecode(await res.readAsString()) as Map<String, Object?>;
    expect(body['error'], 'view_converging');
    expect(body['view'], 'user_role_scopes');
  });
}

/// Stub `ActionDispatcher` whose `dispatch` throws a fixed exception,
/// mirroring an `AuthorizationPolicy` read of a converging view.
class _ThrowingDispatcher implements ActionDispatcher {
  _ThrowingDispatcher(this.error);
  final Exception error;

  @override
  Future<DispatchResult<Object?>> dispatch(
    ActionSubmission submission,
    ActionContext ctx,
  ) async => throw error;

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
