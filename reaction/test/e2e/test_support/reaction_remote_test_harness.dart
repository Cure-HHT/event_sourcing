// reaction/test/e2e/test_support/reaction_remote_test_harness.dart
import 'dart:io';

import 'package:reaction/src/remote/remote_scope.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/reaction_handlers.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_router/shelf_router.dart';

import '../../local/test_support/reaction_test_harness.dart';

/// Fully-wired in-memory substrate + reaction handlers mounted on a
/// real shelf server + RemoteScope. Mirrors `ReactionTestHarness` for
/// the cross-process side: tests interact with `harness.scope.*`
/// exactly as production widget code would.
class ReactionRemoteTestHarness {
  ReactionRemoteTestHarness._({
    required this.substrate,
    required this.reaction,
    required this.httpServer,
    required this.scope,
  });

  final ReactionTestHarness substrate;
  final ReactionHandlers reaction;
  final HttpServer httpServer;
  final RemoteScope scope;

  static Future<ReactionRemoteTestHarness> open({
    String defaultActiveRole = 'install',
  }) async {
    final substrate = await ReactionTestHarness.open();
    final validator = TrustingAuthValidator(
      defaultActiveRole: defaultActiveRole,
    );

    final reaction = ReactionHandlers(
      eventStore: substrate.eventStore,
      dispatcher: substrate.dispatcher,
      policy: substrate.dispatcher.authorization,
    );

    final router = Router()
      ..get('/me', reaction.me)
      ..post('/actions', reaction.actions)
      ..get('/permissions/snapshot', reaction.permissions)
      ..get('/subscriptions', reaction.subscriptionsWithValidator(validator));

    final pipeline = const Pipeline()
        .addMiddleware(authMiddleware(validator))
        .addHandler(router.call);

    final httpServer = await shelf_io.serve(pipeline, '127.0.0.1', 0);

    final scope = RemoteScope(
      baseUrl: Uri.parse('http://127.0.0.1:${httpServer.port}'),
    );

    return ReactionRemoteTestHarness._(
      substrate: substrate,
      reaction: reaction,
      httpServer: httpServer,
      scope: scope,
    );
  }

  Future<void> close() async {
    await scope.dispose();
    await reaction.dispose(); // stops AuthzWatcher
    await httpServer.close(force: true);
    await substrate.close();
  }
}
