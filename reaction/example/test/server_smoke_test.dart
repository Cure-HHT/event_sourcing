// reaction/example/test/server_smoke_test.dart
//
// Boots `bootstrap()` onto a real shelf HTTP server bound to an
// ephemeral port and exercises the four seeded users + the new
// workspace-scoped action surface. Keeps the demo from bit-rotting as
// the library evolves.

import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction_example/server/bootstrap.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

Future<Uri> _serve(Future<void> Function() registerDispose) async {
  final result = await bootstrap();
  final httpServer = await shelf_io.serve(result.router.call, '127.0.0.1', 0);
  await registerDispose();
  addTearDown(() async {
    await httpServer.close(force: true);
    await result.dispose();
  });
  return Uri.parse('http://${httpServer.address.host}:${httpServer.port}');
}

Future<Map<String, Object?>> _get(Uri url, {String? bearer}) async {
  final headers = <String, String>{
    if (bearer != null) 'Authorization': 'Bearer $bearer',
  };
  final res = await http.get(url, headers: headers);
  expect(res.statusCode, 200, reason: 'GET $url -> ${res.body}');
  return jsonDecode(res.body) as Map<String, Object?>;
}

Future<Map<String, Object?>> _postAction(
  Uri base, {
  required String bearer,
  required String actionName,
  required Map<String, Object?> rawInput,
}) async {
  final res = await http.post(
    base.replace(path: '/actions'),
    headers: <String, String>{
      'Authorization': 'Bearer $bearer',
      'Content-Type': 'application/json',
    },
    body: jsonEncode(<String, Object?>{
      'actionName': actionName,
      'rawInput': rawInput,
    }),
  );
  expect(
    res.statusCode,
    200,
    reason: 'POST /actions $actionName -> ${res.statusCode} ${res.body}',
  );
  return jsonDecode(res.body) as Map<String, Object?>;
}

void main() {
  test('bootstrap() gates /me with bearer auth', () async {
    final base = await _serve(() async {});
    final unauth = await http.get(base.replace(path: '/me'));
    expect(unauth.statusCode, 401);
  });

  test(
    '/me surfaces each seeded user with their highest-privileged role',
    () async {
      final base = await _serve(() async {});
      final cases = <String, String>{
        'alice': 'editor',
        'bob': 'editor',
        'carol': 'admin',
        'dave': 'viewer',
      };
      for (final entry in cases.entries) {
        final body = await _get(base.replace(path: '/me'), bearer: entry.key);
        expect(
          body['userId'],
          entry.key,
          reason: '/me userId for ${entry.key}',
        );
        expect(
          body['activeRole'],
          entry.value,
          reason: '/me activeRole for ${entry.key}',
        );
      }
    },
  );

  test(
    'unknown bearer authenticates as anonymous (not denied at HTTP)',
    () async {
      final base = await _serve(() async {});
      final res = await http.get(
        base.replace(path: '/me'),
        headers: <String, String>{'Authorization': 'Bearer zoe'},
      );
      // Unknown user authenticates anonymously — the substrate's policy
      // will then deny every action against them, demonstrating that
      // identity vs role-binding is split between auth (trust-on-supply)
      // and the closed-under-events trust model.
      expect(res.statusCode, 200);
      final body = jsonDecode(res.body) as Map<String, Object?>;
      expect(body['kind'], 'anonymous');
    },
  );

  group('submit_note (workspace-scoped)', () {
    test('alice succeeds in west', () async {
      final base = await _serve(() async {});
      final result = await _postAction(
        base,
        bearer: 'alice',
        actionName: 'submit_note',
        rawInput: <String, Object?>{'workspace': 'west', 'title': 'hi'},
      );
      expect(result['type'], 'success');
    });

    test('alice is denied in east (no BoundScope(workspace, east))', () async {
      final base = await _serve(() async {});
      final result = await _postAction(
        base,
        bearer: 'alice',
        actionName: 'submit_note',
        rawInput: <String, Object?>{'workspace': 'east', 'title': 'no'},
      );
      expect(result['type'], 'authorization_denied');
      expect((result['permission']! as Map)['name'], 'submit_note');
    });

    test(
      'carol (admin, TotalWildcardScope) succeeds in any workspace',
      () async {
        final base = await _serve(() async {});
        for (final ws in const <String>['west', 'east', 'mars']) {
          final result = await _postAction(
            base,
            bearer: 'carol',
            actionName: 'submit_note',
            rawInput: <String, Object?>{'workspace': ws, 'title': 'k'},
          );
          expect(result['type'], 'success', reason: 'workspace=$ws');
        }
      },
    );

    test('dave (viewer) is denied — no submit_note grant', () async {
      final base = await _serve(() async {});
      final result = await _postAction(
        base,
        bearer: 'dave',
        actionName: 'submit_note',
        rawInput: <String, Object?>{'workspace': 'west', 'title': 'no'},
      );
      expect(result['type'], 'authorization_denied');
    });
  });

  group('role admin actions', () {
    test('alice is denied assign_role (no permission)', () async {
      final base = await _serve(() async {});
      final result = await _postAction(
        base,
        bearer: 'alice',
        actionName: 'assign_role',
        rawInput: <String, Object?>{
          'user_id': 'bob',
          'role': 'admin',
          'scope': <String, Object?>{'wildcard_class': true},
        },
      );
      expect(result['type'], 'authorization_denied');
      expect((result['permission']! as Map)['name'], 'assign_role');
    });

    test(
      'carol can assign_role + the assignee then sees the expanded role',
      () async {
        final base = await _serve(() async {});

        // carol grants alice editor in 'east' (in addition to her seeded
        // 'west' assignment).
        final assignRes = await _postAction(
          base,
          bearer: 'carol',
          actionName: 'assign_role',
          rawInput: <String, Object?>{
            'user_id': 'alice',
            'role': 'editor',
            'scope': <String, Object?>{'class': 'workspace', 'value': 'east'},
          },
        );
        expect(assignRes['type'], 'success');

        // alice can now submit in east.
        final eastRes = await _postAction(
          base,
          bearer: 'alice',
          actionName: 'submit_note',
          rawInput: <String, Object?>{'workspace': 'east', 'title': 'k'},
        );
        expect(eastRes['type'], 'success');
      },
    );

    test('carol can unassign_role + alice loses access', () async {
      final base = await _serve(() async {});
      final unassignRes = await _postAction(
        base,
        bearer: 'carol',
        actionName: 'unassign_role',
        rawInput: <String, Object?>{
          'user_id': 'alice',
          'role': 'editor',
          'scope': <String, Object?>{'class': 'workspace', 'value': 'west'},
        },
      );
      expect(unassignRes['type'], 'success');

      final westRes = await _postAction(
        base,
        bearer: 'alice',
        actionName: 'submit_note',
        rawInput: <String, Object?>{'workspace': 'west', 'title': 'no'},
      );
      expect(westRes['type'], 'authorization_denied');
    });
  });

  test('/permissions/snapshot surfaces effective auth per user', () async {
    final base = await _serve(() async {});
    final aliceSnap = await _get(
      base.replace(path: '/permissions/snapshot'),
      bearer: 'alice',
    );
    expect(aliceSnap['activeRole'], 'editor');
    final perms = (aliceSnap['rolePermissions']! as List).cast<Map>();
    final names = perms.map((p) => p['name']).toSet();
    expect(names, containsAll(<String>['submit_note', 'view:notes_today']));

    final carolSnap = await _get(
      base.replace(path: '/permissions/snapshot'),
      bearer: 'carol',
    );
    expect(carolSnap['activeRole'], 'admin');
    final carolPerms = (carolSnap['rolePermissions']! as List).cast<Map>();
    final carolNames = carolPerms.map((p) => p['name']).toSet();
    expect(
      carolNames,
      containsAll(<String>['assign_role', 'unassign_role', 'submit_note']),
    );
  });
}
