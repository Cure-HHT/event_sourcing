// The demo server's entry point runs the deployment step with --provision
// (creates the schema and provisions it as the owner, declaring the runtime
// role) and, serving as that runtime role, refuses, naming --provision, a
// database that was never provisioned. Gated on PG_TEST_URL. The entry point runs in a
// subprocess, so this is an application-side test and cites no library
// requirement.
@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'support/demo_postgres.dart';

Future<List<String>> _tables(DemoPostgres db) async {
  final c = await db.connectAdmin(inSchema: false);
  try {
    final r = await c.execute(
      Sql.named(
        'SELECT table_name FROM information_schema.tables '
        'WHERE table_schema = @s',
      ),
      parameters: <String, Object?>{'s': db.schema},
    );
    return r.map((row) => row[0]! as String).toList();
  } finally {
    await c.close();
  }
}

/// The advisory-lock key the library derives for a generation [component]
/// of the database and schema [c] reaches: the first eight bytes of a
/// SHA-256 over the prefix, the database, the schema and the component.
Future<int> _componentKey(Connection c, String component) async {
  final scope = await c.execute('SELECT current_database(), current_schema()');
  final digest = sha256.convert(
    utf8.encode(
      jsonEncode(<Object?>[
        'event_sourcing.generation',
        scope.first[0],
        scope.first[1],
        component,
      ]),
    ),
  );
  var key = 0;
  for (var i = 0; i < 8; i++) {
    key = (key << 8) | digest.bytes[i];
  }
  return key;
}

/// Whether the database holds the generation record a boot writes.
Future<bool> _hasGenerationRecord(Connection c) async {
  final r = await c.execute(
    "SELECT count(*) FROM backend_state WHERE key = 'data_generation'",
  );
  return r.first[0] != 0;
}

String _dart() {
  final root = Platform.environment['FLUTTER_ROOT'];
  return root == null || root.isEmpty ? 'dart' : '$root/bin/dart';
}

/// Serves the database as the declared runtime role.
Future<ProcessResult> _server(DemoPostgres db) => Process.run(_dart(), <String>[
  'run',
  'bin/server.dart',
  '--backend=postgres',
  '--postgres-url=${db.runtimeUrl}',
  '--postgres-ssl-mode=disable',
  '--port=0',
]);

/// Runs the deployment step as the owner.
Future<ProcessResult> _provision(DemoPostgres db) => Process.run(
  _dart(),
  <String>['run', 'bin/server.dart', ...db.provisionArgs],
);

void main() {
  final db = DemoPostgres.fromEnvironment();

  setUp(() async {
    if (db == null) return;
    await db.reset(provision: false);
  });

  test(
    '--provision provisions an empty schema and exits',
    () async {
      if (db == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      final result = await _provision(db);
      expect(result.exitCode, 0, reason: '${result.stdout}\n${result.stderr}');
      expect(
        await _tables(db),
        containsAll(<String>['events', 'backend_state']),
      );
    },
    timeout: const Timeout(Duration(minutes: 5)),
  );

  test('serving an unprovisioned database exits non-zero, naming '
      '--provision', () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    final result = await _server(db);
    expect(result.exitCode, isNot(0));
    expect('${result.stderr}', contains('--provision'));
    expect(await _tables(db), isEmpty);
  }, timeout: const Timeout(Duration(minutes: 5)));

  test('a server whose build conflicts with a running instance exits '
      "non-zero with the guard's message", () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    final provisioned = await _provision(db);
    expect(provisioned.exitCode, 0, reason: '${provisioned.stderr}');
    // A live instance of another data-format major: it holds that
    // component's shared lock, and the component's catalog row names the
    // key. The guard reports a component as live only while its lock is
    // held, and the database's generation record is never written here, so
    // the refusal is the live-instance check's: a refusal by the record
    // would be a DataFormatIncompatibleError, not the guard's exception.
    final other = 'data_format:${LibVersion.dataFormat.major + 1}';
    final live = await db.connectAdmin();
    addTearDown(live.close);
    final key = await _componentKey(live, other);
    await live.execute(
      Sql.named('SELECT pg_advisory_lock_shared(@k)'),
      parameters: <String, Object?>{'k': key},
    );
    await live.execute(
      Sql.named('INSERT INTO backend_state (key, value) VALUES (@k, @v:jsonb)'),
      parameters: <String, Object?>{
        'k':
            'generation_component_'
            '${key.toUnsigned(64).toRadixString(16).padLeft(16, '0')}',
        'v': <String, Object?>{
          'kind': 'data_format',
          'id': '',
          'value': LibVersion.dataFormat.major + 1,
        },
      },
    );
    final result = await _server(db);
    expect(result.exitCode, 1, reason: '${result.stdout}\n${result.stderr}');
    expect(
      '${result.stderr}',
      allOf(contains('IncompatibleGenerationException'), contains(other)),
    );
    expect(await _hasGenerationRecord(live), isFalse);
  }, timeout: const Timeout(Duration(minutes: 5)));
}
