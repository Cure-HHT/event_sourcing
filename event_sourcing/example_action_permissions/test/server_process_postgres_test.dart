// The demo server's entry point, run as a process against Postgres: it
// listens before its event store opens, so `/livez` answers 200 and
// `/health` answers 503 with the boot's phase while the boot waits on a
// lock another session holds; it reports ready once the boot is done and
// logs its delivery cycle's state; a second server stands by, and takes
// over when the first stops on SIGTERM, which exits 0. Gated on
// PG_TEST_URL. The entry point runs in a subprocess, so this is an
// application-side test and cites no library requirement.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

Future<void> _resetSchema(String url) async {
  final c = await _connect(url);
  await c.execute('DROP SCHEMA public CASCADE');
  await c.execute('CREATE SCHEMA public');
  await c.close();
}

String _dart() {
  final root = Platform.environment['FLUTTER_ROOT'];
  return root == null || root.isEmpty ? 'dart' : '$root/bin/dart';
}

List<String> _args(String url, int port, [List<String> extra = const []]) =>
    <String>[
      'run',
      'bin/server.dart',
      '--backend=postgres',
      '--postgres-url=$url',
      '--postgres-ssl-mode=disable',
      '--port=$port',
      ...extra,
    ];

Future<int> _freePort() async {
  final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = socket.port;
  await socket.close();
  return port;
}

/// A server process and everything it has written to standard output.
class _Server {
  _Server(this.process, this.port) {
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(_lines.add);
    process.stderr
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen((line) => _lines.add('stderr: $line'));
  }

  static Future<_Server> start(String url) async {
    final port = await _freePort();
    return _Server(await Process.start(_dart(), _args(url, port)), port);
  }

  final Process process;
  final int port;
  final List<String> _lines = <String>[];

  List<String> get lines => List<String>.unmodifiable(_lines);

  Future<(int, Map<String, Object?>?)> get(String path) async {
    final client = HttpClient();
    try {
      final request = await client
          .get('localhost', port, path)
          .timeout(const Duration(seconds: 2));
      final response = await request.close();
      final body = await utf8.decodeStream(response);
      Map<String, Object?>? json;
      try {
        json = jsonDecode(body) as Map<String, Object?>;
      } on FormatException {
        json = null;
      }
      return (response.statusCode, json);
    } finally {
      client.close(force: true);
    }
  }

  /// Polls [path] until it answers [status].
  Future<void> untilStatus(String path, int status) async {
    final deadline = DateTime.now().add(const Duration(seconds: 90));
    Object? last;
    while (DateTime.now().isBefore(deadline)) {
      try {
        final (code, _) = await get(path);
        if (code == status) return;
        last = code;
      } on Object catch (e) {
        last = e;
      }
      await Future<void>.delayed(const Duration(milliseconds: 200));
    }
    fail('$path never answered $status (last: $last)\n${lines.join('\n')}');
  }

  /// Waits until a line of standard output contains [text].
  Future<void> untilLine(String text) async {
    final deadline = DateTime.now().add(const Duration(seconds: 90));
    while (DateTime.now().isBefore(deadline)) {
      if (_lines.any((l) => l.contains(text))) return;
      await Future<void>.delayed(const Duration(milliseconds: 200));
    }
    fail('no line contained "$text"\n${lines.join('\n')}');
  }

  Future<void> kill() async {
    process.kill(ProcessSignal.sigkill);
    await process.exitCode;
  }
}

void main() {
  final url = Platform.environment['PG_TEST_URL'];

  setUp(() async {
    if (url == null || url.isEmpty) return;
    await _resetSchema(url);
  });

  test(
    'the server listens and reports its boot while the boot waits, then '
    'serves; a second server stands by and takes over when the first stops',
    () async {
      if (url == null || url.isEmpty) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      final provisioned = await Process.run(
        _dart(),
        _args(url, 0, const <String>['--provision']),
      );
      expect(provisioned.exitCode, 0, reason: '${provisioned.stderr}');

      // Another session holds a lock that the boot's first statement waits
      // for, so the boot cannot finish until it is released.
      final holder = await _connect(url);
      addTearDown(holder.close);
      final release = Completer<void>();
      final held = Completer<void>();
      final holding = holder.runTx((tx) async {
        await tx.execute('LOCK TABLE backend_state IN EXCLUSIVE MODE');
        held.complete();
        await release.future;
      });
      // Runs before the holder closes: a failure before the release would
      // otherwise leave the transaction open and the close waiting on it.
      addTearDown(() async {
        if (!release.isCompleted) release.complete();
        await holding;
      });
      await held.future;

      final first = await _Server.start(url);
      addTearDown(first.kill);

      // Listening while the boot waits.
      await first.untilStatus('/livez', 200);
      final (code1, body1) = await first.get('/health');
      expect(code1, 503);
      expect(body1, containsPair('status', 'booting'));
      expect(body1, containsPair('phase', 'checks'));
      expect((await first.get('/healthz')).$1, 503);
      await Future<void>.delayed(const Duration(milliseconds: 1200));
      final (code2, body2) = await first.get('/health');
      expect(code2, 503);
      expect(
        body2!['elapsed_s']! as num,
        greaterThan(body1!['elapsed_s']! as num),
      );
      expect(first.lines, isNot(contains('demo server ready')));

      release.complete();
      await holding;
      await first.untilStatus('/health', 200);
      expect((await first.get('/health')).$2, <String, Object?>{
        'status': 'ready',
      });
      await first.untilStatus('/healthz', 200);
      await first.untilLine('delivery cycle: running');

      // A second server on the same database stands by.
      final second = await _Server.start(url);
      addTearDown(second.kill);
      await second.untilStatus('/health', 200);
      await second.untilLine('delivery cycle: standby');

      // SIGTERM stops the first: it closes its cycle, which releases the
      // drain lock, and exits 0; the second takes over without a restart.
      first.process.kill(ProcessSignal.sigterm);
      expect(
        await first.process.exitCode.timeout(const Duration(seconds: 60)),
        0,
        reason: first.lines.join('\n'),
      );
      expect(first.lines, contains(startsWith('demo server stopping')));
      await second.untilLine('delivery cycle: running');
    },
    timeout: const Timeout(Duration(minutes: 6)),
  );
}
