// example_clinical_scopes/bin/server.dart
//
// Console entry point: stands up the clinical-scopes demo server on a
// shelf HTTP/WS pipeline. Calls `bootstrap()` for the composed Router and
// a dispose callback; listens on the configured port; tears down cleanly
// on SIGINT.

import 'dart:async';
import 'dart:io';

import 'package:args/args.dart';
import 'package:example_clinical_scopes/server/clinical_bootstrap.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

/// Permissive CORS so a Flutter **web** client served from a different
/// origin can call this demo server's HTTP endpoints. Answers preflight
/// `OPTIONS` directly and stamps the allow-headers on every response.
/// WebSocket upgrades are not subject to CORS.
///
/// Demo-only: production deployments scope `Allow-Origin` to known origins.
const Map<String, String> _corsHeaders = <String, String>{
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Origin, Content-Type, Authorization',
};

Handler _withCors(Handler inner) => (Request request) async {
  if (request.method == 'OPTIONS') {
    return Response.ok(null, headers: _corsHeaders);
  }
  final response = await inner(request);
  return response.change(headers: _corsHeaders);
};

Future<void> main(List<String> args) async {
  final parser = ArgParser()
    ..addOption('host', defaultsTo: '127.0.0.1', help: 'Interface to bind on.')
    ..addOption('port', defaultsTo: '8080', help: 'TCP port to bind on.');

  final ArgResults parsed;
  try {
    parsed = parser.parse(args);
  } on FormatException catch (e) {
    stderr.writeln('error: ${e.message}\n\n${parser.usage}');
    exitCode = 64;
    return;
  }

  final host = parsed['host'] as String;
  final port = int.parse(parsed['port'] as String);

  final result = await bootstrap();
  final server = await shelf_io.serve(
    _withCors(result.router.call),
    host,
    port,
  );

  stdout.writeln(
    'clinical-scopes example server listening on '
    'http://${server.address.host}:${server.port}',
  );
  stdout.writeln('  ephemeral in-memory sembast; state resets on restart');
  stdout.writeln(
    '  users: dr-investigator (sites A+B), dr-overseer (region-West), '
    'dr-admin (all), dr-unassigned (none)',
  );

  final shutdown = Completer<void>();
  ProcessSignal.sigint.watch().listen((_) {
    if (!shutdown.isCompleted) shutdown.complete();
  });
  await shutdown.future;

  stdout.writeln('shutting down...');
  await server.close();
  await result.dispose();
}
