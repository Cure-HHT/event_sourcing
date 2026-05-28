// reaction/example/bin/server.dart
//
// Console entry point: stands up the reaction example server on a shelf
// HTTP/WS pipeline. Calls `bootstrap()` to obtain the composed Router
// and a dispose callback; listens on the configured port; tears down
// cleanly on SIGINT.

import 'dart:async';
import 'dart:io';

import 'package:args/args.dart';
import 'package:reaction_example/server/bootstrap.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

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
  final server = await shelf_io.serve(result.router.call, host, port);

  stdout.writeln(
    'reaction example server listening on http://${server.address.host}:${server.port}',
  );
  stdout.writeln('  ephemeral in-memory sembast; state resets on restart');

  final shutdown = Completer<void>();
  ProcessSignal.sigint.watch().listen((_) {
    if (!shutdown.isCompleted) shutdown.complete();
  });
  await shutdown.future;

  stdout.writeln('shutting down...');
  await server.close();
  await result.dispose();
}
