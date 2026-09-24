// The demo server's entry point, run as a process over an in-memory
// database: an invalid DEMO_CONFIGURATION_VERSION stops it at start with
// status 1, after it has listened; a valid one starts the delivery cycle,
// whose state it logs, and SIGTERM closes the cycle and exits 0. The entry
// point runs in a subprocess, so this is an application-side test and cites
// no library requirement.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

String _dart() {
  final exe = Platform.executable;
  final lower = exe.toLowerCase();
  if (lower.endsWith('dart') || lower.endsWith('dart.exe')) return exe;
  final root = Platform.environment['FLUTTER_ROOT'];
  return root == null || root.isEmpty ? 'dart' : '$root/bin/dart';
}

Future<Process> _start(Map<String, String> environment) => Process.start(
  _dart(),
  <String>['run', 'bin/server.dart', '--ephemeral', '--port=0'],
  environment: environment,
);

/// Collects [process]'s output lines, stdout and stderr together.
List<String> _collect(Process process) {
  final lines = <String>[];
  process.stdout
      .transform(utf8.decoder)
      .transform(const LineSplitter())
      .listen(lines.add);
  process.stderr
      .transform(utf8.decoder)
      .transform(const LineSplitter())
      .listen((l) => lines.add('stderr: $l'));
  return lines;
}

Future<void> _untilLine(List<String> lines, String text) async {
  final deadline = DateTime.now().add(const Duration(seconds: 90));
  while (DateTime.now().isBefore(deadline)) {
    if (lines.any((l) => l.contains(text))) return;
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }
  fail('no line contained "$text"\n${lines.join('\n')}');
}

void main() {
  test(
    'an invalid DEMO_CONFIGURATION_VERSION exits 1',
    () async {
      final process = await _start(<String, String>{
        'DEMO_CONFIGURATION_VERSION': 'bad value!',
      });
      final lines = _collect(process);
      final code = await process.exitCode.timeout(const Duration(minutes: 2));
      expect(code, 1, reason: lines.join('\n'));
      expect(lines.first, contains('demo server listening'));
      expect(lines, contains(contains('the delivery cycle cannot start')));
      expect(lines, isNot(contains('demo server ready')));
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );

  test('the server logs its delivery cycle running, and SIGTERM closes it '
      'and exits 0', () async {
    final process = await _start(<String, String>{
      'DEMO_CONFIGURATION_VERSION': 'rev-2026.09+build:7',
    });
    final lines = _collect(process);
    addTearDown(() => process.kill(ProcessSignal.sigkill));
    await _untilLine(lines, 'delivery cycle: running');
    expect(lines.indexWhere((l) => l.contains('demo server listening')), 0);
    process.kill(ProcessSignal.sigterm);
    final code = await process.exitCode.timeout(const Duration(seconds: 60));
    expect(code, 0, reason: lines.join('\n'));
    expect(lines, contains(startsWith('demo server stopping')));
  }, timeout: const Timeout(Duration(minutes: 3)));
}
