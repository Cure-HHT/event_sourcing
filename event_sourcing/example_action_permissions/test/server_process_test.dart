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

/// A process's output, stdout and stderr kept apart: the two pipes are read
/// by independent listeners, so no order between their lines is known.
class _Output {
  _Output(Process process) {
    done = Future.wait<void>(<Future<void>>[
      process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .forEach(stdoutLines.add),
      process.stderr
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .forEach(stderrLines.add),
    ]);
  }

  final stdoutLines = <String>[];
  final stderrLines = <String>[];

  /// Completes when both pipes have delivered their last line.
  late final Future<void> done;

  /// Waits for the process to exit and both pipes to drain, and returns
  /// the exit status.
  Future<int> exit(Process process, Duration timeout) async {
    final code = await process.exitCode.timeout(timeout);
    await done.timeout(const Duration(seconds: 30));
    return code;
  }

  @override
  String toString() =>
      'stdout:\n${stdoutLines.join('\n')}\n'
      'stderr:\n${stderrLines.join('\n')}';
}

Future<void> _untilLine(List<String> lines, String text, _Output all) async {
  final deadline = DateTime.now().add(const Duration(seconds: 90));
  while (DateTime.now().isBefore(deadline)) {
    if (lines.any((l) => l.contains(text))) return;
    await Future<void>.delayed(const Duration(milliseconds: 200));
  }
  fail('no line contained "$text"\n$all');
}

void main() {
  test(
    'an invalid DEMO_CONFIGURATION_VERSION exits 1',
    () async {
      final process = await _start(<String, String>{
        'DEMO_CONFIGURATION_VERSION': 'bad value!',
      });
      final output = _Output(process);
      final code = await output.exit(process, const Duration(minutes: 2));
      expect(code, 1, reason: '$output');
      expect(output.stdoutLines, isNotEmpty, reason: '$output');
      expect(output.stdoutLines.first, contains('demo server listening'));
      expect(
        output.stderrLines,
        contains(contains('the delivery cycle cannot start')),
      );
      expect(
        output.stdoutLines,
        isNot(contains(contains('demo server ready'))),
      );
    },
    timeout: const Timeout(Duration(minutes: 3)),
  );

  test('the server logs its delivery cycle running, and SIGTERM closes it '
      'and exits 0', () async {
    final process = await _start(<String, String>{
      'DEMO_CONFIGURATION_VERSION': 'rev-2026.09+build:7',
    });
    final output = _Output(process);
    addTearDown(() => process.kill(ProcessSignal.sigkill));
    await _untilLine(output.stdoutLines, 'delivery cycle: running', output);
    expect(output.stdoutLines.first, contains('demo server listening'));
    process.kill(ProcessSignal.sigterm);
    final code = await output.exit(process, const Duration(seconds: 60));
    expect(code, 0, reason: '$output');
    expect(output.stdoutLines, contains(startsWith('demo server stopping')));
  }, timeout: const Timeout(Duration(minutes: 3)));
}
