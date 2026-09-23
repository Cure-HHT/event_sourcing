// Verifies: EVS-DEV-destination-drain-lock/F
//
// The test seams have no effect without assertions: the probe in
// `tool/hooks_release_probe.dart` installs every seam (the observers, the
// awaited interleaving seams and the failure injections), then runs a
// registry operation and two delivery passes. Run without assertions
// (`dart run --no-enable-asserts`, and as a `dart compile exe` executable)
// no seam fires, the passes deliver and the send's outcome commits; run
// in-process under `flutter test` (assertions on) every seam fires.
@Timeout(Duration(minutes: 5))
library;

import 'dart:convert';
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

import '../../tool/hooks_release_probe.dart';
import '../test_support/tool_subprocess.dart';

Map<String, Object?> _lastJsonLine(String stdout) {
  final lines = LineSplitter.split(stdout).where((l) => l.startsWith('{'));
  if (lines.isEmpty) throw StateError('probe printed no outcome:\n$stdout');
  return jsonDecode(lines.last) as Map<String, Object?>;
}

void _expectInert(ProcessResult result) {
  final outcome = _lastJsonLine('${result.stdout}');
  expect(outcome['fired_seams'], isEmpty, reason: '${result.stdout}');
  expect(outcome['delivered'], 1, reason: '${result.stdout}');
  // The injection was ignored, so the registry operation committed.
  expect(outcome['stored_end_date'], isNotNull, reason: '${result.stdout}');
  expect(outcome['end_date_set_events'], 1, reason: '${result.stdout}');
  // The fill and outcome failure injections were ignored.
  expect(outcome['sent_items'], 1, reason: '${result.stdout}');
  // The wedge failure injection was ignored: the refusal wedged.
  expect(outcome['wedge_events'], 1, reason: '${result.stdout}');
  expect(result.exitCode, 0, reason: '${result.stdout}\n${result.stderr}');
}

void main() {
  final root = Directory.current.path;

  Future<ProcessResult> runOrSkip(
    String tool,
    List<String> args, {
    String? workingDirectory,
  }) async {
    final result = await runSdkTool(
      tool,
      args,
      workingDirectory: workingDirectory ?? root,
    );
    final err = '${result.stderr}';
    if (result.exitCode != 0 &&
        !'${result.stdout}'.contains('fired_seams') &&
        (err.contains('pub get') || err.contains('Could not resolve'))) {
      final reason =
          '$tool ${args.join(' ')} could not resolve packages:\n$err';
      if (runningInCi) fail(reason);
      throw ToolUnavailable(reason);
    }
    return result;
  }

  test(
    'without assertions (dart run --no-enable-asserts) no seam fires',
    () async {
      try {
        final result = await runOrSkip('dart', <String>[
          'run',
          '--no-enable-asserts',
          'tool/hooks_release_probe.dart',
        ]);
        _expectInert(result);
      } on ToolUnavailable catch (e) {
        markTestSkipped('$e');
      }
    },
  );

  test('a compiled executable ignores installed seams', () async {
    final temp = await Directory.systemTemp.createTemp('hooks_probe_');
    try {
      final exe = p.join(temp.path, 'hooks_release_probe');
      final compile = await runOrSkip('dart', <String>[
        'compile',
        'exe',
        'tool/hooks_release_probe.dart',
        '-o',
        exe,
      ]);
      expect(
        compile.exitCode,
        0,
        reason: '${compile.stdout}\n${compile.stderr}',
      );
      final result = await Process.run(exe, const <String>[]);
      _expectInert(result);
    } on ToolUnavailable catch (e) {
      markTestSkipped('$e');
    } finally {
      await temp.delete(recursive: true);
    }
  });

  test(
    'with assertions enabled the same probe body fires every seam',
    () async {
      final outcome = await runHooksReleaseProbe();
      expect(
        outcome.firedSeams,
        containsAll(<Matcher>[
          startsWith('failRegistryAuditAppend'),
          startsWith('registry operation failed'),
          startsWith('onLog event_sourcing.sync_cycle'),
          startsWith('beforeRegistryTransaction setEndDate'),
          startsWith('onRegistryBodyRun setEndDate'),
          startsWith('afterFillReads'),
          startsWith('insideTransform'),
          startsWith('failFillTransaction'),
          startsWith('failOutcomeTransaction'),
          startsWith('afterWedgeHeadInTxn probe_refusing'),
          startsWith('afterWedgeTransaction probe_refusing'),
          contains('recorded alone'),
        ]),
      );
      // The first wedge rolled back and its attempt was recorded alone; the
      // second pass wedged the head from that record, and the failure
      // injected after its commit did not undo it.
      expect(outcome.wedgeEvents, 1);
      // The injected outcome failure rolled the send's outcome back.
      expect(outcome.sentItems, 0);
      expect(outcome.passed, isFalse);
      // The injected failure rolled the registry operation back: no end
      // date stored or reported, no audit event, no sequence number used.
      expect(outcome.storedEndDate, isNull);
      expect(outcome.registryEndDate, isNull);
      expect(outcome.endDateSetEvents, 0);
      expect(outcome.sequenceAdvance, 0);
    },
  );
}
