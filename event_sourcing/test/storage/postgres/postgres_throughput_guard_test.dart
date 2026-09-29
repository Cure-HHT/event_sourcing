// The throughput guard of EVS-DEV-chain-verification/T: on Postgres, the
// current tree's append throughput and its ingest throughput each stay at
// least half the throughput the baseline build (this library on its main
// branch, at the commit immediately before the data-format major step)
// reaches on the same workload and host.
//
// Gated on PG_TEST_URL and on the opt-in EVS_THROUGHPUT_TEST=1: the harness
// checks out the baseline commit into a temporary git worktree and runs
// `flutter pub get` and `flutter test` there, which the default suites must
// stay fast enough to skip.
//
// The workloads themselves live in throughput_workload.dart (the current
// tree's, run in-process) and in throughput_baseline_workload.dart.txt (the
// baseline's, copied into the baseline worktree as its own test file and run
// there with `flutter test`).
//
// Verifies: EVS-DEV-chain-verification/T

@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:test/test.dart';

import '../../test_support/tool_subprocess.dart';
import 'test_postgres_url.dart';
import 'throughput_workload.dart';

/// The library on its main branch, immediately before the data-format major
/// step: the throughput guard's baseline build.
const _kBaselineCommit = '3089bbe';

/// Opt-in variable: unset, the harness (a git checkout, a `pub get` and a
/// nested `flutter test`) does not run, so the default suites stay fast.
const _kOptInVar = 'EVS_THROUGHPUT_TEST';

/// The last line of [output] that decodes as a JSON object, or null when no
/// line does. The test runner's own reporter lines are not JSON.
Map<String, Object?>? _lastJsonObject(String output) {
  Map<String, Object?>? found;
  for (final line in const LineSplitter().convert(output)) {
    final trimmed = line.trim();
    if (!trimmed.startsWith('{')) continue;
    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is Map<String, Object?>) found = decoded;
    } on FormatException {
      // Not a JSON line; part of the nested test runner's own output.
    }
  }
  return found;
}

/// Runs [tool] with [args] in [workingDirectory], returning its combined
/// stdout and stderr. Throws with that output included when it exits
/// non-zero, so a harness failure shows what the subprocess printed.
Future<String> _runTool(
  String tool,
  List<String> args, {
  required String workingDirectory,
  Map<String, String> environment = const <String, String>{},
}) async {
  final result = await Process.run(
    tool,
    args,
    workingDirectory: workingDirectory,
    environment: <String, String>{
      'PUB_ENVIRONMENT': 'event_sourcing_test',
      ...environment,
    },
  );
  final combined = '${result.stdout}${result.stderr}';
  if (result.exitCode != 0) {
    fail(
      '$tool ${args.join(' ')} (in $workingDirectory) exited '
      '${result.exitCode}:\n$combined',
    );
  }
  return combined;
}

Future<String> _runGit(List<String> args, {required String workingDirectory}) =>
    _runTool('git', args, workingDirectory: workingDirectory);

void main() {
  final pgUrl = testPostgresUrl();
  final optedIn = Platform.environment[_kOptInVar] == '1';
  final skipReason = pgUrl == null
      ? 'PG_TEST_URL is not set'
      : (optedIn ? null : '$_kOptInVar is not set to 1');

  test(
    "append and ingest throughput stay at least half the baseline build's",
    () async {
      final repoRoot = (await _runGit([
        'rev-parse',
        '--show-toplevel',
      ], workingDirectory: Directory.current.path)).trim();

      // A worktree a prior run left registered (e.g. a killed process, whose
      // `finally` block below never ran) would otherwise make `worktree add`
      // below fail with a stale-path collision.
      await _runGit(['worktree', 'prune'], workingDirectory: repoRoot);

      final tempParent = await Directory.systemTemp.createTemp(
        'evs-throughput-guard-',
      );
      final worktreePath = p.join(tempParent.path, 'evs-baseline');
      var worktreeAdded = false;
      try {
        await _runGit([
          'worktree',
          'add',
          '--detach',
          worktreePath,
          _kBaselineCommit,
        ], workingDirectory: repoRoot);
        worktreeAdded = true;

        final baselineEventSourcing = p.join(worktreePath, 'event_sourcing');
        await _runTool(sdkTool('flutter'), [
          'pub',
          'get',
        ], workingDirectory: baselineEventSourcing);

        final workloadSource = File(
          p.join(
            Directory.current.path,
            'test',
            'storage',
            'postgres',
            'throughput_baseline_workload.dart.txt',
          ),
        ).readAsStringSync();
        final baselineTestRelativePath = p.join(
          'test',
          'throughput_baseline_workload_test.dart',
        );
        File(
          p.join(baselineEventSourcing, baselineTestRelativePath),
        ).writeAsStringSync(workloadSource);

        final baselineOutput = await _runTool(
          sdkTool('flutter'),
          ['test', '--no-pub', '--concurrency=1', baselineTestRelativePath],
          workingDirectory: baselineEventSourcing,
          environment: <String, String>{'PG_TEST_URL': pgUrl!},
        );
        final baselineJson = _lastJsonObject(baselineOutput);
        expect(
          baselineJson,
          isNotNull,
          reason: 'baseline workload printed no JSON line:\n$baselineOutput',
        );
        final baselineAppend = (baselineJson!['append_per_sec']! as num)
            .toDouble();
        final baselineIngest = (baselineJson['ingest_per_sec']! as num)
            .toDouble();

        final current = await runThroughputWorkload(pgUrl);

        final appendRatio = current.appendPerSec / baselineAppend;
        final ingestRatio = current.ingestPerSec / baselineIngest;

        // ignore: avoid_print
        print(
          'throughput per second -- baseline: append '
          '${baselineAppend.toStringAsFixed(1)}, ingest '
          '${baselineIngest.toStringAsFixed(1)}; current: append '
          '${current.appendPerSec.toStringAsFixed(1)}, ingest '
          '${current.ingestPerSec.toStringAsFixed(1)}; ratios: append '
          '${appendRatio.toStringAsFixed(3)}, ingest '
          '${ingestRatio.toStringAsFixed(3)}',
        );

        expect(
          current.appendPerSec,
          greaterThanOrEqualTo(baselineAppend / 2),
          reason:
              'append throughput ${current.appendPerSec}/s is below half '
              'the baseline $baselineAppend/s',
        );
        expect(
          current.ingestPerSec,
          greaterThanOrEqualTo(baselineIngest / 2),
          reason:
              'ingest throughput ${current.ingestPerSec}/s is below half '
              'the baseline $baselineIngest/s',
        );
      } finally {
        if (worktreeAdded) {
          await _runGit([
            'worktree',
            'remove',
            '--force',
            worktreePath,
          ], workingDirectory: repoRoot);
        }
        final listing = await _runGit([
          'worktree',
          'list',
        ], workingDirectory: repoRoot);
        expect(
          listing.contains(worktreePath),
          isFalse,
          reason: 'the baseline worktree was not removed:\n$listing',
        );
        await tempParent.delete(recursive: true);
      }
    },
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 20)),
  );
}
