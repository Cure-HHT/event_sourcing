// Tooling test: verifies repository configuration, not a requirement.
//
// Every Postgres-gated test file in this package and in
// `example_action_permissions` must run in CI against a Postgres server. The
// unit targets remove the Postgres URL from their environment, so a gated file
// that no Postgres target runs never runs anywhere and its assertions go
// unexercised. A file is gated when it names the Postgres test URL variable,
// calls the URL helper, or imports the helper's library (see
// [isPostgresGated]). Only `_test.dart` files are considered; harness files
// that mention the variable are not test entry points. This file spells the
// gating tokens in pieces, so it is not gated itself.
//
// The test suites are the `[[scanning.test.targets]]` of `.elspais.toml`, and
// CI runs them through `elspais test --targets <names>`. A gated
// file counts as run in CI when all three links hold:
//   1. `tools/run-checks.sh target-files <kind>`, run from the file's package,
//      lists it: that is the set the package's `target <kind>` command runs
//      (the script and [isPostgresGated] must agree);
//   2. `.elspais.toml` has a target whose working directory is the package and
//      whose command runs `run-checks.sh target <kind>`;
//   3. `.github/workflows/event-sourcing-tests.yml` has a step that runs
//      `elspais test --targets` naming that target or one of its groups, in a
//      job or step whose environment sets the Postgres URL. A
//      mention in a YAML comment or in a job without the database does not
//      count.
//
// CI's `postgres` job runs the Postgres targets in PG_PART slices, one per
// value of its matrix's `part`. Each slice computes the same shard plan and
// runs its own buckets of it, so together the slices must run every listed
// file, and every piece of a file split into pieces, exactly once:
// `tools/run-checks.sh target-plan postgres` prints what one slice runs, and
// the matrix's values must be 1..n with PG_PART dividing the plan by n.

@TestOn('vm')
library;

import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:test/test.dart';
import 'package:yaml/yaml.dart';

// The gating tokens, spelled in pieces (see the file comment): the Postgres
// test URL variable, the URL helper's call and the helper's library.
const _pgUrlVar =
    'PG_'
    'TEST_URL';
const _helperCall =
    'testPostgresUrl'
    '(';
const _helperLibrary =
    'test_postgres_url'
    '.dart';

/// A test file as the listing check sees it: its path relative to its package
/// root, and its source text.
class TestSource {
  const TestSource(this.packageRelativePath, this.contents);

  final String packageRelativePath;
  final String contents;
}

/// Whether [contents] gates its tests on a Postgres test URL.
bool isPostgresGated(String contents) =>
    contents.contains(_pgUrlVar) ||
    contents.contains(_helperCall) ||
    contents.contains(_helperLibrary);

/// The package-relative paths of the gated `_test.dart` files in [sources].
Set<String> gatedTestFiles(List<TestSource> sources) => <String>{
  for (final source in sources)
    if (source.packageRelativePath.endsWith('_test.dart') &&
        isPostgresGated(source.contents))
      source.packageRelativePath,
};

/// A `[[scanning.test.targets]]` entry of `.elspais.toml`.
class ElspaisTarget {
  const ElspaisTarget(this.name, this.cwd, this.command, this.groups);

  final String name;
  final String cwd;
  final String command;
  final List<String> groups;

  /// The groups a run can name this target by: the ones it claims, or
  /// `default` when it claims none, and `all`.
  Set<String> get selectors => {
    name,
    ...(groups.isEmpty ? const ['default'] : groups),
    'all',
  };

  /// The `run-checks.sh target <kind>` kind its command runs, if any.
  String? get runChecksKind =>
      RegExp(r'run-checks\.sh\s+target\s+(\w+)').firstMatch(command)?.group(1);
}

final _stringKey = RegExp(r'^(\w+)\s*=\s*"((?:[^"\\]|\\.)*)"\s*(#.*)?$');
final _listKey = RegExp(r'^(\w+)\s*=\s*\[(.*)\]\s*(#.*)?$');
final _quoted = RegExp(r'"((?:[^"\\]|\\.)*)"');

/// The test targets of an `.elspais.toml` text. Reads the subset of TOML the
/// file uses for them: `[[scanning.test.targets]]` tables of one-line
/// `key = "string"` and `key = ["string", ...]` entries.
List<ElspaisTarget> elspaisTargets(String toml) {
  final targets = <ElspaisTarget>[];
  Map<String, Object>? current;
  void close() {
    final t = current;
    if (t == null) return;
    targets.add(
      ElspaisTarget(
        (t['name'] as String?) ?? '',
        (t['cwd'] as String?) ?? '',
        (t['command'] as String?) ?? '',
        (t['groups'] as List<String>?) ?? const <String>[],
      ),
    );
    current = null;
  }

  for (final raw in toml.split('\n')) {
    final line = raw.trim();
    if (line.startsWith('[')) {
      close();
      if (line == '[[scanning.test.targets]]') current = <String, Object>{};
      continue;
    }
    final t = current;
    if (t == null) continue;
    final s = _stringKey.firstMatch(line);
    if (s != null) {
      t[s.group(1)!] = s.group(2)!;
      continue;
    }
    final l = _listKey.firstMatch(line);
    if (l != null) {
      t[l.group(1)!] = [
        for (final m in _quoted.allMatches(l.group(2)!)) m.group(1)!,
      ];
    }
  }
  close();
  return targets;
}

bool _setsPgUrl(Object? env) => env is YamlMap && env.containsKey(_pgUrlVar);

/// The names passed to `--targets` of `elspais test` by the steps of
/// [workflowText] whose environment (workflow, job or step) sets the Postgres
/// URL.
Set<String> targetsRunWithPostgres(String workflowText) {
  final doc = loadYaml(workflowText);
  if (doc is! YamlMap) return const <String>{};
  final workflowPg = _setsPgUrl(doc['env']);
  final jobs = doc['jobs'];
  if (jobs is! YamlMap) return const <String>{};
  final names = <String>{};
  for (final job in jobs.values) {
    if (job is! YamlMap) continue;
    final jobPg = workflowPg || _setsPgUrl(job['env']);
    final steps = job['steps'];
    if (steps is! YamlList) continue;
    for (final step in steps) {
      if (step is! YamlMap) continue;
      final run = step['run'];
      if (run is! String) continue;
      if (!(jobPg || _setsPgUrl(step['env']))) continue;
      for (final line in run.split('\n')) {
        final tokens = line.trim().split(RegExp(r'\s+'));
        final command = tokens.indexOf('test');
        if (command < 1 || !tokens[command - 1].endsWith('elspais')) {
          continue;
        }
        var selecting = false;
        for (final token in tokens.skip(command + 1)) {
          if (token.startsWith('--')) {
            selecting = token == '--targets';
          } else if (selecting) {
            names.add(token.replaceAll('"', '').replaceAll("'", ''));
          }
        }
      }
    }
  }
  return names;
}

/// The slices CI's `postgres` job cuts the Postgres plan into: the values of
/// its matrix's `part`, and the count its `PG_PART` (`<part>/<count>`) names,
/// null when the job, the matrix or the count is missing.
({List<Object?> parts, int? count}) postgresSlices(String workflowText) {
  final doc = loadYaml(workflowText);
  final job = doc is YamlMap && doc['jobs'] is YamlMap
      ? (doc['jobs'] as YamlMap)['postgres']
      : null;
  if (job is! YamlMap) return (parts: const <Object?>[], count: null);
  final strategy = job['strategy'];
  final matrix = strategy is YamlMap ? strategy['matrix'] : null;
  final parts = matrix is YamlMap && matrix['part'] is YamlList
      ? (matrix['part'] as YamlList).toList()
      : const <Object?>[];
  final env = job['env'];
  final pgPart = env is YamlMap ? env['PG_PART'] : null;
  final count = pgPart is String
      ? int.tryParse(
          RegExp(r'/\s*(\d+)\s*$').firstMatch(pgPart)?.group(1) ?? '',
        )
      : null;
  return (parts: parts, count: count);
}

/// What is wrong with [slices], the `target-plan` output of each of n slices
/// of one plan (`<path>\t<pieces>\t<piece index>` lines), given the files the
/// target runs: a file no slice runs, a piece run twice or by no slice, or a
/// path the target does not run. Empty when every file runs exactly once.
List<String> planCoverageProblems(List<String> slices, Set<String> files) {
  final problems = <String>[];
  final runs = <String, int>{};
  final pieces = <String, Set<int>>{};
  for (final slice in slices) {
    for (final line in slice.split('\n')) {
      if (line.trim().isEmpty) continue;
      final fields = line.split('\t');
      if (fields.length != 3) {
        problems.add('unreadable plan line: $line');
        continue;
      }
      final [path, k, idx] = fields;
      final key = '$path piece ${int.parse(idx) + 1}/$k';
      runs[key] = (runs[key] ?? 0) + 1;
      pieces.putIfAbsent('$path\t$k', () => <int>{}).add(int.parse(idx));
      if (!files.contains(path)) problems.add('$path is not a listed file');
    }
  }
  for (final MapEntry(:key, :value) in runs.entries) {
    if (value > 1) problems.add('$key runs $value times');
  }
  final planned = <String>{};
  for (final MapEntry(:key, :value) in pieces.entries) {
    final [path, k] = key.split('\t');
    planned.add(path);
    for (var i = 0; i < int.parse(k); i++) {
      if (!value.contains(i)) {
        problems.add('$path piece ${i + 1}/$k runs nowhere');
      }
    }
  }
  if (pieces.keys.map((k) => k.split('\t').first).toSet().length !=
      pieces.length) {
    problems.add('a file is split two different ways');
  }
  for (final f in files.difference(planned).toList()..sort()) {
    problems.add('$f runs in no slice');
  }
  return problems;
}

/// The gated files of the package at [packageDir] (repository-relative) that
/// no Postgres target run in CI covers. [listFiles] returns the files
/// `run-checks.sh target-files <kind>` lists for the package.
List<String> uncoveredPostgresTests({
  required Set<String> gated,
  required String packageDir,
  required List<ElspaisTarget> targets,
  required Set<String> ciTargets,
  required Set<String> Function(String kind) listFiles,
}) {
  final covered = <String>{};
  for (final target in targets) {
    final kind = target.runChecksKind;
    if (kind != 'postgres' && kind != 'throughput') continue;
    if (p.posix.normalize(target.cwd) != p.posix.normalize(packageDir)) {
      continue;
    }
    if (target.selectors.intersection(ciTargets).isEmpty) continue;
    covered.addAll(listFiles(kind!));
  }
  return (gated.difference(covered).toList()..sort());
}

/// Reads every `.dart` file under `<packageRoot>/test`, keyed by its path
/// relative to [packageRoot] with forward slashes.
List<TestSource> _readTestTree(String packageRoot) {
  final testDir = Directory(p.join(packageRoot, 'test'));
  return <TestSource>[
    for (final entity in testDir.listSync(recursive: true))
      if (entity is File && entity.path.endsWith('.dart'))
        TestSource(
          p.posix.joinAll(p.split(p.relative(entity.path, from: packageRoot))),
          entity.readAsStringSync(),
        ),
  ];
}

void main() {
  // `flutter test` runs with the package root as the working directory.
  final packageRoot = Directory.current.path;
  final repoRoot = p.dirname(packageRoot);
  final workflowFile = File(
    p.join(repoRoot, '.github', 'workflows', 'event-sourcing-tests.yml'),
  );
  final configFile = File(p.join(repoRoot, '.elspais.toml'));
  final script = p.join(repoRoot, 'tools', 'run-checks.sh');

  Set<String> Function(String) scriptListing(String packageDir) => (kind) {
    final result = Process.runSync('bash', [
      script,
      'target-files',
      kind,
    ], workingDirectory: p.join(repoRoot, packageDir));
    expect(result.exitCode, 0, reason: '${result.stderr}');
    return (result.stdout as String)
        .split('\n')
        .map((l) => l.trim())
        .where((l) => l.isNotEmpty)
        .toSet();
  };

  // What slice [part] of [count] runs of the package at [packageDir], as CI's
  // `postgres` job computes it: one given server and no local durations.
  String scriptPlan(String packageDir, int part, int count) {
    final result = Process.runSync(
      'bash',
      [script, 'target-plan', 'postgres'],
      workingDirectory: p.join(repoRoot, packageDir),
      environment: {
        'CI': 'true',
        _pgUrlVar: 'postgres://unused@localhost/unused',
        '${_pgUrlVar}S': '',
        'PG_PART': '$part/$count',
      },
    );
    expect(result.exitCode, 0, reason: '${result.stderr}');
    return result.stdout as String;
  }

  group("CI's Postgres slices run every gated file exactly once", () {
    test("the matrix's parts are 1..n and PG_PART divides by n", () {
      final slices = postgresSlices(workflowFile.readAsStringSync());
      expect(slices.parts, isNotEmpty, reason: 'no postgres matrix parts');
      expect(slices.count, slices.parts.length);
      expect(slices.parts, [for (var i = 1; i <= slices.parts.length; i++) i]);
    });

    for (final packageDir in const [
      'event_sourcing',
      'event_sourcing/example_action_permissions',
    ]) {
      test("the slices together run $packageDir's files once", () {
        final count = postgresSlices(workflowFile.readAsStringSync()).count!;
        expect(
          planCoverageProblems([
            for (var part = 1; part <= count; part++)
              scriptPlan(packageDir, part, count),
          ], scriptListing(packageDir)('postgres')),
          isEmpty,
        );
      });
    }

    test('a slice count PG_PART does not name is read as missing', () {
      final slices = postgresSlices(
        'jobs:\n'
        '  postgres:\n'
        '    strategy:\n'
        '      matrix:\n'
        '        part: [1, 2]\n'
        '    env:\n'
        '      PG_PART: x\n',
      );
      expect(slices.parts, [1, 2]);
      expect(slices.count, isNull);
      expect(postgresSlices('jobs: {}\n').parts, isEmpty);
    });

    test('a file run by two slices, or by none, is reported', () {
      const files = {
        'test/a_test.dart',
        'test/b_test.dart',
        'test/c_test.dart',
      };
      expect(
        planCoverageProblems([
          'test/a_test.dart\t1\t0\ntest/b_test.dart\t2\t0\n',
          'test/b_test.dart\t2\t1\ntest/c_test.dart\t1\t0\n',
        ], files),
        isEmpty,
      );
      expect(
        planCoverageProblems([
          'test/a_test.dart\t1\t0\ntest/b_test.dart\t2\t0\n',
          'test/a_test.dart\t1\t0\n',
        ], files),
        unorderedEquals([
          'test/a_test.dart piece 1/1 runs 2 times',
          'test/b_test.dart piece 2/2 runs nowhere',
          'test/c_test.dart runs in no slice',
        ]),
      );
    });
  });

  group('Postgres-gated test files run in CI through elspais targets', () {
    test('the workflow and the elspais configuration exist', () {
      expect(workflowFile.existsSync(), isTrue, reason: workflowFile.path);
      expect(configFile.existsSync(), isTrue, reason: configFile.path);
    });

    for (final packageDir in const [
      'event_sourcing',
      'event_sourcing/example_action_permissions',
    ]) {
      test('every gated file in $packageDir/test is run', () {
        final gated = gatedTestFiles(
          _readTestTree(p.join(repoRoot, packageDir)),
        );
        expect(
          gated,
          isNotEmpty,
          reason: 'the scan must find the known gated files',
        );
        final listFiles = scriptListing(packageDir);
        final targets = elspaisTargets(configFile.readAsStringSync());
        final pgTargets = targets.where(
          (t) =>
              t.cwd == packageDir &&
              (t.runChecksKind == 'postgres' ||
                  t.runChecksKind == 'throughput'),
        );
        expect(pgTargets, isNotEmpty, reason: 'no Postgres target runs here');
        // The script's selection and the rule above agree exactly.
        final listed = <String>{
          for (final t in pgTargets) ...listFiles(t.runChecksKind!),
        };
        expect(listed, gated);
        expect(
          uncoveredPostgresTests(
            gated: gated,
            packageDir: packageDir,
            targets: targets,
            ciTargets: targetsRunWithPostgres(workflowFile.readAsStringSync()),
            listFiles: listFiles,
          ),
          isEmpty,
        );
      });
    }

    test('an unlisted file gated by the literal variable is gated', () {
      expect(
        isPostgresGated("final url = Platform.environment['$_pgUrlVar'];"),
        isTrue,
      );
    });

    test('a file gated only through the helper is gated', () {
      expect(isPostgresGated('final url = $_helperCall);'), isTrue);
      expect(isPostgresGated("import '$_helperLibrary';"), isTrue);
      expect(isPostgresGated('void main() {}'), isFalse);
    });

    test('a gated file that is not a _test.dart entry point is ignored', () {
      const sources = <TestSource>[
        TestSource(
          'test/storage/storage_backend_conformance.dart',
          "Platform.environment['$_pgUrlVar']",
        ),
        TestSource('test/listed_test.dart', "env['$_pgUrlVar']"),
      ];
      expect(gatedTestFiles(sources), {'test/listed_test.dart'});
    });

    test('the target tables of the configuration are read', () {
      final targets = elspaisTargets(_config());
      expect(targets.map((t) => t.name), ['pkg', 'pkg/postgres']);
      expect(targets.first.selectors, {'pkg', 'default', 'all'});
      expect(targets.first.runChecksKind, isNull);
      expect(targets.last.groups, ['default', 'postgres']);
      expect(targets.last.runChecksKind, 'postgres');
    });

    test('a file run by a Postgres target CI runs by its group is covered', () {
      expect(_uncovered(_workflow()), isEmpty);
      expect(_uncovered(_workflow(targets: 'pkg/postgres')), isEmpty);
    });

    test('a file whose target CI does not run is reported', () {
      expect(_uncovered(_workflow(targets: 'unit')), ['test/listed_test.dart']);
    });

    test('a file run only by a job without the Postgres URL is reported', () {
      expect(_uncovered(_workflow(jobEnv: '')), ['test/listed_test.dart']);
    });

    test('a file run with the variable set on the step is covered', () {
      final workflow = _workflow(
        jobEnv: '',
        stepEnv: '        env:\n          $_pgUrlVar: postgres://x\n',
      );
      expect(_uncovered(workflow), isEmpty);
    });

    test('a run named only in a YAML comment is reported', () {
      const workflow =
          'jobs:\n'
          '  postgres:\n'
          '    env:\n'
          '      $_pgUrlVar: postgres://x\n'
          '    steps:\n'
          '      - name: Tests\n'
          '        run: echo none\n'
          '      # run: elspais test --targets postgres\n';
      expect(_uncovered(workflow), ['test/listed_test.dart']);
    });

    test('a target in another package directory is not a run', () {
      expect(
        uncoveredPostgresTests(
          gated: {'test/listed_test.dart'},
          packageDir: 'other',
          targets: elspaisTargets(_config()),
          ciTargets: targetsRunWithPostgres(_workflow()),
          listFiles: (_) => {'test/listed_test.dart'},
        ),
        ['test/listed_test.dart'],
      );
    });

    test('a gated file the script does not list is reported', () {
      expect(
        uncoveredPostgresTests(
          gated: {'test/listed_test.dart', 'test/other_test.dart'},
          packageDir: 'pkg',
          targets: elspaisTargets(_config()),
          ciTargets: targetsRunWithPostgres(_workflow()),
          listFiles: (_) => {'test/listed_test.dart'},
        ),
        ['test/other_test.dart'],
      );
    });
  });
}

/// The gated files of `pkg` that [workflow] leaves unrun, given [_config] and
/// a script listing `test/listed_test.dart`.
List<String> _uncovered(String workflow) => uncoveredPostgresTests(
  gated: {'test/listed_test.dart'},
  packageDir: 'pkg',
  targets: elspaisTargets(_config()),
  ciTargets: targetsRunWithPostgres(workflow),
  listFiles: (_) => {'test/listed_test.dart'},
);

/// A configuration with a unit target and a Postgres target in `pkg`.
String _config() =>
    '[scanning.test.groups]\n'
    'postgres = "Postgres"\n'
    '\n'
    '[[scanning.test.targets]]\n'
    'name = "pkg"\n'
    'cwd = "pkg"\n'
    'command = "flutter test --machine"\n'
    '\n'
    '# The Postgres files.\n'
    '[[scanning.test.targets]]\n'
    'name = "pkg/postgres"\n'
    'groups = ["default", "postgres"]\n'
    'cwd = "pkg"\n'
    'command = "../tools/run-checks.sh target postgres"\n'
    '\n'
    '[scanning.docs]\n'
    'directories = ["docs"]\n';

/// A one-job workflow that runs `elspais test --targets [targets]`. [jobEnv]
/// is the job's `env:` block (empty for none); [stepEnv] is the step's.
String _workflow({
  String targets = 'postgres',
  String jobEnv = '    env:\n      $_pgUrlVar: postgres://x\n',
  String stepEnv = '',
}) =>
    'jobs:\n'
    '  postgres:\n'
    '$jobEnv'
    '    steps:\n'
    '      - name: Tests\n'
    '        run: elspais test --targets $targets\n'
    '$stepEnv';
