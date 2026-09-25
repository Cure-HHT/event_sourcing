// Tooling test: verifies repository configuration, not a requirement.
//
// Every Postgres-gated test file in this package and in
// `example_action_permissions` must be run by name in
// `.github/workflows/conformance-tests.yml`. The elspais test targets run
// without `PG_TEST_URL`, so a gated file that the workflow does not run
// never runs anywhere and its assertions go unexercised. A file is gated when
// it names the `PG_TEST_URL` variable, calls `testPostgresUrl(`, or imports
// `test_postgres_url.dart`. Only `_test.dart` files are considered; harness
// files that mention the variable are not test entry points. This file names
// the gating tokens in order to detect them, so the scan skips it.
//
// A file counts as run only when the parsed workflow has a step whose
// `working-directory` is the file's package, whose `run:` command runs
// `flutter test` or `dart test` with the file's path as an argument, and
// whose job, step or workflow environment sets `PG_TEST_URL`. A mention in a
// YAML comment, under another package's directory, or in a job without the
// database does not count.

@TestOn('vm')
library;

import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:test/test.dart';
import 'package:yaml/yaml.dart';

/// A test file as the listing check sees it: its path relative to the
/// package root the workflow runs it from, and its source text.
class TestSource {
  const TestSource(this.packageRelativePath, this.contents);

  final String packageRelativePath;
  final String contents;
}

/// Whether [contents] gates its tests on a Postgres test URL.
bool isPostgresGated(String contents) =>
    contents.contains('PG_TEST_URL') ||
    contents.contains('testPostgresUrl(') ||
    contents.contains('test_postgres_url.dart');

/// A workflow step that runs tests against Postgres: its working directory
/// (repository-relative, normalised) and the test paths its `run:` command
/// passes to `flutter test` or `dart test`.
class PostgresTestStep {
  const PostgresTestStep(this.workingDirectory, this.testPaths);

  final String workingDirectory;
  final Set<String> testPaths;
}

bool _setsPgUrl(Object? env) =>
    env is YamlMap && env.containsKey('PG_TEST_URL');

/// The steps of [workflowText] whose environment (workflow, job or step)
/// sets `PG_TEST_URL` and that run `flutter test` or `dart test`.
List<PostgresTestStep> postgresTestSteps(String workflowText) {
  final doc = loadYaml(workflowText);
  if (doc is! YamlMap) return const <PostgresTestStep>[];
  final workflowPg = _setsPgUrl(doc['env']);
  final jobs = doc['jobs'];
  if (jobs is! YamlMap) return const <PostgresTestStep>[];
  final steps = <PostgresTestStep>[];
  for (final job in jobs.values) {
    if (job is! YamlMap) continue;
    final jobPg = workflowPg || _setsPgUrl(job['env']);
    final defaults = job['defaults'];
    final defaultRun = defaults is YamlMap ? defaults['run'] : null;
    final defaultDir = defaultRun is YamlMap
        ? defaultRun['working-directory'] as String?
        : null;
    final jobSteps = job['steps'];
    if (jobSteps is! YamlList) continue;
    for (final step in jobSteps) {
      if (step is! YamlMap) continue;
      final run = step['run'];
      if (run is! String) continue;
      if (!(jobPg || _setsPgUrl(step['env']))) continue;
      final dir = (step['working-directory'] as String?) ?? defaultDir ?? '.';
      final paths = <String>{};
      for (final line in run.split('\n')) {
        final tokens = line.trim().split(RegExp(r'\s+'));
        for (var i = 0; i + 1 < tokens.length; i++) {
          final runsTests =
              (tokens[i] == 'flutter' || tokens[i] == 'dart') &&
              tokens[i + 1] == 'test';
          if (runsTests) {
            paths.addAll(
              tokens.skip(i + 2).where((t) => t.endsWith('_test.dart')),
            );
          }
        }
      }
      if (paths.isNotEmpty) {
        steps.add(PostgresTestStep(p.posix.normalize(dir), paths));
      }
    }
  }
  return steps;
}

/// The package-relative paths of gated `_test.dart` files in [sources] that
/// no Postgres step of [workflowText] runs from [packageDir] (the package's
/// repository-relative directory).
List<String> unlistedPostgresTests(
  List<TestSource> sources,
  String packageDir,
  String workflowText,
) {
  final steps = postgresTestSteps(workflowText);
  final dir = p.posix.normalize(packageDir);
  bool isRun(String path) =>
      steps.any((s) => s.workingDirectory == dir && s.testPaths.contains(path));
  final missing = <String>[
    for (final source in sources)
      if (source.packageRelativePath.endsWith('_test.dart') &&
          isPostgresGated(source.contents) &&
          !isRun(source.packageRelativePath))
        source.packageRelativePath,
  ]..sort();
  return missing;
}

/// This file's path relative to the package root; the scan skips it.
const _selfPath = 'test/ci/postgres_ci_listing_test.dart';

/// Reads every `.dart` file under `<packageRoot>/test` except this one,
/// keyed by its path relative to [packageRoot] with forward slashes.
List<TestSource> _readTestTree(String packageRoot) {
  final testDir = Directory(p.join(packageRoot, 'test'));
  return <TestSource>[
    for (final entity in testDir.listSync(recursive: true))
      if (entity is File &&
          entity.path.endsWith('.dart') &&
          !p.equals(entity.path, p.join(packageRoot, _selfPath)))
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
    p.join(repoRoot, '.github', 'workflows', 'conformance-tests.yml'),
  );

  group('Postgres-gated test files are listed in conformance-tests.yml', () {
    test('the workflow file exists', () {
      expect(workflowFile.existsSync(), isTrue, reason: workflowFile.path);
    });

    test('every gated file in event_sourcing/test is listed', () {
      final sources = _readTestTree(packageRoot);
      expect(
        sources.where((s) => isPostgresGated(s.contents)),
        isNotEmpty,
        reason: 'the scan must find the known gated files',
      );
      expect(
        unlistedPostgresTests(
          sources,
          'event_sourcing',
          workflowFile.readAsStringSync(),
        ),
        isEmpty,
      );
    });

    test('every gated file in example_action_permissions/test is listed', () {
      final sources = _readTestTree(
        p.join(packageRoot, 'example_action_permissions'),
      );
      expect(
        sources.where((s) => isPostgresGated(s.contents)),
        isNotEmpty,
        reason: 'the scan must find the known gated files',
      );
      expect(
        unlistedPostgresTests(
          sources,
          'event_sourcing/example_action_permissions',
          workflowFile.readAsStringSync(),
        ),
        isEmpty,
      );
    });

    test('an unlisted file gated by the literal variable is reported', () {
      final sources = <TestSource>[
        const TestSource('test/listed_test.dart', "env['PG_TEST_URL']"),
        const TestSource(
          'test/storage/postgres/synthetic_test.dart',
          "final url = Platform.environment['PG_TEST_URL'];",
        ),
      ];
      expect(unlistedPostgresTests(sources, 'pkg', _workflow()), [
        'test/storage/postgres/synthetic_test.dart',
      ]);
    });

    test('an unlisted file gated only through the helper is reported', () {
      final sources = <TestSource>[
        const TestSource(
          'test/storage/postgres/helper_call_test.dart',
          'final url = testPostgresUrl();',
        ),
        const TestSource(
          'test/storage/postgres/helper_import_test.dart',
          "import 'test_postgres_url.dart';",
        ),
      ];
      expect(unlistedPostgresTests(sources, 'pkg', _workflow()), [
        'test/storage/postgres/helper_call_test.dart',
        'test/storage/postgres/helper_import_test.dart',
      ]);
    });

    test('a file run only by a job without PG_TEST_URL is reported', () {
      final sources = <TestSource>[_gated('test/listed_test.dart')];
      expect(unlistedPostgresTests(sources, 'pkg', _workflow(jobEnv: '')), [
        'test/listed_test.dart',
      ]);
    });

    test('a file run from another package directory is reported', () {
      final sources = <TestSource>[_gated('test/listed_test.dart')];
      expect(
        unlistedPostgresTests(sources, 'pkg', _workflow(workingDir: 'other')),
        ['test/listed_test.dart'],
      );
      expect(
        unlistedPostgresTests(sources, 'other', _workflow()),
        ['test/listed_test.dart'],
        reason: 'the same relative path under another package is not a run',
      );
    });

    test('a file named only in a YAML comment is reported', () {
      final sources = <TestSource>[_gated('test/commented_test.dart')];
      final workflow =
          '${_workflow()}\n'
          '      # run: flutter test test/commented_test.dart\n';
      expect(unlistedPostgresTests(sources, 'pkg', workflow), [
        'test/commented_test.dart',
      ]);
    });

    test('a file run with the variable set on the step is listed', () {
      final sources = <TestSource>[_gated('test/listed_test.dart')];
      final workflow = _workflow(
        jobEnv: '',
        stepEnv: '        env:\n          PG_TEST_URL: postgres://x\n',
      );
      expect(unlistedPostgresTests(sources, 'pkg', workflow), isEmpty);
    });

    test('a gated file that is not a _test.dart entry point is ignored', () {
      final sources = <TestSource>[
        const TestSource(
          'test/storage/storage_backend_conformance.dart',
          "Platform.environment['PG_TEST_URL']",
        ),
      ];
      expect(unlistedPostgresTests(sources, 'pkg', ''), isEmpty);
    });

    test('an ungated file need not be listed', () {
      final sources = <TestSource>[
        const TestSource('test/plain_test.dart', 'void main() {}'),
      ];
      expect(unlistedPostgresTests(sources, 'pkg', ''), isEmpty);
    });
  });
}

TestSource _gated(String path) =>
    TestSource(path, "Platform.environment['PG_TEST_URL']");

/// A one-job workflow that runs `test/listed_test.dart` from [workingDir].
/// [jobEnv] is the job's `env:` block (empty for none); [stepEnv] is the
/// step's.
String _workflow({
  String workingDir = 'pkg',
  String jobEnv = '    env:\n      PG_TEST_URL: postgres://x\n',
  String stepEnv = '',
}) =>
    'jobs:\n'
    '  postgres:\n'
    '$jobEnv'
    '    steps:\n'
    '      - name: Listed\n'
    '        run: flutter test test/listed_test.dart\n'
    '        working-directory: $workingDir\n'
    '$stepEnv';
