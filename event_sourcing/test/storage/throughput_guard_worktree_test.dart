@TestOn('vm')
library;

import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:test/test.dart';

import '../test_support/throughput_guard_worktree.dart';

/// A recorded git invocation, for assertions on what the helper ran.
class _Call {
  _Call(this.args, this.workingDirectory);
  final List<String> args;
  final String workingDirectory;
}

/// A fake [GitRunner] that records every call and answers from [answers] (by
/// the args it was given, joined with a space), or succeeds with '' when no
/// answer is registered. An entry that is an [Exception] is thrown instead.
class _FakeGit {
  final calls = <_Call>[];
  final Map<String, Object> answers = {};

  Future<String> call(
    List<String> args, {
    required String workingDirectory,
  }) async {
    calls.add(_Call(args, workingDirectory));
    final key = args.join(' ');
    final answer = answers[key];
    if (answer is Exception) throw answer;
    if (answer is String) return answer;
    return '';
  }
}

void main() {
  const repoRoot = '/repo';
  const commit = 'deadbeef';

  test('a body failure propagates even when worktree remove also fails, and '
      'the temp directory is always deleted', () async {
    final git = _FakeGit();
    final failures = <String>[];
    String? createdTempPath;

    final bodyError = StateError('throughput regression');

    Object? caught;
    try {
      await withThroughputGuardWorktree<void>(
        repoRoot: repoRoot,
        commit: commit,
        runGit: git.call,
        onCleanupFailure: failures.add,
        body: (worktreePath) async {
          createdTempPath = p.dirname(worktreePath);
          // Make 'worktree remove' for this path fail, so cleanup cannot
          // succeed quietly.
          git.answers['worktree remove --force $worktreePath'] = Exception(
            'remove failed',
          );
          throw bodyError;
        },
      );
    } catch (e) {
      caught = e;
    }

    // The body's own failure is what propagates, not a cleanup failure.
    expect(caught, same(bodyError));
    // The cleanup failure was reported, not swallowed silently.
    expect(failures, isNotEmpty);
    expect(failures.single, contains('remove failed'));
    // The temp directory is gone regardless of the failed 'worktree
    // remove'.
    expect(createdTempPath, isNotNull);
    expect(Directory(createdTempPath!).existsSync(), isFalse);
  });

  test('a successful body whose worktree remove fails still throws, after '
      'deleting the temp directory and reporting the failure', () async {
    final git = _FakeGit();
    final failures = <String>[];
    String? createdTempPath;
    String? createdWorktreePath;

    Object? caught;
    try {
      await withThroughputGuardWorktree<void>(
        repoRoot: repoRoot,
        commit: commit,
        runGit: git.call,
        onCleanupFailure: failures.add,
        body: (worktreePath) async {
          createdWorktreePath = worktreePath;
          createdTempPath = p.dirname(worktreePath);
          git.answers['worktree remove --force $worktreePath'] = Exception(
            'remove failed',
          );
        },
      );
    } catch (e) {
      caught = e;
    }

    // A cleanup failure after a successful body is not swallowed: it
    // throws, so a leaked worktree never passes as green.
    expect(caught, isNotNull);
    expect(caught.toString(), contains('remove failed'));
    // The failure was still reported, not only thrown.
    expect(failures, isNotEmpty);
    expect(failures.any((f) => f.contains('remove failed')), isTrue);
    // The temp directory delete was still attempted despite the failed
    // 'worktree remove'.
    expect(createdTempPath, isNotNull);
    expect(Directory(createdTempPath!).existsSync(), isFalse);
    // The post-removal listing check still ran.
    expect(
      git.calls.any(
        (c) =>
            c.args.length == 3 &&
            c.args[0] == 'worktree' &&
            c.args[1] == 'list' &&
            c.args[2] == '--porcelain',
      ),
      isTrue,
    );
    expect(createdWorktreePath, isNotNull);
  });

  test(
    'a stale registration under temp with the throughput-guard prefix is '
    'removed before add; a registration outside that prefix is untouched',
    () async {
      final git = _FakeGit();
      final systemTempPath = Directory.systemTemp.path;
      final stalePath = p.join(
        systemTempPath,
        'evs-throughput-guard-stale123',
        'evs-baseline',
      );
      final foreignPath = p.join(systemTempPath, 'some-other-worktree');
      git.answers['worktree list --porcelain'] =
          'worktree $stalePath\nHEAD 0000000000000000000000000000000000000000\n'
          'detached\n\n'
          'worktree $foreignPath\nHEAD 0000000000000000000000000000000000000000\n'
          'detached\n';

      await withThroughputGuardWorktree<void>(
        repoRoot: repoRoot,
        commit: commit,
        runGit: git.call,
        body: (worktreePath) async {},
      );

      final removedPaths = [
        for (final call in git.calls)
          if (call.args case ['worktree', 'remove', '--force', final path])
            path,
      ];
      expect(removedPaths, contains(stalePath));
      expect(removedPaths, isNot(contains(foreignPath)));
    },
  );

  test('a prefixed worktree locked with a live owner pid is left alone by the '
      'sweep', () async {
    final git = _FakeGit();
    final systemTempPath = Directory.systemTemp.path;
    final livePath = p.join(
      systemTempPath,
      'evs-throughput-guard-live1',
      'evs-baseline',
    );
    git.answers['worktree list --porcelain'] =
        'worktree $livePath\nHEAD 0000000000000000000000000000000000000000\n'
        'locked evs-throughput-guard pid=4242\ndetached\n';

    await withThroughputGuardWorktree<void>(
      repoRoot: repoRoot,
      commit: commit,
      runGit: git.call,
      isProcessAlive: (ownerPid) => ownerPid == 4242,
      body: (worktreePath) async {},
    );

    final removedPaths = [
      for (final call in git.calls)
        if (call.args case ['worktree', 'remove', '--force', final path]) path,
    ];
    expect(removedPaths, isNot(contains(livePath)));
  });

  test('a prefixed worktree locked by a dead owner pid, or left unlocked, is '
      'still swept', () async {
    final git = _FakeGit();
    final systemTempPath = Directory.systemTemp.path;
    final deadOwnerPath = p.join(
      systemTempPath,
      'evs-throughput-guard-dead1',
      'evs-baseline',
    );
    final unlockedPath = p.join(
      systemTempPath,
      'evs-throughput-guard-unlocked1',
      'evs-baseline',
    );
    git.answers['worktree list --porcelain'] =
        'worktree $deadOwnerPath\nHEAD 0000000000000000000000000000000000000000\n'
        'locked evs-throughput-guard pid=999999\ndetached\n\n'
        'worktree $unlockedPath\nHEAD 0000000000000000000000000000000000000000\n'
        'detached\n';

    await withThroughputGuardWorktree<void>(
      repoRoot: repoRoot,
      commit: commit,
      runGit: git.call,
      isProcessAlive: (ownerPid) => false,
      body: (worktreePath) async {},
    );

    final removedPaths = [
      for (final call in git.calls)
        if (call.args case ['worktree', 'remove', '--force', final path]) path,
    ];
    expect(removedPaths, containsAll([deadOwnerPath, unlockedPath]));
  });

  test("the guard's own worktree is created locked with its pid, unlocked, "
      'then removed after a successful body', () async {
    final git = _FakeGit();
    String? worktreePath;

    await withThroughputGuardWorktree<void>(
      repoRoot: repoRoot,
      commit: commit,
      runGit: git.call,
      isProcessAlive: (ownerPid) => true,
      body: (path) async {
        worktreePath = path;
      },
    );

    expect(worktreePath, isNotNull);

    final addCall = git.calls.firstWhere(
      (c) => c.args.length > 1 && c.args[0] == 'worktree' && c.args[1] == 'add',
    );
    expect(addCall.args, contains('--lock'));
    expect(addCall.args.any((a) => a.contains('pid=$pid')), isTrue);

    final unlockIndex = git.calls.indexWhere(
      (c) => switch (c.args) {
        ['worktree', 'unlock', final path] => path == worktreePath,
        _ => false,
      },
    );
    final removeIndex = git.calls.indexWhere(
      (c) => switch (c.args) {
        ['worktree', 'remove', '--force', final path] => path == worktreePath,
        _ => false,
      },
    );
    expect(unlockIndex, greaterThanOrEqualTo(0));
    expect(removeIndex, greaterThan(unlockIndex));
  });
}
