// The throughput guard's git-worktree lifecycle (EVS-DEV-chain-verification/T):
// checking out a baseline commit into a scratch worktree, running a body
// against it, and cleaning up afterward without losing a body failure or
// leaking the worktree.

import 'dart:io';

import 'package:path/path.dart' as p;

/// Runs `git` with [args] in [workingDirectory], returning its combined
/// stdout and stderr, and throwing when it exits non-zero. Matches the
/// signature callers already use for their own git subprocess helper, so a
/// test can pass it straight through, and a test can substitute a fake to
/// exercise failure paths without a real git checkout.
typedef GitRunner =
    Future<String> Function(
      List<String> args, {
      required String workingDirectory,
    });

/// The prefix on the temporary directory this helper creates per run. Only a
/// worktree registered under the system temp directory with this prefix is
/// ever removed as "stale"; every other worktree registration is left alone.
const kThroughputGuardWorktreePrefix = 'evs-throughput-guard-';

/// Checks out [commit] as a detached worktree under a fresh temp directory,
/// runs [body] with that worktree's path, and cleans up afterward.
///
/// Each run creates its own uniquely-named temp directory, so a stale
/// registration from a prior run never collides with this run's `git
/// worktree add`; the sweep below exists only to reclaim a checkout a prior
/// run leaked, not to avoid a naming clash.
///
/// Before adding the worktree, removes any worktree registration left behind
/// by a prior run that did not reach its own cleanup (e.g. one killed
/// mid-run): one whose path lies under the system temp directory inside a
/// directory named with [kThroughputGuardWorktreePrefix]. No other
/// registration is touched. Running two guards concurrently against the
/// same repository is unsupported: each would sweep the other's still-live
/// registration.
///
/// Every cleanup failure is reported through [onCleanupFailure], and the
/// temp directory deletion is always attempted even when the worktree
/// removal itself failed. When [body] throws, its failure is what
/// propagates and a cleanup failure never replaces it — cleanup failures are
/// only ever logged in that case. When [body] succeeds, a cleanup failure
/// (a failed removal, a failed temp-directory delete, or the worktree still
/// showing up in `git worktree list` after a reported-successful removal)
/// is not just logged: it is thrown, after every cleanup step has still
/// been attempted, so a run that leaks a worktree is never reported as
/// green.
///
/// Implements: EVS-DEV-chain-verification/T
Future<T> withThroughputGuardWorktree<T>({
  required String repoRoot,
  required String commit,
  required GitRunner runGit,
  required Future<T> Function(String worktreePath) body,
  void Function(String message)? onCleanupFailure,
}) async {
  final systemTempPath = Directory.systemTemp.path;
  await _removeStaleRegistrations(
    repoRoot: repoRoot,
    runGit: runGit,
    systemTempPath: systemTempPath,
    onCleanupFailure: onCleanupFailure,
  );

  final tempParent = await Directory.systemTemp.createTemp(
    kThroughputGuardWorktreePrefix,
  );
  final worktreePath = p.join(tempParent.path, 'evs-baseline');
  var worktreeAdded = false;
  var bodySucceeded = false;
  late final T result;
  Object? bodyError;
  StackTrace? bodyStack;
  try {
    await runGit([
      'worktree',
      'add',
      '--detach',
      worktreePath,
      commit,
    ], workingDirectory: repoRoot);
    worktreeAdded = true;
    result = await body(worktreePath);
    bodySucceeded = true;
  } catch (e, st) {
    bodyError = e;
    bodyStack = st;
  }

  final cleanupFailures = <String>[];
  if (worktreeAdded) {
    try {
      await runGit([
        'worktree',
        'remove',
        '--force',
        worktreePath,
      ], workingDirectory: repoRoot);
    } catch (e) {
      cleanupFailures.add('removing worktree $worktreePath failed: $e');
    }
  }
  try {
    await tempParent.delete(recursive: true);
  } catch (e) {
    cleanupFailures.add('deleting ${tempParent.path} failed: $e');
  }
  if (bodySucceeded && worktreeAdded) {
    try {
      final listing = await runGit([
        'worktree',
        'list',
        '--porcelain',
      ], workingDirectory: repoRoot);
      if (_registeredWorktreePaths(listing).contains(worktreePath)) {
        cleanupFailures.add(
          'worktree $worktreePath is still registered after removal',
        );
      }
    } catch (e) {
      cleanupFailures.add('listing worktrees after cleanup failed: $e');
    }
  }

  for (final failure in cleanupFailures) {
    onCleanupFailure?.call(failure);
  }

  if (bodyError != null) {
    Error.throwWithStackTrace(bodyError, bodyStack!);
  }
  if (cleanupFailures.isNotEmpty) {
    throw StateError(
      'throughput guard cleanup failed after a successful run: '
      '${cleanupFailures.join('; ')}',
    );
  }
  return result;
}

Future<void> _removeStaleRegistrations({
  required String repoRoot,
  required GitRunner runGit,
  required String systemTempPath,
  void Function(String message)? onCleanupFailure,
}) async {
  final String listing;
  try {
    listing = await runGit([
      'worktree',
      'list',
      '--porcelain',
    ], workingDirectory: repoRoot);
  } catch (e) {
    onCleanupFailure?.call('listing worktrees failed: $e');
    return;
  }
  for (final path in _registeredWorktreePaths(listing)) {
    if (!_isOwnStaleWorktree(path, systemTempPath: systemTempPath)) continue;
    try {
      await runGit([
        'worktree',
        'remove',
        '--force',
        path,
      ], workingDirectory: repoRoot);
    } catch (e) {
      onCleanupFailure?.call('removing stale worktree $path failed: $e');
    }
  }
}

/// The `worktree <path>` lines of `git worktree list --porcelain` output.
List<String> _registeredWorktreePaths(String porcelain) => [
  for (final line in porcelain.split('\n'))
    if (line.startsWith('worktree ')) line.substring('worktree '.length).trim(),
];

/// True when [path] lies under [systemTempPath] inside a first-level
/// directory named with [kThroughputGuardWorktreePrefix] — i.e. a temp
/// directory this helper itself created on a prior run.
bool _isOwnStaleWorktree(String path, {required String systemTempPath}) {
  final rel = p.relative(path, from: systemTempPath);
  if (p.isAbsolute(rel) || rel.startsWith('..')) return false;
  final firstSegment = p.split(rel).first;
  return firstSegment.startsWith(kThroughputGuardWorktreePrefix);
}
