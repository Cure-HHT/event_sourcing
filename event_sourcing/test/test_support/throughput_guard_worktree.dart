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
/// ever swept as "stale"; every other worktree registration is left alone.
const kThroughputGuardWorktreePrefix = 'evs-throughput-guard-';

/// Reports whether the process identified by [pid] is still running.
/// Injectable so a test can fake liveness deterministically; the default
/// implementation shells out to `kill -0` (POSIX) or `tasklist` (Windows)
/// and, when that check itself cannot run, answers `true` so an unreadable
/// answer never causes a live guard's worktree to be swept.
typedef ProcessLivenessChecker = bool Function(int pid);

bool _defaultIsProcessAlive(int ownerPid) {
  try {
    if (Platform.isWindows) {
      final result = Process.runSync('tasklist', [
        '/FI',
        'PID eq $ownerPid',
        '/NH',
      ]);
      return result.exitCode == 0 &&
          (result.stdout as String).contains('$ownerPid');
    }
    final result = Process.runSync('kill', ['-0', '$ownerPid']);
    return result.exitCode == 0;
  } catch (_) {
    return true;
  }
}

/// The lock reason a guard's own worktree is added with, naming the owning
/// process so a concurrently running guard's sweep can tell this worktree
/// is still live and skip it.
String _lockReasonFor(int ownerPid) => 'evs-throughput-guard pid=$ownerPid';

final _lockReasonPidPattern = RegExp(r'^evs-throughput-guard pid=(\d+)$');

int? _pidFromLockReason(String? reason) {
  if (reason == null) return null;
  final match = _lockReasonPidPattern.firstMatch(reason);
  if (match == null) return null;
  return int.tryParse(match.group(1)!);
}

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
/// directory named with [kThroughputGuardWorktreePrefix] and whose owning
/// process (recorded in the worktree's lock reason when it was added) is no
/// longer running, or that was never locked at all. Each worktree this
/// helper adds is locked with a reason naming its own process id, so a
/// concurrently running guard's still-live worktree is recognised and left
/// alone; only a registration whose owner has died, or an unlocked leftover
/// from before this liveness check existed, is swept. No registration
/// outside [kThroughputGuardWorktreePrefix] is ever touched.
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
  ProcessLivenessChecker isProcessAlive = _defaultIsProcessAlive,
}) async {
  final systemTempPath = Directory.systemTemp.path;
  await _removeStaleRegistrations(
    repoRoot: repoRoot,
    runGit: runGit,
    systemTempPath: systemTempPath,
    isProcessAlive: isProcessAlive,
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
      '--lock',
      '--reason',
      _lockReasonFor(pid),
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
      // Unlock before removing: the worktree was added locked (so a
      // concurrent guard's sweep leaves it alone while live), and
      // `worktree remove --force` alone refuses a locked worktree. Best
      // effort — an unlock failure does not stop the removal attempt.
      try {
        await runGit([
          'worktree',
          'unlock',
          worktreePath,
        ], workingDirectory: repoRoot);
      } catch (_) {
        // Ignored: the removal below reports the failure that matters.
      }
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
      if (_parseWorktreeEntries(
        listing,
      ).any((entry) => entry.path == worktreePath)) {
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
  required ProcessLivenessChecker isProcessAlive,
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
  for (final entry in _parseWorktreeEntries(listing)) {
    if (!_isOwnStaleWorktree(entry.path, systemTempPath: systemTempPath)) {
      continue;
    }
    final ownerPid = _pidFromLockReason(entry.lockReason);
    if (ownerPid != null && isProcessAlive(ownerPid)) {
      // A running guard still owns this worktree; it is not stale.
      continue;
    }
    if (entry.lockReason != null) {
      try {
        await runGit([
          'worktree',
          'unlock',
          entry.path,
        ], workingDirectory: repoRoot);
      } catch (_) {
        // Ignored: the removal below reports the failure that matters.
      }
    }
    try {
      await runGit([
        'worktree',
        'remove',
        '--force',
        entry.path,
      ], workingDirectory: repoRoot);
    } catch (e) {
      onCleanupFailure?.call(
        'removing stale worktree ${entry.path} failed: $e',
      );
    }
  }
}

/// One block of `git worktree list --porcelain` output: a worktree's path
/// and, when the worktree is locked, its lock reason (empty string when
/// locked with no reason recorded).
class _WorktreeEntry {
  _WorktreeEntry(this.path, this.lockReason);
  final String path;
  final String? lockReason;
}

/// Parses `git worktree list --porcelain` output into one entry per
/// worktree block (blocks are separated by blank lines).
List<_WorktreeEntry> _parseWorktreeEntries(String porcelain) {
  final entries = <_WorktreeEntry>[];
  String? path;
  String? lockReason;
  void flush() {
    if (path != null) entries.add(_WorktreeEntry(path!, lockReason));
    path = null;
    lockReason = null;
  }

  for (final line in porcelain.split('\n')) {
    if (line.isEmpty) {
      flush();
      continue;
    }
    if (line.startsWith('worktree ')) {
      flush();
      path = line.substring('worktree '.length).trim();
    } else if (line == 'locked') {
      lockReason = '';
    } else if (line.startsWith('locked ')) {
      lockReason = line.substring('locked '.length);
    }
  }
  flush();
  return entries;
}

/// True when [path] lies under [systemTempPath] inside a first-level
/// directory named with [kThroughputGuardWorktreePrefix] — i.e. a temp
/// directory this helper itself created on a prior run.
bool _isOwnStaleWorktree(String path, {required String systemTempPath}) {
  final rel = p.relative(path, from: systemTempPath);
  if (p.isAbsolute(rel) || rel.startsWith('..')) return false;
  final firstSegment = p.split(rel).first;
  return firstSegment.startsWith(kThroughputGuardWorktreePrefix);
}
