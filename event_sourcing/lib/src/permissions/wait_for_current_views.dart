// lib/src/permissions/wait_for_current_views.dart
// Shared wait loop for an operation that waits until one or more views are
// current for the instance before it reads them, or before it reports
// itself done: the permission bootstrap and permission-seed operations,
// and rebuildView. Each throws a typed, deadline-bounded error naming what
// is still converging otherwise.
// Implements: EVS-DEV-converging-view-reads/I
// waitForViewsCurrent polls EventStore.reader.viewProgress(), returning
//   once every named view is current, and throws ViewConvergenceTimeout,
//   naming each view still converging and its copy's progress, once the
//   caller-supplied deadline passes first.
// Implements: EVS-DEV-view-convergence/V
// rebuildView reaches the same wait loop and the same typed error, naming
//   the view it just replaced the copy of and that copy's progress.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart' show internal;

/// How often [waitForViewsCurrent] re-polls the reader's view progress
/// while a named view is still converging.
const Duration _kPollInterval = Duration(milliseconds: 20);

/// Waits until every view named in [viewNames] is current for [eventStore]'s
/// instance, polling its reader's view progress. Returns as soon
/// as none of them is converging -- immediately when none ever was.
///
/// Throws [ViewConvergenceTimeout], naming every view of [viewNames] still
/// converging and its copy's progress, once [deadline] has passed and at
/// least one of them still is. A [deadline] already passed when this is
/// called throws after one check, without waiting.
///
/// Shared internal helper: reached from `bootstrapRoleAssignments`,
/// `PermissionSeedApplier.apply` and `rebuildView`, not a library operation
/// of its own.
@internal
Future<void> waitForViewsCurrent(
  EventStore eventStore,
  Set<String> viewNames,
  DateTime deadline,
) async {
  while (true) {
    final statuses = await eventStore.reader.viewProgress();
    final converging = [
      for (final status in statuses)
        if (viewNames.contains(status.viewName) &&
            status.state == ViewConvergenceState.converging)
          status,
    ];
    if (converging.isEmpty) return;
    if (!DateTime.now().isBefore(deadline)) {
      throw ViewConvergenceTimeout(converging);
    }
    await Future<void>.delayed(_kPollInterval);
  }
}
