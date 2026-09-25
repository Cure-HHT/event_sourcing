// Starts a delivery cycle that is, or is not, hand-driven.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// Runs [begin] (a `SyncCycle.start`) so that the cycle it starts is
/// hand-driven exactly when [handDriven] is true: a wake then runs no pass
/// of it, and only the test's calls, its cadence and its lock requests do
/// (the `handDrivenCycle` seam).
///
/// With no seams installed, a hand-driven start installs that seam alone.
/// Under installed seams it installs nothing, since a nested installation
/// would hide the installed seams from the cycle's timers, and instead
/// requires the installed seams to say the same; otherwise it throws
/// [StateError] before starting.
Future<SyncCycle> startCycle(
  Future<SyncCycle> Function() begin, {
  required bool handDriven,
}) {
  final installed = DeliveryTestHooks.current;
  if (installed == null) {
    return handDriven
        ? runWithDeliveryTestHooks(
            const DeliveryTestHooks(handDrivenCycle: true),
            begin,
          )
        : begin();
  }
  if (installed.handDrivenCycle != handDriven) {
    throw StateError(
      'the installed seams set handDrivenCycle: '
      '${installed.handDrivenCycle}, and this start asks for $handDriven; '
      'set handDrivenCycle on the installed seams',
    );
  }
  return begin();
}
