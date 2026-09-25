// Implements: EVS-DEV-version-compatibility/H
// outside the browser a Sembast database is used by one process, so its
//   registration with the generation guard holds nothing.
// The counterpart of web_locks.dart on runtimes without a browser lock
// manager.
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/isolate_drain_lock.dart';
import 'package:meta/meta.dart' show internal;

/// Registers [descriptor] for the database named [path]. Outside the
/// browser a Sembast database is used by one process: the registration
/// holds nothing.
@internal
Future<GenerationRegistration> registerBrowserGeneration({
  required String path,
  required GenerationDescriptor descriptor,
  required Duration bootLockWait,
}) async => const UnguardedGenerationRegistration();

/// The guard's locks the page holds for the database [path]; empty outside
/// the browser.
@internal
Future<List<({String name, String mode})>> heldBrowserLocks(
  String path,
) async => const [];

/// Runs [body]; outside the browser a Sembast database is used by one
/// process, whose transactions run one at a time.
@internal
Future<T> runHoldingBrowserWriteLock<T>(
  String path, {
  required bool exclusive,
  required Future<T> Function() body,
  Duration? timeout,
  Exception Function(String name, Duration timeout)? timeoutError,
}) => body();

/// Outside the browser a Sembast database's drain lock is the isolate
/// registry alone: there is no browser exclusion to obtain.
@internal
Future<DrainExclusionHold?> tryBrowserDrainExclusion({
  required String path,
  required String databaseId,
}) async => null;

/// Outside the browser there is no browser request for the drain lock; the
/// backend requests it through the isolate registry.
@internal
DrainLockRequest? requestBrowserDrainLock({
  required String path,
  required String databaseId,
  required Future<DrainLock> Function(DrainExclusionHold exclusion)
  acquireHolding,
  required Duration retryInterval,
  required Future<void> Function() wake,
  required Future<Exception> ended,
}) => null;
