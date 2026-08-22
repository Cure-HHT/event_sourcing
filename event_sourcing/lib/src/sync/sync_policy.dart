// Implements: EVS-PRD-destinations/C
// (FIFO order — SyncPolicy governs the
//   retry/backoff curve that controls when drain re-attempts a queued entry;
//   the maxAttempts cap and backoff prevent indefinite blocking on a bad head
//   while preserving strict-FIFO ordering within each drain pass)
import 'dart:math';

/// Retry-curve and timing settings for the per-destination FIFO drain.
///
/// `SyncPolicy` is a value class: all fields are `final`, the constructor is
/// `const`, and [SyncPolicy.defaults] is a `static const` instance whose
/// field values equal the  constants (60s initial backoff, 5.0
/// multiplier, 2h cap, ±10% jitter, 20-attempt lifetime cap, 15-minute
/// foreground cadence).
///
/// Tests that need a different schedule construct their own `const
/// SyncPolicy(...)` and pass it into `drain()` or `SyncCycle`. Production
/// code passes `null` (or omits the parameter); the drain loop falls back
/// to `SyncPolicy.defaults`. Curve changes in production still require a
/// spec amendment and a coordinated app release — the injectability is a
/// test-affordance only, not a runtime tuning knob.
///
/// Curve shape: `initialBackoff * backoffMultiplier^attemptCount`, capped
/// at `maxBackoff`, shaken by `±jitterFraction` multiplicative jitter to
/// avoid synchronized retry storms.
class SyncPolicy {
  const SyncPolicy({
    required this.initialBackoff,
    required this.backoffMultiplier,
    required this.maxBackoff,
    required this.jitterFraction,
    required this.maxAttempts,
    required this.periodicInterval,
  });

  /// First retry backoff.
  final Duration initialBackoff;

  /// Per-attempt multiplier.
  final double backoffMultiplier;

  /// Cap on the computed backoff.
  final Duration maxBackoff;

  /// Fraction of the base backoff applied as uniform ±jitter.
  final double jitterFraction;

  /// Per-entry lifetime attempt cap.
  final int maxAttempts;

  /// Foreground sync-cycle cadence.
  final Duration periodicInterval;

  /// The production policy: 60s / 5.0 / 2h / 0.1 / 20 / 15min, matching
  /// its constants exactly. Call sites that do not inject a
  /// custom policy resolve to this instance.
  // whose field values equal the  constants.
  static const SyncPolicy defaults = SyncPolicy(
    initialBackoff: Duration(seconds: 60),
    backoffMultiplier: 5.0,
    maxBackoff: Duration(hours: 2),
    jitterFraction: 0.1,
    maxAttempts: 20,
    periodicInterval: Duration(minutes: 15),
  );

  /// Returns the backoff for the [attemptCount]-th attempt (0-based).
  ///
  /// Formula: `baseline = min(initialBackoff * multiplier^n, maxBackoff)`,
  /// then apply `±jitterFraction` multiplicative jitter:
  ///
  ///     backoff = baseline * (1 + uniform(-jitterFraction, jitterFraction))
  ///
  /// Pass a [random] for deterministic jitter in tests; production passes
  /// `null` (a process-wide default `Random` is used).
  // custom policy yields a custom curve.
  Duration backoffFor(int attemptCount, {Random? random}) {
    if (attemptCount < 0) {
      throw ArgumentError.value(
        attemptCount,
        'attemptCount',
        'attemptCount must be non-negative',
      );
    }
    final baselineMs =
        initialBackoff.inMilliseconds * pow(backoffMultiplier, attemptCount);
    final capMs = maxBackoff.inMilliseconds.toDouble();
    final clampedMs = baselineMs > capMs ? capMs : baselineMs;
    final r = random ?? _defaultRandom;
    // uniform in (-jitterFraction, +jitterFraction)
    final jitter = (r.nextDouble() * 2 - 1) * jitterFraction;
    final ms = (clampedMs * (1 + jitter)).round();
    return Duration(milliseconds: ms);
  }

  static final Random _defaultRandom = Random();
}
