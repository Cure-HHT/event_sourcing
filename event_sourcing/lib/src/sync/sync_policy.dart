// Implements: EVS-PRD-destinations/C
// (FIFO order — SyncPolicy governs the
//   retry/backoff curve that controls when drain re-attempts a queued entry;
//   the maxAttempts cap and backoff prevent indefinite blocking on a bad head
//   while preserving strict-FIFO ordering within each drain pass)
import 'dart:math';

/// Consumer-supplied delivery configuration for the per-destination queue
/// drain: the retry curve (backoff between attempts) and the attempt budget
/// (`maxAttempts`, the number of attempts after which an undelivered queue
/// item wedges).
///
/// The application passes a policy to `SyncCycle` statically (`policy:`) or
/// resolves one per cycle (`policyResolver:`), so it may tune the curve and
/// the budget at run time; with neither, the drain uses
/// [SyncPolicy.defaults]. The library trusts the policy it is given: the
/// curve decides when the drain retries, and the budget decides when an
/// item whose attempts keep failing wedges. The budget in effect at each
/// wedge is recorded in the wedge event (`max_attempts`), so every wedge
/// decision is auditable from the log. A budget lowered below an item's
/// recorded attempt count wedges that item at the next pass, without a
/// further send.
///
/// `SyncPolicy` is a value class: all fields are `final` and the
/// constructor is `const`.
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
  }) : assert(maxAttempts >= 1, 'the retry budget must be at least one');

  /// First retry backoff.
  final Duration initialBackoff;

  /// Per-attempt multiplier.
  final double backoffMultiplier;

  /// Cap on the computed backoff.
  final Duration maxBackoff;

  /// Fraction of the base backoff applied as uniform ±jitter.
  final double jitterFraction;

  /// The attempt budget: the number of recorded attempts after which an
  /// undelivered queue item wedges. At least one: `SyncCycle` refuses a
  /// policy with a lower budget (a static policy with an [ArgumentError], a
  /// resolved one by logging and skipping the cycle), so every item is sent
  /// at least once before its budget can wedge it.
  final int maxAttempts;

  /// The default policy: 60 s initial backoff, multiplier 5.0, 2 h cap,
  /// ±10% jitter, 20 attempts. A cycle given no policy uses it.
  static const SyncPolicy defaults = SyncPolicy(
    initialBackoff: Duration(seconds: 60),
    backoffMultiplier: 5.0,
    maxBackoff: Duration(hours: 2),
    jitterFraction: 0.1,
    maxAttempts: 20,
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
