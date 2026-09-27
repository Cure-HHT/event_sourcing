// Implements: EVS-PRD-destinations/C
// (FIFO order — SyncPolicy governs the
//   retry/backoff curve that controls when drain re-attempts a queued entry;
//   the maxAttempts cap and backoff prevent indefinite blocking on a bad head
//   while preserving strict-FIFO ordering within each drain pass)
import 'dart:math';

/// Consumer-supplied delivery configuration for the per-destination queue
/// drain: the retry curve (backoff between attempts) and the retry budget
/// that bounds how long a queue item may sit at the head of its queue while
/// its retryable failures are retried, stated both as a number of attempts
/// (`maxAttempts`) and as a span of time (`maxRetryTime`).
///
/// The application passes a policy to `SyncCycle` statically (`policy:`) or
/// resolves one per cycle (`policyResolver:`), so it may tune the curve and
/// the budget at run time; with neither, the drain uses
/// [SyncPolicy.defaults]. The library trusts the policy it is given: the
/// curve decides when the drain retries, and the budget decides when an
/// item whose attempts keep failing wedges. The budget in effect at each
/// wedge is recorded in the wedge event (`max_attempts`, `max_retry_ms`), so
/// every wedge decision is auditable from the log. A budget lowered below an
/// item's recorded attempts, or below the time its recorded attempts span,
/// wedges that item at the next pass, without a further send.
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
    this.maxRetryTime = const Duration(hours: 24),
  });

  /// First retry backoff.
  final Duration initialBackoff;

  /// Per-attempt multiplier.
  final double backoffMultiplier;

  /// Cap on the computed backoff.
  final Duration maxBackoff;

  /// Fraction of the base backoff applied as uniform ±jitter.
  final double jitterFraction;

  /// The attempt-count half of the retry budget: the number of recorded
  /// attempts after which an undelivered queue item wedges. `SyncCycle`
  /// refuses a policy whose bound is below one (a static policy with an
  /// [ArgumentError], a resolved one by logging and skipping the pass), so
  /// every item is sent at least once before its budget can wedge it.
  final int maxAttempts;

  /// The time half of the retry budget: how long retryable failures may
  /// hold an item at the head of its queue before it wedges, measured as
  /// the sum of the gaps between its recorded attempts, each capped at the
  /// retry curve's longest allowed delay plus the delivery cycle's cadence
  /// (`EVS-DEV-destination-retry-budget/A`). `SyncCycle` refuses a policy
  /// whose bound is negative (a static policy with an [ArgumentError], a
  /// resolved one by logging and skipping the pass); a zero bound is
  /// accepted.
  final Duration maxRetryTime;

  /// The default policy: 60 s initial backoff, multiplier 5.0, 2 h cap,
  /// ±10% jitter, 20 attempts, a 24 h time bound. A cycle given no policy
  /// uses it.
  static const SyncPolicy defaults = SyncPolicy(
    initialBackoff: Duration(seconds: 60),
    backoffMultiplier: 5.0,
    maxBackoff: Duration(hours: 2),
    jitterFraction: 0.1,
    maxAttempts: 20,
    maxRetryTime: Duration(hours: 24),
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

  /// Returns the longest delay the retry curve allows after the
  /// [attemptCount]-th attempt: the capped baseline
  /// (`min(initialBackoff * backoffMultiplier^attemptCount, maxBackoff)`)
  /// with the full `+jitterFraction` jitter, never the sampled jitter
  /// [backoffFor] draws. Deterministic: two calls with the same
  /// [attemptCount] return the same value. Used to cap each gap counted
  /// toward the time retry budget (`EVS-DEV-destination-retry-budget/A`,
  /// `/B`): counting the full jitter never undercounts a gap the curve
  /// could actually have produced.
  Duration longestDelayAfter(int attemptCount) {
    final baselineMs =
        initialBackoff.inMilliseconds * pow(backoffMultiplier, attemptCount);
    final capMs = maxBackoff.inMilliseconds.toDouble();
    final clampedMs = baselineMs > capMs ? capMs : baselineMs;
    final ms = (clampedMs * (1 + jitterFraction)).round();
    return Duration(milliseconds: ms);
  }

  static final Random _defaultRandom = Random();
}
