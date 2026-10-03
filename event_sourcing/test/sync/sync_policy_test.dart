import 'dart:math';

import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SyncPolicy.defaults constants', () {
    test('defaults.initialBackoff == Duration(seconds: 60)', () {
      expect(SyncPolicy.defaults.initialBackoff, const Duration(seconds: 60));
    });

    test('defaults.backoffMultiplier == 5.0', () {
      expect(SyncPolicy.defaults.backoffMultiplier, 5.0);
    });

    test('defaults.maxBackoff == Duration(hours: 2)', () {
      expect(SyncPolicy.defaults.maxBackoff, const Duration(hours: 2));
    });

    test('defaults.jitterFraction == 0.1', () {
      expect(SyncPolicy.defaults.jitterFraction, 0.1);
    });

    test('defaults.maxAttempts == 20', () {
      expect(SyncPolicy.defaults.maxAttempts, 20);
    });
  });

  group('SyncPolicy is a value class', () {
    // instantiated with custom field values.
    test('const constructor accepts all fields and exposes them', () {
      const custom = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(minutes: 30),
        jitterFraction: 0.2,
        maxAttempts: 7,
      );
      expect(custom.initialBackoff, const Duration(seconds: 10));
      expect(custom.backoffMultiplier, 2.0);
      expect(custom.maxBackoff, const Duration(minutes: 30));
      expect(custom.jitterFraction, 0.2);
      expect(custom.maxAttempts, 7);
    });

    test('SyncPolicy.defaults is a static const instance', () {
      const ref = SyncPolicy.defaults;
      expect(ref, isA<SyncPolicy>());
      // Identity is preserved for a static const — same reference each access.
      expect(identical(SyncPolicy.defaults, ref), isTrue);
    });

    test('defaults field values', () {
      expect(SyncPolicy.defaults.initialBackoff, const Duration(seconds: 60));
      expect(SyncPolicy.defaults.backoffMultiplier, 5.0);
      expect(SyncPolicy.defaults.maxBackoff, const Duration(hours: 2));
      expect(SyncPolicy.defaults.jitterFraction, 0.1);
      expect(SyncPolicy.defaults.maxAttempts, 20);
    });
  });

  group('SyncPolicy.backoffFor (instance method)', () {
    // Deterministic fixed-seed Random produces reproducible jitter.
    Random seeded() => Random(42);

    Duration within10Percent(Duration base) =>
        Duration(milliseconds: (base.inMilliseconds * 0.1).round());

    void expectWithinJitter(
      Duration actual,
      Duration expectedBase, {
      String? reason,
    }) {
      final tolerance = within10Percent(expectedBase);
      final low = expectedBase - tolerance;
      final high = expectedBase + tolerance;
      expect(
        actual >= low && actual <= high,
        isTrue,
        reason:
            reason ??
            'expected $actual in [$low, $high] (±10% of $expectedBase)',
      );
    }

    // backoffFor(0) ≈ 60s ± 10%.
    test('backoffFor(0) ≈ 60s ± 10% jitter', () {
      final d = SyncPolicy.defaults.backoffFor(0, random: seeded());
      expectWithinJitter(d, const Duration(seconds: 60));
    });

    // backoffFor(1) ≈ 300s (60*5) ± 10%.
    test('backoffFor(1) ≈ 300s (60*5) ± 10%', () {
      final d = SyncPolicy.defaults.backoffFor(1, random: seeded());
      expectWithinJitter(d, const Duration(seconds: 300));
    });

    // backoffFor(2) ≈ 1500s (5m*5 = 25m) ± 10%.
    test('backoffFor(2) ≈ 1500s (60*5*5) ± 10%', () {
      final d = SyncPolicy.defaults.backoffFor(2, random: seeded());
      expectWithinJitter(d, const Duration(seconds: 1500));
    });

    // backoffFor(3) caps at 2h (raw would be 7500s > 7200s cap).
    test('backoffFor(3) ≈ capped at 7200s (2h) ± 10%', () {
      final d = SyncPolicy.defaults.backoffFor(3, random: seeded());
      expectWithinJitter(d, const Duration(hours: 2));
    });

    // backoffFor(n) for large n stays at the cap (± 10%).
    test('backoffFor(n) stays at cap for large n', () {
      for (final n in [3, 5, 10, 19, 20]) {
        final d = SyncPolicy.defaults.backoffFor(n, random: seeded());
        expectWithinJitter(
          d,
          const Duration(hours: 2),
          reason: 'backoffFor($n) should be at the 2h cap ± 10%; got $d',
        );
      }
    });

    // Jitter is deterministic when a seed is supplied.
    test('same seed produces the same jitter', () {
      final a = SyncPolicy.defaults.backoffFor(2, random: Random(7));
      final b = SyncPolicy.defaults.backoffFor(2, random: Random(7));
      expect(a, b);
    });

    // Jitter is actually applied (not identically zero). With 200 draws,
    // at least some should differ from the base by a non-trivial amount.
    test('jitter is actually applied (values vary across random seeds)', () {
      final values = <int>{};
      for (var i = 0; i < 200; i++) {
        final d = SyncPolicy.defaults.backoffFor(0, random: Random(i));
        values.add(d.inMilliseconds);
      }
      // If jitter were zero, every seed would produce the same value.
      expect(values.length, greaterThan(1));
    });

    // Jitter stays within ±10% — no draw exceeds those bounds, across many
    // seeds.
    test('jitter draws stay within ±jitterFraction bound', () {
      for (var i = 0; i < 500; i++) {
        final d = SyncPolicy.defaults.backoffFor(1, random: Random(i));
        expectWithinJitter(
          d,
          const Duration(seconds: 300),
          reason: 'seed=$i, got $d',
        );
      }
    });

    // Default (no seed) still returns a plausible value.
    test(
      'backoffFor without a seed returns a value within the jitter range',
      () {
        final d = SyncPolicy.defaults.backoffFor(0);
        expectWithinJitter(d, const Duration(seconds: 60));
      },
    );

    // Negative attemptCount is a caller bug; reject it rather than return
    // a degenerate near-zero backoff.
    test('backoffFor rejects negative attemptCount', () {
      expect(
        () => SyncPolicy.defaults.backoffFor(-1, random: seeded()),
        throwsArgumentError,
      );
    });

    // curve from its own instance fields, not from the defaults.
    test('custom policy uses its own fields for backoffFor', () {
      const fast = SyncPolicy(
        initialBackoff: Duration(seconds: 1),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(seconds: 10),
        jitterFraction: 0.0, // disable jitter for a deterministic check
        maxAttempts: 5,
      );
      expect(fast.backoffFor(0), const Duration(seconds: 1));
      expect(fast.backoffFor(1), const Duration(seconds: 2));
      expect(fast.backoffFor(2), const Duration(seconds: 4));
      expect(fast.backoffFor(3), const Duration(seconds: 8));
      // Capped:
      expect(fast.backoffFor(4), const Duration(seconds: 10));
      expect(fast.backoffFor(10), const Duration(seconds: 10));
    });

    // The constructor accepts a budget below one without throwing; refusal
    // is the delivery cycle's job (checkRetryBudget), not the value
    // class's, so the same field can be read back before it reaches a
    // cycle.
    test('a budget below one does not throw at construction', () {
      for (final n in <int>[0, -1]) {
        final policy = SyncPolicy(
          initialBackoff: Duration.zero,
          backoffMultiplier: 1.0,
          maxBackoff: Duration.zero,
          jitterFraction: 0.0,
          maxAttempts: n,
        );
        expect(policy.maxAttempts, n);
      }
    });
  });

  group('SyncPolicy retry time bound', () {
    // Verifies: EVS-PRD-destinations/W
    // the policy states its budget as time
    //   as well as attempts, and defaults gives it a documented value.
    test('defaults.maxRetryTime == Duration(hours: 24)', () {
      expect(SyncPolicy.defaults.maxRetryTime, const Duration(hours: 24));
    });

    // Verifies: EVS-PRD-destinations/W
    // an omitted maxRetryTime takes the
    //   defaults' value, so a construction site that names no time bound
    //   keeps compiling and keeps the default budget.
    test('maxRetryTime defaults when omitted from a custom policy', () {
      const custom = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(minutes: 30),
        jitterFraction: 0.2,
        maxAttempts: 7,
      );
      expect(custom.maxRetryTime, SyncPolicy.defaults.maxRetryTime);
    });

    // longestDelayAfter(k) is the longest delay the curve allows after the
    // k-th attempt: the capped baseline with the jitter counted in full,
    // per the retry-budget Rationale. No assertion covers this helper yet;
    // it is exercised by the time retry budget's own assertion once that
    // lands.
    test('longestDelayAfter(0) is initialBackoff with full jitter', () {
      const policy = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(seconds: 1000),
        jitterFraction: 0.25,
        maxAttempts: 5,
      );
      expect(policy.longestDelayAfter(0), const Duration(milliseconds: 12500));
    });

    test('longestDelayAfter(k) matches the curve below the cap', () {
      const policy = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(seconds: 1000),
        jitterFraction: 0.25,
        maxAttempts: 5,
      );
      // baseline at k=2: 10 * 2^2 = 40s; with full +25% jitter: 50s.
      expect(policy.longestDelayAfter(2), const Duration(seconds: 50));
    });

    test('longestDelayAfter(k) is capped at maxBackoff plus full jitter', () {
      const policy = SyncPolicy(
        initialBackoff: Duration(seconds: 10),
        backoffMultiplier: 2.0,
        maxBackoff: Duration(seconds: 100),
        jitterFraction: 0.25,
        maxAttempts: 20,
      );
      // baseline at k=10 would far exceed the 100s cap, so it clamps to
      // 100s, then the full +25% jitter: 125s.
      for (final k in [10, 15, 19]) {
        expect(policy.longestDelayAfter(k), const Duration(seconds: 125));
      }
    });

    test('SyncPolicy.defaults.longestDelayAfter matches its own curve', () {
      // defaults: 60s initial, x5 multiplier, 2h cap, 10% jitter.
      expect(
        SyncPolicy.defaults.longestDelayAfter(0),
        const Duration(milliseconds: 66000),
      );
      expect(
        SyncPolicy.defaults.longestDelayAfter(3),
        const Duration(milliseconds: 7920000), // 2h cap * 1.1
      );
    });
  });
}
