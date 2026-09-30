import 'package:event_sourcing/event_sourcing.dart';

// Short backoff (1s initial, 10s ceiling, no jitter, 1M attempts, a 30-day
// time bound) makes retry cadence observable live on a reviewer's desktop
// without them waiting minutes between attempts, and without the time
// bound wedging a run long before the attempt bound would. Production's
// SyncPolicy.defaults (60s initial, 2h ceiling, 0.1 jitter, 20 attempts,
// a 24h time bound) are unchanged.
const SyncPolicy demoDefaultSyncPolicy = SyncPolicy(
  initialBackoff: Duration(seconds: 1),
  backoffMultiplier: 1.0,
  maxBackoff: Duration(seconds: 10),
  jitterFraction: 0.0,
  maxAttempts: 1000000,
  maxRetryTime: Duration(days: 30),
);
