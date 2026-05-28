// Implements: EVS-PRD-reaction-widget-contract/E — idempotency-key
// generator the widget library uses to mint UUID v4 keys (with the
// caching-during-Submitting and reset-after-terminal-state policy
// applied at the widget layer, not in this generator). Default impl
// is Uuid4IdempotencyKeyGenerator; the abstract interface admits
// deterministic stubs for tests.
import 'package:meta/meta.dart';
import 'package:uuid/uuid.dart';

/// Generates idempotency keys for action submissions. Defaults to UUID
/// v4 ([Uuid4IdempotencyKeyGenerator]).
///
/// `ActionBuilder` in `reaction_widgets` caches a generated
/// key for the lifetime of an in-flight submission and resets on
/// terminal state — so retries during `Submitting` reuse the same key
/// (idempotent dedupe), but a new press after Success/Denied/Failed
/// gets a fresh key (new logical action).
///
/// Defining this as an interface lets tests inject a deterministic
/// stub for reproducible assertions.
abstract interface class IdempotencyKeyGenerator {
  String generate();
}

/// Default impl: generates random UUID v4 strings using the `uuid`
/// package.
class Uuid4IdempotencyKeyGenerator implements IdempotencyKeyGenerator {
  final Uuid _uuid;

  Uuid4IdempotencyKeyGenerator() : _uuid = const Uuid();

  /// Injection point for tests that want a deterministic UUID source.
  @visibleForTesting
  Uuid4IdempotencyKeyGenerator.withUuid(this._uuid);

  @override
  String generate() => _uuid.v4();
}
