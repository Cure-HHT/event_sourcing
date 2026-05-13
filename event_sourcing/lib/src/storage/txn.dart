/// Lexical-scope handle for an atomic write sequence inside a
/// `StorageBackend.transaction` body.
///
/// `Txn` is abstract; concrete backends subclass it with their own
/// transactional state (e.g., a Sembast `Transaction` for `SembastBackend`).
/// Callers never instantiate `Txn` directly — they receive it as the
/// argument to `transaction(body)` and MUST NOT use it after that body
/// returns or throws.
// Implements: EVS-PRD-portability/D — platform-divergent transaction handle
//   abstracted as a pure-Dart type; concrete backends subclass per platform.
// Implements: EVS-PRD-event-log/A — used by appendEvent to guarantee append-
//   only atomic writes under a single backend transaction.
// its defining transaction() body. Concrete backends are responsible for
// enforcing that constraint and raising an error if the handle escapes.
abstract class Txn {
  const Txn();
}
