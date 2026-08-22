/// Lexical-scope handle for an atomic write sequence inside a
/// `StorageBackend.transaction` body.
///
/// `Transaction` is abstract; concrete backends subclass it with their own
/// transactional state (e.g., a Sembast `Transaction` for `SembastBackend`).
/// Callers never instantiate `Transaction` directly — they receive it as the
/// argument to `transaction(body)` and MUST NOT use it after that body
/// returns or throws.
// Implements: EVS-PRD-portability/D
// platform-divergent transaction handle
//   abstracted as a pure-Dart type; concrete backends subclass per platform.
// Implements: EVS-PRD-event-log/A
// used by appendEvent to guarantee append-
//   only atomic writes under a single backend transaction.
// Concrete backends are responsible for enforcing that the handle is not used
// outside its defining transaction() body and raising an error if it escapes.
abstract class Transaction {
  const Transaction();
}
