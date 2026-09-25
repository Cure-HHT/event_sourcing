// Implements: EVS-DEV-postgres-backend/C
// Transaction handle SHALL be invalidated
// after the transaction() body returns or throws.
// Implements: EVS-DEV-storage-capability/F
// the handle's type and every member of it are private to the Postgres
//   backend's Dart library, so code outside it holds an opaque handle and
//   reaches no session through it.

part of 'postgres_backend.dart';

/// Postgres-backed [Transaction] handle. Holds a `package:postgres`
/// [TxSession] for the duration of the surrounding `transaction()` body.
/// After the body returns (or throws) the handle is invalidated; reading
/// its session on an invalid handle throws [StateError] so accidental
/// escape surfaces loudly instead of silently writing against a closed
/// transaction.
class _PostgresTxn extends Transaction {
  _PostgresTxn(this._txSession, {required PostgresBackend owner})
    : _owner = owner;

  final TxSession _txSession;

  /// The backend whose `transaction()` produced this handle. A backend
  /// refuses a handle another backend instance produced.
  final PostgresBackend _owner;
  bool _valid = true;

  /// Set when the body issued a write to the `backend_state` table (the
  /// sequence counter of every append among them), so a re-run after a
  /// serialization failure knows whether it must wait behind that table's
  /// writers.
  bool _wroteBackendState = false;

  /// The underlying postgres session. Throws [StateError] when read
  /// outside the `transaction()` body that produced this handle.
  TxSession get _session {
    if (!_valid) {
      throw StateError('Transaction used outside its transaction() body');
    }
    return _txSession;
  }

  /// Mark this handle as invalid. Called by `PostgresBackend.transaction`
  /// after the body completes (success or failure).
  void _invalidate() {
    _valid = false;
  }
}
