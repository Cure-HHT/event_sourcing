// Implements: EVS-DEV-postgres-backend/C
// Transaction handle SHALL be invalidated
// after the transaction() body returns or throws.

import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// Postgres-backed [Transaction] handle. Holds a `package:postgres` [TxSession]
/// for the duration of the surrounding `transaction()` body. After the
/// body returns (or throws) the handle is invalidated; calling [session]
/// on an invalid handle throws [StateError] so accidental escape
/// surfaces loudly instead of silently writing against a closed
/// transaction.
class PostgresTxn extends Transaction {
  PostgresTxn(this._session);

  final TxSession _session;
  bool _valid = true;

  /// Set when the body issued a write to the `backend_state` table (the
  /// sequence counter of every append among them), so a re-run after a
  /// serialization failure knows whether it must wait behind that table's
  /// writers.
  @internal
  bool wroteBackendState = false;

  /// The underlying postgres session. Throws [StateError] when accessed
  /// outside the `transaction()` body that produced this handle.
  @internal
  TxSession get session {
    if (!_valid) {
      throw StateError('Transaction used outside its transaction() body');
    }
    return _session;
  }

  /// Mark this handle as invalid. Called by `PostgresBackend.transaction`
  /// after the body completes (success or failure).
  @internal
  void invalidate() {
    _valid = false;
  }
}
