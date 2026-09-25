/// The database handle cannot commit: the storage ran a transaction body
/// [runs] times while every other writer of the database was held back,
/// and no run committed. Nothing the runs wrote was committed.
///
/// On the web, sembast re-runs a transaction body when its commit finds
/// that another tab committed first. A transaction that loses a few runs
/// that way runs again holding the database's write lock exclusively, so
/// no other tab's write can come between: there a handle that has seen
/// every commit commits by its second run. Contention between tabs
/// therefore never raises this exception. A handle that still fails is one
/// that another opener of the database compacted past the commits the
/// handle had seen: such a handle fails every later commit, whoever else
/// writes. Every later transaction on the handle fails with this exception
/// at once, and a delivery cycle over it stops, releasing the drain lock to
/// another tab. The application closes the database and opens it again.
class TransactionRerunLimitException implements Exception {
  const TransactionRerunLimitException(this.runs);

  /// The most runs of one transaction body while every other writer is held
  /// back: one on data that predates the hold, one on fresh data, and two
  /// for writers of the database that do not take the library's write lock.
  static const int maxRuns = 4;

  /// The runs made while every other writer was held back.
  final int runs;

  @override
  String toString() =>
      'TransactionRerunLimitException: the database handle cannot commit. '
      'The storage ran the transaction body $runs times while every other '
      'writer of the database was held back, and nothing was written: '
      'another opener of the database (another tab) compacted it past the '
      'commits this handle had seen. Every later transaction on this handle '
      'fails the same way. Close the database and open it again.';
}
