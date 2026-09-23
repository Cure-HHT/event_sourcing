// Test support: a SembastBackend that runs every transaction body twice,
// rolling the first run back and committing the second, as Postgres does
// after a serialization conflict and sembast_web does after another tab
// commits first. This file declares no tests, so it carries no citation.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

class _RerunSignal implements Exception {
  const _RerunSignal();
}

/// A [SembastBackend] whose every transaction runs its body twice: the first
/// run is rolled back after the body returns, the second commits.
///
/// Set [rerunEnabled] to false to run bodies once (for setup work such as
/// opening an `EventStore`). [bodyRuns] counts every run of every body.
class RerunningSembastBackend extends SembastBackend {
  RerunningSembastBackend({required super.database});

  /// Opens a fresh in-memory database under a unique name.
  static Future<RerunningSembastBackend> openInMemory(String prefix) async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      '$prefix-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    return RerunningSembastBackend(database: db);
  }

  bool rerunEnabled = true;
  int bodyRuns = 0;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) async {
    if (rerunEnabled) {
      try {
        await super.transaction<T>((txn) async {
          bodyRuns += 1;
          await body(txn);
          throw const _RerunSignal();
        });
      } on _RerunSignal {
        // The first run rolled back; run the body again, as a backend does
        // after a transient conflict.
      }
    }
    return super.transaction<T>((txn) async {
      bodyRuns += 1;
      return body(txn);
    });
  }
}
