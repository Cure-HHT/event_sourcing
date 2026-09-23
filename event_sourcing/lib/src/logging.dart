// Implements: EVS-DEV-destination-drain-lock/F
// every library log line passes through one
//   internal logger; the test seams observe it only when assertions are on,
//   and an observer that throws cannot change what the library does.
import 'dart:developer' as developer;

import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:logging/logging.dart' as logging;
import 'package:meta/meta.dart';

/// Severity of a library log line, on the `dart:developer` level scale
/// (the same scale `package:logging` uses).
@internal
enum LibraryLogLevel {
  /// Routine progress detail.
  fine(500, logging.Level.FINE),

  /// A notable state change.
  info(800, logging.Level.INFO),

  /// A condition an operator may need to look at.
  warning(900, logging.Level.WARNING),

  /// A failure the library caught and survived.
  severe(1000, logging.Level.SEVERE);

  const LibraryLogLevel(this.value, this.loggingLevel);

  /// The `dart:developer` level.
  final int value;

  /// The matching `package:logging` level.
  final logging.Level loggingLevel;
}

/// One line the library logged.
@internal
@immutable
class LibraryLogRecord {
  const LibraryLogRecord({
    required this.name,
    required this.level,
    required this.message,
    this.error,
    this.stackTrace,
  });

  /// Logger name, `event_sourcing.<component>`.
  final String name;
  final LibraryLogLevel level;
  final String message;
  final Object? error;
  final StackTrace? stackTrace;

  @override
  String toString() =>
      '[$name ${level.name}] $message${error == null ? '' : ': $error'}';
}

/// Logs [message] under `event_sourcing.<component>` through
/// `dart:developer` (visible to a debugger or DevTools) and through the
/// `package:logging` logger of the same name, which an application routes
/// wherever it wants by listening to `Logger.root.onRecord` (a compiled
/// server with no VM service attached sees library lines only that way).
/// When seams are active, the line is also forwarded to the `onLog` test
/// seam; a seam that throws is reported through `dart:developer` and does
/// not change what the caller does next.
@internal
void libraryLog(
  String component,
  String message, {
  LibraryLogLevel level = LibraryLogLevel.info,
  Object? error,
  StackTrace? stackTrace,
}) {
  final record = LibraryLogRecord(
    name: 'event_sourcing.$component',
    level: level,
    message: message,
    error: error,
    stackTrace: stackTrace,
  );
  developer.log(
    message,
    name: record.name,
    level: level.value,
    error: error,
    stackTrace: stackTrace,
  );
  logging.Logger(
    record.name,
  ).log(level.loggingLevel, message, error, stackTrace);
  final onLog = DeliveryTestHooks.current?.onLog;
  if (onLog == null) return;
  try {
    onLog(record);
  } on Object catch (e, st) {
    developer.log(
      'the onLog test seam threw',
      name: 'event_sourcing.logging',
      level: LibraryLogLevel.severe.value,
      error: e,
      stackTrace: st,
    );
  }
}
