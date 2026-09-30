// The severe-log-to-standard-error sink in the browser: there is no
// process-level standard error stream on the web, so the line reaches the
// browser's console through `print`, which every browser routes there and
// which needs no browser-only import, keeping this file (unlike
// lib/src/storage/web_locks.dart) plain Dart. The counterpart of
// logging_sink_io.dart, selected by a conditional import in logging.dart.
import 'package:meta/meta.dart' show internal;

/// Writes [line] to the browser console.
@internal
void writeSevereLogLine(String line) => print(line); // ignore: avoid_print
