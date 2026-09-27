// The severe-log-to-standard-error sink on the native runtimes: writes each
// line to the process's standard error. The counterpart of
// logging_sink_web.dart, selected by a conditional import in logging.dart.
import 'dart:io' as io;

import 'package:meta/meta.dart' show internal;

/// Writes [line] to the process's standard error.
@internal
void writeSevereLogLine(String line) => io.stderr.writeln(line);
