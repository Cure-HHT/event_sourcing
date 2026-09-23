import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';

/// Whether [error], thrown while a pane opened its database, means the
/// database file was written by a build this one does not open: an earlier
/// data format, or a build of another data-format major.
bool needsDatabaseReset(Object error) =>
    error is DatabaseResetRequiredError || error is DataFormatIncompatibleError;

/// The message the demo shows instead of its panes when a database file
/// does not open under this build. A database an earlier build wrote
/// ([DatabaseResetRequiredError]) must be reset, so the message names the
/// files to delete. A database of another data-format major
/// ([DataFormatIncompatibleError]) is not deleted: it opens under a build
/// of its own data-format major, or is restored from a backup.
String databaseResetMessage(Object error, List<String> dbPaths) {
  final files = dbPaths.map((path) => '  $path').join('\n');
  if (error is DataFormatIncompatibleError) {
    return 'The demo database was written by a library build of another '
        'data-format major, which this build does not open, so the demo '
        'cannot start.\n\n'
        'Open it with a build of the data-format major it records, or '
        'restore a backup this build opens. The database files:\n$files\n\n'
        'Reason: $error';
  }
  return 'The demo database was written by an earlier library build, which '
      'this build does not open, so the demo cannot start.\n\n'
      'Delete these files and start the demo again:\n$files\n\n'
      'Reason: $error';
}

/// The app the demo runs instead of its panes when a database file does
/// not open under this build.
class DatabaseResetRequiredApp extends StatelessWidget {
  const DatabaseResetRequiredApp({required this.message, super.key});

  /// The message to show, from [databaseResetMessage].
  final String message;

  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'event_sourcing demo',
    home: Scaffold(
      body: Padding(
        padding: const EdgeInsets.all(24),
        child: SingleChildScrollView(
          child: SelectableText(
            message,
            key: const Key('database-reset-message'),
          ),
        ),
      ),
    ),
  );
}
