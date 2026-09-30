// One library instance in a process of its own, for the Postgres generation
// guard test's process-exit case: it opens an event store registering one
// entry type at 1.0 on the database at the URL given as its first argument,
// whose library tables are in the schema given as its second, prints
// `ready`, and waits to be killed. Declares no requirement behaviour of its
// own.
import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';

Future<void> main(List<String> args) async {
  final [url, schema] = args;
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: 'guard_x',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'guard_x',
    ),
  );
  await EventStore.open(
    storage: PostgresStorage(
      url: url,
      schema: schema,
      sslMode: SslMode.disable,
    ),
    entryTypes: registry,
    source: const Source(
      hopId: 'guard-process',
      identifier: 'guard-process',
      softwareVersion: 'guard-test',
    ),
  );
  stdout.writeln('ready');
  await Completer<void>().future;
}
